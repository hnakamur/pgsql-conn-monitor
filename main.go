package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"time"

	"github.com/hnakamur/ltsvlog/v3"
	"github.com/jackc/pgx/v5"
)

func main() {
	if err := run(); err != nil {
		ltsvlog.Logger.Err(err)
	}
}

func run() error {
	logfilename := flag.String("log", "-", "log filename")
	databaseURL := flag.String("db-url", os.Getenv("DATABASE_URL"), "database URL (can be set with DATABASE_URL environment variable)")
	databaseName := flag.String("db-name", "", "database name")
	interval := flag.Duration("interval", time.Second, "query interval")
	showVersion := flag.Bool("version", false, "show version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version())
		return nil
	}

	if *logfilename != "-" {
		errorLogFile, err := os.OpenFile(*logfilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return err
		}
		defer errorLogFile.Close()
		ltsvlog.Logger = ltsvlog.NewLTSVLogger(errorLogFile, false)
	}

	conn, err := pgx.Connect(context.Background(), *databaseURL)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	timer := time.NewTimer(*interval)
	for {
		var numBackends int
		q := `SELECT numbackends FROM pg_stat_database WHERE datname = $1`
		if err := conn.QueryRow(ctx, q, *databaseName).Scan(&numBackends); err != nil {
			return err
		}

		ltsvlog.Logger.Info().Int("numBackends", numBackends).Log()

		select {
		case <-ctx.Done():
			stop() // stop receiving signal notifications as soon as possible.
			return nil
		case <-timer.C:
			timer.Reset(*interval)
		}
	}
}

func version() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "(devel)"
	}
	return info.Main.Version
}
