# clickhouse-benchmark

Connects to a ClickHouse server and repeatedly sends a specified query.

Syntax:

```
echo "query" | clickhouse-benchmark [keys]
```

Keys:

- `-c  N`, `--concurrency=N` — Number of queries that `clickhouse-benchmark` sends simultaneously. Default value: 1.
- `-d N`, `--delay=N` — Interval in seconds between intermediate reports (set 0 to disable reports). Default value: 1.
- `-h WORD`, `--host=WORD` — Server host. Default value: `localhost`.
- `-p N`, `--port=N` — Server port. Default value: 9000.
- `-i N`, `--iterations=N` — Total number of queries. Default value: 0.
- `-r [ --randomize ] arg (=0)` — randomize order of execution
- `-s [ --secure ]` — Use TLS connection
- `-t [ --timelimit ] arg (=0)` — stop launch of queries after specified time limit
- `--confidence arg (=5)` — set the level of confidence for T-test [0=80%, 1=90%, 2=95%, 3=98%, 4=99%, 5=99.5%(default)
- `--cumulative` — prints cumulative data instead of data per interval
- `--database arg (=default)` —
- `--json arg` — write final report to specified file in JSON format
- `--user arg (=default)` —
- `--password arg` —
- `--stacktrace` — print stack traces of exceptions
- `--stage arg (=complete)` — request query processing up to specified stage: complete,fetch_columns,with_mergeable_state
- `--<session setting name> arg` — [Setting](../../operations/settings/index.md) for queries. For example, `--max_memory_usage arg`. You can pass any number of setting by this way.
- `--help` — Shows the help message.


