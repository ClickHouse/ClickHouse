# clickhouse-benchmark

Connects to server and sends the specified query with different settings.

Syntax:

```
echo "query" | clickhouse-benchmark [keys]
```

Keys:

- `--help` — produce help message
- `-c [ --concurrency ] arg (=1)` — number of parallel queries
- `-d [ --delay ] arg (=1)` — delay between intermediate reports in seconds (set 0 to disable reports)
- `--stage arg (=complete)` — request query processing up to specified stage: complete,fetch_columns,with_mergeable_state
- `-i [ --iterations ] arg (=0)` — amount of queries to be executed
- `-t [ --timelimit ] arg (=0)` — stop launch of queries after specified time limit
- `-r [ --randomize ] arg (=0)` — randomize order of execution
- `--json arg` — write final report to specified file in JSON format
- `-h [ --host ] arg` —
- `-p [ --port ] arg` —
- `--cumulative` — prints cumulative data instead of data per interval
- `-s [ --secure ]` — Use TLS connection
- `--user arg (=default)` —
- `--password arg` —
- `--database arg (=default)` —
- `--stacktrace` — print stack traces of exceptions
- `--confidence arg (=5)` — set the level of confidence for T-test [0=80%, 1=90%, 2=95%, 3=98%, 4=99%, 5=99.5%(default)
- `--<session setting name> arg` — [Setting](../settings/index.md) for queries. For example, `--max_memory_usage arg`. You can pass any number of setting by this way.


