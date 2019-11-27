# clickhouse-benchmark

Connects to a ClickHouse server and repeatedly sends specified queries.

Syntax:

```bash
$ echo "single query" | clickhouse-benchmark [keys]
```
or
```bash
$ clickhouse-benchmark [keys] <<< "single query"
```

If you want to send a set of queries, create a text file and place each query on the individual string in this file. For example:

```sql
SELECT * FROM system.numbers LIMIT 10000000
SELECT 1
```

Then pass this file to a standard input of `clickhouse-benchmark`.

```bash
clickhouse-benchmark [keys] < queries_file
```

## Keys

- `-c  N`, `--concurrency=N` — Number of queries that `clickhouse-benchmark` sends simultaneously. Default value: 1.
- `-d N`, `--delay=N` — Interval in seconds between intermediate reports (set 0 to disable reports). Default value: 1.
- `-h WORD`, `--host=WORD` — Server host. Default value: `localhost`.
- `-p N`, `--port=N` — Server port. Default value: 9000.
- `-i N`, `--iterations=N` — Total number of queries. Default value: 0.
- `-r`, `--randomize` — Random order of queries execution if there is more then one input query.
- `-s`, `--secure` — Using TLS connection.
- `-t N`, `--timelimit=N` — Time limit in seconds. `clickhouse-benchmark` stops sending queries when the specified time limit is reached. Default value: 0 (time limit disabled).
- `--confidence=N` — Level of confidence for T-test. Possible values: 0 (80%), 1 (90%), 2 (95%), 3 (98%), 4 (99%), 5 (99.5%). Default value: 5.
- `--cumulative` — Printing cumulative data instead of data per interval.
- `--database=DATABASE_NAME` — ClickHouse database name. Default value: `default`.
- `--json=FILEPATH` — JSON output. When the key is set, `clickhouse-benchmark` outputs a report to the specified JSON-file.
- `--user=USERNAME` — ClickHouse user name. Default value: `default`.
- `--password=PSWD` — ClickHouse user password. Default value: empty string.
- `--stacktrace` — Stack traces output. When the key is set, `clickhouse-bencmark` outputs stack traces of exceptions.
- `--stage=WORD` — Query processing stage at server. ClickHouse stops query processing and returns answer to `clickhouse-benchmark` at the specified stage. Possible values: `complete`, `fetch_columns`, `with_mergeable_state`. Default value: `complete`.
- `--help` — Shows the help message.

If you want to apply some [settings](../../operations/settings/index.md) for queries, pass them as a key `--<session setting name>= SETTING_VALUE`. For example, `--max_memory_usage=1048576`.

## Output


By default, `clickhouse-benchmark` reports for each `--delay` interval.

Example of the report:

```text
Queries executed: 10.

localhost:9000, queries 10, QPS: 6.772, RPS: 67904487.440, MiB/s: 518.070, result RPS: 67721584.984, result MiB/s: 516.675.

0.000%		0.145 sec.	
10.000%		0.146 sec.	
20.000%		0.146 sec.	
30.000%		0.146 sec.	
40.000%		0.147 sec.	
50.000%		0.148 sec.	
60.000%		0.148 sec.	
70.000%		0.148 sec.	
80.000%		0.149 sec.	
90.000%		0.150 sec.	
95.000%		0.150 sec.	
99.000%		0.150 sec.	
99.900%		0.150 sec.	
99.990%		0.150 sec.	
```

In the report you can find:

- Number of queries in the `Queries executed: ` field.
- Status string containing (in order):

    - Endpoint of ClickHouse server.
    - Number of processed queries.
    - QPS. Queries Per Second server metric for the period (`--delay`) of reporting.
    - RPS. Rows Per Second server metric for the period (`--delay`) of reporting.
    - MiB/s. Megabytes Per Second server metric for the period (`--delay`) of reporting.
    - result RPS. Rows Per Second server metric for the whole period.
    - result MiB/s. Megabytes Per Second server metric for the whole period.

- Percentiles of queries execution time.


## Comparison mode

`clickhouse-benchmark` can compare performances for two running ClickHouse servers. To use the comparison mode, specify endpoints of both servers by two pairs of `--host`, `--port` keys. `clickhouse-benchmark` establishes connections to both servers, then sends queries selecting destination server for each query randomly. The results are shown for each server separately.

## Example

```bash
$ echo "SELECT * FROM system.numbers LIMIT 10000000" | clickhouse-benchmark -i 10
```
```text
Loaded 1 queries.

Queries executed: 4.

localhost:9000, queries 4, QPS: 4.381, RPS: 43930096.411, MiB/s: 335.160, result RPS: 43811769.584, result MiB/s: 334.257.

0.000%		0.221 sec.	
10.000%		0.222 sec.	
20.000%		0.224 sec.	
30.000%		0.225 sec.	
40.000%		0.226 sec.	
50.000%		0.228 sec.	
60.000%		0.229 sec.	
70.000%		0.231 sec.	
80.000%		0.233 sec.	
90.000%		0.235 sec.	
95.000%		0.236 sec.	
99.000%		0.236 sec.	
99.900%		0.236 sec.	
99.990%		0.236 sec.	



Queries executed: 10.

localhost:9000, queries 10, QPS: 4.943, RPS: 49568312.594, MiB/s: 378.176, result RPS: 49434799.089, result MiB/s: 377.158.

0.000%		0.153 sec.	
10.000%		0.155 sec.	
20.000%		0.155 sec.	
30.000%		0.182 sec.	
40.000%		0.210 sec.	
50.000%		0.221 sec.	
60.000%		0.223 sec.	
70.000%		0.227 sec.	
80.000%		0.231 sec.	
90.000%		0.233 sec.	
95.000%		0.235 sec.	
99.000%		0.236 sec.	
99.900%		0.236 sec.	
99.990%		0.236 sec.	
```
