<a name="utils-clickhouse-local"></a>

# clickhouse-local

Accepts data that can be represented as a table and performs the operations that are specified in the ClickHouse [query language](../../query_language/index.md#queries).

`clickhouse-local` uses the ClickHouse server engine, meaning it supports all date formats and table engines that ClickHouse works with, and operations do not require a running server.

`When clickhouse-local` is configured by default, it does not have access to data managed by the ClickHouse server that is installed on the same host, but you can use the `--config-file` key to connect the server configuration.

!!! Warning:
We do not recommend connecting the server configuration to `clickhouse-local`, since data can be easily damaged by accident.

## Invoking the program

Basic format of the call:

```bash
clickhouse-local --structure "table_structure" --input-format "format_of_incoming_data" -q "query"
```

Command keys:

- `-S`, `--structure` — The structure of the table where the input data will be placed.
- `-if`, `--input-format` — The input data format. By default,  it is `TSV`.
- `-f`, `--file` — The path to the data file. By default, it is `stdin`.
- `-q`, `--query` — Queries to run. The query separator is `;`.
- `-N`, `-- table` — The name of the table where the input data will be placed. By default, it is `table`.
- `-of`, `--format`, `--output-format` — The output data format. By default,  it is `TSV`.
- `--stacktrace` — Output debugging information for exceptions.
- `--verbose` — Verbose output when running a query.
- `-s` — Suppresses displaying the system log in `stderr`.
- `--config-file` — The path to the configuration file. By default, `clickhouse-local` starts with an empty configuration. The configuration file has the same format as the ClickHouse server and can use all the server configuration parameters. Typically, the connection configuration is not required. If you want to set a specific parameter, you can use a key with the parameter name.
- `--help` — Output reference information about `clickhouse-local`.

## Examples

```bash
echo -e "1,2\n3,4" | clickhouse-local -S "a Int64, b Int64" -if "CSV" -q "SELECT * FROM table"
Read 2 rows, 32.00 B in 0.000 sec., 5182 rows/sec., 80.97 KiB/sec.
1	2
3	4
```

The above command is equivalent to the following:

```bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
Read 2 rows, 32.00 B in 0.000 sec., 4987 rows/sec., 77.93 KiB/sec.
1	2
3	4
```

Now let's show the amount of RAM occupied by users (Unix) on the screen:

```bash
$ ps aux | tail -n +2 | awk '{ printf("%s\t%s\n", $1, $4) }' | clickhouse-local -S "user String, mem Float64" -q "SELECT user, round(sum(mem), 2) as memTotal FROM table GROUP BY user ORDER BY memTotal DESC FORMAT Pretty"
Read 186 rows, 4.15 KiB in 0.035 sec., 5302 rows/sec., 118.34 KiB/sec.
┏━━━━━━━━━━┳━━━━━━━━━━┓
┃ user     ┃ memTotal ┃
┡━━━━━━━━━━╇━━━━━━━━━━┩
│ bayonet  │    113.5 │
├──────────┼──────────┤
│ root     │      8.8 │
├──────────┼──────────┤
...
```
