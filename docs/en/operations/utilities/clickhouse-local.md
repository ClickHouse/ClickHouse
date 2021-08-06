---
toc_priority: 60
toc_title: clickhouse-local
---

# clickhouse-local {#clickhouse-local}

The `clickhouse-local` program enables you to perform fast processing on local files, without having to deploy and configure the ClickHouse server.

Accepts data that represent tables and queries them using [ClickHouse SQL dialect](../../sql-reference/index.md).

`clickhouse-local` uses the same core as ClickHouse server, so it supports most of the features and the same set of formats and table engines.

By default `clickhouse-local` does not have access to data on the same host, but it supports loading server configuration using `--config-file` argument.

!!! warning "Warning"
    It is not recommended to load production server configuration into `clickhouse-local` because data can be damaged in case of human error.

For temporary data an unique temporary data directory is created by default. If you want to override this behavior the data directory can be explicitly specified with the `-- --path` option.

## Usage {#usage}

Basic usage:

``` bash
$ clickhouse-local --structure "table_structure" --input-format "format_of_incoming_data" \
    --query "query"
```

Arguments:

-   `-S`, `--structure` — table structure for input data.
-   `-if`, `--input-format` — input format, `TSV` by default.
-   `-f`, `--file` — path to data, `stdin` by default.
-   `-q` `--query` — queries to execute with `;` as delimeter.
-   `-N`, `--table` — table name where to put output data, `table` by default.
-   `-of`, `--format`, `--output-format` — output format, `TSV` by default.
-   `--stacktrace` — whether to dump debug output in case of exception.
-   `--verbose` — more details on query execution.
-   `-s` — disables `stderr` logging.
-   `--config-file` — path to configuration file in same format as for ClickHouse server, by default the configuration empty.
-   `--help` — arguments references for `clickhouse-local`.

Also there are arguments for each ClickHouse configuration variable which are more commonly used instead of `--config-file`.


## Examples {#examples}

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local --structure "a Int64, b Int64" \
    --input-format "CSV" --query "SELECT * FROM table"
Read 2 rows, 32.00 B in 0.000 sec., 5182 rows/sec., 80.97 KiB/sec.
1   2
3   4
```

Previous example is the same as:

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local --query "
    CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin);
    SELECT a, b FROM table;
    DROP TABLE table"
Read 2 rows, 32.00 B in 0.000 sec., 4987 rows/sec., 77.93 KiB/sec.
1   2
3   4
```

You don't have to use `stdin` or `--file` argument, and can open any number of files using the [`file` table function](../../sql-reference/table-functions/file.md):

``` bash
$ echo 1 | tee 1.tsv
1

$ echo 2 | tee 2.tsv
2

$ clickhouse-local --query "
    select * from file('1.tsv', TSV, 'a int') t1
    cross join file('2.tsv', TSV, 'b int') t2"
1	2
```

Now let’s output memory user for each Unix user:

``` bash
$ ps aux | tail -n +2 | awk '{ printf("%s\t%s\n", $1, $4) }' \
    | clickhouse-local --structure "user String, mem Float64" \
        --query "SELECT user, round(sum(mem), 2) as memTotal
            FROM table GROUP BY user ORDER BY memTotal DESC FORMAT Pretty"
```

``` text
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

[Original article](https://clickhouse.tech/docs/en/operations/utils/clickhouse-local/) <!--hide-->
