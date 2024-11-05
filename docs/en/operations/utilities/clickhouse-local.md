---
slug: /en/operations/utilities/clickhouse-local
sidebar_position: 60
sidebar_label: clickhouse-local
---

# clickhouse-local

## Related Content

- Blog: [Extracting, Converting, and Querying Data in Local Files using clickhouse-local](https://clickhouse.com/blog/extracting-converting-querying-local-files-with-sql-clickhouse-local)

## When to use clickhouse-local vs. ClickHouse

`clickhouse-local` is an easy-to-use version of ClickHouse that is ideal for developers who need to perform fast processing on local and remote files using SQL without having to install a full database server. With `clickhouse-local`, developers can use SQL commands (using the [ClickHouse SQL dialect](../../sql-reference/index.md)) directly from the command line, providing a simple and efficient way to access ClickHouse features without the need for a full ClickHouse installation. One of the main benefits of `clickhouse-local` is that it is already included when installing [clickhouse-client](https://clickhouse.com/docs/en/integrations/sql-clients/clickhouse-client-local). This means that developers can get started with `clickhouse-local` quickly, without the need for a complex installation process.

While `clickhouse-local` is a great tool for development and testing purposes, and for processing files, it is not suitable for serving end users or applications. In these scenarios, it is recommended to use the open-source [ClickHouse](https://clickhouse.com/docs/en/install). ClickHouse is a powerful OLAP database that is designed to handle large-scale analytical workloads. It provides fast and efficient processing of complex queries on large datasets, making it ideal for use in production environments where high-performance is critical. Additionally, ClickHouse offers a wide range of features such as replication, sharding, and high availability, which are essential for scaling up to handle large datasets and serving applications. If you need to handle larger datasets or serve end users or applications, we recommend using open-source ClickHouse instead of `clickhouse-local`.

Please read the docs below that show example use cases for `clickhouse-local`, such as [querying local file](#query_data_in_file) or [reading a parquet file in S3](#query-data-in-a-parquet-file-in-aws-s3).

## Download clickhouse-local

`clickhouse-local` is executed using the same `clickhouse` binary that runs the ClickHouse server and `clickhouse-client`. The easiest way to download the latest version is with the following command:

```bash
curl https://clickhouse.com/ | sh
```

:::note
The binary you just downloaded can run all sorts of ClickHouse tools and utilities. If you want to run ClickHouse as a database server, check out the [Quick Start](../../quick-start.mdx).
:::

## Query data in a file using SQL {#query_data_in_file}

A common use of `clickhouse-local` is to run ad-hoc queries on files: where you don't have to insert the data into a table. `clickhouse-local` can stream the data from a file into a temporary table and execute your SQL.

If the file is sitting on the same machine as `clickhouse-local`, you can simply specify the file to load. The following `reviews.tsv` file contains a sampling of Amazon product reviews:

```bash
./clickhouse local -q "SELECT * FROM 'reviews.tsv'"
```

This command is a shortcut of:

```bash
./clickhouse local -q "SELECT * FROM file('reviews.tsv')"
```

ClickHouse knows the file uses a tab-separated format from filename extension. If you need to explicitly specify the format, simply add one of the [many ClickHouse input formats](../../interfaces/formats.md):
```bash
./clickhouse local -q "SELECT * FROM file('reviews.tsv', 'TabSeparated')"
```

The `file` table function creates a table, and you can use `DESCRIBE` to see the inferred schema:

```bash
./clickhouse local -q "DESCRIBE file('reviews.tsv')"
```

:::tip
You are allowed to use globs in file name (See [glob substitutions](/docs/en/sql-reference/table-functions/file.md/#globs-in-path)).

Examples:

```bash
./clickhouse local -q "SELECT * FROM 'reviews*.jsonl'"
./clickhouse local -q "SELECT * FROM 'review_?.csv'"
./clickhouse local -q "SELECT * FROM 'review_{1..3}.csv'"
```

:::

```response
marketplace	Nullable(String)
customer_id	Nullable(Int64)
review_id	Nullable(String)
product_id	Nullable(String)
product_parent	Nullable(Int64)
product_title	Nullable(String)
product_category	Nullable(String)
star_rating	Nullable(Int64)
helpful_votes	Nullable(Int64)
total_votes	Nullable(Int64)
vine	Nullable(String)
verified_purchase	Nullable(String)
review_headline	Nullable(String)
review_body	Nullable(String)
review_date	Nullable(Date)
```

Let's find a product with the highest rating:

```bash
./clickhouse local -q "SELECT
    argMax(product_title,star_rating),
    max(star_rating)
FROM file('reviews.tsv')"
```

```response
Monopoly Junior Board Game	5
```

## Query data in a Parquet file in AWS S3

If you have a file in S3, use `clickhouse-local` and the `s3` table function to query the file in place (without inserting the data into a ClickHouse table). We have a file named `house_0.parquet` in a public bucket that contains home prices of property sold in the United Kingdom. Let's see how many rows it has:

```bash
./clickhouse local -q "
SELECT count()
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')"
```

The file has 2.7M rows:

```response
2772030
```

It's always useful to see what the inferred schema that ClickHouse determines from the file:

```bash
./clickhouse local -q "DESCRIBE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')"
```

```response
price	Nullable(Int64)
date	Nullable(UInt16)
postcode1	Nullable(String)
postcode2	Nullable(String)
type	Nullable(String)
is_new	Nullable(UInt8)
duration	Nullable(String)
addr1	Nullable(String)
addr2	Nullable(String)
street	Nullable(String)
locality	Nullable(String)
town	Nullable(String)
district	Nullable(String)
county	Nullable(String)
```

Let's see what the most expensive neighborhoods are:

```bash
./clickhouse local -q "
SELECT
    town,
    district,
    count() AS c,
    round(avg(price)) AS price,
    bar(price, 0, 5000000, 100)
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')
GROUP BY
    town,
    district
HAVING c >= 100
ORDER BY price DESC
LIMIT 10"
```

```response
LONDON	CITY OF LONDON	886	2271305	█████████████████████████████████████████████▍
LEATHERHEAD	ELMBRIDGE	206	1176680	███████████████████████▌
LONDON	CITY OF WESTMINSTER	12577	1108221	██████████████████████▏
LONDON	KENSINGTON AND CHELSEA	8728	1094496	█████████████████████▉
HYTHE	FOLKESTONE AND HYTHE	130	1023980	████████████████████▍
CHALFONT ST GILES	CHILTERN	113	835754	████████████████▋
AMERSHAM	BUCKINGHAMSHIRE	113	799596	███████████████▉
VIRGINIA WATER	RUNNYMEDE	356	789301	███████████████▊
BARNET	ENFIELD	282	740514	██████████████▊
NORTHWOOD	THREE RIVERS	184	731609	██████████████▋
```

:::tip
When you are ready to insert your files into ClickHouse, startup a ClickHouse server and insert the results of your `file` and `s3` table functions into a `MergeTree` table. View the [Quick Start](../../quick-start.mdx) for more details.
:::


## Format Conversions

You can use `clickhouse-local` for converting data between different formats. Example:

``` bash
$ clickhouse-local --input-format JSONLines --output-format CSV --query "SELECT * FROM table" < data.json > data.csv
```

Formats are auto-detected from file extensions: 

``` bash
$ clickhouse-local --query "SELECT * FROM table" < data.json > data.csv
```

As a shortcut, you can write it using the `--copy` argument:
``` bash
$ clickhouse-local --copy < data.json > data.csv
```


## Usage {#usage}

By default `clickhouse-local` has access to data of a ClickHouse server on the same host, and it does not depend on the server's configuration. It also supports loading server configuration using `--config-file` argument. For temporary data, a unique temporary data directory is created by default.

Basic usage (Linux):

``` bash
$ clickhouse-local --structure "table_structure" --input-format "format_of_incoming_data" --query "query"
```

Basic usage (Mac):

``` bash
$ ./clickhouse local --structure "table_structure" --input-format "format_of_incoming_data" --query "query"
```

:::note
`clickhouse-local` is also supported on Windows through WSL2.
:::

Arguments:

- `-S`, `--structure` — table structure for input data.
- `--input-format` — input format, `TSV` by default.
- `-F`, `--file` — path to data, `stdin` by default.
- `-q`, `--query` — queries to execute with `;` as delimiter. `--query` can be specified multiple times, e.g. `--query "SELECT 1" --query "SELECT 2"`. Cannot be used simultaneously with `--queries-file`.
- `--queries-file` - file path with queries to execute. `--queries-file` can be specified multiple times, e.g. `--query queries1.sql --query queries2.sql`. Cannot be used simultaneously with `--query`.
- `--multiquery, -n` – If specified, multiple queries separated by semicolons can be listed after the `--query` option. For convenience, it is also possible to omit `--query` and pass the queries directly after `--multiquery`.
- `-N`, `--table` — table name where to put output data, `table` by default.
- `-f`, `--format`, `--output-format` — output format, `TSV` by default.
- `-d`, `--database` — default database, `_local` by default.
- `--stacktrace` — whether to dump debug output in case of exception.
- `--echo` — print query before execution.
- `--verbose` — more details on query execution.
- `--logger.console` — Log to console.
- `--logger.log` — Log file name.
- `--logger.level` — Log level.
- `--ignore-error` — do not stop processing if a query failed.
- `-c`, `--config-file` — path to configuration file in same format as for ClickHouse server, by default the configuration empty.
- `--no-system-tables` — do not attach system tables.
- `--help` — arguments references for `clickhouse-local`.
- `-V`, `--version` — print version information and exit.

Also, there are arguments for each ClickHouse configuration variable which are more commonly used instead of `--config-file`.


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
$ echo -e "1,2\n3,4" | clickhouse-local -n --query "
    CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin);
    SELECT a, b FROM table;
    DROP TABLE table;"
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

Query:

``` bash
$ ps aux | tail -n +2 | awk '{ printf("%s\t%s\n", $1, $4) }' \
    | clickhouse-local --structure "user String, mem Float64" \
        --query "SELECT user, round(sum(mem), 2) as memTotal
            FROM table GROUP BY user ORDER BY memTotal DESC FORMAT Pretty"
```

Result:

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



## Related Content

- [Extracting, converting, and querying data in local files using clickhouse-local](https://clickhouse.com/blog/extracting-converting-querying-local-files-with-sql-clickhouse-local)
- [Getting Data Into ClickHouse - Part 1](https://clickhouse.com/blog/getting-data-into-clickhouse-part-1)
- [Exploring massive, real-world data sets: 100+ Years of Weather Records in ClickHouse](https://clickhouse.com/blog/real-world-data-noaa-climate-data)
