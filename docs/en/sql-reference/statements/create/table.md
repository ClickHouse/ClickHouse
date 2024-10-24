---
slug: /en/sql-reference/statements/create/table
sidebar_position: 36
sidebar_label: TABLE
title: "CREATE TABLE"
keywords: [compression, codec, schema, DDL]
---

Creates a new table. This query can have various syntax forms depending on a use case.

By default, tables are created only on the current server. Distributed DDL queries are implemented as `ON CLUSTER` clause, which is [described separately](../../../sql-reference/distributed-ddl.md).

## Syntax Forms

### With Explicit Schema

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr1] [COMMENT 'comment for column'] [compression_codec] [TTL expr1],
    name2 [type2] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr2] [COMMENT 'comment for column'] [compression_codec] [TTL expr2],
    ...
) ENGINE = engine
  [COMMENT 'comment for table']
```

Creates a table named `table_name` in the `db` database or the current database if `db` is not set, with the structure specified in brackets and the `engine` engine.
The structure of the table is a list of column descriptions, secondary indexes and constraints . If [primary key](#primary-key) is supported by the engine, it will be indicated as parameter for the table engine.

A column description is `name type` in the simplest case. Example: `RegionID UInt32`.

Expressions can also be defined for default values (see below).

If necessary, primary key can be specified, with one or more key expressions.

Comments can be added for columns and for the table.

### With a Schema Similar to Other Table

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS [db2.]name2 [ENGINE = engine]
```

Creates a table with the same structure as another table. You can specify a different engine for the table. If the engine is not specified, the same engine will be used as for the `db2.name2` table.

### With a Schema and Data Cloned from Another Table 

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name CLONE AS [db2.]name2 [ENGINE = engine]
```

Creates a table with the same structure as another table. You can specify a different engine for the table. If the engine is not specified, the same engine will be used as for the `db2.name2` table. After the new table is created, all partitions from `db2.name2` are attached to it. In other words, the data of `db2.name2` is cloned into `db.table_name` upon creation. This query is equivalent to the following:

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS [db2.]name2 [ENGINE = engine];
ALTER TABLE [db.]table_name ATTACH PARTITION ALL FROM [db2].name2;
```

### From a Table Function

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS table_function()
```

Creates a table with the same result as that of the [table function](../../../sql-reference/table-functions/index.md#table-functions) specified. The created table will also work in the same way as the corresponding table function that was specified.

### From SELECT query

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name[(name1 [type1], name2 [type2], ...)] ENGINE = engine AS SELECT ...
```

Creates a table with a structure like the result of the `SELECT` query, with the `engine` engine, and fills it with data from `SELECT`. Also you can explicitly specify columns description.

If the table already exists and `IF NOT EXISTS` is specified, the query won’t do anything.

There can be other clauses after the `ENGINE` clause in the query. See detailed documentation on how to create tables in the descriptions of [table engines](../../../engines/table-engines/index.md#table_engines).

:::tip
In ClickHouse Cloud please split this into two steps:
1. Create the table structure

  ```sql
  CREATE TABLE t1
  ENGINE = MergeTree
  ORDER BY ...
  # highlight-next-line
  EMPTY AS
  SELECT ...
  ```

2. Populate the table

  ```sql
  INSERT INTO t1
  SELECT ...
  ```

:::

**Example**

Query:

``` sql
CREATE TABLE t1 (x String) ENGINE = Memory AS SELECT 1;
SELECT x, toTypeName(x) FROM t1;
```

Result:

```text
┌─x─┬─toTypeName(x)─┐
│ 1 │ String        │
└───┴───────────────┘
```

## NULL Or NOT NULL Modifiers

`NULL` and `NOT NULL` modifiers after data type in column definition allow or do not allow it to be [Nullable](../../../sql-reference/data-types/nullable.md#data_type-nullable).

If the type is not `Nullable` and if `NULL` is specified, it will be treated as `Nullable`; if `NOT NULL` is specified, then no. For example, `INT NULL` is the same as `Nullable(INT)`. If the type is `Nullable` and `NULL` or `NOT NULL` modifiers are specified, the exception will be thrown.

See also [data_type_default_nullable](../../../operations/settings/settings.md#data_type_default_nullable) setting.

## Default Values {#default_values}

The column description can specify a default value expression in the form of `DEFAULT expr`, `MATERIALIZED expr`, or `ALIAS expr`. Example: `URLDomain String DEFAULT domain(URL)`.

The expression `expr` is optional. If it is omitted, the column type must be specified explicitly and the default value will be `0` for numeric columns, `''` (the empty string) for string columns, `[]` (the empty array) for array columns, `1970-01-01` for date columns, or `NULL` for nullable columns.

The column type of a default value column can be omitted in which case it is inferred from `expr`'s type. For example the type of column `EventDate DEFAULT toDate(EventTime)` will be date.

If both a data type and a default value expression are specified, an implicit type casting function inserted which converts the expression to the specified type. Example: `Hits UInt32 DEFAULT 0` is internally represented as `Hits UInt32 DEFAULT toUInt32(0)`.

A default value expression `expr` may reference arbitrary table columns and constants. ClickHouse checks that changes of the table structure do not introduce loops in the expression calculation. For INSERT, it checks that expressions are resolvable – that all columns they can be calculated from have been passed.

### DEFAULT

`DEFAULT expr`

Normal default value. If the value of such a column is not specified in an INSERT query, it is computed from `expr`.

Example:

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    updated_at DateTime DEFAULT now(),
    updated_at_date Date DEFAULT toDate(updated_at)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test (id) Values (1);

SELECT * FROM test;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:06:46 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘
```

### MATERIALIZED

`MATERIALIZED expr`

Materialized expression. Values of such columns are automatically calculated according to the specified materialized expression when rows are inserted. Values cannot be explicitly specified during `INSERT`s.

Also, default value columns of this type are not included in the result of `SELECT *`. This is to preserve the invariant that the result of a `SELECT *` can always be inserted back into the table using `INSERT`. This behavior can be disabled with setting `asterisk_include_materialized_columns`.

Example:

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    updated_at DateTime MATERIALIZED now(),
    updated_at_date Date MATERIALIZED toDate(updated_at)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test Values (1);

SELECT * FROM test;
┌─id─┐
│  1 │
└────┘

SELECT id, updated_at, updated_at_date FROM test;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:08:08 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘

SELECT * FROM test SETTINGS asterisk_include_materialized_columns=1;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:08:08 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘
```

### EPHEMERAL

`EPHEMERAL [expr]`

Ephemeral column. Columns of this type are not stored in the table and it is not possible to SELECT from them. The only purpose of ephemeral columns is to build default value expressions of other columns from them.

An insert without explicitly specified columns will skip columns of this type. This is to preserve the invariant that the result of a `SELECT *` can always be inserted back into the table using `INSERT`.

Example:

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    unhexed String EPHEMERAL,
    hexed FixedString(4) DEFAULT unhex(unhexed)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test (id, unhexed) Values (1, '5a90b714');

SELECT
    id,
    hexed,
    hex(hexed)
FROM test
FORMAT Vertical;

Row 1:
──────
id:         1
hexed:      Z��
hex(hexed): 5A90B714
```

### ALIAS

`ALIAS expr`

Calculated columns (synonym). Column of this type are not stored in the table and it is not possible to INSERT values into them.

When SELECT queries explicitly reference columns of this type, the value is computed at query time from `expr`. By default, `SELECT *` excludes ALIAS columns. This behavior can be disabled with setting `asterisk_include_alias_columns`.

When using the ALTER query to add new columns, old data for these columns is not written. Instead, when reading old data that does not have values for the new columns, expressions are computed on the fly by default. However, if running the expressions requires different columns that are not indicated in the query, these columns will additionally be read, but only for the blocks of data that need it.

If you add a new column to a table but later change its default expression, the values used for old data will change (for data where values were not stored on the disk). Note that when running background merges, data for columns that are missing in one of the merging parts is written to the merged part.

It is not possible to set default values for elements in nested data structures.

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    size_bytes Int64,
    size String ALIAS formatReadableSize(size_bytes)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test VALUES (1, 4678899);

SELECT id, size_bytes, size FROM test;
┌─id─┬─size_bytes─┬─size─────┐
│  1 │    4678899 │ 4.46 MiB │
└────┴────────────┴──────────┘

SELECT * FROM test SETTINGS asterisk_include_alias_columns=1;
┌─id─┬─size_bytes─┬─size─────┐
│  1 │    4678899 │ 4.46 MiB │
└────┴────────────┴──────────┘
```

## Primary Key

You can define a [primary key](../../../engines/table-engines/mergetree-family/mergetree.md#primary-keys-and-indexes-in-queries) when creating a table. Primary key can be specified in two ways:

- Inside the column list

``` sql
CREATE TABLE db.table_name
(
    name1 type1, name2 type2, ...,
    PRIMARY KEY(expr1[, expr2,...])
)
ENGINE = engine;
```

- Outside the column list

``` sql
CREATE TABLE db.table_name
(
    name1 type1, name2 type2, ...
)
ENGINE = engine
PRIMARY KEY(expr1[, expr2,...]);
```

:::tip
You can't combine both ways in one query.
:::

## Constraints

Along with columns descriptions constraints could be defined:

### CONSTRAINT

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    ...
    CONSTRAINT constraint_name_1 CHECK boolean_expr_1,
    ...
) ENGINE = engine
```

`boolean_expr_1` could by any boolean expression. If constraints are defined for the table, each of them will be checked for every row in `INSERT` query. If any constraint is not satisfied — server will raise an exception with constraint name and checking expression.

Adding large amount of constraints can negatively affect performance of big `INSERT` queries.

### ASSUME

The `ASSUME` clause is used to define a `CONSTRAINT` on a table that is assumed to be true. This constraint can then be used by the optimizer to enhance the performance of SQL queries.

Take this example where `ASSUME CONSTRAINT` is used in the creation of the `users_a` table:

```sql
CREATE TABLE users_a (
    uid Int16, 
    name String, 
    age Int16, 
    name_len UInt8 MATERIALIZED length(name), 
    CONSTRAINT c1 ASSUME length(name) = name_len
) 
ENGINE=MergeTree 
ORDER BY (name_len, name);
```

Here, `ASSUME CONSTRAINT` is used to assert that the `length(name)` function always equals the value of the `name_len` column. This means that whenever `length(name)` is called in a query, ClickHouse can replace it with `name_len`, which should be faster because it avoids calling the `length()` function.

Then, when executing the query `SELECT name FROM users_a WHERE length(name) < 5;`, ClickHouse can optimize it to `SELECT name FROM users_a WHERE name_len < 5`; because of the `ASSUME CONSTRAINT`. This can make the query run faster because it avoids calculating the length of `name` for each row.

`ASSUME CONSTRAINT` **does not enforce the constraint**, it merely informs the optimizer that the constraint holds true. If the constraint is not actually true, the results of the queries may be incorrect. Therefore, you should only use `ASSUME CONSTRAINT` if you are sure that the constraint is true.

## TTL Expression

Defines storage time for values. Can be specified only for MergeTree-family tables. For the detailed description, see [TTL for columns and tables](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).

## Column Compression Codecs {#column_compression_codec}

By default, ClickHouse applies `lz4` compression in the self-managed version, and `zstd` in ClickHouse Cloud. 

For `MergeTree`-engine family you can change the default compression method in the [compression](../../../operations/server-configuration-parameters/settings.md#server-settings-compression) section of a server configuration.

You can also define the compression method for each individual column in the `CREATE TABLE` query.

``` sql
CREATE TABLE codec_example
(
    dt Date CODEC(ZSTD),
    ts DateTime CODEC(LZ4HC),
    float_value Float32 CODEC(NONE),
    double_value Float64 CODEC(LZ4HC(9)),
    value Float32 CODEC(Delta, ZSTD)
)
ENGINE = <Engine>
...
```

The `Default` codec can be specified to reference default compression which may depend on different settings (and properties of data) in runtime.
Example: `value UInt64 CODEC(Default)` — the same as lack of codec specification.

Also you can remove current CODEC from the column and use default compression from config.xml:

``` sql
ALTER TABLE codec_example MODIFY COLUMN float_value CODEC(Default);
```

Codecs can be combined in a pipeline, for example, `CODEC(Delta, Default)`.

:::tip
You can’t decompress ClickHouse database files with external utilities like `lz4`. Instead, use the special [clickhouse-compressor](https://github.com/ClickHouse/ClickHouse/tree/master/programs/compressor) utility.
:::

Compression is supported for the following table engines:

- [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) family. Supports column compression codecs and selecting the default compression method by [compression](../../../operations/server-configuration-parameters/settings.md#server-settings-compression) settings.
- [Log](../../../engines/table-engines/log-family/index.md) family. Uses the `lz4` compression method by default and supports column compression codecs.
- [Set](../../../engines/table-engines/special/set.md). Only supported the default compression.
- [Join](../../../engines/table-engines/special/join.md). Only supported the default compression.

ClickHouse supports general purpose codecs and specialized codecs.

### General Purpose Codecs

#### NONE

`NONE` — No compression.

#### LZ4

`LZ4` — Lossless [data compression algorithm](https://github.com/lz4/lz4) used by default. Applies LZ4 fast compression.

#### LZ4HC

`LZ4HC[(level)]` — LZ4 HC (high compression) algorithm with configurable level. Default level: 9. Setting `level <= 0` applies the default level. Possible levels: \[1, 12\]. Recommended level range: \[4, 9\].

#### ZSTD

`ZSTD[(level)]` — [ZSTD compression algorithm](https://en.wikipedia.org/wiki/Zstandard) with configurable `level`. Possible levels: \[1, 22\]. Default level: 1.

High compression levels are useful for asymmetric scenarios, like compress once, decompress repeatedly. Higher levels mean better compression and higher CPU usage.

#### ZSTD_QAT

`ZSTD_QAT[(level)]` — [ZSTD compression algorithm](https://en.wikipedia.org/wiki/Zstandard) with configurable level, implemented by [Intel® QATlib](https://github.com/intel/qatlib) and [Intel® QAT ZSTD Plugin](https://github.com/intel/QAT-ZSTD-Plugin). Possible levels: \[1, 12\]. Default level: 1. Recommended level range: \[6, 12\]. Some limitations apply:

- ZSTD_QAT is disabled by default and can only be used after enabling configuration setting [enable_zstd_qat_codec](../../../operations/settings/settings.md#enable_zstd_qat_codec).
- For compression, ZSTD_QAT tries to use an Intel® QAT offloading device ([QuickAssist Technology](https://www.intel.com/content/www/us/en/developer/topic-technology/open/quick-assist-technology/overview.html)). If no such device was found, it will fallback to ZSTD compression in software.
- Decompression is always performed in software.

:::note
ZSTD_QAT is not available in ClickHouse Cloud.
:::

#### DEFLATE_QPL

`DEFLATE_QPL` — [Deflate compression algorithm](https://github.com/intel/qpl) implemented by Intel® Query Processing Library. Some limitations apply:

- DEFLATE_QPL is disabled by default and can only be used after enabling configuration setting [enable_deflate_qpl_codec](../../../operations/settings/settings.md#enable_deflate_qpl_codec).
- DEFLATE_QPL requires a ClickHouse build compiled with SSE 4.2 instructions (by default, this is the case). Refer to [Build Clickhouse with DEFLATE_QPL](/docs/en/development/building_and_benchmarking_deflate_qpl.md/#Build-Clickhouse-with-DEFLATE_QPL) for more details.
- DEFLATE_QPL works best if the system has a Intel® IAA (In-Memory Analytics Accelerator) offloading device. Refer to [Accelerator Configuration](https://intel.github.io/qpl/documentation/get_started_docs/installation.html#accelerator-configuration) and [Benchmark with DEFLATE_QPL](/docs/en/development/building_and_benchmarking_deflate_qpl.md/#Run-Benchmark-with-DEFLATE_QPL) for more details.
- DEFLATE_QPL-compressed data can only be transferred between ClickHouse nodes compiled with SSE 4.2 enabled.

:::note
DEFLATE_QPL is not available in ClickHouse Cloud.
:::

### Specialized Codecs

These codecs are designed to make compression more effective by exploiting specific features of the data. Some of these codecs do not compress data themselves, they instead preprocess the data such that a second compression stage using a general-purpose codec can achieve a higher data compression rate.

#### Delta

`Delta(delta_bytes)` — Compression approach in which raw values are replaced by the difference of two neighboring values, except for the first value that stays unchanged. Up to `delta_bytes` are used for storing delta values, so `delta_bytes` is the maximum size of raw values. Possible `delta_bytes` values: 1, 2, 4, 8. The default value for `delta_bytes` is `sizeof(type)` if equal to 1, 2, 4, or 8. In all other cases, it’s 1. Delta is a data preparation codec, i.e. it cannot be used stand-alone.

#### DoubleDelta

`DoubleDelta(bytes_size)` — Calculates delta of deltas and writes it in compact binary form. Possible `bytes_size` values: 1, 2, 4, 8, the default value is `sizeof(type)` if equal to 1, 2, 4, or 8. In all other cases, it’s 1. Optimal compression rates are achieved for monotonic sequences with a constant stride, such as time series data. Can be used with any fixed-width type. Implements the algorithm used in Gorilla TSDB, extending it to support 64-bit types. Uses 1 extra bit for 32-bit deltas: 5-bit prefixes instead of 4-bit prefixes. For additional information, see Compressing Time Stamps in [Gorilla: A Fast, Scalable, In-Memory Time Series Database](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf). DoubleDelta is a data preparation codec, i.e. it cannot be used stand-alone.

#### GCD

`GCD()` - - Calculates the greatest common denominator (GCD) of the values in the column, then divides each value by the GCD. Can be used with integer, decimal and date/time columns. The codec is well suited for columns with values that change (increase or decrease) in multiples of the GCD, e.g. 24, 28, 16, 24, 8, 24 (GCD = 4). GCD is a data preparation codec, i.e. it cannot be used stand-alone.

#### Gorilla

`Gorilla(bytes_size)` — Calculates XOR between current and previous floating point value and writes it in compact binary form. The smaller the difference between consecutive values is, i.e. the slower the values of the series changes, the better the compression rate. Implements the algorithm used in Gorilla TSDB, extending it to support 64-bit types. Possible `bytes_size` values: 1, 2, 4, 8, the default value is `sizeof(type)` if equal to 1, 2, 4, or 8. In all other cases, it’s 1. For additional information, see section 4.1 in [Gorilla: A Fast, Scalable, In-Memory Time Series Database](https://doi.org/10.14778/2824032.2824078).

#### FPC

`FPC(level, float_size)` - Repeatedly predicts the next floating point value in the sequence using the better of two predictors, then XORs the actual with the predicted value, and leading-zero compresses the result. Similar to Gorilla, this is efficient when storing a series of floating point values that change slowly. For 64-bit values (double), FPC is faster than Gorilla, for 32-bit values your mileage may vary. Possible `level` values: 1-28, the default value is 12.  Possible `float_size` values: 4, 8, the default value is `sizeof(type)` if type is Float. In all other cases, it’s 4. For a detailed description of the algorithm see [High Throughput Compression of Double-Precision Floating-Point Data](https://userweb.cs.txstate.edu/~burtscher/papers/dcc07a.pdf).

#### T64

`T64` — Compression approach that crops unused high bits of values in integer data types (including `Enum`, `Date` and `DateTime`). At each step of its algorithm, codec takes a block of 64 values, puts them into 64x64 bit matrix, transposes it, crops the unused bits of values and returns the rest as a sequence. Unused bits are the bits, that do not differ between maximum and minimum values in the whole data part for which the compression is used.

`DoubleDelta` and `Gorilla` codecs are used in Gorilla TSDB as the components of its compressing algorithm. Gorilla approach is effective in scenarios when there is a sequence of slowly changing values with their timestamps. Timestamps are effectively compressed by the `DoubleDelta` codec, and values are effectively compressed by the `Gorilla` codec. For example, to get an effectively stored table, you can create it in the following configuration:

``` sql
CREATE TABLE codec_example
(
    timestamp DateTime CODEC(DoubleDelta),
    slow_values Float32 CODEC(Gorilla)
)
ENGINE = MergeTree()
```

### Encryption Codecs

These codecs don't actually compress data, but instead encrypt data on disk. These are only available when an encryption key is specified by [encryption](../../../operations/server-configuration-parameters/settings.md#server-settings-encryption) settings. Note that encryption only makes sense at the end of codec pipelines, because encrypted data usually can't be compressed in any meaningful way.

Encryption codecs:


#### AES_128_GCM_SIV

`CODEC('AES-128-GCM-SIV')` — Encrypts data with AES-128 in [RFC 8452](https://tools.ietf.org/html/rfc8452) GCM-SIV mode.


#### AES-256-GCM-SIV

`CODEC('AES-256-GCM-SIV')` — Encrypts data with AES-256 in GCM-SIV mode.

These codecs use a fixed nonce and encryption is therefore deterministic. This makes it compatible with deduplicating engines such as [ReplicatedMergeTree](../../../engines/table-engines/mergetree-family/replication.md) but has a weakness: when the same data block is encrypted twice, the resulting ciphertext will be exactly the same so an adversary who can read the disk can see this equivalence (although only the equivalence, without getting its content).

:::note
Most engines including the "\*MergeTree" family create index files on disk without applying codecs. This means plaintext will appear on disk if an encrypted column is indexed.
:::

:::note
If you perform a SELECT query mentioning a specific value in an encrypted column (such as in its WHERE clause), the value may appear in [system.query_log](../../../operations/system-tables/query_log.md). You may want to disable the logging.
:::

**Example**

```sql
CREATE TABLE mytable
(
    x String CODEC(AES_128_GCM_SIV)
)
ENGINE = MergeTree ORDER BY x;
```

:::note
If compression needs to be applied, it must be explicitly specified. Otherwise, only encryption will be applied to data.
:::

**Example**

```sql
CREATE TABLE mytable
(
    x String Codec(Delta, LZ4, AES_128_GCM_SIV)
)
ENGINE = MergeTree ORDER BY x;
```

## Temporary Tables

:::note
Please note that temporary tables are not replicated. As a result, there is no guarantee that data inserted into a temporary table will be available in other replicas. The primary use case where temporary tables can be useful is for querying or joining small external datasets during a single session.
:::

ClickHouse supports temporary tables which have the following characteristics:

- Temporary tables disappear when the session ends, including if the connection is lost.
- A temporary table uses the Memory table engine when engine is not specified and it may use any table engine except Replicated and `KeeperMap` engines.
- The DB can’t be specified for a temporary table. It is created outside of databases.
- Impossible to create a temporary table with distributed DDL query on all cluster servers (by using `ON CLUSTER`): this table exists only in the current session.
- If a temporary table has the same name as another one and a query specifies the table name without specifying the DB, the temporary table will be used.
- For distributed query processing, temporary tables with Memory engine used in a query are passed to remote servers.

To create a temporary table, use the following syntax:

``` sql
CREATE TEMPORARY TABLE [IF NOT EXISTS] table_name
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) [ENGINE = engine]
```

In most cases, temporary tables are not created manually, but when using external data for a query, or for distributed `(GLOBAL) IN`. For more information, see the appropriate sections

It’s possible to use tables with [ENGINE = Memory](../../../engines/table-engines/special/memory.md) instead of temporary tables.

## REPLACE TABLE

'REPLACE' query allows you to update the table atomically.

:::note
This query is supported only for [Atomic](../../../engines/database-engines/atomic.md) database engine.
:::

If you need to delete some data from a table, you can create a new table and fill it with a `SELECT` statement that does not retrieve unwanted data, then drop the old table and rename the new one:

```sql
CREATE TABLE myNewTable AS myOldTable;
INSERT INTO myNewTable SELECT * FROM myOldTable WHERE CounterID <12345;
DROP TABLE myOldTable;
RENAME TABLE myNewTable TO myOldTable;
```

Instead of above, you can use the following:

```sql
REPLACE TABLE myOldTable ENGINE = MergeTree() ORDER BY CounterID AS SELECT * FROM myOldTable WHERE CounterID <12345;
```

### Syntax

``` sql
{CREATE [OR REPLACE] | REPLACE} TABLE [db.]table_name
```

All syntax forms for `CREATE` query also work for this query. `REPLACE` for a non-existent table will cause an error.

### Examples:

Consider the table:

```sql
CREATE DATABASE base ENGINE = Atomic;
CREATE OR REPLACE TABLE base.t1 (n UInt64, s String) ENGINE = MergeTree ORDER BY n;
INSERT INTO base.t1 VALUES (1, 'test');
SELECT * FROM base.t1;
```

```text
┌─n─┬─s────┐
│ 1 │ test │
└───┴──────┘
```

Using `REPLACE` query to clear all data:

```sql
CREATE OR REPLACE TABLE base.t1 (n UInt64, s Nullable(String)) ENGINE = MergeTree ORDER BY n;
INSERT INTO base.t1 VALUES (2, null);
SELECT * FROM base.t1;
```

```text
┌─n─┬─s──┐
│ 2 │ \N │
└───┴────┘
```

Using `REPLACE` query to change table structure:

```sql
REPLACE TABLE base.t1 (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO base.t1 VALUES (3);
SELECT * FROM base.t1;
```

```text
┌─n─┐
│ 3 │
└───┘
```

## COMMENT Clause

You can add a comment to the table when you creating it.

**Syntax**

``` sql
CREATE TABLE db.table_name
(
    name1 type1, name2 type2, ...
)
ENGINE = engine
COMMENT 'Comment'
```

**Example**

Query:

``` sql
CREATE TABLE t1 (x String) ENGINE = Memory COMMENT 'The temporary table';
SELECT name, comment FROM system.tables WHERE name = 't1';
```

Result:

```text
┌─name─┬─comment─────────────┐
│ t1   │ The temporary table │
└──────┴─────────────────────┘
```


## Related content

- Blog: [Optimizing ClickHouse with Schemas and Codecs](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
- Blog: [Working with time series data in ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
