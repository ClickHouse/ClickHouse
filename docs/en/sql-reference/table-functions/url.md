---
description: 'Creates a table from the `URL` with given `format` and `structure`'
sidebar_label: 'url'
sidebar_position: 200
slug: /sql-reference/table-functions/url
title: 'url'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# url Table Function

`url` function creates a table from the `URL` with given `format` and `structure`.

`url` function may be used in `SELECT` and `INSERT` queries on data in [URL](../../engines/table-engines/special/url.md) tables.

## Syntax {#syntax}

```sql
url(URL [,format] [,structure] [,headers])
```

## Parameters {#parameters}

| Parameter   | Description                                                                                                                                            |
|-------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| `URL`       | Single quoted HTTP or HTTPS server address, which can accept `GET` or `POST` requests (for `SELECT` or `INSERT` queries correspondingly). Type: [String](../../sql-reference/data-types/string.md). |
| `format`    | [Format](/sql-reference/formats) of the data. Type: [String](../../sql-reference/data-types/string.md).                                                  |
| `structure` | Table structure in `'UserID UInt64, Name String'` format. Determines column names and types. Type: [String](../../sql-reference/data-types/string.md).     |
| `headers`   | Headers in `'headers('key1'='value1', 'key2'='value2')'` format. You can set headers for HTTP call.                                                  |

## Returned value {#returned_value}

A table with the specified format and structure and with data from the defined `URL`.

## Examples {#examples}

Getting the first 3 lines of a table that contains columns of `String` and [UInt32](../../sql-reference/data-types/int-uint.md) type from HTTP-server which answers in [CSV](../../interfaces/formats.md#csv) format.

```sql
SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32', headers('Accept'='text/csv; charset=utf-8')) LIMIT 3;
```

Inserting data from a `URL` into a table:

```sql
CREATE TABLE test_table (column1 String, column2 UInt32) ENGINE=Memory;
INSERT INTO FUNCTION url('http://127.0.0.1:8123/?query=INSERT+INTO+test_table+FORMAT+CSV', 'CSV', 'column1 String, column2 UInt32') VALUES ('http interface', 42);
SELECT * FROM test_table;
```

## Globs in URL {#globs-in-url}

Patterns in curly brackets `{ }` are used to generate a set of shards or to specify failover addresses. Supported pattern types and examples see in the description of the [remote](remote.md#globs-in-addresses) function.
Character `|` inside patterns is used to specify failover addresses. They are iterated in the same order as listed in the pattern. The number of generated addresses is limited by [glob_expansion_max_elements](../../operations/settings/settings.md#glob_expansion_max_elements) setting.

## Virtual Columns {#virtual-columns}

- `_path` — Path to the `URL`. Type: `LowCardinality(String)`.
- `_file` — Resource name of the `URL`. Type: `LowCardinality(String)`.
- `_size` — Size of the resource in bytes. Type: `Nullable(UInt64)`. If the size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.
- `_headers` - HTTP response headers. Type: `Map(LowCardinality(String), LowCardinality(String))`.

## Hive-style partitioning {#hive-style-partitioning}

When setting `use_hive_partitioning` is set to 1, ClickHouse will detect Hive-style partitioning in the path (`/name=value/`) and will allow to use partition columns as virtual columns in the query. These virtual columns will have the same names as in the partitioned path, but starting with `_`.

**Example**

Use virtual column, created with Hive-style partitioning

```sql
SELECT * FROM url('http://data/path/date=*/country=*/code=*/*.parquet') WHERE _date > '2020-01-01' AND _country = 'Netherlands' AND _code = 42;
```

## Storage Settings {#storage-settings}

- [engine_url_skip_empty_files](/operations/settings/settings.md#engine_url_skip_empty_files) - allows to skip empty files while reading. Disabled by default.
- [enable_url_encoding](/operations/settings/settings.md#enable_url_encoding) - allows to enable/disable decoding/encoding path in uri. Enabled by default.

## Permissions {#permissions}

`url` function requires `CREATE TEMPORARY TABLE` permission. As such - it'll not work for users with [readonly](/operations/settings/permissions-for-queries#readonly) = 1 setting. At least readonly = 2 is required.


## Related {#related}

- [Virtual columns](/engines/table-engines/index.md#table_engines-virtual_columns)
