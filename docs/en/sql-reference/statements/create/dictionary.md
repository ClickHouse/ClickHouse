---
description: 'Documentation for Dictionary'
sidebar_label: 'DICTIONARY'
sidebar_position: 38
slug: /sql-reference/statements/create/dictionary
title: 'CREATE DICTIONARY'
---

Creates a new [dictionary](../../../sql-reference/dictionaries/index.md) with given [structure](../../../sql-reference/dictionaries/index.md#dictionary-key-and-fields), [source](../../../sql-reference/dictionaries/index.md#dictionary-sources), [layout](/sql-reference/dictionaries#storing-dictionaries-in-memory) and [lifetime](/sql-reference/dictionaries#refreshing-dictionary-data-using-lifetime).

## Syntax {#syntax}

```sql
CREATE [OR REPLACE] DICTIONARY [IF NOT EXISTS] [db.]dictionary_name [ON CLUSTER cluster]
(
    key1 type1  [DEFAULT|EXPRESSION expr1] [IS_OBJECT_ID],
    key2 type2  [DEFAULT|EXPRESSION expr2],
    attr1 type2 [DEFAULT|EXPRESSION expr3] [HIERARCHICAL|INJECTIVE],
    attr2 type2 [DEFAULT|EXPRESSION expr4] [HIERARCHICAL|INJECTIVE]
)
PRIMARY KEY key1, key2
SOURCE(SOURCE_NAME([param1 value1 ... paramN valueN]))
LAYOUT(LAYOUT_NAME([param_name param_value]))
LIFETIME({MIN min_val MAX max_val | max_val})
SETTINGS(setting_name = setting_value, setting_name = setting_value, ...)
COMMENT 'Comment'
```

The dictionary structure consists of attributes. Dictionary attributes are specified similarly to table columns. The only required attribute property is its type, all other properties may have default values.

`ON CLUSTER` clause allows creating dictionary on a cluster, see [Distributed DDL](../../../sql-reference/distributed-ddl.md).

Depending on dictionary [layout](/sql-reference/dictionaries#storing-dictionaries-in-memory) one or more attributes can be specified as dictionary keys.

## SOURCE {#source}

The source for a dictionary can be a:
- table in the current ClickHouse service
- table in a remote ClickHouse service
- file available by HTTP(S)
- another database

### Create a dictionary from a table in the current ClickHouse service {#create-a-dictionary-from-a-table-in-the-current-clickhouse-service}

Input table `source_table`:

```text
┌─id─┬─value──┐
│  1 │ First  │
│  2 │ Second │
└────┴────────┘
```

Creating the dictionary:

```sql
CREATE DICTIONARY id_value_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'source_table'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000)
```

Output the dictionary:

```sql
SHOW CREATE DICTIONARY id_value_dictionary;
```

```response
CREATE DICTIONARY default.id_value_dictionary
(
    `id` UInt64,
    `value` String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'source_table'))
LIFETIME(MIN 0 MAX 1000)
LAYOUT(FLAT())
```

:::note
When using the SQL console in [ClickHouse Cloud](https://clickhouse.com), you must specify a user (`default` or any other user with the role `default_role`) and password when creating a dictionary.
:::

```sql
CREATE USER IF NOT EXISTS clickhouse_admin
IDENTIFIED WITH sha256_password BY 'passworD43$x';

GRANT default_role TO clickhouse_admin;

CREATE DATABASE foo_db;

CREATE TABLE foo_db.source_table (
    id UInt64,
    value String
) ENGINE = MergeTree
PRIMARY KEY id;

CREATE DICTIONARY foo_db.id_value_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'source_table' USER 'clickhouse_admin' PASSWORD 'passworD43$x' DB 'foo_db' ))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);
```

### Create a dictionary from a table in a remote ClickHouse service {#create-a-dictionary-from-a-table-in-a-remote-clickhouse-service}

Input table (in the remote ClickHouse service) `source_table`:

```text
┌─id─┬─value──┐
│  1 │ First  │
│  2 │ Second │
└────┴────────┘
```

Creating the dictionary:

```sql
CREATE DICTIONARY id_value_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'HOSTNAME' PORT 9000 USER 'default' PASSWORD 'PASSWORD' TABLE 'source_table' DB 'default'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000)
```

### Create a dictionary from a file available by HTTP(S) {#create-a-dictionary-from-a-file-available-by-https}

```sql
CREATE DICTIONARY default.taxi_zone_dictionary
(
    `LocationID` UInt16 DEFAULT 0,
    `Borough` String,
    `Zone` String,
    `service_zone` String
)
PRIMARY KEY LocationID
SOURCE(HTTP(URL 'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/taxi_zone_lookup.csv' FORMAT 'CSVWithNames'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(HASHED())
```

### Create a dictionary from another database {#create-a-dictionary-from-another-database}

Please see the details in [Dictionary sources](/sql-reference/dictionaries#dbms).

**See Also**

- For more information, see the [Dictionaries](../../../sql-reference/dictionaries/index.md) section.
- [system.dictionaries](../../../operations/system-tables/dictionaries.md) — This table contains information about [Dictionaries](../../../sql-reference/dictionaries/index.md).
