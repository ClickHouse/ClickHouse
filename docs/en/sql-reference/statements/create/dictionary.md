---
slug: /en/sql-reference/statements/create/dictionary
sidebar_position: 38
sidebar_label: DICTIONARY
title: "CREATE DICTIONARY"
---

Creates a new [dictionary](../../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) with given [structure](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md), [source](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md), [layout](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md) and [lifetime](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md).

## Syntax

``` sql
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

Depending on dictionary [layout](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md) one or more attributes can be specified as dictionary keys.

### SOURCE

The source for a dictionary can be a:
- table in the current ClickHouse service
- table in a remote ClickHouse service
- file available by HTTP(S)
- another database


You can add a comment to the dictionary when you creating it using `COMMENT` clause.

#### Create a dictionary from a table in the current ClickHouse service


Input table `source_table`:

``` text
┌─id─┬─value──┐
│  1 │ First  │
│  2 │ Second │
└────┴────────┘
```

Creating the dictionary:

``` sql
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

``` sql
SHOW CREATE DICTIONARY id_value_dictionary;
```

```text
┌─statement───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE DICTIONARY default.id_value_dictionary
(
    `id` UInt64,
    `value` String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'source_table'))
LIFETIME(MIN 0 MAX 1000)
LAYOUT(FLAT()) |
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```
#### Create a dictionary from a table in a remote ClickHouse service                                                                                                                                  
Creating the dictionary:

``` sql
CREATE DICTIONARY id_value_dictionary
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'HOSTNAME' PORT '8443' TABLE 'source_table'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000)
```
#### Create a dictionary from a file available by HTTP(S)                                                                                                                                             
```sql
statement: CREATE DICTIONARY default.taxi_zone_dictionary
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

#### Create a dictionary from another database



Output the comment to dictionary:

``` sql
SELECT comment FROM system.dictionaries WHERE name == 'dictionary_with_comment' AND database == currentDatabase();
```

```text
┌─comment──────────────────┐
│ The temporary dictionary │
└──────────────────────────┘
```

**See Also**

- For more information, see the [Dictionaries](../../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) section.
- [system.dictionaries](../../../operations/system-tables/dictionaries.md) — This table contains information about [dictionaries](../../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).
