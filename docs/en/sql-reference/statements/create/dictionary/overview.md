---
description: 'Documentation for creating and configuring dictionaries'
sidebar_label: 'Overview'
sidebar_position: 1
slug: /sql-reference/statements/create/dictionary
title: 'CREATE DICTIONARY'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import CloudSupportedBadge from '@theme/badges/CloudSupportedBadge';

# CREATE DICTIONARY

A dictionary is a mapping (`key -> attributes`) that is convenient for various types of reference lists.
ClickHouse supports special functions for working with dictionaries that can be used in queries. It is easier and more efficient to use dictionaries with functions than a `JOIN` with reference tables.

Dictionaries can be created in two ways:
- [With a DDL query](#creating-a-dictionary-with-a-ddl-query) (recommended)
- [With a configuration file](#creating-a-dictionary-with-a-configuration-file)

## Creating a dictionary with a DDL query {#creating-a-dictionary-with-a-ddl-query}

<CloudSupportedBadge/>

Dictionaries can be created with DDL queries. 
This is the recommended method because with DDL created dictionaries:
- No additional records are added to server configuration files.
- Dictionaries can be used like first-class entities such as tables or views.
- Data can be read directly, using familiar `SELECT` syntax rather than dictionary table functions. Note that when accessing a dictionary directly via a `SELECT` statement, cached dictionary will return only cached data, while for a non-cached dictionary it will return all the data that it stores.
- Dictionaries can be easily renamed.

### Syntax {#syntax}

```sql
CREATE [OR REPLACE] DICTIONARY [IF NOT EXISTS] [db.]dictionary_name [ON CLUSTER cluster]
(
    key1  type1  [DEFAULT | EXPRESSION expr1] [IS_OBJECT_ID],
    key2  type2  [DEFAULT | EXPRESSION expr2],
    attr1 type2  [DEFAULT | EXPRESSION expr3] [HIERARCHICAL|INJECTIVE],
    attr2 type2  [DEFAULT | EXPRESSION expr4] [HIERARCHICAL|INJECTIVE]
)
PRIMARY KEY key1, key2
SOURCE(SOURCE_NAME([param1 value1 ... paramN valueN]))
LAYOUT(LAYOUT_NAME([param_name param_value]))
LIFETIME({MIN min_val MAX max_val | max_val})
SETTINGS(setting_name = setting_value, setting_name = setting_value, ...)
COMMENT 'Comment'
```

| Clause | Description |
|---|---|
| [Attributes](./attributes.md) | Dictionary attributes are specified similarly to table columns. The only required property is the type, all others may have default values. |
| PRIMARY KEY | Defines the key column(s) for dictionary lookups. Depending on the layout, one or more attributes can be specified as keys. |
| [`SOURCE`](./sources/overview.md) | Defines the data source for the dictionary (e.g. ClickHouse table, HTTP, PostgreSQL). |
| [`LAYOUT`](./layouts/overview.md) | Controls how the dictionary is stored in memory (e.g. `FLAT`, `HASHED`, `CACHE`). |
| [`LIFETIME`](./lifetime.md) | Sets the refresh interval for the dictionary. |
| [`ON CLUSTER`](../../../distributed-ddl.md) | Creates the dictionary on a cluster. Optional. |
| `SETTINGS` | Additional dictionary settings. Optional. |
| `COMMENT` | Adds a text comment to the dictionary. Optional. |

## Creating a dictionary with a configuration file {#creating-a-dictionary-with-a-configuration-file}

<CloudNotSupportedBadge/>

:::note
Creating a dictionary with a configuration file is not applicable to ClickHouse Cloud. Please use DDL (see above), and create your dictionary as the `default` user.
:::

The dictionary configuration file has the following format:

```xml
<clickhouse>
    <comment>An optional element with any content. Ignored by the ClickHouse server.</comment>

    <!--Optional element. File name with substitutions-->
    <include_from>/etc/metrika.xml</include_from>


    <dictionary>
        <!-- Dictionary configuration. -->
        <!-- There can be any number of dictionary sections in a configuration file. -->
    </dictionary>

</clickhouse>
```

You can configure any number of dictionaries in the same file.

## Related content {#related-content}

- [Layouts](/sql-reference/statements/create/dictionary/layouts) — How dictionaries are stored in memory
- [Sources](/sql-reference/statements/create/dictionary/sources) — Connecting to data sources
- [Lifetime](./lifetime.md) — Automatic refresh configuration
- [Attributes](./attributes.md) — Key and attribute configuration
- [Embedded Dictionaries](./embedded.md) — Built-in geobase dictionaries
- [system.dictionaries](../../../../operations/system-tables/dictionaries.md) — System table with dictionary information
