---
description: 'Displays the dictionary data as a ClickHouse table. Works the same way
  as the Dictionary engine.'
sidebar_label: 'dictionary'
sidebar_position: 47
slug: /sql-reference/table-functions/dictionary
title: 'dictionary'
doc_type: 'reference'
---

# dictionary Table Function

Displays the [dictionary](../statements/create/dictionary/overview.md) data as a ClickHouse table. Works the same way as [Dictionary](../../engines/table-engines/special/dictionary.md) engine.

## Syntax {#syntax}

```sql
dictionary('dict')
```

## Arguments {#arguments}

- `dict` — A dictionary name. [String](../../sql-reference/data-types/string.md).

## Returned value {#returned_value}

A ClickHouse table.

## Examples {#examples}

Input table `dictionary_source_table`:

```text
┌─id─┬─value─┐
│  0 │     0 │
│  1 │     1 │
└────┴───────┘
```

Create a dictionary:

```sql
CREATE DICTIONARY new_dictionary(id UInt64, value UInt64 DEFAULT 0) PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dictionary_source_table')) LAYOUT(DIRECT());
```

Query:

```sql
SELECT * FROM dictionary('new_dictionary');
```

Result:

```text
┌─id─┬─value─┐
│  0 │     0 │
│  1 │     1 │
└────┴───────┘
```

## Related {#related}

- [Dictionary engine](/engines/table-engines/special/dictionary)
