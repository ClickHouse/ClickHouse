---
sidebar_position: 54
sidebar_label: dictionary function
---

# dictionary {#dictionary-function}

Displays the [dictionary](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) data as a ClickHouse table. Works the same way as [Dictionary](../../engines/table-engines/special/dictionary.md) engine.

**Syntax**

``` sql
dictionary('dict')
```

**Arguments**

-   `dict` — A dictionary name. [String](../../sql-reference/data-types/string.md).

**Returned value**

A ClickHouse table.

**Example**

Input table `dictionary_source_table`:

``` text
┌─id─┬─value─┐
│  0 │     0 │
│  1 │     1 │
└────┴───────┘
```

Create a dictionary:

``` sql
CREATE DICTIONARY new_dictionary(id UInt64, value UInt64 DEFAULT 0) PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dictionary_source_table')) LAYOUT(DIRECT());
```

Query:

``` sql
SELECT * FROM dictionary('new_dictionary');
```

Result:

``` text
┌─id─┬─value─┐
│  0 │     0 │
│  1 │     1 │
└────┴───────┘
```

**See Also**

-   [Dictionary engine](../../engines/table-engines/special/dictionary.md#dictionary)
