---
description: 'The `Dictionary` engine displays the dictionary data as a ClickHouse
  table.'
sidebar_label: 'Dictionary'
sidebar_position: 20
slug: /engines/table-engines/special/dictionary
title: 'Dictionary Table Engine'
---

# Dictionary Table Engine

The `Dictionary` engine displays the [dictionary](../../../sql-reference/dictionaries/index.md) data as a ClickHouse table.

## Example {#example}

As an example, consider a dictionary of `products` with the following configuration:

```xml
<dictionaries>
    <dictionary>
        <name>products</name>
        <source>
            <odbc>
                <table>products</table>
                <connection_string>DSN=some-db-server</connection_string>
            </odbc>
        </source>
        <lifetime>
            <min>300</min>
            <max>360</max>
        </lifetime>
        <layout>
            <flat/>
        </layout>
        <structure>
            <id>
                <name>product_id</name>
            </id>
            <attribute>
                <name>title</name>
                <type>String</type>
                <null_value></null_value>
            </attribute>
        </structure>
    </dictionary>
</dictionaries>
```

Query the dictionary data:

```sql
SELECT
    name,
    type,
    key,
    attribute.names,
    attribute.types,
    bytes_allocated,
    element_count,
    source
FROM system.dictionaries
WHERE name = 'products'
```

```text
┌─name─────┬─type─┬─key────┬─attribute.names─┬─attribute.types─┬─bytes_allocated─┬─element_count─┬─source──────────┐
│ products │ Flat │ UInt64 │ ['title']       │ ['String']      │        23065376 │        175032 │ ODBC: .products │
└──────────┴──────┴────────┴─────────────────┴─────────────────┴─────────────────┴───────────────┴─────────────────┘
```

You can use the [dictGet\*](/sql-reference/functions/ext-dict-functions#dictget-dictgetordefault-dictgetornull) function to get the dictionary data in this format.

This view isn't helpful when you need to get raw data, or when performing a `JOIN` operation. For these cases, you can use the `Dictionary` engine, which displays the dictionary data in a table.

Syntax:

```sql
CREATE TABLE %table_name% (%fields%) engine = Dictionary(%dictionary_name%)`
```

Usage example:

```sql
CREATE TABLE products (product_id UInt64, title String) ENGINE = Dictionary(products);
```

      Ok

Take a look at what's in the table.

```sql
SELECT * FROM products LIMIT 1;
```

```text
┌────product_id─┬─title───────────┐
│        152689 │ Some item       │
└───────────────┴─────────────────┘
```

**See Also**

- [Dictionary function](/sql-reference/table-functions/dictionary)
