---
sidebar_position: 20
sidebar_label: Dictionary
---

# Dictionary Table Engine {#dictionary}

The `Dictionary` engine displays the [dictionary](../../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) data as a ClickHouse table.

## Example {#example}

As an example, consider a dictionary of `products` with the following configuration:

``` xml
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

``` sql
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

``` text
┌─name─────┬─type─┬─key────┬─attribute.names─┬─attribute.types─┬─bytes_allocated─┬─element_count─┬─source──────────┐
│ products │ Flat │ UInt64 │ ['title']       │ ['String']      │        23065376 │        175032 │ ODBC: .products │
└──────────┴──────┴────────┴─────────────────┴─────────────────┴─────────────────┴───────────────┴─────────────────┘
```

You can use the [dictGet\*](../../../sql-reference/functions/ext-dict-functions.md#ext_dict_functions) function to get the dictionary data in this format.

This view isn’t helpful when you need to get raw data, or when performing a `JOIN` operation. For these cases, you can use the `Dictionary` engine, which displays the dictionary data in a table.

Syntax:

``` sql
CREATE TABLE %table_name% (%fields%) engine = Dictionary(%dictionary_name%)`
```

Usage example:

``` sql
create table products (product_id UInt64, title String) Engine = Dictionary(products);
```

      Ok

Take a look at what’s in the table.

``` sql
select * from products limit 1;
```

``` text
┌────product_id─┬─title───────────┐
│        152689 │ Some item       │
└───────────────┴─────────────────┘
```

**See Also**

-   [Dictionary function](../../../sql-reference/table-functions/dictionary.md#dictionary-function)

[Original article](https://clickhouse.com/docs/en/engines/table-engines/special/dictionary/) <!--hide-->
