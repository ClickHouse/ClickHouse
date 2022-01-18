# 字典 {#dictionary}

`Dictionary` 引擎将字典数据展示为一个ClickHouse的表。

例如，考虑使用一个具有以下配置的 `products` 字典：

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

查询字典中的数据：

``` sql
select name, type, key, attribute.names, attribute.types, bytes_allocated, element_count,source from system.dictionaries where name = 'products';

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

    ┌─name─────┬─type─┬─key────┬─attribute.names─┬─attribute.types─┬─bytes_allocated─┬─element_count─┬─source──────────┐
    │ products │ Flat │ UInt64 │ ['title']       │ ['String']      │        23065376 │        175032 │ ODBC: .products │
    └──────────┴──────┴────────┴─────────────────┴─────────────────┴─────────────────┴───────────────┴─────────────────┘

你可以使用 [dictGet\*](../../../engines/table-engines/special/dictionary.md) 函数来获取这种格式的字典数据。

当你需要获取原始数据，或者是想要使用 `JOIN` 操作的时候，这种视图并没有什么帮助。对于这些情况，你可以使用 `Dictionary` 引擎，它可以将字典数据展示在表中。

语法：

    CREATE TABLE %table_name% (%fields%) engine = Dictionary(%dictionary_name%)`

示例：

``` sql
create table products (product_id UInt64, title String) Engine = Dictionary(products);

CREATE TABLE products
(
    product_id UInt64,
    title String,
)
ENGINE = Dictionary(products)
```

    Ok.

    0 rows in set. Elapsed: 0.004 sec.

看一看表中的内容。

``` sql
select * from products limit 1;

SELECT *
FROM products
LIMIT 1
```

    ┌────product_id─┬─title───────────┐
    │        152689 │ Some item       │
    └───────────────┴─────────────────┘

    1 rows in set. Elapsed: 0.006 sec.

[来源文章](https://clickhouse.tech/docs/en/operations/table_engines/dictionary/) <!--hide-->
