---
description: '`Dictionary` 引擎以 ClickHouse 表的形式显示字典数据。'
sidebar_label: '字典'
sidebar_position: 20
slug: /engines/table-engines/special/dictionary
title: '字典 表引擎'
doc_type: 'reference'
---

`Dictionary` 引擎以 ClickHouse 表的形式显示[字典](../../../sql-reference/statements/create/dictionary/overview.md)数据。

<div id="example">
  ## 示例
</div>

以一个 `products` 字典为例，其配置如下：

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

查询字典中的数据：

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

你可以使用 [dictGet*](/zh/sql-reference/functions/ext-dict-functions) 函数以这种格式获取字典中的数据。

但在需要获取原始数据或执行 `JOIN` 操作时，这种视图并不实用。此时可以使用 `Dictionary` 引擎，它会以表的形式显示字典数据。

语法：

```sql
CREATE TABLE %table_name% (%fields%) engine = Dictionary(%dictionary_name%)`
```

使用示例：

```sql
CREATE TABLE products (product_id UInt64, title String) ENGINE = Dictionary(products);
```

好

来看一下表中的内容。

```sql
SELECT * FROM products LIMIT 1;
```

```text
┌────product_id─┬─title───────────┐
│        152689 │ Some item       │
└───────────────┴─────────────────┘
```

**另请参阅**

* [字典函数](/zh/sql-reference/table-functions/dictionary)