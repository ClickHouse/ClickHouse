---
description: '`Dictionary` エンジンは、Dictionaryデータを ClickHouse テーブルとして表示します。'
sidebar_label: 'Dictionary'
sidebar_position: 20
slug: /engines/table-engines/special/dictionary
title: 'Dictionary テーブルエンジン'
doc_type: 'reference'
---

`Dictionary` エンジンは、[Dictionary](../../../sql-reference/statements/create/dictionary/overview.md)のデータを ClickHouse テーブルとして表示します。

<div id="example">
  ## 例
</div>

例として、次のような設定の `products` Dictionary を考えてみましょう。

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

Dictionary データをクエリします:

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

このフォーマットでDictionaryデータを取得するには、[dictGet*](/ja/sql-reference/functions/ext-dict-functions) 関数を使用できます。

ただし、生データを取得する必要がある場合や、`JOIN` 操作を行う場合には、このビューは適していません。こうしたケースでは、Dictionaryデータをテーブルとして表示する `Dictionary` エンジンを使用できます。

構文:

```sql
CREATE TABLE %table_name% (%fields%) engine = Dictionary(%dictionary_name%)`
```

使用例:

```sql
CREATE TABLE products (product_id UInt64, title String) ENGINE = Dictionary(products);
```

では

テーブルの中身を見てみましょう。

```sql
SELECT * FROM products LIMIT 1;
```

```text
┌────product_id─┬─title───────────┐
│        152689 │ Some item       │
└───────────────┴─────────────────┘
```

**関連項目**

* [Dictionary関数](/ja/sql-reference/table-functions/dictionary)