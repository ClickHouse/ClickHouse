---
slug: /ja/engines/table-engines/special/dictionary
sidebar_position: 20
sidebar_label: Dictionary
---

# Dictionary テーブルエンジン

`Dictionary`エンジンは、[Dictionary](../../../sql-reference/dictionaries/index.md) データを ClickHouse テーブルとして表示します。

## 例 {#example}

以下は、`products` という名前の dictionary の設定例です:

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

Dictionary データをクエリします:

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

この形式で dictionary データを取得するには、[dictGet\*](../../../sql-reference/functions/ext-dict-functions.md#ext_dict_functions) 関数を使用できます。

このビューは、元のデータが必要な場合や`JOIN`操作を行う際には役に立ちません。こうした場合には、dictionary データをテーブルとして表示する `Dictionary` エンジンを使用できます。

構文:

``` sql
CREATE TABLE %table_name% (%fields%) engine = Dictionary(%dictionary_name%)
```

使用例:

``` sql
create table products (product_id UInt64, title String) Engine = Dictionary(products);
```

      Ok

テーブルの内容を確認します。

``` sql
select * from products limit 1;
```

``` text
┌────product_id─┬─title───────────┐
│        152689 │ Some item       │
└───────────────┴─────────────────┘
```

**関連項目**

- [Dictionary 関数](../../../sql-reference/table-functions/dictionary.md#dictionary-function)
