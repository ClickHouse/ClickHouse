---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 35
toc_title: "\u8F9E\u66F8"
---

# 辞書 {#dictionary}

その `Dictionary` エンジンは表示します [辞書](../../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) ClickHouseテーブルとしてのデータ。

例として、の辞書を考えてみましょう `products` 以下の構成で:

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

辞書データの照会:

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

を使用することができます [dictGet\*](../../../sql-reference/functions/ext-dict-functions.md#ext_dict_functions) この形式の辞書データを取得する関数。

このビューは、生データを取得する必要がある場合や、 `JOIN` 作戦だ これらのケースでは、 `Dictionary` ディクショナリデータをテーブルに表示するエンジン。

構文:

``` sql
CREATE TABLE %table_name% (%fields%) engine = Dictionary(%dictionary_name%)`
```

使用例:

``` sql
create table products (product_id UInt64, title String) Engine = Dictionary(products);
```

      Ok

テーブルにあるものを見てみなさい。

``` sql
select * from products limit 1;
```

``` text
┌────product_id─┬─title───────────┐
│        152689 │ Some item       │
└───────────────┴─────────────────┘
```

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/dictionary/) <!--hide-->
