---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 35
toc_title: "S\xF6zl\xFCk"
---

# Sözlük {#dictionary}

Bu `Dictionary` motor görüntüler [sözlük](../../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) bir ClickHouse tablo olarak veri.

Örnek olarak, bir sözlük düşünün `products` aşağıdaki yapılandırma ile:

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

Sözlük verilerini sorgula:

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

Kullanabilirsiniz [dictGet\*](../../../sql-reference/functions/ext-dict-functions.md#ext_dict_functions) sözlük verilerini bu formatta almak için işlev.

Bu görünüm, ham veri almanız gerektiğinde veya bir `JOIN` operasyon. Bu durumlar için şunları kullanabilirsiniz `Dictionary` bir tabloda sözlük verilerini görüntüleyen motor.

Sözdizimi:

``` sql
CREATE TABLE %table_name% (%fields%) engine = Dictionary(%dictionary_name%)`
```

Kullanım örneği:

``` sql
create table products (product_id UInt64, title String) Engine = Dictionary(products);
```

      Ok

Masada ne olduğuna bir bak.

``` sql
select * from products limit 1;
```

``` text
┌────product_id─┬─title───────────┐
│        152689 │ Some item       │
└───────────────┴─────────────────┘
```

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/dictionary/) <!--hide-->
