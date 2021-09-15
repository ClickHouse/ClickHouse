---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 35
toc_title: Dictionnaire
---

# Dictionnaire {#dictionary}

Le `Dictionary` le moteur affiche le [dictionnaire](../../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) données comme une table ClickHouse.

À titre d'exemple, considérons un dictionnaire de `products` avec la configuration suivante:

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

Interroger les données du dictionnaire:

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

Vous pouvez utiliser l' [dictGet\*](../../../sql-reference/functions/ext-dict-functions.md#ext_dict_functions) fonction pour obtenir les données du dictionnaire dans ce format.

Cette vue n'est pas utile lorsque vous avez besoin d'obtenir des données brutes ou `JOIN` opération. Pour ces cas, vous pouvez utiliser le `Dictionary` moteur, qui affiche les données du dictionnaire dans une table.

Syntaxe:

``` sql
CREATE TABLE %table_name% (%fields%) engine = Dictionary(%dictionary_name%)`
```

Exemple d'utilisation:

``` sql
create table products (product_id UInt64, title String) Engine = Dictionary(products);
```

      Ok

Jetez un oeil à ce qui est dans le tableau.

``` sql
select * from products limit 1;
```

``` text
┌────product_id─┬─title───────────┐
│        152689 │ Some item       │
└───────────────┴─────────────────┘
```

[Article Original](https://clickhouse.tech/docs/en/operations/table_engines/dictionary/) <!--hide-->
