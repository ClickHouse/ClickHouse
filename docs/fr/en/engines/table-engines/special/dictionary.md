---
description: 'Le moteur `Dictionary` affiche les données du dictionnaire sous la forme d’une table ClickHouse.'
sidebar_label: 'Dictionary'
sidebar_position: 20
slug: /engines/table-engines/special/dictionary
title: 'Moteur de table Dictionary'
doc_type: 'reference'
---

Le moteur `Dictionary` affiche les données du [dictionnaire](../../../sql-reference/statements/create/dictionary/overview.md) sous la forme d’une table ClickHouse.

<div id="example">
  ## Exemple
</div>

Prenons l’exemple d’un dictionnaire `products` avec la configuration suivante :

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

Interrogez les données du dictionnaire :

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

Vous pouvez utiliser les fonctions [dictGet*](/fr/sql-reference/functions/ext-dict-functions) pour récupérer les données du dictionnaire dans ce format.

Cette vue n’est pas utile lorsque vous devez accéder aux données brutes ou effectuer une opération `JOIN`. Dans ce cas, vous pouvez utiliser le moteur `Dictionary`, qui affiche les données du dictionnaire dans une table.

Syntaxe :

```sql
CREATE TABLE %table_name% (%fields%) engine = Dictionary(%dictionary_name%)`
```

Exemple d’utilisation :

```sql
CREATE TABLE products (product_id UInt64, title String) ENGINE = Dictionary(products);
```

D’accord

Voyons le contenu de la table.

```sql
SELECT * FROM products LIMIT 1;
```

```text
┌────product_id─┬─title───────────┐
│        152689 │ Some item       │
└───────────────┴─────────────────┘
```

**Voir aussi**

* [Fonction Dictionary](/fr/sql-reference/table-functions/dictionary)