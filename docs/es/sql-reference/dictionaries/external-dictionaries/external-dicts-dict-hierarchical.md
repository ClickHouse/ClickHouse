---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 45
toc_title: "Diccionarios jer\xE1rquicos"
---

# Diccionarios jerárquicos {#hierarchical-dictionaries}

ClickHouse soporta diccionarios jerárquicos con un [llave numérica](external-dicts-dict-structure.md#ext_dict-numeric-key).

Mira la siguiente estructura jerárquica:

``` text
0 (Common parent)
│
├── 1 (Russia)
│   │
│   └── 2 (Moscow)
│       │
│       └── 3 (Center)
│
└── 4 (Great Britain)
    │
    └── 5 (London)
```

Esta jerarquía se puede expresar como la siguiente tabla de diccionario.

| region_id | parent_region | nombre_región |
|------------|----------------|----------------|
| 1          | 0              | Rusia          |
| 2          | 1              | Moscu          |
| 3          | 2              | Centrar        |
| 4          | 0              | Gran Bretaña   |
| 5          | 4              | Londres        |

Esta tabla contiene una columna `parent_region` que contiene la clave del padre más cercano para el elemento.

ClickHouse soporta el [jerárquica](external-dicts-dict-structure.md#hierarchical-dict-attr) propiedad para [diccionario externo](index.md) atributo. Esta propiedad le permite configurar el diccionario jerárquico similar al descrito anteriormente.

El [dictGetHierarchy](../../../sql-reference/functions/ext-dict-functions.md#dictgethierarchy) función le permite obtener la cadena principal de un elemento.

Para nuestro ejemplo, la estructura del diccionario puede ser la siguiente:

``` xml
<dictionary>
    <structure>
        <id>
            <name>region_id</name>
        </id>

        <attribute>
            <name>parent_region</name>
            <type>UInt64</type>
            <null_value>0</null_value>
            <hierarchical>true</hierarchical>
        </attribute>

        <attribute>
            <name>region_name</name>
            <type>String</type>
            <null_value></null_value>
        </attribute>

    </structure>
</dictionary>
```

[Artículo Original](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_hierarchical/) <!--hide-->
