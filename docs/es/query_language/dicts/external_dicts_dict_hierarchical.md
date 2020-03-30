---
machine_translated: true
---

# Diccionarios jerárquicos {#hierarchical-dictionaries}

ClickHouse soporta diccionarios jerárquicos con un [llave numérica](external_dicts_dict_structure.md#ext_dict-numeric-key).

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

| region\_id | parent\_region | nombre\_región |
|------------|----------------|----------------|
| Uno        | Cero           | Rusia          |
| Cómo hacer | Uno            | Moscu          |
| Cómo hacer | Cómo hacer     | Centrar        |
| Cuatro     | Cero           | Gran Bretaña   |
| Cinco      | Cuatro         | Londres        |

Esta tabla contiene una columna `parent_region` que contiene la clave del padre más cercano para el elemento.

ClickHouse soporta el [jerárquica](external_dicts_dict_structure.md#hierarchical-dict-attr) propiedad para [diccionario externo](index.md) atributo. Esta propiedad le permite configurar el diccionario jerárquico similar al descrito anteriormente.

El [dictGetHierarchy](../functions/ext_dict_functions.md#dictgethierarchy) función le permite obtener la cadena principal de un elemento.

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

[Artículo Original](https://clickhouse.tech/docs/es/query_language/dicts/external_dicts_dict_hierarchical/) <!--hide-->
