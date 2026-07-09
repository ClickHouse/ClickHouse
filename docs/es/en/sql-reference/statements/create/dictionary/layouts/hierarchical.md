---
slug: /sql-reference/statements/create/dictionary/layouts/hierarchical
title: 'Diccionarios jerárquicos'
sidebar_label: 'Jerárquico'
sidebar_position: 10
description: 'Configure diccionarios jerárquicos con relaciones padre-hijo entre claves.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="hierarchical-dictionaries">
  ## Diccionarios jerárquicos
</div>

ClickHouse admite diccionarios jerárquicos con una [clave numérica](../attributes.md#numeric-key).

Vea la siguiente estructura jerárquica:

```text
0 (Common parent)
│
├── 1 (United States of America)
│   │
│   └── 2 (California)
│       │
│       └── 3 (San Francisco)
│
└── 4 (Great Britain)
    │
    └── 5 (London)
```

Esta jerarquía puede expresarse como la siguiente tabla del diccionario.

| region&#95;id | parent&#95;region | region&#95;name           |
| ------------- | ----------------- | ------------------------- |
| 1             | 0                 | Estados Unidos de América |
| 2             | 1                 | California                |
| 3             | 2                 | San Francisco             |
| 4             | 0                 | Gran Bretaña              |
| 5             | 4                 | Londres                   |

Esta tabla contiene una columna `parent_region` que contiene la clave del padre inmediato del elemento.

ClickHouse admite la propiedad jerárquica para los atributos de diccionarios externos. Esta propiedad permite configurar el diccionario jerárquico de forma similar a la descrita anteriormente.

La función [dictGetHierarchy](/es/sql-reference/functions/ext-dict-functions.md#dictGetHierarchy) permite obtener la cadena de padres de un elemento.

Para este ejemplo, la estructura del diccionario puede ser la siguiente:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY regions_dict
    (
        region_id UInt64,
        parent_region UInt64 DEFAULT 0 HIERARCHICAL,
        region_name String DEFAULT ''
    )
    PRIMARY KEY region_id
    SOURCE(...)
    LAYOUT(HASHED())
    LIFETIME(3600);
    ```
  </TabItem>

  <TabItem value="xml" label="Archivo de configuración">
    ```xml
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
  </TabItem>
</Tabs>

<br />