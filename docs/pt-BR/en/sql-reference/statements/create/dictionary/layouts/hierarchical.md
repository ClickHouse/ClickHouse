---
slug: /sql-reference/statements/create/dictionary/layouts/hierarchical
title: 'Dicionários hierárquicos'
sidebar_label: 'Hierárquico'
sidebar_position: 10
description: 'Configure dicionários hierárquicos com relações entre chaves pai e filho.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="hierarchical-dictionaries">
  ## Dicionários hierárquicos
</div>

O ClickHouse oferece suporte a dicionários hierárquicos com uma [chave numérica](../attributes.md#numeric-key).

Veja a estrutura hierárquica a seguir:

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

Essa hierarquia pode ser expressa na seguinte tabela de dicionário.

| region&#95;id | parent&#95;region | region&#95;name           |
| ------------- | ----------------- | ------------------------- |
| 1             | 0                 | Estados Unidos da América |
| 2             | 1                 | Califórnia                |
| 3             | 2                 | San Francisco             |
| 4             | 0                 | Grã-Bretanha              |
| 5             | 4                 | Londres                   |

Essa tabela contém uma coluna `parent_region`, que contém a chave do pai imediato do elemento.

O ClickHouse oferece suporte à propriedade hierárquica para atributos de dicionários externos. Essa propriedade permite configurar o dicionário hierárquico de forma semelhante à descrita acima.

A função [dictGetHierarchy](/pt-BR/sql-reference/functions/ext-dict-functions.md#dictGetHierarchy) permite obter a sequência de pais de um elemento.

No nosso exemplo, a estrutura do dicionário pode ser a seguinte:

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

  <TabItem value="xml" label="Arquivo de configuração">
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