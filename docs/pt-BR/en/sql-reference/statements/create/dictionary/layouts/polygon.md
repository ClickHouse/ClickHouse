---
slug: /sql-reference/statements/create/dictionary/layouts/polygon
title: 'Dicionários de polígono'
sidebar_label: 'Polígono'
sidebar_position: 12
description: 'Configure dicionários de polígono para consultas de ponto em polígono.'
doc_type: 'reference'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

O dicionário `polygon` (`POLYGON`) é otimizado para consultas de ponto em polígono, essencialmente buscas de &quot;geocodificação reversa&quot;.
Dada uma coordenada (latitude/longitude), ele encontra com eficiência qual polígono/região (de um conjunto com muitos polígonos, como fronteiras de países ou regiões) contém esse ponto.
Ele é ideal para mapear coordenadas de localização para a região à qual pertencem.

<iframe width="1024" height="576" src="https://www.youtube.com/embed/FyRsriQp46E?si=Kf8CXoPKEpGQlC-Y" title="Dicionários de polígono no ClickHouse" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />

Exemplo de configuração de um dicionário `polygon`:

<CloudDetails />

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY polygon_dict_name (
        key Array(Array(Array(Array(Float64)))),
        name String,
        value UInt64
    )
    PRIMARY KEY key
    LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))
    ...
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <dictionary>
        <structure>
            <key>
                <attribute>
                    <name>key</name>
                    <type>Array(Array(Array(Array(Float64))))</type>
                </attribute>
            </key>

            <attribute>
                <name>name</name>
                <type>String</type>
                <null_value></null_value>
            </attribute>

            <attribute>
                <name>value</name>
                <type>UInt64</type>
                <null_value>0</null_value>
            </attribute>
        </structure>

        <layout>
            <polygon>
                <store_polygon_key_column>1</store_polygon_key_column>
            </polygon>
        </layout>

        ...
    </dictionary>
    ```
  </TabItem>
</Tabs>

<br />

Ao configurar o dicionário `polygon`, a chave deve ter um de dois tipos:

* Um polígono simples. É um array de pontos.
* MultiPolygon. É um array de polígonos. Cada polígono é um array bidimensional de pontos. O primeiro elemento desse array é o contorno externo do polígono, e os elementos subsequentes especificam áreas a serem excluídas dele.

Os pontos podem ser especificados como um array ou uma tupla de coordenadas. Na implementação atual, apenas pontos bidimensionais são compatíveis.

O usuário pode enviar seus próprios dados em todos os formatos compatíveis com o ClickHouse.

Há 3 tipos de [armazenamento em memória](./#storing-dictionaries-in-memory) disponíveis:

| Layout               | Descrição                                                                                                                                                                                                                                                                                                                                                                                 |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `POLYGON_SIMPLE`     | Implementação ingênua. Para cada consulta, é feita uma varredura linear em todos os polígonos, verificando a pertinência sem índices adicionais.                                                                                                                                                                                                                                          |
| `POLYGON_INDEX_EACH` | Um índice separado é criado para cada polígono, permitindo verificações rápidas de pertinência na maioria dos casos (otimizado para regiões geográficas). Uma grade é sobreposta à área, dividindo recursivamente as células em 16 partes iguais. A divisão para quando a profundidade da recursão atinge `MAX_DEPTH` ou quando uma célula cruza no máximo `MIN_INTERSECTIONS` polígonos. |
| `POLYGON_INDEX_CELL` | Também cria a grade descrita acima com as mesmas opções. Para cada célula folha, é criado um índice sobre todos os fragmentos de polígonos que caem nela, permitindo respostas rápidas às consultas.                                                                                                                                                                                      |
| `POLYGON`            | Sinônimo de `POLYGON_INDEX_CELL`.                                                                                                                                                                                                                                                                                                                                                         |

As consultas ao dicionário são feitas usando as [funções](/pt-BR/sql-reference/functions/ext-dict-functions.md) padrão para trabalhar com dicionários.
Uma diferença importante é que, aqui, as chaves serão os pontos para os quais você deseja encontrar o polígono que os contém.

**Exemplo**

Exemplo de uso do dicionário definido acima:

```sql
CREATE TABLE points (
    x Float64,
    y Float64
)
...
SELECT tuple(x, y) AS key, dictGet(dict_name, 'name', key), dictGet(dict_name, 'value', key) FROM points ORDER BY x, y;
```

Como resultado da execução do último comando para cada ponto da tabela &#39;points&#39;, será encontrado um polígono de área mínima que contém esse ponto, e os atributos solicitados serão exibidos.

**Exemplo**

Você pode ler colunas de dicionários de polígonos usando uma consulta SELECT; basta ativar `store_polygon_key_column = 1` na configuração do dicionário ou na consulta DDL correspondente.

```sql title="Query"
CREATE TABLE polygons_test_table
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String
) ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO polygons_test_table VALUES ([[[(3, 1), (0, 1), (0, -1), (3, -1)]]], 'Value');

CREATE DICTIONARY polygons_test_dictionary
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE 'polygons_test_table'))
LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))
LIFETIME(0);

SELECT * FROM polygons_test_dictionary;
```

```text title="Response"
┌─key─────────────────────────────┬─name──┐
│ [[[(3,1),(0,1),(0,-1),(3,-1)]]] │ Value │
└─────────────────────────────────┴───────┘
```