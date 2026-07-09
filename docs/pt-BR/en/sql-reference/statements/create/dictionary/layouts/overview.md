---
description: 'Tipos de layout de dicionário para armazenar dicionários na memória'
sidebar_label: 'Visão geral'
sidebar_position: 1
slug: /sql-reference/statements/create/dictionary/layouts
title: 'Layouts de dicionário'
doc_type: 'reference'
---

import CloudDetails from '@site/docs/sql-reference/statements/create/dictionary/_snippet_dictionary_in_cloud.md';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="storing-dictionaries-in-memory">
  ## Tipos de layout de dicionário
</div>

Há várias formas de armazenar dicionários em memória, cada uma com diferentes trade-offs entre uso de CPU e RAM.

| Layout                                                                                                     | Descrição                                                                                                                                       |
| ---------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| [flat](./flat.md)                                                                                          | Armazena dados em arrays planos indexados por chave. É o layout mais rápido, mas as chaves devem ser `UInt64` e limitadas por `max_array_size`. |
| [hashed](./hashed.md)                                                                                      | Armazena dados em uma tabela hash. Não há limite para o tamanho da chave e ele suporta qualquer número de elementos.                            |
| [sparse&#95;hashed](./hashed.md#sparse_hashed)                                                             | Como `hashed`, mas troca CPU por menor uso de memória.                                                                                          |
| [complex&#95;key&#95;hashed](./hashed.md#complex_key_hashed)                                               | Como `hashed`, para chaves compostas.                                                                                                           |
| [complex&#95;key&#95;sparse&#95;hashed](./hashed.md#complex_key_sparse_hashed)                             | Como `sparse_hashed`, para chaves compostas.                                                                                                    |
| [hashed&#95;array](./hashed-array.md)                                                                      | Atributos armazenados em arrays com uma tabela hash que mapeia chaves para índices do array. Eficiente em memória para muitos atributos.        |
| [complex&#95;key&#95;hashed&#95;array](./hashed-array.md#complex_key_hashed_array)                         | Como `hashed_array`, para chaves compostas.                                                                                                     |
| [range&#95;hashed](./range-hashed.md)                                                                      | Tabela hash com intervalos ordenados. Suporta buscas por chave + intervalo de data/hora.                                                        |
| [complex&#95;key&#95;range&#95;hashed](./range-hashed.md#complex_key_range_hashed)                         | Como `range_hashed`, para chaves compostas.                                                                                                     |
| [cache](./cache.md)                                                                                        | Cache em memória de tamanho fixo. Apenas as chaves acessadas com frequência são armazenadas.                                                    |
| [complex&#95;key&#95;cache](/pt-BR/sql-reference/statements/create/dictionary/layouts/hashed#complex_key_hashed) | Como `cache`, para chaves compostas.                                                                                                            |
| [ssd&#95;cache](./ssd-cache.md)                                                                            | Como `cache`, mas armazena dados em SSD com um índice em memória.                                                                               |
| [complex&#95;key&#95;ssd&#95;cache](./ssd-cache.md#complex_key_ssd_cache)                                  | Como `ssd_cache`, para chaves compostas.                                                                                                        |
| [direct](./direct.md)                                                                                      | Sem armazenamento em memória — consulta a origem diretamente a cada solicitação.                                                                |
| [complex&#95;key&#95;direct](./direct.md#complex_key_direct)                                               | Como `direct`, para chaves compostas.                                                                                                           |
| [ip&#95;trie](./ip-trie.md)                                                                                | Estrutura trie para buscas rápidas de prefixos de IP (baseadas em CIDR).                                                                        |

:::tip Layouts recomendados
[flat](./flat.md), [hashed](./hashed.md) e [complex&#95;key&#95;hashed](./hashed.md#complex_key_hashed) oferecem o melhor desempenho de consultas.
Layouts de cache não são recomendados devido ao desempenho potencialmente ruim e à dificuldade de ajustar parâmetros — veja [cache](./cache.md) para mais detalhes.
:::

<div id="specify-dictionary-layout">
  ## Especifique o layout de dicionário
</div>

<CloudDetails />

Você pode configurar um layout de dicionário com a cláusula `LAYOUT` (para DDL) ou com a configuração `layout` em definições no arquivo de configuração.

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY (...)
    ...
    LAYOUT(LAYOUT_TYPE(param value)) -- configurações de layout
    ...
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <clickhouse>
        <dictionary>
            ...
            <layout>
                <layout_type>
                    <!-- configurações de layout -->
                </layout_type>
            </layout>
            ...
        </dictionary>
    </clickhouse>
    ```
  </TabItem>
</Tabs>

<br />

Veja também [CREATE DICTIONARY](../overview.md) para a sintaxe DDL completa.

Dicionários cujo layout não contém a palavra `complex-key*` têm uma chave do tipo [UInt64](/pt-BR/sql-reference/data-types/int-uint.md); dicionários `complex-key*` têm uma chave composta (complexa, com tipos arbitrários).

**Exemplo de chave numérica** (a coluna key&#95;column tem o tipo [UInt64](/pt-BR/sql-reference/data-types/int-uint.md)):

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY dict_name (
        key_column UInt64,
        ...
    )
    PRIMARY KEY key_column
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <structure>
        <id>
            <name>key_column</name>
        </id>
        ...
    </structure>
    ```
  </TabItem>
</Tabs>

<br />

**Exemplo de chave composta** (a chave tem um elemento do tipo [String](/pt-BR/sql-reference/data-types/string.md)):

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    CREATE DICTIONARY dict_name (
        country_code String,
        ...
    )
    PRIMARY KEY country_code
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <structure>
        <key>
            <attribute>
                <name>country_code</name>
                <type>String</type>
            </attribute>
        </key>
        ...
    </structure>
    ```
  </TabItem>
</Tabs>

<div id="improve-performance">
  ## Melhore o desempenho do dicionário
</div>

Há várias maneiras de melhorar o desempenho do dicionário:

* Chame a função que trabalha com o dicionário após `GROUP BY`.
* Marque como injetivos os atributos a serem extraídos.
  Um atributo é chamado de injetivo se chaves diferentes corresponderem a valores de atributo diferentes.
  Portanto, quando `GROUP BY` usa uma função que busca um valor de atributo pela chave, essa função é automaticamente removida de `GROUP BY`.

O ClickHouse gera uma exceção quando ocorrem erros com dicionários.
Exemplos de erros podem ser:

* Não foi possível carregar o dicionário acessado.
* Erro ao consultar um dicionário `cached`.

Você pode ver a lista de dicionários e seus status na tabela [system.dictionaries](/pt-BR/operations/system-tables/dictionaries.md).