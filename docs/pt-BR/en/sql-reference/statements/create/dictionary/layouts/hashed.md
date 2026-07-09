---
slug: /sql-reference/statements/create/dictionary/layouts/hashed
title: 'tipos de layout de dicionário do tipo hashed'
sidebar_label: 'hashed'
sidebar_position: 3
description: 'Armazena um dicionário na memória usando tabelas hash: hashed, sparse_hashed, complex_key_hashed, complex_key_sparse_hashed'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="hashed">
  ## hashed
</div>

O dicionário é armazenado inteiramente na memória na forma de uma tabela hash. O dicionário pode conter qualquer quantidade de elementos com quaisquer identificadores. Na prática, o número de chaves pode chegar a dezenas de milhões de itens.

A chave do dicionário é do tipo [UInt64](/pt-BR/sql-reference/data-types/int-uint.md).

Todos os tipos de fontes são suportados. Ao atualizar, os dados (de um arquivo ou de uma tabela) são lidos por completo.

Exemplo de configuração:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(HASHED())
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <layout>
      <hashed />
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

Exemplo de configuração com parâmetros:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <layout>
      <hashed>
        <!-- Se o número de shards for maior que 1 (o padrão é `1`), o dicionário carregará
             dados em paralelo, o que é útil se você tiver uma grande quantidade de elementos em um
             dicionário. -->
        <shards>10</shards>

        <!-- Tamanho do backlog de blocos na fila paralela.

             Como o gargalo no carregamento em paralelo é o rehash, para evitar
             interrupções porque uma thread está executando o rehash, é necessário ter algum
             backlog.

             10000 é um bom equilíbrio entre memória e velocidade.
             Mesmo para 10e10 elementos, consegue lidar com toda a carga sem starvation. -->
        <shard_load_queue_backlog>10000</shard_load_queue_backlog>

        <!-- Fator de carga máximo da tabela hash; com valores maiores, a memória
             é utilizada de forma mais eficiente (menos memória é desperdiçada), mas a leitura e o desempenho
             podem se deteriorar.

             Valores válidos: [0.5, 0.99]
             Padrão: 0.5 -->
        <max_load_factor>0.5</max_load_factor>
      </hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="sparse_hashed">
  ## sparse_hashed
</div>

Semelhante a `hashed`, mas usa menos memória à custa de maior uso de CPU.

A chave do dicionário é do tipo [UInt64](/pt-BR/sql-reference/data-types/int-uint.md).

Exemplo de configuração:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <layout>
      <sparse_hashed>
        <!-- <shards>1</shards> -->
        <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
        <!-- <max_load_factor>0.5</max_load_factor> -->
      </sparse_hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

Também é possível usar `shards` para esse tipo de dicionário e, novamente, isso é mais importante para `sparse_hashed` do que para `hashed`, já que `sparse_hashed` é mais lento.

<div id="complex_key_hashed">
  ## complex_key_hashed
</div>

Esse tipo de armazenamento é usado com [chaves](../attributes.md#composite-key) compostas. Semelhante a `hashed`.

Exemplo de configuração:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(COMPLEX_KEY_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <layout>
      <complex_key_hashed>
        <!-- <shards>1</shards> -->
        <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
        <!-- <max_load_factor>0.5</max_load_factor> -->
      </complex_key_hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_sparse_hashed">
  ## complex_key_sparse_hashed
</div>

Esse tipo de armazenamento é destinado ao uso com [chaves compostas](../attributes.md#composite-key). Semelhante a [sparse&#95;hashed](#sparse_hashed).

Exemplo de configuração:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(COMPLEX_KEY_SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <layout>
      <complex_key_sparse_hashed>
        <!-- <shards>1</shards> -->
        <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
        <!-- <max_load_factor>0.5</max_load_factor> -->
      </complex_key_sparse_hashed>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />