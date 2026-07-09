---
slug: /sql-reference/statements/create/dictionary/layouts/hashed-array
title: 'tipos de layout de dicionário hashed_array'
sidebar_label: 'hashed_array'
sidebar_position: 4
description: 'Armazena um dicionário na memória em uma tabela hash com arrays de atributos.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="hashed_array">
  ## hashed_array
</div>

O dicionário é armazenado inteiramente em memória. Cada atributo é armazenado em um array. O atributo-chave é armazenado na forma de uma tabela hash, em que o valor é um índice no array de atributos. O dicionário pode conter qualquer número de elementos com quaisquer identificadores. Na prática, o número de chaves pode chegar a dezenas de milhões de itens.

A chave do dicionário é do tipo [UInt64](/pt-BR/sql-reference/data-types/int-uint.md).

Todos os tipos de fontes são compatíveis. Ao atualizar, os dados (de um arquivo ou de uma tabela) são lidos por completo.

Exemplo de configuração:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(HASHED_ARRAY([SHARDS 1]))
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <layout>
      <hashed_array>
      </hashed_array>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_hashed_array">
  ## complex_key_hashed_array
</div>

Esse tipo de armazenamento é usado com [chaves compostas](../attributes.md#composite-key). Semelhante a [hashed&#95;array](#hashed_array).

Exemplo de configuração:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(COMPLEX_KEY_HASHED_ARRAY([SHARDS 1]))
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <layout>
      <complex_key_hashed_array />
    </layout>
    ```
  </TabItem>
</Tabs>

<br />