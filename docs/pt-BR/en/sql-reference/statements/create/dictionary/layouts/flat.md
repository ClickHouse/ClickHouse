---
slug: /sql-reference/statements/create/dictionary/layouts/flat
title: 'layout de dicionário flat'
sidebar_label: 'flat'
sidebar_position: 2
description: 'Armazena um dicionário na memória como arrays planos.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Com o layout `flat`, o dicionário é armazenado inteiramente na memória na forma de arrays planos.
A quantidade de memória usada é proporcional ao tamanho da maior chave (em termos de espaço ocupado).

:::tip
Esse tipo de layout oferece o melhor desempenho entre todos os métodos disponíveis de armazenamento de dicionários.
:::

A chave do dicionário tem o tipo [UInt64](/pt-BR/sql-reference/data-types/int-uint.md), e seu valor é limitado a `max_array_size` (por padrão — 500.000).
Se uma chave maior for encontrada ao criar o dicionário, o ClickHouse lança uma exceção e não cria o dicionário.
O tamanho inicial dos arrays planos do dicionário é controlado pela configuração `initial_array_size` (por padrão — 1024).

Todos os tipos de fontes de dados são compatíveis.
Ao atualizar o dicionário, os dados (de um arquivo ou de uma tabela) são lidos por completo.

Exemplo de configuração:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(FLAT(INITIAL_ARRAY_SIZE 50000 MAX_ARRAY_SIZE 5000000))
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <layout>
      <flat>
        <initial_array_size>50000</initial_array_size>
        <max_array_size>5000000</max_array_size>
      </flat>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />