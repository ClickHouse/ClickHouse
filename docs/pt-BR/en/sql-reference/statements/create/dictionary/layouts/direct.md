---
slug: /sql-reference/statements/create/dictionary/layouts/direct
title: 'layout de dicionário direct'
sidebar_label: 'direct'
sidebar_position: 9
description: 'Um layout de dicionário que consulta a fonte diretamente, sem armazenamento em cache.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="direct">
  ## direct
</div>

O dicionário não é armazenado na memória e consulta diretamente a fonte durante o processamento de uma solicitação.

A chave do dicionário é do tipo [UInt64](/pt-BR/sql-reference/data-types/int-uint.md).

Há suporte a todos os tipos de [fontes](../sources/#dictionary-sources), exceto arquivos locais.

Exemplo de configuração:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(DIRECT())
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <layout>
      <direct />
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_direct">
  ## complex_key_direct
</div>

Este tipo de armazenamento é usado com [chaves](../attributes.md#composite-key) compostas. Semelhante a `direct`.