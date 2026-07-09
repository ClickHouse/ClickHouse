---
slug: /sql-reference/statements/create/dictionary/layouts/ssd-cache
title: 'Tipos de layout de dicionário ssd_cache'
sidebar_label: 'ssd_cache'
sidebar_position: 8
description: 'Armazena dados do dicionário em SSD com um índice em memória: tipos ssd_cache ou complex_key_ssd_cache'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="ssd_cache">
  ## ssd_cache
</div>

Semelhante a `cache`, mas armazena os dados em SSD e o índice na RAM. Todas as configurações de dicionários de cache relacionadas à fila de atualização também podem ser aplicadas a dicionários de cache SSD.

A chave do dicionário é do tipo [UInt64](/pt-BR/sql-reference/data-types/int-uint.md).

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 16777216 READ_BUFFER_SIZE 1048576
        PATH '/var/lib/clickhouse/user_files/test_dict'))
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <layout>
        <ssd_cache>
            <!-- Tamanho do bloco elementar de leitura em bytes. Recomenda-se que seja igual ao tamanho da página do SSD. -->
            <block_size>4096</block_size>
            <!-- Tamanho máximo do arquivo de cache em bytes. -->
            <file_size>16777216</file_size>
            <!-- Tamanho do buffer em RAM, em bytes, para ler elementos do SSD. -->
            <read_buffer_size>131072</read_buffer_size>
            <!-- Tamanho do buffer em RAM, em bytes, para agregar elementos antes de gravá-los no SSD. -->
            <write_buffer_size>1048576</write_buffer_size>
            <!-- Caminho em que o arquivo de cache será armazenado. -->
            <path>/var/lib/clickhouse/user_files/test_dict</path>
        </ssd_cache>
    </layout>
    ```
  </TabItem>
</Tabs>

<br />

<div id="complex_key_ssd_cache">
  ## complex_key_ssd_cache
</div>

Este tipo de armazenamento é usado com [chaves compostas](../attributes.md#composite-key). Semelhante a `ssd_cache`.