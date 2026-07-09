---
slug: /sql-reference/statements/create/dictionary/sources/redis
title: 'Fonte de dicionário Redis'
sidebar_position: 10
sidebar_label: 'Redis'
description: 'Configure o Redis como fonte de dicionário no ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Exemplo de configurações:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(REDIS(
        host 'localhost'
        port 6379
        storage_type 'simple'
        db_index 0
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <source>
        <redis>
            <host>localhost</host>
            <port>6379</port>
            <storage_type>simple</storage_type>
            <db_index>0</db_index>
        </redis>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Campos de configuração:

| Configuração   | Descrição                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| -------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`         | O host do Redis.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `port`         | A porta do servidor Redis.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `storage_type` | A estrutura do armazenamento interno do Redis usada para trabalhar com chaves. `simple` usa um map simples de chave-valor e oferece suporte a layouts de chave simples, bem como a layouts de chave complexa com uma única coluna (como `complex_key_cache` e `complex_key_direct`). `hash_map` usa um hash do Redis e é necessário para chaves complexas compostas; ele espera exatamente duas colunas-chave. As colunas-chave devem ser do tipo inteiro ou string. Layouts com intervalos não são compatíveis. O valor padrão é `simple`. Opcional. |
| `db_index`     | O índice numérico específico do banco de dados lógico do Redis. O valor padrão é `0`. Opcional.                                                                                                                                                                                                                                                                                                                                                                                                                                                       |