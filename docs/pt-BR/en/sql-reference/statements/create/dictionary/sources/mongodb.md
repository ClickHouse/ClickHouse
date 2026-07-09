---
slug: /sql-reference/statements/create/dictionary/sources/mongodb
title: 'Fonte de dicionário do MongoDB'
sidebar_position: 9
sidebar_label: 'MongoDB'
description: 'Configure o MongoDB como fonte de dicionário no ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Exemplo de configurações:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(MONGODB(
        host 'localhost'
        port 27017
        user ''
        password ''
        db 'test'
        collection 'dictionary_source'
        options 'ssl=true'
    ))
    ```

    Ou usando uma URI:

    ```sql
    SOURCE(MONGODB(
        uri 'mongodb://localhost:27017/clickhouse'
        collection 'dictionary_source'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <source>
        <mongodb>
            <host>localhost</host>
            <port>27017</port>
            <user></user>
            <password></password>
            <db>test</db>
            <collection>dictionary_source</collection>
            <options>ssl=true</options>
        </mongodb>
    </source>
    ```

    Ou usando uma URI:

    ```xml
    <source>
        <mongodb>
            <uri>mongodb://localhost:27017/test?ssl=true</uri>
            <collection>dictionary_source</collection>
        </mongodb>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Campos de configuração:

| Configuração | Descrição                                                                         |
| ------------ | --------------------------------------------------------------------------------- |
| `host`       | O host do MongoDB.                                                                |
| `port`       | A porta no servidor MongoDB.                                                      |
| `user`       | Nome do usuário do MongoDB.                                                       |
| `password`   | Senha do usuário do MongoDB.                                                      |
| `db`         | Nome do banco de dados.                                                           |
| `collection` | Nome da coleção.                                                                  |
| `options`    | Opções da string de conexão do MongoDB. Opcional.                                 |
| `uri`        | URI para estabelecer a conexão (alternativa aos campos individuais host/port/db). |

[Mais informações sobre o motor](/pt-BR/engines/table-engines/integrations/mongodb)