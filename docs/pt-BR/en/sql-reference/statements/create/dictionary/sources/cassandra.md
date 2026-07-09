---
slug: /sql-reference/statements/create/dictionary/sources/cassandra
title: 'Fonte de dicionário do Cassandra'
sidebar_position: 11
sidebar_label: 'Cassandra'
description: 'Configure o Cassandra como fonte de dicionário no ClickHouse.'
doc_type: 'referência'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Exemplo de configurações:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(CASSANDRA(
        host 'localhost'
        port 9042
        user 'username'
        password 'qwerty123'
        keyspace 'database_name'
        column_family 'table_name'
        allow_filtering 1
        partition_key_prefix 1
        consistency 'One'
        where '"SomeColumn" = 42'
        max_threads 8
        query 'SELECT id, value_1, value_2 FROM database_name.table_name'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <source>
        <cassandra>
            <host>localhost</host>
            <port>9042</port>
            <user>username</user>
            <password>qwerty123</password>
            <keyspase>database_name</keyspase>
            <column_family>table_name</column_family>
            <allow_filtering>1</allow_filtering>
            <partition_key_prefix>1</partition_key_prefix>
            <consistency>One</consistency>
            <where>"SomeColumn" = 42</where>
            <max_threads>8</max_threads>
            <query>SELECT id, value_1, value_2 FROM database_name.table_name</query>
        </cassandra>
    </source>
    ```
  </TabItem>
</Tabs>

Campos de configuração:

| Configuração           | Descrição                                                                                                                                                                                                                                                                                                                                          |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host`                 | O host do Cassandra ou uma lista de hosts separada por vírgulas.                                                                                                                                                                                                                                                                                   |
| `port`                 | A porta dos servidores Cassandra. Se não for especificada, a porta padrão `9042` será usada.                                                                                                                                                                                                                                                       |
| `user`                 | Nome do usuário do Cassandra.                                                                                                                                                                                                                                                                                                                      |
| `password`             | Senha do usuário do Cassandra.                                                                                                                                                                                                                                                                                                                     |
| `keyspace`             | Nome do keyspace (banco de dados).                                                                                                                                                                                                                                                                                                                 |
| `column_family`        | Nome da família de colunas (tabela).                                                                                                                                                                                                                                                                                                               |
| `allow_filtering`      | Flag para permitir ou não condições potencialmente custosas nas colunas da chave de clustering. O valor padrão é `1`.                                                                                                                                                                                                                              |
| `partition_key_prefix` | Número de colunas da chave de partição na chave primária da tabela Cassandra. Obrigatório para dicionários com chave composta. A ordem das colunas-chave na definição do dicionário deve ser a mesma que no Cassandra. O valor padrão é `1` (a primeira coluna-chave é uma chave de partição, e as demais colunas-chave são chaves de clustering). |
| `consistency`          | Nível de consistência. Valores possíveis: `One`, `Two`, `Three`, `All`, `EachQuorum`, `Quorum`, `LocalQuorum`, `LocalOne`, `Serial`, `LocalSerial`. O valor padrão é `One`.                                                                                                                                                                        |
| `where`                | Critérios de seleção opcionais.                                                                                                                                                                                                                                                                                                                    |
| `max_threads`          | O número máximo de threads a serem usadas para carregar dados de várias partições em dicionários com chave composta.                                                                                                                                                                                                                               |
| `query`                | A consulta personalizada. Opcional.                                                                                                                                                                                                                                                                                                                |

:::note
Os campos `column_family` ou `where` não podem ser usados junto com o campo `query`. Além disso, é obrigatório declarar um dos campos `column_family` ou `query`.
:::