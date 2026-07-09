---
slug: /sql-reference/statements/create/dictionary/sources/mysql
title: 'Fonte de dicionário MySQL'
sidebar_position: 7
sidebar_label: 'MySQL'
description: 'Configure o MySQL como fonte de dicionário no ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Exemplo de configurações:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(MYSQL(
        port 3306
        user 'clickhouse'
        password 'qwerty'
        replica(host 'example01-1' priority 1)
        replica(host 'example01-2' priority 1)
        db 'db_name'
        table 'table_name'
        where 'id=10'
        invalidate_query 'SQL_QUERY'
        fail_on_connection_loss 'true'
        query 'SELECT id, value_1, value_2 FROM db_name.table_name'
        enable_compression 1
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <source>
      <mysql>
          <port>3306</port>
          <user>clickhouse</user>
          <password>qwerty</password>
          <replica>
              <host>example01-1</host>
              <priority>1</priority>
          </replica>
          <replica>
              <host>example01-2</host>
              <priority>1</priority>
          </replica>
          <db>db_name</db>
          <table>table_name</table>
          <where>id=10</where>
          <invalidate_query>SQL_QUERY</invalidate_query>
          <fail_on_connection_loss>true</fail_on_connection_loss>
          <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
          <enable_compression>1</enable_compression>
      </mysql>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Campos de configuração:

| Configuração              | Descrição                                                                                                                                                                                                                                                                                                                                                       |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `port`                    | A porta no servidor MySQL. Você pode especificá-la para todas as réplicas ou para cada uma individualmente (dentro de `<replica>`).                                                                                                                                                                                                                             |
| `user`                    | Nome do usuário MySQL. Você pode especificá-lo para todas as réplicas ou para cada uma individualmente (dentro de `<replica>`).                                                                                                                                                                                                                                 |
| `password`                | Senha do usuário MySQL. Você pode especificá-la para todas as réplicas ou para cada uma individualmente (dentro de `<replica>`).                                                                                                                                                                                                                                |
| `replica`                 | Seção de configurações da réplica. Pode haver várias seções.                                                                                                                                                                                                                                                                                                    |
| `replica/host`            | O host do MySQL.                                                                                                                                                                                                                                                                                                                                                |
| `replica/priority`        | A prioridade da réplica. Ao tentar se conectar, o ClickHouse percorre as réplicas em ordem de prioridade. Quanto menor o número, maior a prioridade.                                                                                                                                                                                                            |
| `db`                      | Nome do banco de dados.                                                                                                                                                                                                                                                                                                                                         |
| `table`                   | Nome da tabela.                                                                                                                                                                                                                                                                                                                                                 |
| `where`                   | Os critérios de seleção. A sintaxe das condições é a mesma da cláusula `WHERE` no MySQL, por exemplo, `id > 10 AND id < 20`. Opcional.                                                                                                                                                                                                                          |
| `invalidate_query`        | Consulta para verificar o status do dicionário. Opcional. Leia mais na seção [Atualização de dados de dicionário usando LIFETIME](../lifetime.md).                                                                                                                                                                                                              |
| `fail_on_connection_loss` | Controla o comportamento do servidor em caso de perda de conexão. Se `true`, uma exceção é gerada imediatamente se a conexão entre cliente e servidor for perdida. Se `false`, o servidor tenta buscar os dados novamente pelo menos três vezes antes de relatar um erro. Observe que repetir as tentativas aumenta o tempo de resposta. Valor padrão: `false`. |
| `query`                   | A consulta personalizada. Opcional.                                                                                                                                                                                                                                                                                                                             |
| `enable_compression`      | Habilita a compressão zlib para a conexão do protocolo MySQL. Quando definido como `1`, o ClickHouse solicita compressão no nível do protocolo ao servidor MySQL. Também pode ser definido por réplica dentro de `<replica>`. Valor padrão: `0`.                                                                                                                |

:::note
Os campos `table` e `where` não podem ser usados junto com o campo `query`. Além disso, um dos campos `table` ou `query` deve ser declarado.
:::

:::note
Não há um parâmetro explícito `secure`. Ao estabelecer uma conexão SSL, a segurança é obrigatória.
:::

É possível se conectar ao MySQL em um host local por meio de sockets. Para isso, defina `host` e `socket`.

Exemplo de configurações:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(MYSQL(
        host 'localhost'
        socket '/path/to/socket/file.sock'
        user 'clickhouse'
        password 'qwerty'
        db 'db_name'
        table 'table_name'
        where 'id=10'
        invalidate_query 'SQL_QUERY'
        fail_on_connection_loss 'true'
        query 'SELECT id, value_1, value_2 FROM db_name.table_name'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <source>
      <mysql>
          <host>localhost</host>
          <socket>/path/to/socket/file.sock</socket>
          <user>clickhouse</user>
          <password>qwerty</password>
          <db>db_name</db>
          <table>table_name</table>
          <where>id=10</where>
          <invalidate_query>SQL_QUERY</invalidate_query>
          <fail_on_connection_loss>true</fail_on_connection_loss>
          <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
      </mysql>
    </source>
    ```
  </TabItem>
</Tabs>