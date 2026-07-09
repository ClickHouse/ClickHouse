---
slug: /sql-reference/statements/create/dictionary/sources/clickhouse
title: 'Fonte de dicionário do ClickHouse'
sidebar_position: 8
sidebar_label: 'ClickHouse'
description: 'Configure uma tabela do ClickHouse como fonte de dicionário.'
doc_type: 'referência'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Exemplo de configurações:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(CLICKHOUSE(
        host 'example01-01-1'
        port 9000
        user 'default'
        password ''
        db 'default'
        table 'ids'
        where 'id=10'
        secure 1
        query 'SELECT id, value_1, value_2 FROM default.ids'
    ));
    ```
  </TabItem>

  <TabItem value="xml" label="Arquivo de configuração">
    ```xml
    <source>
        <clickhouse>
            <host>example01-01-1</host>
            <port>9000</port>
            <user>default</user>
            <password></password>
            <db>default</db>
            <table>ids</table>
            <where>id=10</where>
            <secure>1</secure>
            <query>SELECT id, value_1, value_2 FROM default.ids</query>
        </clickhouse>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

Campos de configuração:

| Setting            | Description                                                                                                                                                                                                                                                              |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `host`             | O host do ClickHouse. Se for um host local, a consulta será processada sem nenhuma atividade de rede. Para melhorar a tolerância a falhas, você pode criar uma tabela [Distributed](/pt-BR/engines/table-engines/special/distributed) e usá-la nas configurações subsequentes. |
| `port`             | A porta no servidor ClickHouse.                                                                                                                                                                                                                                          |
| `user`             | Nome do usuário do ClickHouse.                                                                                                                                                                                                                                           |
| `password`         | Senha do usuário do ClickHouse.                                                                                                                                                                                                                                          |
| `db`               | Nome do banco de dados.                                                                                                                                                                                                                                                  |
| `table`            | Nome da tabela.                                                                                                                                                                                                                                                          |
| `where`            | Critério de seleção. Opcional.                                                                                                                                                                                                                                           |
| `invalidate_query` | Consulta para verificar o status do dicionário. Opcional. Leia mais na seção [Atualização de dados de dicionário usando LIFETIME](../lifetime.md).                                                                                                                       |
| `secure`           | Usa SSL para a conexão.                                                                                                                                                                                                                                                  |
| `query`            | Consulta personalizada. Opcional.                                                                                                                                                                                                                                        |

:::note
Os campos `table` e `where` não podem ser usados junto com o campo `query`. Além disso, é obrigatório declarar um dos campos `table` ou `query`.
:::