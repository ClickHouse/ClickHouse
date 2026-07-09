---
description: 'O motor `ExternalDistributed` permite executar consultas `SELECT`
  em dados armazenados em servidores MySQL ou PostgreSQL remotos. Aceita motores
  MySQL ou PostgreSQL como argumento, o que torna o sharding possível.'
sidebar_label: 'ExternalDistributed'
sidebar_position: 55
slug: /engines/table-engines/integrations/ExternalDistributed
title: 'Motor de tabela ExternalDistributed'
doc_type: 'reference'
---

O motor `ExternalDistributed` permite executar consultas `SELECT` em dados armazenados em servidores MySQL ou PostgreSQL remotos. Aceita motores [MySQL](../../../engines/table-engines/integrations/mysql.md) ou [PostgreSQL](../../../engines/table-engines/integrations/postgresql.md) como argumento, o que torna o sharding possível.

<div id="creating-a-table">
  ## Criar uma tabela
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = ExternalDistributed('engine', 'host:port', 'database', 'table', 'user', 'password');
```

Veja uma descrição detalhada da consulta [CREATE TABLE](/pt-BR/sql-reference/statements/create/table).

A estrutura da tabela pode ser diferente da estrutura da tabela original:

* Os nomes das colunas devem ser os mesmos da tabela original, mas você pode usar apenas algumas delas, em qualquer ordem.
* Os tipos das colunas podem ser diferentes dos da tabela original. O ClickHouse tenta [converter](/pt-BR/sql-reference/functions/type-conversion-functions#CAST) os valores para os tipos de dados do ClickHouse.

**Parâmetros do motor**

* `engine` — O motor de tabela `MySQL` ou `PostgreSQL`.
* `host:port` — Endereço do servidor MySQL ou PostgreSQL.
* `database` — Nome do banco de dados remoto.
* `table` — Nome da tabela remota.
* `user` — Nome do usuário.
* `password` — Senha do usuário.

<div id="implementation-details">
  ## Detalhes de implementação
</div>

Oferece suporte a várias réplicas, que devem ser listadas com `|`, e a shards, que devem ser listados com `,`. Por exemplo:

```sql
CREATE TABLE test_shards (id UInt32, name String, age UInt32, money UInt32) ENGINE = ExternalDistributed('MySQL', `mysql{1|2}:3306,mysql{3|4}:3306`, 'clickhouse', 'test_replicas', 'root', 'clickhouse');
```

Ao especificar réplicas, uma das réplicas disponíveis é selecionada para cada shard durante a leitura. Se a conexão falhar, a próxima réplica será selecionada, e assim por diante com todas as réplicas. Se a tentativa de conexão falhar para todas as réplicas, ela será repetida da mesma forma várias vezes.

Você pode especificar qualquer número de shards e qualquer número de réplicas para cada shard.

**Veja também**

* [motor de tabela MySQL](../../../engines/table-engines/integrations/mysql.md)
* [motor de tabela PostgreSQL](../../../engines/table-engines/integrations/postgresql.md)
* [motor de tabela Distributed](../../../engines/table-engines/special/distributed.md)