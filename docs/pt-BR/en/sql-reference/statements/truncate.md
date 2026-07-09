---
description: 'Documentação sobre as instruções TRUNCATE'
sidebar_label: 'TRUNCATE'
sidebar_position: 52
slug: /sql-reference/statements/truncate
title: 'Instruções TRUNCATE'
doc_type: 'reference'
---

A instrução `TRUNCATE` no ClickHouse é usada para remover rapidamente todos os dados de uma tabela ou banco de dados, preservando sua estrutura.

<div id="truncate-table">
  ## TRUNCATE TABLE
</div>

```sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]
```

<br />

| Parâmetro            | Descrição                                                                                                                                   |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `IF EXISTS`          | Evita um erro se a tabela não existir. Se omitido, a consulta retorna um erro.                                                              |
| `db.name`            | Nome do banco de dados opcional.                                                                                                            |
| `ON CLUSTER cluster` | Executa o comando em um cluster especificado.                                                                                               |
| `SYNC`               | Torna o truncamento síncrono entre as réplicas ao usar tabelas replicadas. Se omitido, o truncamento ocorre de forma assíncrona por padrão. |

Você pode usar a configuração [alter&#95;sync](/pt-BR/operations/settings/settings#alter_sync) para configurar a espera pela execução de ações nas réplicas.

Você pode especificar por quanto tempo (em segundos) esperar que réplicas inativas executem consultas `TRUNCATE` com a configuração [replication&#95;wait&#95;for&#95;inactive&#95;replica&#95;timeout](/pt-BR/operations/settings/settings#replication_wait_for_inactive_replica_timeout).

:::note
Se `alter_sync` estiver definido como `2` e algumas réplicas permanecerem inativas por mais tempo do que o especificado pela configuração `replication_wait_for_inactive_replica_timeout`, será lançada uma exceção `UNFINISHED`.
:::

A consulta `TRUNCATE TABLE` **não tem suporte** para os seguintes motores de tabela:

* [`View`](../../engines/table-engines/special/view.md)
* [`File`](../../engines/table-engines/special/file.md)
* [`URL`](../../engines/table-engines/special/url.md)
* [`Buffer`](../../engines/table-engines/special/buffer.md)
* [`Null`](../../engines/table-engines/special/null.md)

<div id="truncate-all-tables">
  ## TRUNCATE TODAS AS TABELAS
</div>

```sql
TRUNCATE [ALL] TABLES FROM [IF EXISTS] db [LIKE | ILIKE | NOT LIKE '<pattern>'] [ON CLUSTER cluster]
```

<br />

| Parâmetro                               | Descrição                                              |
| --------------------------------------- | ------------------------------------------------------ |
| `ALL`                                   | Remove os dados de todas as tabelas no banco de dados. |
| `IF EXISTS`                             | Evita um erro se o banco de dados não existir.         |
| `db`                                    | O nome do banco de dados.                              |
| `LIKE \| ILIKE \| NOT LIKE '<pattern>'` | Filtra as tabelas pelo padrão.                         |
| `ON CLUSTER cluster`                    | Executa o comando em um cluster.                       |

Remove todos os dados de todas as tabelas de um banco de dados.

<div id="truncate-database">
  ## TRUNCATE DATABASE
</div>

```sql
TRUNCATE DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```

<br />

| Parâmetro            | Descrição                                         |
| -------------------- | ------------------------------------------------- |
| `IF EXISTS`          | Evita um erro se o banco de dados não existir.    |
| `db`                 | O nome do banco de dados.                         |
| `ON CLUSTER cluster` | Executa o comando em todo o cluster especificado. |

Remove todas as tabelas de um banco de dados, mas mantém o próprio banco de dados. Quando a cláusula `IF EXISTS` é omitida, a consulta retorna um erro se o banco de dados não existir.

:::note
`TRUNCATE DATABASE` não é compatível com bancos de dados `Replicated`. Em vez disso, basta aplicar `DROP` e `CREATE` ao banco de dados.
:::