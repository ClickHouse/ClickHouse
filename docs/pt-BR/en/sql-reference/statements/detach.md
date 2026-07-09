---
description: 'Documentação sobre DETACH'
sidebar_label: 'DETACH'
sidebar_position: 43
slug: /sql-reference/statements/detach
title: 'Instrução DETACH'
doc_type: 'reference'
---

Faz com que o servidor &quot;esqueça&quot; a existência de uma tabela, de uma visão materializada, de um dicionário ou de um banco de dados.

**Sintaxe**

```sql
DETACH TABLE|VIEW|DICTIONARY|DATABASE [IF EXISTS] [db.]name [ON CLUSTER cluster] [PERMANENTLY] [SYNC]
```

Desanexar não exclui os dados nem os metadados de uma tabela, de uma visão materializada, de um dicionário ou de um banco de dados. Se uma entidade não foi desanexada `PERMANENTLY`, na próxima inicialização do servidor, o servidor lerá os metadados e reanexará a tabela/visão/dicionário/banco de dados. Se uma entidade foi desanexada `PERMANENTLY`, não haverá reanexação automática.

Independentemente de uma tabela, um dicionário ou um banco de dados ter sido desanexado permanentemente ou não, em ambos os casos é possível reanexá-los usando a consulta [ATTACH](../../sql-reference/statements/attach.md).
As tabelas de log do sistema também podem ser reanexadas (por exemplo, `query_log`, `text_log` etc.). Outras tabelas do sistema n&#39;ão podem ser reanexadas. Na próxima inicialização do servidor, o servidor reanexará essas tabelas novamente.

`ATTACH MATERIALIZED VIEW` não funciona com a sintaxe curta (sem `SELECT`), mas você pode anexá-la usando a consulta `ATTACH TABLE`.

Observe que você não pode desanexar permanentemente uma tabela que já está desanexada (temporariamente). Mas pode anexá-la novamente e depois desanexá-la permanentemente outra vez.

Além disso, você não pode fazer [DROP](../../sql-reference/statements/drop.md#drop-table) da tabela desanexada, nem [CREATE TABLE](../../sql-reference/statements/create/table.md) com o mesmo nome de uma tabela desanexada permanentemente, nem substituí-la por outra tabela com a consulta [RENAME TABLE](../../sql-reference/statements/rename.md).

O modificador `SYNC` executa a ação sem atraso.

**Exemplo**

Criando uma tabela:

```sql title="Query"
CREATE TABLE test ENGINE = MergeTree ORDER BY () AS SELECT * FROM numbers(10);
SELECT * FROM test;
```

```text title="Response"
┌─number─┐
│      0 │
│      1 │
│      2 │
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
│      9 │
└────────┘
```

Desanexar a tabela:

```sql title="Query"
DETACH TABLE test;
SELECT * FROM test;
```

```text title="Response"
Received exception from server (version 21.4.1):
Code: 60. DB::Exception: Received from localhost:9000. DB::Exception: Table default.test does not exist.
```

:::note
No ClickHouse Cloud, os usuários devem usar a cláusula `PERMANENTLY`, por exemplo, `DETACH TABLE <table> PERMANENTLY`. Se essa cláusula não for usada, as tabelas serão anexadas novamente na reinicialização do cluster, por exemplo, durante upgrades.
:::

**Veja também**

* [Visão materializada](/pt-BR/sql-reference/statements/create/view#materialized-view)
* [Dicionários](./create/dictionary/overview.md)