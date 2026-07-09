---
description: 'Documentação do CREATE DATABASE'
sidebar_label: 'DATABASE'
sidebar_position: 35
slug: /sql-reference/statements/create/database
title: 'CREATE DATABASE'
doc_type: 'reference'
---

Cria um novo banco de dados.

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)] [SETTINGS ...] [COMMENT 'Comment']
```

<div id="clauses">
  ## Cláusulas
</div>

<div id="if-not-exists">
  ### IF NOT EXISTS
</div>

Se o banco de dados `db_name` já existir, o ClickHouse não criará um novo banco de dados e:

* Não lançará uma exceção se a cláusula for especificada.
* Lançará uma exceção se a cláusula não for especificada.

<div id="on-cluster">
  ### ON CLUSTER
</div>

O ClickHouse cria o banco de dados `db_name` em todos os servidores do cluster especificado. Mais detalhes no artigo sobre [DDL distribuído](../../../sql-reference/distributed-ddl.md).

<div id="engine">
  ### MOTOR
</div>

Por padrão, o ClickHouse usa seu próprio motor de banco de dados [Atomic](../../../engines/database-engines/atomic.md). Também há [MySQL](../../../engines/database-engines/mysql.md), [PostgresSQL](../../../engines/database-engines/postgresql.md), [MaterializedPostgreSQL](../../../engines/database-engines/materialized-postgresql.md), [Replicated](../../../engines/database-engines/replicated.md), [SQLite](../../../engines/database-engines/sqlite.md).

<div id="comment">
  ### COMENTÁRIO
</div>

Você pode adicionar um comentário ao banco de dados ao criá-lo.

Há suporte a comentários em todos os motores de banco de dados.

**Sintaxe**

```sql
CREATE DATABASE db_name ENGINE = engine(...) COMMENT 'Comment'
```

**Exemplo**

```sql title="Query"
CREATE DATABASE db_comment ENGINE = Memory COMMENT 'The temporary database';
SELECT name, comment FROM system.databases WHERE name = 'db_comment';
```

```text title="Response"
┌─name───────┬─comment────────────────┐
│ db_comment │ The temporary database │
└────────────┴────────────────────────┘
```

<div id="settings">
  ### CONFIGURAÇÕES
</div>

<div id="lazy-load-tables">
  #### lazy_load_tables
</div>

Quando habilitado, as tabelas não são carregadas completamente durante a inicialização do banco de dados. Em vez disso, é criado um proxy leve para cada tabela, e o motor de tabela real é materializado no primeiro acesso. Isso reduz o tempo de inicialização e o uso de memória em bancos de dados com muitas tabelas, nos quais apenas um subconjunto é consultado ativamente.

```sql
CREATE DATABASE db_name ENGINE = Atomic SETTINGS lazy_load_tables = 1;
```

Aplica-se a motores de banco de dados que armazenam metadados de tabelas em disco (por exemplo, `Atomic`, `Ordinary`). Views, visões materializadas, dicionários e tabelas baseadas em funções de tabela são sempre carregados imediatamente, independentemente dessa configuração.

**Quando usar:** Esta configuração é útil para bancos de dados com um grande número de tabelas (centenas ou milhares) em que apenas um subconjunto recebe consultas com frequência. Ela reduz o tempo de inicialização do servidor e o uso de memória ao adiar a criação dos objetos do motor da tabela, a varredura das partes de dados e a inicialização de threads em segundo plano até o primeiro acesso.

**Impacto em `system.tables`:**

* Antes de uma tabela ser acessada, `system.tables` mostra seu motor como `TableProxy`. Após o primeiro acesso, mostra o nome real do motor (por exemplo, `MergeTree`).
* Colunas como `total_rows` e `total_bytes` retornam `NULL` para tabelas não carregadas porque o armazenamento real ainda não foi criado.

**Interação com operações DDL:**

* `SELECT`, `INSERT`, `ALTER`, `DROP` acionam automaticamente o carregamento do motor real da tabela no primeiro uso.
* `RENAME TABLE` funciona sem disparar o carregamento.
* Depois que uma tabela é carregada, ela permanece carregada durante todo o ciclo de vida do processo do servidor.

**Limitações:**

* Ferramentas de monitoramento que dependem dos metadados de `system.tables` (por exemplo, `total_rows`, `engine`) podem exibir informações incompletas para tabelas não carregadas.
* A primeira consulta a uma tabela não carregada tem um custo único de carregamento (análise da instrução `CREATE TABLE` armazenada e inicialização do motor).

Valor padrão: `0` (desabilitado).