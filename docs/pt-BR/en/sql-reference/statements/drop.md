---
description: 'Documentação para Instruções DROP'
sidebar_label: 'DROP'
sidebar_position: 44
slug: /sql-reference/statements/drop
title: 'Instruções DROP'
doc_type: 'reference'
---

Exclui uma entidade existente. Se a cláusula `IF EXISTS` for especificada, essas instruções não retornarão erro se a entidade não existir. Se o modificador `SYNC` for especificado, a entidade será excluída imediatamente.

<div id="drop-database">
  ## DROP DATABASE
</div>

Exclui todas as tabelas do banco de dados `db` e, em seguida, exclui o próprio banco de dados `db`.

Sintaxe:

```sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster] [SYNC]
```

<div id="drop-table">
  ## DROP TABLE
</div>

Exclui uma ou mais tabelas.

:::tip
Para reverter a exclusão de uma tabela, consulte [UNDROP TABLE](/pt-BR/sql-reference/statements/undrop.md)
:::

Sintaxe:

```sql
DROP [TEMPORARY] TABLE [IF EXISTS] [IF EMPTY]  [db1.]name_1[, [db2.]name_2, ...] [ON CLUSTER cluster] [SYNC]
```

Limitações:

* Se a cláusula `IF EMPTY` for especificada, o servidor verificará se a tabela está vazia apenas na réplica que recebeu a consulta.
* Excluir várias tabelas de uma só vez não é uma operação atômica; ou seja, se a exclusão de uma tabela falhar, as tabelas subsequentes não serão excluídas.

<div id="drop-dictionary">
  ## DROP DICTIONARY
</div>

Exclui o dicionário.

Sintaxe:

```sql
DROP DICTIONARY [IF EXISTS] [db.]name [SYNC]
```

<div id="drop-user">
  ## DROP USER
</div>

Exclui um usuário.

Sintaxe:

```sql
DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-role">
  ## DROP ROLE
</div>

Exclui um role. O role excluído é revogado de todas as entidades às quais foi atribuído.

Sintaxe:

```sql
DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-row-policy">
  ## DROP ROW POLICY
</div>

Exclui uma ROW POLICY. A ROW POLICY excluída é revogada de todas as entidades às quais estava atribuída.

Sintaxe:

```sql
DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-masking-policy">
  ## DROP MASKING POLICY
</div>

Exclui uma política de mascaramento.

Sintaxe:

```sql
DROP MASKING POLICY [IF EXISTS] name ON [database.]table [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-quota">
  ## DROP QUOTA
</div>

Exclui uma QUOTA. A QUOTA excluída é revogada de todas as entidades às quais foi atribuída.

Sintaxe:

```sql
DROP QUOTA [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-settings-profile">
  ## DROP SETTINGS PROFILE
</div>

Exclui um perfil de configurações. O perfil de configurações excluído é removido de todas as entidades às quais foi atribuído.

Sintaxe:

```sql
DROP [SETTINGS] PROFILE [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-view">
  ## DROP VIEW
</div>

Exclui uma view. Views também podem ser excluídas com o comando `DROP TABLE`, mas `DROP VIEW` verifica se `[db.]name` é uma view.

Sintaxe:

```sql
DROP VIEW [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]
```

<div id="drop-function">
  ## DROP FUNCTION
</div>

Exclui uma função definida pelo usuário criada com [CREATE FUNCTION](./create/function.md).
Funções do sistema não podem ser excluídas.

**Sintaxe**

```sql
DROP FUNCTION [IF EXISTS] function_name [on CLUSTER cluster]
```

**Exemplo**

```sql
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
DROP FUNCTION linear_equation;
```

<div id="drop-named-collection">
  ## DROP NAMED COLLECTION
</div>

Exclui uma coleção nomeada.

**Sintaxe**

```sql
DROP NAMED COLLECTION [IF EXISTS] name [on CLUSTER cluster]
```

**Exemplo**

```sql
CREATE NAMED COLLECTION foobar AS a = '1', b = '2';
DROP NAMED COLLECTION foobar;
```