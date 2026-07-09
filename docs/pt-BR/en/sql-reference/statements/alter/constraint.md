---
description: 'Documentação sobre manipulação de restrições'
sidebar_label: 'CONSTRAINT'
sidebar_position: 43
slug: /sql-reference/statements/alter/constraint
title: 'Manipulação de restrições'
doc_type: 'reference'
---

As restrições podem ser adicionadas, modificadas ou excluídas usando a seguinte sintaxe:

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD CONSTRAINT [IF NOT EXISTS] constraint_name {CHECK|ASSUME} expression;
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY CONSTRAINT [IF EXISTS] constraint_name {CHECK|ASSUME} expression;
ALTER TABLE [db].name [ON CLUSTER cluster] DROP CONSTRAINT [IF EXISTS] constraint_name;
```

Assim como na criação de tabelas, uma restrição pode ser declarada como `CHECK` (aplicada em `INSERT`) ou como `ASSUME` (assumida pelo otimizador sem ser verificada). Veja [restrições](../../../sql-reference/statements/create/table.md#constraints) para entender a diferença entre as duas.

`MODIFY CONSTRAINT` substitui a declaração de uma restrição existente, mantendo sua posição na definição da tabela. Também pode alterar o tipo da restrição (por exemplo, de `CHECK` para `ASSUME`). Isso equivale a remover a restrição e adicioná-la novamente com a nova declaração. Se a restrição não existir, a consulta gerará um erro, a menos que `IF EXISTS` seja especificado.

Veja mais sobre [restrições](../../../sql-reference/statements/create/table.md#constraints).

As consultas adicionam, alteram ou removem metadados de restrições da tabela, por isso são processadas imediatamente.

:::tip
A verificação da restrição **não será executada** nos dados existentes caso ela tenha sido adicionada ou modificada.
:::

Todas as alterações em tabelas replicadas são transmitidas ao ZooKeeper e também serão aplicadas às outras réplicas.