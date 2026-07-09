---
description: 'Documentação do APPLY DELETED MASK'
sidebar_label: 'APPLY DELETED MASK'
sidebar_position: 46
slug: /sql-reference/statements/alter/apply-deleted-mask
title: 'Aplicar máscara de linhas excluídas'
doc_type: 'reference'
---

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] APPLY DELETED MASK [IN PARTITION partition_id]
```

O comando aplica a máscara criada pela [exclusão leve](/pt-BR/sql-reference/statements/delete) e remove à força do disco as linhas marcadas como excluídas. Este comando é uma mutação pesada e, semanticamente, equivale à consulta `ALTER TABLE [db].name DELETE WHERE _row_exists = 0`.

:::note
Funciona apenas para tabelas da família [`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) (incluindo tabelas [replicadas](../../../engines/table-engines/mergetree-family/replication.md)).
:::

**Veja também**

* [Exclusões leves](/pt-BR/sql-reference/statements/delete)
* [Exclusões pesadas](/pt-BR/sql-reference/statements/alter/delete.md)