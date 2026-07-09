---
description: 'Documentação sobre a manipulação de expressões-chave'
sidebar_label: 'ORDER BY'
sidebar_position: 41
slug: /sql-reference/statements/alter/order-by
title: 'Manipulação de expressões-chave'
doc_type: 'referência'
---

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY ORDER BY new_expression
```

O comando altera a [chave de ordenação](../../../engines/table-engines/mergetree-family/mergetree.md) da tabela para `new_expression` (uma expressão ou uma tupla de expressões). A chave primária permanece a mesma.

O comando é lightweight no sentido de que altera apenas os metadados. Para preservar a propriedade de que as linhas das partes de dados são ordenadas pela expressão da chave de ordenação, não é possível adicionar à chave de ordenação expressões que contenham colunas existentes (apenas colunas adicionadas pelo comando `ADD COLUMN` na mesma consulta `ALTER`, sem valor padrão para a coluna).

:::note
Funciona apenas para tabelas da família [`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) (incluindo tabelas [replicadas](../../../engines/table-engines/mergetree-family/replication.md)).
:::