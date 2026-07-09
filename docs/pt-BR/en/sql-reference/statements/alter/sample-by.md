---
description: 'Documentação para manipular a expressão SAMPLE BY'
sidebar_label: 'SAMPLE BY'
sidebar_position: 41
slug: /sql-reference/statements/alter/sample-by
title: 'Manipulação de expressões da chave de amostragem'
doc_type: 'reference'
---

As operações a seguir estão disponíveis:

<div id="modify">
  ## ALTERAR
</div>

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY SAMPLE BY new_expression
```

O comando altera a [chave de amostragem](../../../engines/table-engines/mergetree-family/mergetree.md) da tabela para `new_expression` (uma expressão ou uma tupla de expressões). A chave primária deve conter a nova chave de amostragem.

<div id="remove">
  ## REMOVER
</div>

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] REMOVE SAMPLE BY
```

O comando remove a [chave de amostragem](../../../engines/table-engines/mergetree-family/mergetree.md) da tabela.

Os comandos `MODIFY` e `REMOVE` são leves, no sentido de que apenas alteram metadados ou removem arquivos.

:::note
Funciona apenas para tabelas da família [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) (incluindo tabelas [replicadas](../../../engines/table-engines/mergetree-family/replication.md)).
:::