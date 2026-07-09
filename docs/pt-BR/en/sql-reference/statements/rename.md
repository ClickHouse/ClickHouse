---
description: 'Documentação da instrução RENAME'
sidebar_label: 'RENAME'
sidebar_position: 48
slug: /sql-reference/statements/rename
title: 'Instrução RENAME'
doc_type: 'reference'
---

Renomeia bancos de dados, tabelas ou dicionários. É possível renomear várias entidades em uma única consulta.
Observe que a consulta `RENAME` com várias entidades não é atômica. Para trocar os nomes das entidades atomicamente, use a instrução [EXCHANGE](./exchange.md).

**Sintaxe**

```sql
RENAME [DATABASE|TABLE|DICTIONARY] name TO new_name [,...] [ON CLUSTER cluster]
```

<div id="rename-database">
  ## RENAME DATABASE
</div>

Renomeia bancos de dados.

**Sintaxe**

```sql
RENAME DATABASE atomic_database1 TO atomic_database2 [,...] [ON CLUSTER cluster]
```

<div id="rename-table">
  ## RENAME TABLE
</div>

Renomeia uma ou mais tabelas.

Renomear tabelas é uma operação leve. Se você informar um banco de dados diferente após `TO`, a tabela será movida para esse banco de dados. No entanto, os diretórios dos bancos de dados devem estar no mesmo sistema de arquivos. Caso contrário, será retornado um erro.
Se você renomear várias tabelas em uma única consulta, a operação não será atômica. Ela pode ser executada parcialmente, e consultas em outras sessões podem receber o erro `Table ... does not exist ...`.

**Sintaxe**

```sql
RENAME TABLE [db1.]name1 TO [db2.]name2 [,...] [ON CLUSTER cluster]
```

**Exemplo**

```sql
RENAME TABLE table_A TO table_A_bak, table_B TO table_B_bak;
```

E você pode usar um SQL mais simples:

```sql
RENAME table_A TO table_A_bak, table_B TO table_B_bak;
```

<div id="rename-dictionary">
  ## RENAME DICTIONARY
</div>

Renomeia um ou mais dicionários. Esta consulta pode ser usada para mover dicionários entre bancos de dados.

**Sintaxe**

```sql
RENAME DICTIONARY [db0.]dict_A TO [db1.]dict_B [,...] [ON CLUSTER cluster]
```

**Veja também**

* [Dicionários](./create/dictionary/overview.md)