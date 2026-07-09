---
description: 'Cria uma tabela Merge temporária. O esquema é derivado das tabelas subjacentes usando uma união de suas colunas e inferindo tipos comuns.'
sidebar_label: 'merge'
sidebar_position: 130
slug: /sql-reference/table-functions/merge
title: 'merge'
doc_type: 'reference'
---

Cria uma tabela [Merge](../../engines/table-engines/special/merge.md) temporária.
O esquema da tabela é derivado das tabelas subjacentes usando uma união de suas colunas e inferindo tipos comuns.
Estão disponíveis as mesmas colunas virtuais do motor de tabela [Merge](../../engines/table-engines/special/merge.md).

<div id="syntax">
  ## Sintaxe
</div>

```sql
merge(['db_name',] 'tables_regexp')
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento       | Descrição                                                                                                                                                                                                                                                                                                                                             |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `db_name`       | Valores possíveis (opcional; o padrão é `currentDatabase()`):<br />    - nome do banco de dados,<br />    - expressão constante que retorna uma string com um nome de banco de dados, por exemplo, `currentDatabase()`,<br />    - `REGEXP(expression)`, em que `expression` é uma expressão regular para corresponder aos nomes dos bancos de dados. |
| `tables_regexp` | Uma expressão regular para corresponder aos nomes das tabelas no banco de dados ou nos bancos de dados especificados.                                                                                                                                                                                                                                 |

<div id="related">
  ## Relacionado
</div>

* [Merge](../../engines/table-engines/special/merge.md) motor de tabela