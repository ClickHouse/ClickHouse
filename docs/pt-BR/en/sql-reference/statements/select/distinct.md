---
description: 'Documentação da cláusula DISTINCT'
sidebar_label: 'DISTINCT'
slug: /sql-reference/statements/select/distinct
title: 'Cláusula DISTINCT'
doc_type: 'reference'
---

Se `SELECT DISTINCT` for especificado, apenas linhas únicas permanecerão no resultado da consulta. Assim, de cada conjunto de linhas completamente iguais no resultado, restará apenas uma única linha.

Você pode especificar a lista de colunas que devem ter valores únicos: `SELECT DISTINCT ON (column1, column2,...)`. Se as colunas não forem especificadas, todas elas serão levadas em consideração.

Considere a tabela:

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 2 │ 2 │ 2 │
│ 1 │ 1 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

Usando `DISTINCT` sem especificar colunas:

```sql
SELECT DISTINCT * FROM t1;
```

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 1 │ 1 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

Usando `DISTINCT` com colunas específicas:

```sql
SELECT DISTINCT ON (a,b) * FROM t1;
```

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

<div id="distinct-and-order-by">
  ## DISTINCT e ORDER BY
</div>

O ClickHouse oferece suporte ao uso das cláusulas `DISTINCT` e `ORDER BY` em colunas diferentes na mesma consulta. A cláusula `DISTINCT` é executada antes da cláusula `ORDER BY`.

Considere a tabela:

```text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 1 │ 2 │
│ 3 │ 3 │
│ 2 │ 4 │
└───┴───┘
```

Seleção de dados:

```sql
SELECT DISTINCT a FROM t1 ORDER BY b ASC;
```

```text
┌─a─┐
│ 2 │
│ 1 │
│ 3 │
└───┘
```

Selecionando dados com uma direção de ordenação diferente:

```sql
SELECT DISTINCT a FROM t1 ORDER BY b DESC;
```

```text
┌─a─┐
│ 3 │
│ 1 │
│ 2 │
└───┘
```

A linha `2, 4` foi truncada antes da ordenação.

Leve essa particularidade da implementação em conta ao programar consultas.

<div id="null-processing">
  ## Processamento de NULL
</div>

`DISTINCT` funciona com [NULL](/pt-BR/sql-reference/syntax#null) como se `NULL` fosse um valor específico e `NULL==NULL`. Em outras palavras, nos resultados de `DISTINCT`, diferentes combinações com `NULL` aparecem apenas uma vez. Isso difere do processamento de `NULL` na maioria dos demais contextos.

<div id="alternatives">
  ## Alternativas
</div>

É possível obter o mesmo resultado aplicando [GROUP BY](/pt-BR/sql-reference/statements/select/group-by) ao mesmo conjunto de valores especificado na cláusula `SELECT`, sem usar nenhuma função de agregação. Mas há algumas diferenças em relação à abordagem com `GROUP BY`:

* `DISTINCT` pode ser aplicado junto com `GROUP BY`.
* Quando [ORDER BY](../../../sql-reference/statements/select/order-by.md) é omitido e [LIMIT](../../../sql-reference/statements/select/limit.md) é definido, a consulta para de ser executada imediatamente após a leitura do número necessário de linhas distintas.
* Blocos de dados são gerados à medida que são processados, sem esperar a conclusão de toda a consulta.