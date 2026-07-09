---
description: 'Documentação para OFFSET'
sidebar_label: 'OFFSET'
slug: /sql-reference/statements/select/offset
title: 'Cláusula OFFSET FETCH'
doc_type: 'reference'
---

`OFFSET` e `FETCH` permitem recuperar dados em partes. Eles especificam um bloco de linhas que você deseja obter com uma única consulta.

```sql
-- SQL Standard style:
[OFFSET offset_row_count {ROW | ROWS}] [FETCH {FIRST | NEXT} fetch_row_count {ROW | ROWS} {ONLY | WITH TIES}]

-- MySQL/PostgreSQL style:
[LIMIT [n, ]m] [OFFSET offset_row_count]
```

O valor de `offset_row_count` ou `fetch_row_count` pode ser um número ou uma constante literal. Você pode omitir `fetch_row_count`; por padrão, ele é igual a 1.

`OFFSET` especifica o número de linhas a ignorar antes de começar a retornar linhas do conjunto de resultados da consulta. `OFFSET n` ignora as primeiras `n` linhas do resultado.

Há suporte para `OFFSET` negativo: `OFFSET -n` ignora as últimas `n` linhas do resultado.

Também há suporte para `OFFSET` fracionário: `OFFSET n` — se 0 &lt; n &lt; 1, então os primeiros n * 100% do resultado são ignorados.

Exemplo:
• `OFFSET 0.1` - ignora os primeiros 10% do resultado.

> **Nota**
> • A fração deve ser um número [Float64](../../data-types/float.md) menor que 1 e maior que zero.
> • Se o cálculo resultar em um número fracionário de linhas, ele será arredondado para cima até o próximo número inteiro.

`FETCH` especifica o número máximo de linhas que podem estar no resultado de uma consulta.

A opção `ONLY` é usada para retornar linhas que vêm imediatamente após as linhas omitidas por `OFFSET`. Nesse caso, `FETCH` é uma alternativa à cláusula [LIMIT](../../../sql-reference/statements/select/limit.md). Por exemplo, a consulta a seguir

```sql
SELECT * FROM test_fetch ORDER BY a OFFSET 1 ROW FETCH FIRST 3 ROWS ONLY;
```

é idêntico à consulta

```sql
SELECT * FROM test_fetch ORDER BY a LIMIT 3 OFFSET 1;
```

A opção `WITH TIES` é usada para retornar todas as linhas adicionais empatadas na última posição do conjunto de resultados, de acordo com a cláusula `ORDER BY`. Por exemplo, se `fetch_row_count` estiver definido como 5, mas houver duas linhas adicionais com os mesmos valores nas colunas do `ORDER BY` que a quinta linha, o conjunto de resultados conterá sete linhas.

:::note
De acordo com o padrão, a cláusula `OFFSET` deve vir antes da cláusula `FETCH`, se ambas estiverem presentes.
:::

:::note
O offset real também pode depender da configuração [offset](../../../operations/settings/settings.md#offset).
:::

<div id="examples">
  ## Exemplos
</div>

Tabela de entrada:

```text
┌─a─┬─b─┐
│ 1 │ 1 │
│ 2 │ 1 │
│ 3 │ 4 │
│ 1 │ 3 │
│ 5 │ 4 │
│ 0 │ 6 │
│ 5 │ 7 │
└───┴───┘
```

Uso da opção `ONLY`:

```sql title="Query"
SELECT * FROM test_fetch ORDER BY a OFFSET 3 ROW FETCH FIRST 3 ROWS ONLY;
```

```text title="Response"
┌─a─┬─b─┐
│ 2 │ 1 │
│ 3 │ 4 │
│ 5 │ 4 │
└───┴───┘
```

Uso da opção `WITH TIES`:

```sql title="Query"
SELECT * FROM test_fetch ORDER BY a OFFSET 3 ROW FETCH FIRST 3 ROWS WITH TIES;
```

```text title="Response"
┌─a─┬─b─┐
│ 2 │ 1 │
│ 3 │ 4 │
│ 5 │ 4 │
│ 5 │ 7 │
└───┴───┘
```