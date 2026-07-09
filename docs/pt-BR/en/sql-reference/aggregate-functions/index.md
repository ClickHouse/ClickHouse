---
description: 'Documentação sobre funções de agregação'
sidebar_label: 'Funções de agregação'
sidebar_position: 33
slug: /sql-reference/aggregate-functions/
title: 'Funções de agregação'
doc_type: 'reference'
---

As funções de agregação funcionam da forma [normal](http://www.sql-tutorial.com/sql-aggregate-functions-sql-tutorial), como esperado por especialistas em bancos de dados.

O ClickHouse também oferece suporte a:

* [Funções de agregação paramétricas](/pt-BR/sql-reference/aggregate-functions/parametric-functions), que aceitam outros parâmetros além das colunas.
* [Combinadores](/pt-BR/sql-reference/aggregate-functions/combinators), que alteram o comportamento das funções de agregação.

<div id="null-processing">
  ## Processamento de NULL
</div>

Durante a agregação, todos os argumentos `NULL` são ignorados. Se a agregação tiver vários argumentos, qualquer linha em que um ou mais deles sejam NULL será ignorada.

Há uma exceção a essa regra: as funções [`first_value`](../../sql-reference/aggregate-functions/reference/first_value.md), [`last_value`](../../sql-reference/aggregate-functions/reference/last_value.md) e seus aliases (`any` e `anyLast`, respectivamente), quando seguidos pelo modificador `RESPECT NULLS`. Por exemplo, `FIRST_VALUE(b) RESPECT NULLS`.

**Exemplos:**

Considere esta tabela:

```text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Digamos que você precise somar os valores da coluna `y`:

```sql
SELECT sum(y) FROM t_null_big
```

```text
┌─sum(y)─┐
│      7 │
└────────┘
```

Agora você pode usar a função `groupArray` para criar um array da coluna `y`:

```sql
SELECT groupArray(y) FROM t_null_big
```

```text
┌─groupArray(y)─┐
│ [2,2,3]       │
└───────────────┘
```

`groupArray` não inclui `NULL` no array resultante.

Você pode usar [COALESCE](../../sql-reference/functions/functions-for-nulls.md#coalesce) para transformar `NULL` em um valor que faça sentido no seu caso de uso. Por exemplo: `avg(COALESCE(column, 0))` usará o valor da coluna na agregação, ou zero se for `NULL`:

```sql
SELECT
    avg(y),
    avg(coalesce(y, 0))
FROM t_null_big
```

```text
┌─────────────avg(y)─┬─avg(coalesce(y, 0))─┐
│ 2.3333333333333335 │                 1.4 │
└────────────────────┴─────────────────────┘
```

Além disso, você pode usar [Tuple](/pt-BR/sql-reference/data-types/tuple.md) para contornar o comportamento de ignorar `NULL`. Um `Tuple` que contém apenas um valor `NULL` não é `NULL`, portanto as funções de agregação não vão ignorar essa linha por causa desse valor `NULL`.

```sql
SELECT
    groupArray(y),
    groupArray(tuple(y)).1
FROM t_null_big;

┌─groupArray(y)─┬─tupleElement(groupArray(tuple(y)), 1)─┐
│ [2,2,3]       │ [2,NULL,2,3,NULL]                     │
└───────────────┴───────────────────────────────────────┘
```

Observe que as agregações são ignoradas quando as colunas são usadas como argumentos de uma função agregada.  Por exemplo, [`count`](../../sql-reference/aggregate-functions/reference/count.md) sem parâmetros (`count()`) ou com argumentos constantes (`count(1)`) contará todas as linhas no bloco (independentemente do valor da coluna usada no GROUP BY, já que ela não é um argumento), enquanto `count(column)` retornará apenas o número de linhas em que a coluna não é NULL.

```sql
SELECT
    v,
    count(1),
    count(v)
FROM
(
    SELECT if(number < 10, NULL, number % 3) AS v
    FROM numbers(15)
)
GROUP BY v

┌────v─┬─count()─┬─count(v)─┐
│ ᴺᵁᴸᴸ │      10 │        0 │
│    0 │       1 │        1 │
│    1 │       2 │        2 │
│    2 │       2 │        2 │
└──────┴─────────┴──────────┘
```

E aqui está um exemplo de first&#95;value com `RESPECT NULLS`, em que podemos ver que as entradas NULL são respeitadas e que ele retornará o primeiro valor lido, seja ele NULL ou não:

```sql
SELECT
    col || '_' || ((col + 1) * 5 - 1) AS range,
    first_value(odd_or_null) AS first,
    first_value(odd_or_null) IGNORE NULLS as first_ignore_null,
    first_value(odd_or_null) RESPECT NULLS as first_respect_nulls
FROM
(
    SELECT
        intDiv(number, 5) AS col,
        if(number % 2 == 0, NULL, number) AS odd_or_null
    FROM numbers(15)
)
GROUP BY col
ORDER BY col

┌─range─┬─first─┬─first_ignore_null─┬─first_respect_nulls─┐
│ 0_4   │     1 │                 1 │                ᴺᵁᴸᴸ │
│ 1_9   │     5 │                 5 │                   5 │
│ 2_14  │    11 │                11 │                ᴺᵁᴸᴸ │
└───────┴───────┴───────────────────┴─────────────────────┘
```