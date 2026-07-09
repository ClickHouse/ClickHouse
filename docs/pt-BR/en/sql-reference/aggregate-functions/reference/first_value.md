---
description: 'É um alias para `any`, mas foi introduzido para compatibilidade com
  Funções de Janela, nas quais às vezes é necessário processar valores `NULL` (por padrão
  todas as funções agregadas do ClickHouse ignoram valores `NULL`).'
slug: /sql-reference/aggregate-functions/reference/first_value
title: 'first_value'
doc_type: 'reference'
---

É um alias para [`any`](../../../sql-reference/aggregate-functions/reference/any.md), mas foi introduzido para compatibilidade com [Funções de Janela](../../window-functions/index.md), nas quais às vezes é necessário processar valores `NULL` (por padrão, todas as funções agregadas do ClickHouse ignoram valores `NULL`).

Ele oferece suporte à declaração de um modificador para considerar valores nulos (`RESPECT NULLS`), tanto em [Funções de Janela](../../window-functions/index.md) quanto em agregações normais.

Assim como em `any`, sem Funções de Janela o resultado será aleatório se o fluxo de origem não estiver ordenado e o tipo de retorno
corresponder ao tipo de entrada (`NULL` só é retornado se a entrada for Nullable ou se o combinador -OrNull for adicionado).

<div id="examples">
  ## exemplos
</div>

```sql
CREATE TABLE test_data
(
    a Int64,
    b Nullable(Int64)
)
ENGINE = Memory;

INSERT INTO test_data (a, b) VALUES (1,null), (2,3), (4, 5), (6,null);
```

<div id="example1">
  ### Exemplo 1
</div>

Por padrão, o valor NULL é ignorado.

```sql
SELECT first_value(b) FROM test_data;
```

```text
┌─any(b)─┐
│      3 │
└────────┘
```

<div id="example2">
  ### Exemplo 2
</div>

O valor NULL é ignorado.

```sql
SELECT first_value(b) ignore nulls FROM test_data
```

```text
┌─any(b) IGNORE NULLS ─┐
│                    3 │
└──────────────────────┘
```

<div id="example3">
  ### Exemplo 3
</div>

O valor NULL é aceito.

```sql
SELECT first_value(b) respect nulls FROM test_data
```

```text
┌─any(b) RESPECT NULLS ─┐
│                  ᴺᵁᴸᴸ │
└───────────────────────┘
```

<div id="example4">
  ### Exemplo 4
</div>

Resultado estabilizado com o uso da subconsulta com `ORDER BY`.

```sql
SELECT
    first_value_respect_nulls(b),
    first_value(b)
FROM
(
    SELECT *
    FROM test_data
    ORDER BY a ASC
)
```

```text
┌─any_respect_nulls(b)─┬─any(b)─┐
│                 ᴺᵁᴸᴸ │      3 │
└──────────────────────┴────────┘
```