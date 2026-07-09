---
description: 'Es un alias de any, pero se introdujo por compatibilidad con
  las funciones de ventana, donde a veces es necesario procesar valores `NULL` (de forma predeterminada
  todas las funciones de agregación de ClickHouse ignoran los valores NULL).'
slug: /sql-reference/aggregate-functions/reference/first_value
title: 'first_value'
doc_type: 'reference'
---

Es un alias de [`any`](../../../sql-reference/aggregate-functions/reference/any.md), pero se introdujo por compatibilidad con las [funciones de ventana](../../window-functions/index.md), donde a veces es necesario procesar valores `NULL` (de forma predeterminada, todas las funciones de agregación de ClickHouse ignoran los valores NULL).

Admite declarar un modificador para respetar los valores NULL (`RESPECT NULLS`), tanto en [funciones de ventana](../../window-functions/index.md) como en agregaciones normales.

Al igual que con `any`, sin funciones de ventana el resultado será aleatorio si el flujo de entrada no está ordenado y el tipo de retorno
coincide con el tipo de entrada (solo se devuelve NULL si la entrada es Nullable o si se añade el combinador -OrNull).

<div id="examples">
  ## ejemplos
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
  ### Ejemplo 1
</div>

De forma predeterminada, se ignora el valor NULL.

```sql
SELECT first_value(b) FROM test_data;
```

```text
┌─any(b)─┐
│      3 │
└────────┘
```

<div id="example2">
  ### Ejemplo 2
</div>

Se omite el valor NULL.

```sql
SELECT first_value(b) ignore nulls FROM test_data
```

```text
┌─any(b) IGNORE NULLS ─┐
│                    3 │
└──────────────────────┘
```

<div id="example3">
  ### Ejemplo 3
</div>

Se acepta el valor NULL.

```sql
SELECT first_value(b) respect nulls FROM test_data
```

```text
┌─any(b) RESPECT NULLS ─┐
│                  ᴺᵁᴸᴸ │
└───────────────────────┘
```

<div id="example4">
  ### Ejemplo 4
</div>

Resultado estabilizado mediante la subconsulta con `ORDER BY`.

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