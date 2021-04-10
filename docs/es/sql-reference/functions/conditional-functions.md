---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 43
toc_title: 'Condicional '
---

# Funciones condicionales {#conditional-functions}

## si {#if}

Controla la bifurcación condicional. A diferencia de la mayoría de los sistemas, ClickHouse siempre evalúa ambas expresiones `then` y `else`.

**Sintaxis**

``` sql
SELECT if(cond, then, else)
```

Si la condición `cond` evalúa a un valor distinto de cero, devuelve el resultado de la expresión `then` y el resultado de la expresión `else` si está presente, se omite. Si el `cond` es cero o `NULL` el resultado de la `then` expresión se omite y el resultado de la `else` expresión, si está presente, se devuelve.

**Parámetros**

-   `cond` – The condition for evaluation that can be zero or not. The type is UInt8, Nullable(UInt8) or NULL.
-   `then` - La expresión que se va a devolver si se cumple la condición.
-   `else` - La expresión a devolver si no se cumple la condición.-

**Valores devueltos**

La función se ejecuta `then` y `else` expresiones y devuelve su resultado, dependiendo de si la condición `cond` terminó siendo cero o no.

**Ejemplo**

Consulta:

``` sql
SELECT if(1, plus(2, 2), plus(2, 6))
```

Resultado:

``` text
┌─plus(2, 2)─┐
│          4 │
└────────────┘
```

Consulta:

``` sql
SELECT if(0, plus(2, 2), plus(2, 6))
```

Resultado:

``` text
┌─plus(2, 6)─┐
│          8 │
└────────────┘
```

-   `then` y `else` debe tener el tipo común más bajo.

**Ejemplo:**

Toma esto `LEFT_RIGHT` tabla:

``` sql
SELECT *
FROM LEFT_RIGHT

┌─left─┬─right─┐
│ ᴺᵁᴸᴸ │     4 │
│    1 │     3 │
│    2 │     2 │
│    3 │     1 │
│    4 │  ᴺᵁᴸᴸ │
└──────┴───────┘
```

La siguiente consulta compara `left` y `right` valor:

``` sql
SELECT
    left,
    right,
    if(left < right, 'left is smaller than right', 'right is greater or equal than left') AS is_smaller
FROM LEFT_RIGHT
WHERE isNotNull(left) AND isNotNull(right)

┌─left─┬─right─┬─is_smaller──────────────────────────┐
│    1 │     3 │ left is smaller than right          │
│    2 │     2 │ right is greater or equal than left │
│    3 │     1 │ right is greater or equal than left │
└──────┴───────┴─────────────────────────────────────┘
```

Nota: `NULL` los valores no se utilizan en este ejemplo, compruebe [Valores NULL en condicionales](#null-values-in-conditionals) apartado.

## Operador ternario {#ternary-operator}

Funciona igual que `if` función.

Sintaxis: `cond ? then : else`

Devoluciones `then` si el `cond` evalúa que es verdadero (mayor que cero), de lo contrario devuelve `else`.

-   `cond` debe ser de tipo de `UInt8`, y `then` y `else` debe tener el tipo común más bajo.

-   `then` y `else` puede ser `NULL`

**Ver también**

-   [ifNotFinite](other-functions.md#ifnotfinite).

## MultiIf {#multiif}

Le permite escribir el [CASE](../operators/index.md#operator_case) operador más compacto en la consulta.

Sintaxis: `multiIf(cond_1, then_1, cond_2, then_2, ..., else)`

**Parámetros:**

-   `cond_N` — The condition for the function to return `then_N`.
-   `then_N` — The result of the function when executed.
-   `else` — The result of the function if none of the conditions is met.

La función acepta `2N+1` parámetros.

**Valores devueltos**

La función devuelve uno de los valores `then_N` o `else` dependiendo de las condiciones `cond_N`.

**Ejemplo**

De nuevo usando `LEFT_RIGHT` tabla.

``` sql
SELECT
    left,
    right,
    multiIf(left < right, 'left is smaller', left > right, 'left is greater', left = right, 'Both equal', 'Null value') AS result
FROM LEFT_RIGHT

┌─left─┬─right─┬─result──────────┐
│ ᴺᵁᴸᴸ │     4 │ Null value      │
│    1 │     3 │ left is smaller │
│    2 │     2 │ Both equal      │
│    3 │     1 │ left is greater │
│    4 │  ᴺᵁᴸᴸ │ Null value      │
└──────┴───────┴─────────────────┘
```

## Uso directo de resultados condicionales {#using-conditional-results-directly}

Los condicionales siempre dan como resultado `0`, `1` o `NULL`. Entonces puedes usar resultados condicionales directamente como este:

``` sql
SELECT left < right AS is_small
FROM LEFT_RIGHT

┌─is_small─┐
│     ᴺᵁᴸᴸ │
│        1 │
│        0 │
│        0 │
│     ᴺᵁᴸᴸ │
└──────────┘
```

## Valores NULL en condicionales {#null-values-in-conditionals}

Cuando `NULL` están involucrados en condicionales, el resultado también será `NULL`.

``` sql
SELECT
    NULL < 1,
    2 < NULL,
    NULL < NULL,
    NULL = NULL

┌─less(NULL, 1)─┬─less(2, NULL)─┬─less(NULL, NULL)─┬─equals(NULL, NULL)─┐
│ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ             │ ᴺᵁᴸᴸ               │
└───────────────┴───────────────┴──────────────────┴────────────────────┘
```

Por lo tanto, debe construir sus consultas cuidadosamente si los tipos son `Nullable`.

El siguiente ejemplo demuestra esto al no agregar la condición equals a `multiIf`.

``` sql
SELECT
    left,
    right,
    multiIf(left < right, 'left is smaller', left > right, 'right is smaller', 'Both equal') AS faulty_result
FROM LEFT_RIGHT

┌─left─┬─right─┬─faulty_result────┐
│ ᴺᵁᴸᴸ │     4 │ Both equal       │
│    1 │     3 │ left is smaller  │
│    2 │     2 │ Both equal       │
│    3 │     1 │ right is smaller │
│    4 │  ᴺᵁᴸᴸ │ Both equal       │
└──────┴───────┴──────────────────┘
```

[Artículo Original](https://clickhouse.tech/docs/en/query_language/functions/conditional_functions/) <!--hide-->
