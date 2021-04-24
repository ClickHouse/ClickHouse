---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 51
toc_title: "Generaci\xF3n de n\xFAmeros pseudo-aleatorios"
---

# Funciones para generar números pseudoaleatorios {#functions-for-generating-pseudo-random-numbers}

Se utilizan generadores no criptográficos de números pseudoaleatorios.

Todas las funciones aceptan cero argumentos o un argumento.
Si se pasa un argumento, puede ser de cualquier tipo y su valor no se usa para nada.
El único propósito de este argumento es evitar la eliminación de subexpresiones comunes, de modo que dos instancias diferentes de la misma función devuelvan columnas diferentes con números aleatorios diferentes.

## rand {#rand}

Devuelve un número pseudoaleatorio UInt32, distribuido uniformemente entre todos los números de tipo UInt32.
Utiliza un generador congruente lineal.

## rand64 {#rand64}

Devuelve un número pseudoaleatorio UInt64, distribuido uniformemente entre todos los números de tipo UInt64.
Utiliza un generador congruente lineal.

## randConstant {#randconstant}

Produce una columna constante con un valor aleatorio.

**Sintaxis**

``` sql
randConstant([x])
```

**Parámetros**

-   `x` — [Expresion](../syntax.md#syntax-expressions) resultante en cualquiera de los [tipos de datos compatibles](../data-types/index.md#data_types). El valor resultante se descarta, pero la expresión en sí si se usa para omitir [Eliminación de subexpresiones común](index.md#common-subexpression-elimination) si la función se llama varias veces en una consulta. Parámetro opcional.

**Valor devuelto**

-   Número pseudoaleatorio.

Tipo: [UInt32](../data-types/int-uint.md).

**Ejemplo**

Consulta:

``` sql
SELECT rand(), rand(1), rand(number), randConstant(), randConstant(1), randConstant(number)
FROM numbers(3)
```

Resultado:

``` text
┌─────rand()─┬────rand(1)─┬─rand(number)─┬─randConstant()─┬─randConstant(1)─┬─randConstant(number)─┐
│ 3047369878 │ 4132449925 │   4044508545 │     2740811946 │      4229401477 │           1924032898 │
│ 2938880146 │ 1267722397 │   4154983056 │     2740811946 │      4229401477 │           1924032898 │
│  956619638 │ 4238287282 │   1104342490 │     2740811946 │      4229401477 │           1924032898 │
└────────────┴────────────┴──────────────┴────────────────┴─────────────────┴──────────────────────┘
```

[Artículo Original](https://clickhouse.tech/docs/en/query_language/functions/random_functions/) <!--hide-->
