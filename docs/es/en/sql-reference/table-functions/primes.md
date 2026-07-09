---
slug: /sql-reference/table-functions/primes
sidebar_position: 145
sidebar_label: 'primes'
title: 'primes'
description: 'Devuelve una tabla con una sola columna `prime` que contiene números primos.'
doc_type: 'reference'
---

* `primes()` – Devuelve una tabla infinita con una sola columna `prime` (UInt64) que contiene números primos en orden ascendente, a partir de 2. Utilice `LIMIT` (y, opcionalmente, `OFFSET`) para limitar el número de filas.

* `primes(N)` – Devuelve una tabla con una sola columna `prime` (UInt64) que contiene los primeros `N` números primos, a partir de 2.

* `primes(N, M)` – Devuelve una tabla con una sola columna `prime` (UInt64) que contiene `M` números primos a partir del `N`-ésimo número primo (con índice basado en 0).

* `primes(N, M, S)` – Devuelve una tabla con una sola columna `prime` (UInt64) que contiene `M` números primos a partir del `N`-ésimo número primo (con índice basado en 0), con paso `S` según el índice primo. Los primos devueltos corresponden a los índices `N, N + S, N + 2S, ..., N + (M - 1)S`. `S` debe ser `>= 1`.

Esto es similar a la tabla del sistema [`system.primes`](/es/operations/system-tables/primes).

Las siguientes consultas son equivalentes:

```sql
SELECT * FROM primes(10);
SELECT * FROM primes(0, 10);
SELECT * FROM primes() LIMIT 10;
SELECT * FROM system.primes LIMIT 10;
SELECT * FROM system.primes WHERE prime IN (2, 3, 5, 7, 11, 13, 17, 19, 23, 29);
```

Las siguientes consultas también son equivalentes:

```sql
SELECT * FROM primes(10, 10);
SELECT * FROM primes() LIMIT 10 OFFSET 10;
SELECT * FROM system.primes LIMIT 10 OFFSET 10;
```

<div id="examples">
  ### Ejemplos
</div>

Los primeros 10 números primos.

```sql
SELECT * FROM primes(10);
```

```response
  ┌─prime─┐
  │     2 │
  │     3 │
  │     5 │
  │     7 │
  │    11 │
  │    13 │
  │    17 │
  │    19 │
  │    23 │
  │    29 │
  └───────┘
```

El primer número primo mayor que 1e15.

```sql
SELECT prime FROM primes() WHERE prime > 1e15 LIMIT 1;
```

```response
  ┌────────────prime─┐
  │ 1000000000000037 │ -- 1.00 quadrillion
  └──────────────────┘
```

Resuelva una restricción modular sobre números primos en un intervalo muy grande: encuentre el primer número primo `p >= 10^15` tal que `p` modulo `65537` equals `1`.

```sql
SELECT prime
FROM primes()
WHERE prime >= 1e15
  AND prime % 65537 = 1
LIMIT 1;
```

```response
 ┌────────────prime─┐
 │ 1000000001218399 │ -- 1.00 quadrillion
 └──────────────────┘
```

Los primeros 7 números primos de Mersenne.

```sql
SELECT prime
FROM primes()
WHERE bitAnd(prime, prime + 1) = 0
LIMIT 7;
```

```response
  ┌──prime─┐
  │      3 │
  │      7 │
  │     31 │
  │    127 │
  │   8191 │
  │ 131071 │
  │ 524287 │
  └────────┘
```

<div id="notes">
  ### Notas
</div>

* Las variantes más rápidas son las consultas de rango simple y las consultas con filtro por punto que usan el paso predeterminado (`1`), por ejemplo, `primes(N)` o `primes() LIMIT N`. Estas variantes usan un generador de números primos optimizado para calcular números primos muy grandes de forma eficiente.
* Para fuentes no acotadas (`primes()` / `system.primes`), se pueden aplicar durante la generación filtros de valor simples como `prime BETWEEN ...`, `prime IN (...)` o `prime = ...` para restringir los rangos de valores buscados. Por ejemplo, la siguiente consulta se ejecuta casi al instante:

```sql
SELECT sum(prime)
FROM primes()
WHERE prime BETWEEN 1e6 AND 1e6 + 100
   OR prime BETWEEN 1e12 AND 1e12 + 100
   OR prime BETWEEN 1e15 AND 1e15 + 100
   OR prime IN (9999999967, 9999999971, 9999999973)
   OR prime = 1000000000000037;
```

```response
  ┌───────sum(prime)─┐
  │ 2004010006000641 │ -- 2.00 quadrillion
  └──────────────────┘

1 row in set. Elapsed: 0.090 sec. 
```

* Esta optimización del rango de valores no se aplica a las funciones de tabla acotadas (`primes(N)`, `primes(offset, count[, step])`) con `WHERE`, porque esas variantes definen una tabla finita por índice de primos, y el filtro debe evaluarse después de generar esa tabla para preservar la semántica.
* Usar un offset distinto de cero y/o un step mayor que 1 (`primes(offset, count)` / `primes(offset, count, step)`) puede ser más lento, porque internamente puede ser necesario generar y omitir primos adicionales. Si no necesita offset ni step, omítalos.