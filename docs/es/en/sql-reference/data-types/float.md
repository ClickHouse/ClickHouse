---
description: 'Documentación sobre los tipos de datos de punto flotante en ClickHouse: Float32,
  Float64 y BFloat16'
sidebar_label: 'Float32 | Float64 | BFloat16'
sidebar_position: 4
slug: /sql-reference/data-types/float
title: 'Tipos Float32 | Float64 | BFloat16'
doc_type: 'reference'
---

:::note
Si necesita cálculos exactos, en particular si trabaja con datos financieros o empresariales que requieren alta precisión, debería considerar usar [Decimal](../data-types/decimal.md).

Los [números de punto flotante](https://en.wikipedia.org/wiki/IEEE_754) pueden producir resultados inexactos, como se ilustra a continuación:

```sql
CREATE TABLE IF NOT EXISTS float_vs_decimal
(
   my_float Float64,
   my_decimal Decimal64(3)
)
ENGINE=MergeTree
ORDER BY tuple();

# Generate 1 000 000 random numbers with 2 decimal places and store them as a float and as a decimal
INSERT INTO float_vs_decimal SELECT round(randCanonical(), 3) AS res, res FROM system.numbers LIMIT 1000000;
```

```sql
SELECT sum(my_float), sum(my_decimal) FROM float_vs_decimal;

┌──────sum(my_float)─┬─sum(my_decimal)─┐
│ 499693.60500000004 │      499693.605 │
└────────────────────┴─────────────────┘

SELECT sumKahan(my_float), sumKahan(my_decimal) FROM float_vs_decimal;

┌─sumKahan(my_float)─┬─sumKahan(my_decimal)─┐
│         499693.605 │           499693.605 │
└────────────────────┴──────────────────────┘
```

:::

A continuación se indican los tipos equivalentes en ClickHouse y en C:

* `Float32` — `float`.
* `Float64` — `double`.

Los tipos Float de ClickHouse tienen los siguientes alias:

* `Float32` — `FLOAT`, `REAL`, `SINGLE`.
* `Float64` — `DOUBLE`, `DOUBLE PRECISION`.

Al crear tablas, se pueden especificar parámetros numéricos para los números de coma flotante (p. ej., `FLOAT(12)`, `FLOAT(15, 22)`, `DOUBLE(12)`, `DOUBLE(4, 18)`), pero ClickHouse los ignora.

<div id="using-floating-point-numbers">
  ## Uso de números de coma flotante
</div>

* Los cálculos con números de coma flotante pueden producir errores de redondeo.

{/* */ }

```sql
SELECT 1 - 0.9

┌───────minus(1, 0.9)─┐
│ 0.09999999999999998 │
└─────────────────────┘
```

* El resultado del cálculo depende del método de cálculo (el tipo de procesador y la arquitectura del sistema).
* Los cálculos de coma flotante pueden dar como resultado valores como infinito (`Inf`) y &quot;no es un número&quot; (`NaN`). Esto debe tenerse en cuenta al procesar los resultados de los cálculos.
* Al interpretar números de coma flotante a partir de texto, es posible que el resultado no sea el número representable por la máquina más cercano.

<div id="nan-and-inf">
  ## NaN e Inf
</div>

A diferencia del SQL estándar, ClickHouse admite las siguientes categorías de números de coma flotante:

* `Inf` – infinito.

{/* */ }

```sql
SELECT 0.5 / 0

┌─divide(0.5, 0)─┐
│            inf │
└────────────────┘
```

* `-Inf` — infinito negativo.

{/* */ }

```sql
SELECT -0.5 / 0

┌─divide(-0.5, 0)─┐
│            -inf │
└─────────────────┘
```

* `NaN` — No es un número.

{/* */ }

```sql
SELECT 0 / 0

┌─divide(0, 0)─┐
│          nan │
└──────────────┘
```

Consulte las reglas de ordenación de `NaN` en la sección [cláusula ORDER BY](../../sql-reference/statements/select/order-by.md).

<div id="nan-values-in-set-semantics">
  ## Valores `NaN` en la semántica de conjuntos
</div>

El estándar IEEE 754 define `NaN` de modo que la comparación escalar `NaN = NaN` devuelve `false`.
ClickHouse sigue esa regla para el operador `=`.

Sin embargo, `NaN` no es un único valor; puede ser cualquier patrón de bits cuyo exponente sea todo unos y cuya
mantisa no sea cero. Distintas operaciones y distintas arquitecturas de CPU pueden producir valores `NaN`
con distintos bits de signo o distintas cargas útiles en la mantisa. Por ejemplo:

* `0./0.` produce un `NaN` cuyo bit de signo es 1 en la mayoría de las plataformas x86.
* El literal `nan` produce un `NaN` cuyo bit de signo es 0.
* Después de [PR #98230](https://github.com/ClickHouse/ClickHouse/pull/98230), la ruta NEON de AArch64 de
  `log` devuelve un `NaN` cuyo bit de signo difiere del `log` escalar de glibc en entradas negativas.

Las tablas hash de ClickHouse comparan las claves byte a byte, por lo que distintos patrones de bits de `NaN` se distribuyen en
distintos buckets y se tratan como valores distintos en operaciones con semántica de conjuntos, incluidas
`DISTINCT`, `GROUP BY`, `uniqExact`, `countDistinct` y equi-`JOIN` sobre una clave `Float`:

```sql
SELECT countDistinct(arrayJoin([0./0., nan, log(-1.)]));
-- May return 2 or 3 depending on architecture and build, even though all three inputs are NaN.
```

Esto es coherente con el estándar IEEE 754 (cada `NaN` es distinto de cualquier otro valor, incluido a sí mismo),
pero puede resultar sorprendente. Si necesita que las operaciones con semántica de conjuntos traten todos los valores `NaN` como iguales,
canonícelos en la consulta:

```sql
-- Replace every NaN with a single canonical NaN value
SELECT countDistinct(if(isNaN(x), CAST('nan' AS Float64), x))
FROM (SELECT arrayJoin([0./0., nan, log(-1.)]) AS x);
-- Returns 1.

-- Or exclude NaN values from the set entirely
SELECT countDistinct(if(isNaN(x), NULL, x))
FROM (SELECT arrayJoin([0./0., nan, log(-1.)]) AS x);
-- Returns 0.
```

El mismo enfoque funciona con las claves de `DISTINCT`, `GROUP BY` y `JOIN`.

<div id="bfloat16">
  ## BFloat16
</div>

`BFloat16` es un tipo de dato de coma flotante de 16 bits con exponente de 8 bits, signo y mantisa de 7 bits.
Es útil para aplicaciones de aprendizaje automático e IA.

ClickHouse admite conversiones entre `Float32` y `BFloat16`, que
pueden realizarse mediante las funciones [`toFloat32()`](../functions/type-conversion-functions.md/#toFloat32) o [`toBFloat16`](../functions/type-conversion-functions.md/#toBFloat16).

:::note
La mayoría de las demás operaciones no están admitidas.
:::