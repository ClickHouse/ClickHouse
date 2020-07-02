---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 42
toc_title: Decimal
---

# Decimal(P, S), Decimal32(S), Decimal64(S), Decimal128(S) {#decimalp-s-decimal32s-decimal64s-decimal128s}

Números de punto fijo firmados que mantienen la precisión durante las operaciones de suma, resta y multiplicación. Para la división se descartan los dígitos menos significativos (no redondeados).

## Parámetros {#parameters}

-   P - precisión. Rango válido: \[ 1 : 38 \]. Determina cuántos dígitos decimales puede tener el número (incluida la fracción).
-   S - escala. Rango válido: \[ 0 : P \]. Determina cuántos dígitos decimales puede tener la fracción.

Dependiendo del valor del parámetro P Decimal(P, S) es un sinónimo de:
- P de \[ 1 : 9 \] - para Decimal32(S)
- P de \[ 10 : 18 \] - para Decimal64(S)
- P de \[ 19 : 38 \] - para Decimal128(S)

## Rangos de valores decimales {#decimal-value-ranges}

-   Decimal32(S) - ( -1 \* 10^(9 - S), 1 \* 10^(9 - S) )
-   Decimal64(S) - ( -1 \* 10^(18 - S), 1 \* 10^(18 - S) )
-   Decimal128(S) - ( -1 \* 10^(38 - S), 1 \* 10^(38 - S) )

Por ejemplo, Decimal32(4) puede contener números de -99999.9999 a 99999.9999 con el paso 0.0001.

## Representación interna {#internal-representation}

Internamente, los datos se representan como enteros con signo normal con el ancho de bits respectivo. Los rangos de valores reales que se pueden almacenar en la memoria son un poco más grandes que los especificados anteriormente, que se verifican solo en la conversión de una cadena.

Debido a que las CPU modernas no admiten enteros de 128 bits de forma nativa, las operaciones en Decimal128 se emulan. Debido a esto, Decimal128 funciona significativamente más lento que Decimal32 / Decimal64.

## Operaciones y tipo de resultado {#operations-and-result-type}

Las operaciones binarias en Decimal dan como resultado un tipo de resultado más amplio (con cualquier orden de argumentos).

-   `Decimal64(S1) <op> Decimal32(S2) -> Decimal64(S)`
-   `Decimal128(S1) <op> Decimal32(S2) -> Decimal128(S)`
-   `Decimal128(S1) <op> Decimal64(S2) -> Decimal128(S)`

Reglas para la escala:

-   Sumar, restar: S = max(S1, S2).
-   multuply: S = S1 + S2.
-   división: S = S1.

Para operaciones similares entre Decimal y enteros, el resultado es Decimal del mismo tamaño que un argumento.

Las operaciones entre Decimal y Float32 / Float64 no están definidas. Si los necesita, puede convertir explícitamente uno de los argumentos utilizando toDecimal32, toDecimal64, toDecimal128 o toFloat32, toFloat64 builtins. Tenga en cuenta que el resultado perderá precisión y la conversión de tipo es una operación computacionalmente costosa.

Algunas funciones en Decimal devuelven el resultado como Float64 (por ejemplo, var o stddev). Los cálculos intermedios aún se pueden realizar en Decimal, lo que podría dar lugar a resultados diferentes entre las entradas Float64 y Decimal con los mismos valores.

## Comprobaciones de desbordamiento {#overflow-checks}

Durante los cálculos en Decimal, pueden producirse desbordamientos de enteros. Los dígitos excesivos en una fracción se descartan (no se redondean). Los dígitos excesivos en la parte entera conducirán a una excepción.

``` sql
SELECT toDecimal32(2, 4) AS x, x / 3
```

``` text
┌──────x─┬─divide(toDecimal32(2, 4), 3)─┐
│ 2.0000 │                       0.6666 │
└────────┴──────────────────────────────┘
```

``` sql
SELECT toDecimal32(4.2, 8) AS x, x * x
```

``` text
DB::Exception: Scale is out of bounds.
```

``` sql
SELECT toDecimal32(4.2, 8) AS x, 6 * x
```

``` text
DB::Exception: Decimal math overflow.
```

Las comprobaciones de desbordamiento conducen a la desaceleración de las operaciones. Si se sabe que los desbordamientos no son posibles, tiene sentido deshabilitar las verificaciones usando `decimal_check_overflow` configuración. Cuando las comprobaciones están deshabilitadas y se produce el desbordamiento, el resultado será incorrecto:

``` sql
SET decimal_check_overflow = 0;
SELECT toDecimal32(4.2, 8) AS x, 6 * x
```

``` text
┌──────────x─┬─multiply(6, toDecimal32(4.2, 8))─┐
│ 4.20000000 │                     -17.74967296 │
└────────────┴──────────────────────────────────┘
```

Las comprobaciones de desbordamiento ocurren no solo en operaciones aritméticas sino también en la comparación de valores:

``` sql
SELECT toDecimal32(1, 8) < 100
```

``` text
DB::Exception: Can't compare.
```

[Artículo Original](https://clickhouse.tech/docs/en/data_types/decimal/) <!--hide-->
