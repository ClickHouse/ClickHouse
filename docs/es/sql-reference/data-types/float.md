---
machine_translated: true
machine_translated_rev: 3e185d24c9fe772c7cf03d5475247fb829a21dfa
toc_priority: 41
toc_title: "Descripci\xF3n del producto"
---

# Descripción Del Producto {#float32-float64}

[Números de punto flotante](https://en.wikipedia.org/wiki/IEEE_754).

Los tipos son equivalentes a los tipos de C:

-   `Float32` - `float`
-   `Float64` - `double`

Le recomendamos que almacene los datos en formato entero siempre que sea posible. Por ejemplo, convierta números de precisión fija en valores enteros, como importes monetarios o tiempos de carga de página en milisegundos.

## Uso De números De Punto Flotante {#using-floating-point-numbers}

-   Los cálculos con números de punto flotante pueden producir un error de redondeo.

<!-- -->

``` sql
SELECT 1 - 0.9
```

``` text
┌───────minus(1, 0.9)─┐
│ 0.09999999999999998 │
└─────────────────────┘
```

-   El resultado del cálculo depende del método de cálculo (el tipo de procesador y la arquitectura del sistema informático).
-   Los cálculos de puntos flotantes pueden dar como resultado números como el infinito (`Inf`) y “not-a-number” (`NaN`). Esto debe tenerse en cuenta al procesar los resultados de los cálculos.
-   Al analizar números de punto flotante a partir de texto, el resultado puede no ser el número representable por máquina más cercano.

## NaN y Inf {#data_type-float-nan-inf}

A diferencia de SQL estándar, ClickHouse admite las siguientes categorías de números de punto flotante:

-   `Inf` – Infinity.

<!-- -->

``` sql
SELECT 0.5 / 0
```

``` text
┌─divide(0.5, 0)─┐
│            inf │
└────────────────┘
```

-   `-Inf` – Negative infinity.

<!-- -->

``` sql
SELECT -0.5 / 0
```

``` text
┌─divide(-0.5, 0)─┐
│            -inf │
└─────────────────┘
```

-   `NaN` – Not a number.

<!-- -->

``` sql
SELECT 0 / 0
```

``` text
┌─divide(0, 0)─┐
│          nan │
└──────────────┘
```

    See the rules for `NaN` sorting in the section [ORDER BY clause](../sql_reference/statements/select.md).

[Artículo Original](https://clickhouse.tech/docs/en/data_types/float/) <!--hide-->
