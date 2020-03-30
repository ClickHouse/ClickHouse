---
machine_translated: true
---

# Funciones de comparación {#comparison-functions}

Las funciones de comparación siempre devuelven 0 o 1 (Uint8).

Se pueden comparar los siguientes tipos:

-   número
-   cuerdas y cuerdas fijas
-   Fechas
-   fechas con tiempos

dentro de cada grupo, pero no entre diferentes grupos.

Por ejemplo, no puede comparar una fecha con una cadena. Debe usar una función para convertir la cadena a una fecha, o viceversa.

Las cadenas se comparan por bytes. Una cadena más corta es más pequeña que todas las cadenas que comienzan con ella y que contienen al menos un carácter más.

Nota. Hasta la versión 1.1.54134, los números firmados y sin firmar se comparaban de la misma manera que en C ++. En otras palabras, podría obtener un resultado incorrecto en casos como SELECT 9223372036854775807 \> -1 . Este comportamiento cambió en la versión 1.1.54134 y ahora es matemáticamente correcto.

## Por ejemplo: {#function-equals}

## notEquals, un ! operador = b y a `<>` b {#function-notequals}

## menos, `< operator` {#function-less}

## alcalde, `> operator` {#function-greater}

## lessOrEquals, `<= operator` {#function-lessorequals}

## mayorOrEquals, `>= operator` {#function-greaterorequals}

[Artículo Original](https://clickhouse.tech/docs/es/query_language/functions/comparison_functions/) <!--hide-->
