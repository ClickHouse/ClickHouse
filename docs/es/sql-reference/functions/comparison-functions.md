---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: "Comparaci\xF3n"
---

# Funciones de comparación {#comparison-functions}

Las funciones de comparación siempre devuelven 0 o 1 (Uint8).

Se pueden comparar los siguientes tipos:

-   numero
-   cuerdas y cuerdas fijas
-   fechas
-   fechas con tiempos

dentro de cada grupo, pero no entre diferentes grupos.

Por ejemplo, no puede comparar una fecha con una cadena. Debe usar una función para convertir la cadena a una fecha, o viceversa.

Las cadenas se comparan por bytes. Una cadena más corta es más pequeña que todas las cadenas que comienzan con ella y que contienen al menos un carácter más.

## iguales, a = b y a == b operador {#function-equals}

## notEquals, un ! operador = b y un \<\> b {#function-notequals}

## menos, operador \<  {#function-less}

## Saludos {#function-greater}

## lessOrEquals, operador \<=  {#function-lessorequals}

## greaterOrEquals, operador \>=  {#function-greaterorequals}

[Artículo Original](https://clickhouse.tech/docs/en/query_language/functions/comparison_functions/) <!--hide-->
