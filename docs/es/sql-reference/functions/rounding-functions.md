---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 45
toc_title: Redondeo
---

# Funciones de redondeo {#rounding-functions}

## piso(x\[, N\]) {#floorx-n}

Devuelve el número de ronda más grande que es menor o igual que `x`. Un número redondo es un múltiplo de 1 / 10N, o el número más cercano del tipo de datos apropiado si 1 / 10N no es exacto.
‘N’ es una constante entera, parámetro opcional. Por defecto es cero, lo que significa redondear a un entero.
‘N’ puede ser negativo.

Ejemplos: `floor(123.45, 1) = 123.4, floor(123.45, -1) = 120.`

`x` es cualquier tipo numérico. El resultado es un número del mismo tipo.
Para argumentos enteros, tiene sentido redondear con un negativo `N` valor no negativo `N` la función no hace nada).
Si el redondeo causa desbordamiento (por ejemplo, floor(-128, -1)), se devuelve un resultado específico de la implementación.

## Por ejemplo:\]) {#ceilx-n-ceilingx-n}

Devuelve el número redondo más pequeño que es mayor o igual que `x`. En todos los demás sentidos, es lo mismo que el `floor` función (véase más arriba).

## ¿Cómo puedo hacerlo?\]) {#truncx-n-truncatex-n}

Devuelve el número redondo con el valor absoluto más grande que tiene un valor absoluto menor o igual que `x`‘s. In every other way, it is the same as the ’floor’ función (véase más arriba).

## Ronda (x\[, N\]) {#rounding_functions-round}

Redondea un valor a un número especificado de decimales.

La función devuelve el número más cercano del orden especificado. En caso de que el número dado tenga la misma distancia que los números circundantes, la función utiliza el redondeo del banquero para los tipos de números flotantes y se redondea desde cero para los otros tipos de números.

``` sql
round(expression [, decimal_places])
```

**Parámetros:**

-   `expression` — A number to be rounded. Can be any [expresion](../syntax.md#syntax-expressions) devolviendo el numérico [tipo de datos](../../sql-reference/data-types/index.md#data_types).
-   `decimal-places` — An integer value.
    -   Si `decimal-places > 0` luego la función redondea el valor a la derecha del punto decimal.
    -   Si `decimal-places < 0` luego la función redondea el valor a la izquierda del punto decimal.
    -   Si `decimal-places = 0` entonces la función redondea el valor a entero. En este caso, el argumento puede omitirse.

**Valor devuelto:**

El número redondeado del mismo tipo que el número de entrada.

### Ejemplos {#examples}

**Ejemplo de uso**

``` sql
SELECT number / 2 AS x, round(x) FROM system.numbers LIMIT 3
```

``` text
┌───x─┬─round(divide(number, 2))─┐
│   0 │                        0 │
│ 0.5 │                        0 │
│   1 │                        1 │
└─────┴──────────────────────────┘
```

**Ejemplos de redondeo**

Redondeando al número más cercano.

``` text
round(3.2, 0) = 3
round(4.1267, 2) = 4.13
round(22,-1) = 20
round(467,-2) = 500
round(-467,-2) = -500
```

Redondeo del banquero.

``` text
round(3.5) = 4
round(4.5) = 4
round(3.55, 1) = 3.6
round(3.65, 1) = 3.6
```

**Ver también**

-   [roundBankers](#roundbankers)

## roundBankers {#roundbankers}

Redondea un número a una posición decimal especificada.

-   Si el número de redondeo está a medio camino entre dos números, la función utiliza el redondeo del banquero.

        Banker's rounding is a method of rounding fractional numbers. When the rounding number is halfway between two numbers, it's rounded to the nearest even digit at the specified decimal position. For example: 3.5 rounds up to 4, 2.5 rounds down to 2.

        It's the default rounding method for floating point numbers defined in [IEEE 754](https://en.wikipedia.org/wiki/IEEE_754#Roundings_to_nearest). The [round](#rounding_functions-round) function performs the same rounding for floating point numbers. The `roundBankers` function also rounds integers the same way, for example, `roundBankers(45, -1) = 40`.

-   En otros casos, la función redondea los números al entero más cercano.

Usando el redondeo del banquero, puede reducir el efecto que tiene el redondeo de números en los resultados de sumar o restar estos números.

Por ejemplo, suma números 1.5, 2.5, 3.5, 4.5 con redondeo diferente:

-   Sin redondeo: 1.5 + 2.5 + 3.5 + 4.5 = 12.
-   Redondeo del banquero: 2 + 2 + 4 + 4 = 12.
-   Redondeando al entero más cercano: 2 + 3 + 4 + 5 = 14.

**Sintaxis**

``` sql
roundBankers(expression [, decimal_places])
```

**Parámetros**

-   `expression` — A number to be rounded. Can be any [expresion](../syntax.md#syntax-expressions) devolviendo el numérico [tipo de datos](../../sql-reference/data-types/index.md#data_types).
-   `decimal-places` — Decimal places. An integer number.
    -   `decimal-places > 0` — The function rounds the number to the given position right of the decimal point. Example: `roundBankers(3.55, 1) = 3.6`.
    -   `decimal-places < 0` — The function rounds the number to the given position left of the decimal point. Example: `roundBankers(24.55, -1) = 20`.
    -   `decimal-places = 0` — The function rounds the number to an integer. In this case the argument can be omitted. Example: `roundBankers(2.5) = 2`.

**Valor devuelto**

Un valor redondeado por el método de redondeo del banquero.

### Ejemplos {#examples-1}

**Ejemplo de uso**

Consulta:

``` sql
 SELECT number / 2 AS x, roundBankers(x, 0) AS b fROM system.numbers limit 10
```

Resultado:

``` text
┌───x─┬─b─┐
│   0 │ 0 │
│ 0.5 │ 0 │
│   1 │ 1 │
│ 1.5 │ 2 │
│   2 │ 2 │
│ 2.5 │ 2 │
│   3 │ 3 │
│ 3.5 │ 4 │
│   4 │ 4 │
│ 4.5 │ 4 │
└─────┴───┘
```

**Ejemplos de redondeo de Banker**

``` text
roundBankers(0.4) = 0
roundBankers(-3.5) = -4
roundBankers(4.5) = 4
roundBankers(3.55, 1) = 3.6
roundBankers(3.65, 1) = 3.6
roundBankers(10.35, 1) = 10.4
roundBankers(10.755, 2) = 11,76
```

**Ver también**

-   [ronda](#rounding_functions-round)

## ¿Cómo puedo hacerlo?) {#roundtoexp2num}

Acepta un número. Si el número es menor que uno, devuelve 0. De lo contrario, redondea el número al grado más cercano (todo no negativo) de dos.

## RondaDuración(num) {#rounddurationnum}

Acepta un número. Si el número es menor que uno, devuelve 0. De lo contrario, redondea el número a números del conjunto: 1, 10, 30, 60, 120, 180, 240, 300, 600, 1200, 1800, 3600, 7200, 18000, 36000. Esta función es específica de Yandex.Métrica y se utiliza para aplicar el informe sobre la duración del período de sesiones.

## RondaEdad(num) {#roundagenum}

Acepta un número. Si el número es menor que 18, devuelve 0. De lo contrario, redondea el número a un número del conjunto: 18, 25, 35, 45, 55. Esta función es específica de Yandex.Métrica y se utiliza para la aplicación del informe sobre la edad del usuario.

## ¿Cómo puedo hacerlo?) {#rounddownnum-arr}

Acepta un número y lo redondea a un elemento en la matriz especificada. Si el valor es menor que el límite más bajo, se devuelve el límite más bajo.

[Artículo Original](https://clickhouse.tech/docs/en/query_language/functions/rounding_functions/) <!--hide-->
