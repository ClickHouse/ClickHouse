---
description: 'Documentación de funciones de agregación paramétricas'
sidebar_label: 'Paramétricas'
sidebar_position: 38
slug: /sql-reference/aggregate-functions/parametric-functions
title: 'Funciones de agregación paramétricas'
doc_type: 'reference'
---

Algunas funciones de agregación pueden aceptar no solo columnas de argumentos (utilizadas para la compresión), sino también un conjunto de parámetros: constantes de inicialización. La sintaxis utiliza dos pares de paréntesis en lugar de uno. El primero es para los parámetros y el segundo, para los argumentos.

<div id="histogram">
  ## histogram
</div>

Calcula un histograma adaptativo. No garantiza resultados precisos.

```sql
histogram(number_of_bins)(values)
```

La función usa [A Streaming Parallel Decision Tree Algorithm](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf). Los límites de los intervalos del histograma se ajustan a medida que la función recibe nuevos datos. En la mayoría de los casos, los anchos de los intervalos no son iguales.

**Argumentos**

`values` — [Expresión](/es/sql-reference/syntax#expressions) que da como resultado valores de entrada.

**Parámetros**

`number_of_bins` — Límite superior del número de intervalos en el histograma. La función calcula automáticamente el número de intervalos. Intenta alcanzar el número de intervalos especificado, pero si no lo consigue, usa menos intervalos.

**Valores devueltos**

* [Array](../../sql-reference/data-types/array.md) de [Tuples](../../sql-reference/data-types/tuple.md) con el siguiente formato:

  ```
  [(lower_1, upper_1, height_1), ... (lower_N, upper_N, height_N)]
  ```

  * `lower` — Límite inferior del intervalo.
  * `upper` — Límite superior del intervalo.
  * `height` — Altura calculada del intervalo.

**Ejemplo**

```sql
SELECT histogram(5)(number + 1)
FROM (
    SELECT *
    FROM system.numbers
    LIMIT 20
)
```

```text
┌─histogram(5)(plus(number, 1))───────────────────────────────────────────┐
│ [(1,4.5,4),(4.5,8.5,4),(8.5,12.75,4.125),(12.75,17,4.625),(17,20,3.25)] │
└─────────────────────────────────────────────────────────────────────────┘
```

Puedes visualizar un histograma con la función [bar](/es/sql-reference/functions/other-functions#bar), por ejemplo:

```sql
WITH histogram(5)(rand() % 100) AS hist
SELECT
    arrayJoin(hist).3 AS height,
    bar(height, 0, 6, 5) AS bar
FROM
(
    SELECT *
    FROM system.numbers
    LIMIT 20
)
```

```text
┌─height─┬─bar───┐
│  2.125 │ █▋    │
│   3.25 │ ██▌   │
│  5.625 │ ████▏ │
│  5.625 │ ████▏ │
│  3.375 │ ██▌   │
└────────┴───────┘
```

En este caso, debe recordar que desconoce los límites de los intervalos del histograma.

<div id="sequencematch">
  ## sequenceMatch
</div>

Comprueba si la secuencia contiene una cadena de eventos que coincida con el patrón.

**Sintaxis**

```sql
sequenceMatch(pattern)(timestamp, cond1, cond2, ...)
```

:::note
Los eventos que ocurren en el mismo segundo pueden quedar en la secuencia en un orden indefinido, lo que puede afectar al resultado.
:::

**Argumentos**

* `timestamp` — Columna que se considera que contiene datos de tiempo. Los tipos de datos habituales son `Date` y `DateTime`. También puede usar cualquiera de los tipos de datos [UInt](../../sql-reference/data-types/int-uint.md) compatibles.

* `cond1`, `cond2` — Condiciones que describen la cadena de eventos. Tipo de dato: `UInt8`. Puede pasar hasta 32 argumentos de condición. La función solo tiene en cuenta los eventos descritos en estas condiciones. Si la secuencia contiene datos que no estén descritos en una condición, la función los omite.

**Parámetros**

* `pattern` — Cadena de patrón. Consulte [Sintaxis del patrón](#pattern-syntax).

**Valores devueltos**

* 1, si el patrón coincide.
* 0, si el patrón no coincide.

Tipo: `UInt8`.

<div id="pattern-syntax">
  #### Sintaxis del patrón
</div>

* `(?N)` — Coincide con el argumento de condición en la posición `N`. Las condiciones se numeran en el intervalo `[1, 32]`. Por ejemplo, `(?1)` coincide con el argumento pasado al parámetro `cond1`.

* `.*` — Coincide con cualquier número de eventos. No necesita argumentos condicionales para que coincida con este elemento del patrón.

* `(?t operator value)` — Establece el tiempo en segundos que debe separar dos eventos. Por ejemplo, el patrón `(?1)(?t>1800)(?2)` coincide con eventos que ocurren con más de 1800 segundos de diferencia entre sí. Puede haber una cantidad arbitraria de eventos de cualquier tipo entre ellos. Puede usar los operadores `>=`, `>`, `<`, `<=`, `==`.

**Ejemplos**

Considere los datos de la tabla `t`:

```text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
└──────┴────────┘
```

Ejecuta la consulta:

```sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2) FROM t
```

```text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                     1 │
└───────────────────────────────────────────────────────────────────────┘
```

La función encontró la cadena de eventos en la que el número 2 sigue al número 1. Omitió el número 3 entre ambos, ya que ese número no está definido como un evento. Si queremos tener en cuenta este número al buscar la cadena de eventos indicada en el ejemplo, debemos definir una condición para él.

```sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 3) FROM t
```

```text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 3))─┐
│                                                                                        0 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

En este caso, la función no pudo encontrar la cadena de eventos que coincidía con el patrón, porque el evento correspondiente al número 3 ocurrió entre 1 y 2. Si en ese mismo caso comprobáramos la condición para el número 4, la secuencia coincidiría con el patrón.

```sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 4) FROM t
```

```text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 4))─┐
│                                                                                        1 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

**Véase también**

* [sequenceCount](#sequencecount)

<div id="sequencecount">
  ## sequenceCount
</div>

Cuenta el número de cadenas de eventos que coinciden con el patrón. La función busca cadenas de eventos que no se solapan. Empieza a buscar la siguiente cadena después de encontrar la cadena actual.

:::note
Los eventos que ocurren en el mismo segundo pueden quedar en la secuencia en un orden indefinido, lo que afecta al resultado.
:::

**Sintaxis**

```sql
sequenceCount(pattern)(timestamp, cond1, cond2, ...)
```

**Argumentos**

* `timestamp` — Columna que se considera que contiene datos temporales. Los tipos de datos típicos son `Date` y `DateTime`. También puede usar cualquiera de los tipos de datos [UInt](../../sql-reference/data-types/int-uint.md) compatibles.

* `cond1`, `cond2` — Condiciones que describen la cadena de eventos. Tipo de dato: `UInt8`. Puede pasar hasta 32 argumentos de condición. La función solo tiene en cuenta los eventos descritos por estas condiciones. Si la secuencia contiene datos que no están descritos en ninguna condición, la función los omite.

**Parámetros**

* `pattern` — Cadena del patrón. Consulte [Sintaxis del patrón](#pattern-syntax).

**Valores devueltos**

* Número de cadenas de eventos no superpuestas que coinciden.

Tipo: `UInt64`.

**Ejemplo**

Considere los datos de la tabla `t`:

```text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
│    4 │      1 │
│    5 │      3 │
│    6 │      2 │
└──────┴────────┘
```

Cuenta cuántas veces aparece el número 2 después del número 1, con cualquier cantidad de otros números entre ambos:

```sql
SELECT sequenceCount('(?1).*(?2)')(time, number = 1, number = 2) FROM t
```

```text
┌─sequenceCount('(?1).*(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                       2 │
└─────────────────────────────────────────────────────────────────────────┘
```

<div id="sequencematchevents">
  ## sequenceMatchEvents
</div>

Devuelve las marcas de tiempo de los eventos correspondientes a las cadenas de eventos más largas que coinciden con el patrón.

:::note
Los eventos que ocurren en el mismo segundo pueden aparecer en la secuencia en un orden indefinido, lo que afecta al resultado.
:::

**Sintaxis**

```sql
sequenceMatchEvents(pattern)(timestamp, cond1, cond2, ...)
```

**Argumentos**

* `timestamp` — Columna que se considera que contiene datos temporales. Los tipos de datos típicos son `Date` y `DateTime`. También puede usar cualquiera de los tipos de datos [UInt](../../sql-reference/data-types/int-uint.md) compatibles.

* `cond1`, `cond2` — Condiciones que describen la cadena de eventos. Tipo de dato: `UInt8`. Puede pasar hasta 32 argumentos de condición. La función solo tiene en cuenta los eventos descritos en estas condiciones. Si la secuencia contiene datos que no se describen en ninguna condición, la función los omite.

**Parámetros**

* `pattern` — Cadena del patrón. Consulte [Sintaxis del patrón](#pattern-syntax).

**Valores devueltos**

* Array de marcas de tiempo para los argumentos de condición coincidentes (?N) de la cadena de eventos. La posición en el array coincide con la posición del argumento de condición en el patrón.

Tipo: Array.

**Ejemplo**

Considere los datos de la tabla `t`:

```text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
│    4 │      1 │
│    5 │      3 │
│    6 │      2 │
└──────┴────────┘
```

Devuelve las marcas de tiempo de los eventos de la secuencia más larga

```sql
SELECT sequenceMatchEvents('(?1).*(?2).*(?1)(?3)')(time, number = 1, number = 2, number = 4) FROM t
```

```text
┌─sequenceMatchEvents('(?1).*(?2).*(?1)(?3)')(time, equals(number, 1), equals(number, 2), equals(number, 4))─┐
│ [1,3,4]                                                                                                    │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Véase también**

* [sequenceMatch](#sequencematch)

<div id="windowfunnel">
  ## windowFunnel
</div>

Busca cadenas de eventos en una ventana de tiempo deslizante y calcula el número máximo de eventos ocurridos en la cadena.

La función funciona según el siguiente algoritmo:

* La función busca datos que activen la primera condición de la cadena y establece el contador de eventos en 1. Este es el momento en que comienza la ventana deslizante.

* Si los eventos de la cadena ocurren secuencialmente dentro de la ventana, el contador se incrementa. Si la secuencia de eventos se interrumpe, el contador no se incrementa.

* Si los datos contienen múltiples cadenas de eventos en distintos puntos de avance, la función solo devuelve la longitud de la cadena más larga.

**Sintaxis**

```sql
windowFunnel(window, [mode, [mode, ... ]])(timestamp, cond1, cond2, ..., condN)
```

**Argumentos**

* `timestamp` — Nombre de la columna que contiene el timestamp. Tipos de datos admitidos: [Date](../../sql-reference/data-types/date.md), [DateTime](/es/sql-reference/data-types/datetime) y otros tipos de enteros sin signo (ten en cuenta que, aunque `timestamp` admite el tipo `UInt64`, su valor no puede superar el máximo de Int64, que es 2^63 - 1).
* `cond` — Condiciones o datos que describen la cadena de eventos. [UInt8](../../sql-reference/data-types/int-uint.md).

**Parámetros**

* `window` — Longitud de la ventana deslizante; es el intervalo de tiempo entre la primera y la última condición. La unidad de `window` depende del propio `timestamp` y puede variar. Se determina mediante la expresión `timestamp of cond1 <= timestamp of cond2 <= ... <= timestamp of condN <= timestamp of cond1 + window`.
* `mode` — Es un argumento opcional. Se pueden establecer uno o varios modos.
  * `'strict_deduplication'` — Si la misma condición se cumple en la secuencia de eventos, ese evento repetido interrumpe el procesamiento posterior. Nota: puede funcionar de forma inesperada si varias condiciones se cumplen para el mismo evento.
  * `'strict_order'` — No permite la intercalación de otros eventos. Por ejemplo, en el caso de `A->B->D->C`, deja de encontrar `A->B->C` al llegar a `D` y el nivel máximo de evento es 2.
  * `'strict_increase'` — Aplica las condiciones solo a eventos con timestamps estrictamente crecientes.
  * `'strict_once'` — Cuenta cada evento solo una vez en la cadena, aunque cumpla la condición varias veces.
  * `'allow_reentry'` — Ignora los eventos que violan el orden estricto. Por ejemplo, en el caso de A-&gt;A-&gt;B-&gt;C, encuentra A-&gt;B-&gt;C al ignorar la A redundante y el nivel máximo de evento es 3.

**Valor devuelto**

El número máximo de condiciones consecutivas activadas de la cadena dentro de la ventana de tiempo deslizante.
Se analizan todas las cadenas de la selección.

Tipo: `Integer`.

**Ejemplo**

Determina si un período de tiempo determinado es suficiente para que el usuario seleccione un teléfono y lo compre dos veces en la tienda online.

Define la siguiente cadena de eventos:

1. El usuario inició sesión en su cuenta de la tienda (`eventID = 1003`).
2. El usuario busca un teléfono (`eventID = 1007, product = 'phone'`).
3. El usuario hizo un pedido (`eventID = 1009`).
4. El usuario volvió a hacer el pedido (`eventID = 1010`).

Tabla de entrada:

```text
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-28 │       1 │ 2019-01-29 10:00:00 │    1003 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-31 │       1 │ 2019-01-31 09:00:00 │    1007 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-30 │       1 │ 2019-01-30 08:00:00 │    1009 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-02-01 │       1 │ 2019-02-01 08:00:00 │    1010 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
```

Descubra hasta dónde pudo avanzar el usuario `user_id` en la secuencia durante un período de enero a febrero de 2019.

```sql title="Query"
SELECT
    level,
    count() AS c
FROM
(
    SELECT
        user_id,
        windowFunnel(6048000000000000)(timestamp, eventID = 1003, eventID = 1009, eventID = 1007, eventID = 1010) AS level
    FROM trend
    WHERE (event_date >= '2019-01-01') AND (event_date <= '2019-02-02')
    GROUP BY user_id
)
GROUP BY level
ORDER BY level ASC;
```

```text title="Response"
┌─level─┬─c─┐
│     4 │ 1 │
└───────┴───┘
```

**Ejemplo del modo allow&#95;reentry**

Este ejemplo muestra cómo funciona el modo `allow_reentry` con patrones de reentrada de usuario:

```sql
-- Sample data: user visits checkout -> product detail -> checkout again -> payment
-- Without allow_reentry: stops at level 2 (product detail page)
-- With allow_reentry: reaches level 4 (payment completion)

SELECT
    level,
    count() AS users
FROM
(
    SELECT
        user_id,
        windowFunnel(3600, 'strict_order', 'allow_reentry')(
            timestamp,
            action = 'begin_checkout',      -- Step 1: Begin checkout
            action = 'view_product_detail', -- Step 2: View product detail  
            action = 'begin_checkout',      -- Step 3: Begin checkout again (reentry)
            action = 'complete_payment'     -- Step 4: Complete payment
        ) AS level
    FROM user_events
    WHERE event_date = today()
    GROUP BY user_id
)
GROUP BY level
ORDER BY level ASC;
```

<div id="retention">
  ## retention
</div>

La función toma como argumentos un conjunto de condiciones, de 1 a 32 argumentos de tipo `UInt8`, que indican si se cumplió una determinada condición para el evento.
Se puede especificar cualquier condición como argumento (como en [WHERE](/es/sql-reference/statements/select/where)).

Las condiciones, excepto la primera, se aplican por pares: el resultado de la segunda será true si la primera y la segunda son true; el de la tercera, si la primera y la tercera son true; etc.

**Sintaxis**

```sql
retention(cond1, cond2, ..., cond32);
```

**Argumentos**

* `cond` — Una expresión que devuelve un resultado `UInt8` (1 o 0).

**Valor devuelto**

Un array de 1 o 0.

* 1 — La condición se cumplió para el evento.
* 0 — La condición no se cumplió para el evento.

Tipo: `UInt8`.

**Ejemplo**

Consideremos un ejemplo de cálculo de la función `retention` para determinar el tráfico del sitio.

**1.** Cree una tabla para ilustrar el ejemplo.

```sql title="Query"
CREATE TABLE retention_test(date Date, uid Int32) ENGINE = Memory;

INSERT INTO retention_test SELECT '2020-01-01', number FROM numbers(5);
INSERT INTO retention_test SELECT '2020-01-02', number FROM numbers(10);
INSERT INTO retention_test SELECT '2020-01-03', number FROM numbers(15);
```

Tabla de entrada:

```sql title="Query"
SELECT * FROM retention_test
```

```text title="Response"
┌───────date─┬─uid─┐
│ 2020-01-01 │   0 │
│ 2020-01-01 │   1 │
│ 2020-01-01 │   2 │
│ 2020-01-01 │   3 │
│ 2020-01-01 │   4 │
└────────────┴─────┘
┌───────date─┬─uid─┐
│ 2020-01-02 │   0 │
│ 2020-01-02 │   1 │
│ 2020-01-02 │   2 │
│ 2020-01-02 │   3 │
│ 2020-01-02 │   4 │
│ 2020-01-02 │   5 │
│ 2020-01-02 │   6 │
│ 2020-01-02 │   7 │
│ 2020-01-02 │   8 │
│ 2020-01-02 │   9 │
└────────────┴─────┘
┌───────date─┬─uid─┐
│ 2020-01-03 │   0 │
│ 2020-01-03 │   1 │
│ 2020-01-03 │   2 │
│ 2020-01-03 │   3 │
│ 2020-01-03 │   4 │
│ 2020-01-03 │   5 │
│ 2020-01-03 │   6 │
│ 2020-01-03 │   7 │
│ 2020-01-03 │   8 │
│ 2020-01-03 │   9 │
│ 2020-01-03 │  10 │
│ 2020-01-03 │  11 │
│ 2020-01-03 │  12 │
│ 2020-01-03 │  13 │
│ 2020-01-03 │  14 │
└────────────┴─────┘
```

**2.** Agrupe los usuarios por el identificador único `uid` con la función `retention`.

```sql title="Query"
SELECT
    uid,
    retention(date = '2020-01-01', date = '2020-01-02', date = '2020-01-03') AS r
FROM retention_test
WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
GROUP BY uid
ORDER BY uid ASC
```

```text title="Response"
┌─uid─┬─r───────┐
│   0 │ [1,1,1] │
│   1 │ [1,1,1] │
│   2 │ [1,1,1] │
│   3 │ [1,1,1] │
│   4 │ [1,1,1] │
│   5 │ [0,0,0] │
│   6 │ [0,0,0] │
│   7 │ [0,0,0] │
│   8 │ [0,0,0] │
│   9 │ [0,0,0] │
│  10 │ [0,0,0] │
│  11 │ [0,0,0] │
│  12 │ [0,0,0] │
│  13 │ [0,0,0] │
│  14 │ [0,0,0] │
└─────┴─────────┘
```

**3.** Calcula el número total de visitas al sitio web por día.

```sql title="Query"
SELECT
    sum(r[1]) AS r1,
    sum(r[2]) AS r2,
    sum(r[3]) AS r3
FROM
(
    SELECT
        uid,
        retention(date = '2020-01-01', date = '2020-01-02', date = '2020-01-03') AS r
    FROM retention_test
    WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
    GROUP BY uid
)
```

```text title="Response"
┌─r1─┬─r2─┬─r3─┐
│  5 │  5 │  5 │
└────┴────┴────┘
```

Donde:

* `r1`- el número de visitantes únicos que visitaron el sitio el 2020-01-01 (la condición `cond1`).
* `r2`- el número de visitantes únicos que visitaron el sitio durante un período de tiempo concreto entre 2020-01-01 y 2020-01-02 (las condiciones `cond1` y `cond2`).
* `r3`- el número de visitantes únicos que visitaron el sitio durante un período de tiempo concreto en 2020-01-01 y en 2020-01-03 (las condiciones `cond1` y `cond3`).

<div id="uniquptonx">
  ## uniqUpTo(N)(x)
</div>

Calcula el número de valores distintos del argumento hasta un límite especificado, `N`. Si el número de valores distintos del argumento es mayor que `N`, esta función devuelve `N` + 1; de lo contrario, calcula el valor exacto.

Se recomienda usarla con valores pequeños de `N`, hasta 10. El valor máximo de `N` es 100.

Para el estado de una función de agregación, esta función usa una cantidad de memoria igual a 1 + `N` * el tamaño en bytes de un valor.
Al trabajar con cadenas, esta función almacena un hash no criptográfico de 8 bytes; en el caso de las cadenas, el cálculo es aproximado.

Por ejemplo, si tuviera una tabla que registra cada consulta de búsqueda realizada por los usuarios en su sitio web, donde cada fila representa una única consulta de búsqueda, con columnas para el ID del usuario, la consulta de búsqueda y la marca de tiempo de la consulta, puede usar `uniqUpTo` para generar un informe que muestre solo las palabras clave que produjeron al menos 5 usuarios únicos.

```sql
SELECT SearchPhrase
FROM SearchLog
GROUP BY SearchPhrase
HAVING uniqUpTo(4)(UserID) >= 5
```

`uniqUpTo(4)(UserID)` calcula el número de valores únicos de `UserID` para cada `SearchPhrase`, pero solo cuenta hasta 4 valores únicos. Si hay más de 4 valores únicos de `UserID` para un `SearchPhrase`, la función devuelve 5 (4 + 1). La cláusula `HAVING` filtra después los valores de `SearchPhrase` para los que el número de valores únicos de `UserID` es inferior a 5. Esto te dará una lista de palabras clave de búsqueda utilizadas por al menos 5 usuarios únicos.

<div id="summapfiltered">
  ## sumMapFiltered
</div>

Esta función se comporta igual que [sumMap](/es/sql-reference/aggregate-functions/reference/summap), excepto que también acepta como parámetro un array de claves para filtrar. Esto puede resultar especialmente útil al trabajar con claves de alta cardinalidad.

**Sintaxis**

`sumMapFiltered(keys_to_keep)(keys, values)`

**Parámetros**

* `keys_to_keep`: [Array](../data-types/array.md) de claves para filtrar.
* `keys`: [Array](../data-types/array.md) de claves.
* `values`: [Array](../data-types/array.md) de valores.

**Valor devuelto**

* Devuelve una tupla de dos arrays: las claves en orden ordenado y los valores sumados para las claves correspondientes.

**Ejemplo**

```sql title="Query"
CREATE TABLE sum_map
(
    `date` Date,
    `timeslot` DateTime,
    `statusMap` Nested(status UInt16, requests UInt64)
)
ENGINE = Log

INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10]);
```

```sql title="Query"
SELECT sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests) FROM sum_map;
```

```response title="Response"
   ┌─sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests)─┐
1. │ ([1,4,8],[10,20,10])                                            │
   └─────────────────────────────────────────────────────────────────┘
```

<div id="summapfilteredwithoverflow">
  ## sumMapFilteredWithOverflow
</div>

Esta función se comporta igual que [sumMap](/es/sql-reference/aggregate-functions/reference/summap), salvo que también acepta como parámetro un Array de claves por las que filtrar. Esto puede ser especialmente útil al trabajar con una alta cardinalidad de claves. Se diferencia de la función [sumMapFiltered](#summapfiltered) en que realiza la suma con desbordamiento; es decir, devuelve para la suma el mismo tipo de dato que el argumento.

**Sintaxis**

`sumMapFilteredWithOverflow(keys_to_keep)(keys, values)`

**Parámetros**

* `keys_to_keep`: [Array](../data-types/array.md) de claves por las que filtrar.
* `keys`: [Array](../data-types/array.md) de claves.
* `values`: [Array](../data-types/array.md) de valores.

**Valor devuelto**

* Devuelve una tupla de dos arrays: claves en orden y valores sumados para las claves correspondientes.

**Ejemplo**

En este ejemplo, creamos una tabla `sum_map`, insertamos algunos datos en ella y luego usamos tanto `sumMapFilteredWithOverflow` como `sumMapFiltered`, además de la función `toTypeName`, para comparar el resultado. Dado que `requests` era de tipo `UInt8` en la tabla creada, `sumMapFiltered` ha promovido el tipo de los valores sumados a `UInt64` para evitar el desbordamiento, mientras que `sumMapFilteredWithOverflow` ha mantenido el tipo como `UInt8`, que no es lo bastante grande como para almacenar el resultado; es decir, se ha producido un desbordamiento.

```sql title="Query"
CREATE TABLE sum_map
(
    `date` Date,
    `timeslot` DateTime,
    `statusMap` Nested(status UInt8, requests UInt8)
)
ENGINE = Log

INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10]),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10]);
```

```sql title="Query"
SELECT sumMapFilteredWithOverflow([1, 4, 8])(statusMap.status, statusMap.requests) as summap_overflow, toTypeName(summap_overflow) FROM sum_map;
```

```sql title="Query"
SELECT sumMapFiltered([1, 4, 8])(statusMap.status, statusMap.requests) as summap, toTypeName(summap) FROM sum_map;
```

```response title="Response"
   ┌─sum──────────────────┬─toTypeName(sum)───────────────────┐
1. │ ([1,4,8],[10,20,10]) │ Tuple(Array(UInt8), Array(UInt8)) │
   └──────────────────────┴───────────────────────────────────┘
```

```response title="Response"
   ┌─summap───────────────┬─toTypeName(summap)─────────────────┐
1. │ ([1,4,8],[10,20,10]) │ Tuple(Array(UInt8), Array(UInt64)) │
   └──────────────────────┴────────────────────────────────────┘
```

<div id="sequencenextnode">
  ## sequenceNextNode
</div>

Devuelve el valor del siguiente evento que coincide con una cadena de eventos.

*Función experimental; `SET allow_experimental_funnel_functions = 1` para habilitarla.*

**Sintaxis**

```sql
sequenceNextNode(direction, base)(timestamp, event_column, base_condition, event1, event2, event3, ...)
```

**Parámetros**

* `direction` — Se usa para indicar la dirección de navegación.
  * forward — Hacia adelante.
  * backward — Hacia atrás.

* `base` — Se usa para establecer el punto base.
  * head — Establece el punto base en el primer evento.
  * tail — Establece el punto base en el último evento.
  * first&#95;match — Establece el punto base en el primer `event1` coincidente.
  * last&#95;match — Establece el punto base en el último `event1` coincidente.

**Argumentos**

* `timestamp` — Nombre de la columna que contiene la marca de tiempo. Tipos de datos compatibles: [Date](../../sql-reference/data-types/date.md), [DateTime](/es/sql-reference/data-types/datetime) y otros tipos enteros sin signo.
* `event_column` — Nombre de la columna que contiene el valor del siguiente evento que se devolverá. Tipos de datos compatibles: [String](../../sql-reference/data-types/string.md) y [Nullable(String)](../../sql-reference/data-types/nullable.md).
* `base_condition` — Condición que debe cumplir el punto base.
* `event1`, `event2`, ... — Condiciones que describen la cadena de eventos. [UInt8](../../sql-reference/data-types/int-uint.md).

**Valores devueltos**

* `event_column[next_index]` — Si el patrón coincide y existe un valor siguiente.
* `NULL` - Si el patrón no coincide o no existe un valor siguiente.

Tipo: [Nullable(String)](../../sql-reference/data-types/nullable.md).

**Ejemplo**

Se puede usar cuando los eventos son A-&gt;B-&gt;C-&gt;D-&gt;E y se quiere saber qué evento sigue a B-&gt;C, que es D.

La consulta que busca el evento que sigue a A-&gt;B:

```sql title="Query"
CREATE TABLE test_flow (
    dt DateTime,
    id int,
    page String)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(dt)
ORDER BY id;

INSERT INTO test_flow VALUES (1, 1, 'A') (2, 1, 'B') (3, 1, 'C') (4, 1, 'D') (5, 1, 'E');

SELECT id, sequenceNextNode('forward', 'head')(dt, page, page = 'A', page = 'A', page = 'B') as next_flow FROM test_flow GROUP BY id;
```

```text title="Response"
┌─id─┬─next_flow─┐
│  1 │ C         │
└────┴───────────┘
```

**Comportamiento de `forward` y `head`**

```sql
ALTER TABLE test_flow DELETE WHERE 1 = 1 settings mutations_sync = 1;

INSERT INTO test_flow VALUES (1, 1, 'Home') (2, 1, 'Gift') (3, 1, 'Exit');
INSERT INTO test_flow VALUES (1, 2, 'Home') (2, 2, 'Home') (3, 2, 'Gift') (4, 2, 'Basket');
INSERT INTO test_flow VALUES (1, 3, 'Gift') (2, 3, 'Home') (3, 3, 'Gift') (4, 3, 'Basket');
```

```sql
SELECT id, sequenceNextNode('forward', 'head')(dt, page, page = 'Home', page = 'Home', page = 'Gift') FROM test_flow GROUP BY id;

                  dt   id   page
 1970-01-01 09:00:01    1   Home // punto base, Matched with Home
 1970-01-01 09:00:02    1   Gift // Matched with Gift
 1970-01-01 09:00:03    1   Exit // The result

 1970-01-01 09:00:01    2   Home // punto base, Matched with Home
 1970-01-01 09:00:02    2   Home // Unmatched with Gift
 1970-01-01 09:00:03    2   Gift
 1970-01-01 09:00:04    2   Basket

 1970-01-01 09:00:01    3   Gift // punto base, Unmatched with Home
 1970-01-01 09:00:02    3   Home
 1970-01-01 09:00:03    3   Gift
 1970-01-01 09:00:04    3   Basket
```

**Comportamiento de `backward` y `tail`**

```sql
SELECT id, sequenceNextNode('backward', 'tail')(dt, page, page = 'Basket', page = 'Basket', page = 'Gift') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home
1970-01-01 09:00:02    1   Gift
1970-01-01 09:00:03    1   Exit // punto base, Unmatched with Basket

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home // The result
1970-01-01 09:00:03    2   Gift // Matched with Gift
1970-01-01 09:00:04    2   Basket // punto base, Matched with Basket

1970-01-01 09:00:01    3   Gift
1970-01-01 09:00:02    3   Home // The result
1970-01-01 09:00:03    3   Gift // punto base, Matched with Gift
1970-01-01 09:00:04    3   Basket // punto base, Matched with Basket
```

**Comportamiento de `forward` y `first_match`**

```sql
SELECT id, sequenceNextNode('forward', 'first_match')(dt, page, page = 'Gift', page = 'Gift') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home
1970-01-01 09:00:02    1   Gift // punto base
1970-01-01 09:00:03    1   Exit // The result

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home
1970-01-01 09:00:03    2   Gift // punto base
1970-01-01 09:00:04    2   Basket  The result

1970-01-01 09:00:01    3   Gift // punto base
1970-01-01 09:00:02    3   Home // The result
1970-01-01 09:00:03    3   Gift
1970-01-01 09:00:04    3   Basket
```

```sql
SELECT id, sequenceNextNode('forward', 'first_match')(dt, page, page = 'Gift', page = 'Gift', page = 'Home') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home
1970-01-01 09:00:02    1   Gift // punto base
1970-01-01 09:00:03    1   Exit // Unmatched with Home

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home
1970-01-01 09:00:03    2   Gift // punto base
1970-01-01 09:00:04    2   Basket // Unmatched with Home

1970-01-01 09:00:01    3   Gift // punto base
1970-01-01 09:00:02    3   Home // Matched with Home
1970-01-01 09:00:03    3   Gift // The result
1970-01-01 09:00:04    3   Basket
```

**Comportamiento de `backward` y `last_match`**

```sql
SELECT id, sequenceNextNode('backward', 'last_match')(dt, page, page = 'Gift', page = 'Gift') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home // The result
1970-01-01 09:00:02    1   Gift // punto base
1970-01-01 09:00:03    1   Exit

1970-01-01 09:00:01    2   Home
1970-01-01 09:00:02    2   Home // The result
1970-01-01 09:00:03    2   Gift // punto base
1970-01-01 09:00:04    2   Basket

1970-01-01 09:00:01    3   Gift
1970-01-01 09:00:02    3   Home // The result
1970-01-01 09:00:03    3   Gift // punto base
1970-01-01 09:00:04    3   Basket
```

```sql
SELECT id, sequenceNextNode('backward', 'last_match')(dt, page, page = 'Gift', page = 'Gift', page = 'Home') FROM test_flow GROUP BY id;

                 dt   id   page
1970-01-01 09:00:01    1   Home // Matched with Home, the result is null
1970-01-01 09:00:02    1   Gift // punto base
1970-01-01 09:00:03    1   Exit

1970-01-01 09:00:01    2   Home // The result
1970-01-01 09:00:02    2   Home // Matched with Home
1970-01-01 09:00:03    2   Gift // punto base
1970-01-01 09:00:04    2   Basket

1970-01-01 09:00:01    3   Gift // The result
1970-01-01 09:00:02    3   Home // Matched with Home
1970-01-01 09:00:03    3   Gift // punto base
1970-01-01 09:00:04    3   Basket
```

**Comportamiento de `base_condition`**

```sql
CREATE TABLE test_flow_basecond
(
    `dt` DateTime,
    `id` int,
    `page` String,
    `ref` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(dt)
ORDER BY id;

INSERT INTO test_flow_basecond VALUES (1, 1, 'A', 'ref4') (2, 1, 'A', 'ref3') (3, 1, 'B', 'ref2') (4, 1, 'B', 'ref1');
```

```sql
SELECT id, sequenceNextNode('forward', 'head')(dt, page, ref = 'ref1', page = 'A') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4 // The head can not be punto base because the ref column of the head unmatched with 'ref1'.
 1970-01-01 09:00:02    1   A      ref3
 1970-01-01 09:00:03    1   B      ref2
 1970-01-01 09:00:04    1   B      ref1
```

```sql
SELECT id, sequenceNextNode('backward', 'tail')(dt, page, ref = 'ref4', page = 'B') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4
 1970-01-01 09:00:02    1   A      ref3
 1970-01-01 09:00:03    1   B      ref2
 1970-01-01 09:00:04    1   B      ref1 // The tail can not be punto base because the ref column of the tail unmatched with 'ref4'.
```

```sql
SELECT id, sequenceNextNode('forward', 'first_match')(dt, page, ref = 'ref3', page = 'A') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4 // This row can not be punto base because the ref column unmatched with 'ref3'.
 1970-01-01 09:00:02    1   A      ref3 // punto base
 1970-01-01 09:00:03    1   B      ref2 // The result
 1970-01-01 09:00:04    1   B      ref1
```

```sql
SELECT id, sequenceNextNode('backward', 'last_match')(dt, page, ref = 'ref2', page = 'B') FROM test_flow_basecond GROUP BY id;

                  dt   id   page   ref
 1970-01-01 09:00:01    1   A      ref4
 1970-01-01 09:00:02    1   A      ref3 // The result
 1970-01-01 09:00:03    1   B      ref2 // punto base
 1970-01-01 09:00:04    1   B      ref1 // This row can not be punto base because the ref column unmatched with 'ref2'.
```