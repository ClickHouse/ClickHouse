---
description: 'Documentación de Operadores'
sidebar_label: 'Operadores'
sidebar_position: 38
slug: /sql-reference/operators/
title: 'Operadores'
doc_type: 'reference'
---

ClickHouse convierte los operadores en sus funciones correspondientes durante la fase de análisis sintáctico de la consulta, según su prioridad, precedencia y asociatividad.

<div id="access-operators">
  ## Operadores de acceso
</div>

`a[N]` – Acceso a un elemento de un array. La función `arrayElement(a, N)`.

`a.N` – Acceso a un elemento de una tupla. La función `tupleElement(a, N)`.

<div id="numeric-negation-operator">
  ## Operador de negación numérica
</div>

`-a` – La función `negate(a)`.

Para la negación de tuplas: [tupleNegate](../../sql-reference/functions/tuple-functions.md#tupleNegate).

<div id="multiplication-and-division-operators">
  ## Operadores de multiplicación y división
</div>

`a * b` – La función `multiply (a, b)`.

Para multiplicar una tupla por un número: [tupleMultiplyByNumber](../../sql-reference/functions/tuple-functions.md#tupleMultiplyByNumber); para el producto escalar: [dotProduct](/es/sql-reference/functions/array-functions#arrayDotProduct).

`a / b` – La función `divide(a, b)`.

Para dividir una tupla por un número: [tupleDivideByNumber](../../sql-reference/functions/tuple-functions.md#tupleDivideByNumber).

`a % b` – La función `modulo(a, b)`.

<div id="addition-and-subtraction-operators">
  ## Operadores de suma y resta
</div>

`a + b` – La función `plus(a, b)`.

Para la suma de tuplas: [tuplePlus](../../sql-reference/functions/tuple-functions.md#tuplePlus).

`a - b` – La función `minus(a, b)`.

Para la resta de tuplas: [tupleMinus](../../sql-reference/functions/tuple-functions.md#tupleMinus).

<div id="comparison-operators">
  ## Operadores de comparación
</div>

<div id="equals-function">
  ### Función equals
</div>

`a = b` – La función `equals(a, b)`.

`a == b` – La función `equals(a, b)`.

<div id="notequals-function">
  ### función notEquals
</div>

`a != b` – La función `notEquals(a, b)`.

`a <> b` – La función `notEquals(a, b)`.

<div id="lessorequals-function">
  ### función lessOrEquals
</div>

`a <= b` – La función `lessOrEquals(a, b)`.

<div id="greaterorequals-function">
  ### función greaterOrEquals
</div>

`a >= b` – La función `greaterOrEquals(a, b)`.

<div id="less-function">
  ### función less
</div>

`a < b` – La función `less(a, b)`.

<div id="greater-function">
  ### función greater
</div>

`a > b` – La función `greater(a, b)`.

<div id="like-function">
  ### función like
</div>

`a LIKE b` – La función `like(a, b)`.

<div id="notlike-function">
  ### función notLike
</div>

`a NOT LIKE b` – La función `notLike(a, b)`.

<div id="ilike-function">
  ### función ilike
</div>

`a ILIKE b` – La función `ilike(a, b)`.

<div id="between-function">
  ### Función BETWEEN
</div>

`a BETWEEN b AND c` – Equivale a `a >= b AND a <= c`.

`a NOT BETWEEN b AND c` – Equivale a `a < b OR a > c`.

<div id="is-not-distinct-from">
  ### operador «no es distinto de» (`<=>`)
</div>

:::note
A partir de la versión 25.10, puedes usar `<=>` igual que cualquier otro operador.
Antes de la versión 25.10, solo podía usarse en expresiones JOIN, por ejemplo:

```sql
CREATE TABLE a (x String) ENGINE = Memory;
INSERT INTO a VALUES ('ClickHouse');

SELECT * FROM a AS a1 JOIN a AS a2 ON a1.x <=> a2.x;

┌─x──────────┬─a2.x───────┐
│ ClickHouse │ ClickHouse │
└────────────┴────────────┘
```

:::

El operador `<=>` es el operador de igualdad seguro para `NULL`, equivalente a `IS NOT DISTINCT FROM`.
Funciona como el operador de igualdad habitual (`=`), pero considera comparables los valores `NULL`.
Dos valores `NULL` se consideran iguales, y si se compara un `NULL` con cualquier valor distinto de `NULL`, devuelve 0 (`false`) en lugar de `NULL`.

```sql
SELECT
  'ClickHouse' <=> NULL,
  NULL <=> NULL
```

```response
┌─isNotDistinc⋯use', NULL)─┬─isNotDistinc⋯NULL, NULL)─┐
│                        0 │                        1 │
└──────────────────────────┴──────────────────────────┘
```

<div id="operators-for-working-with-strings">
  ## Operadores para trabajar con cadenas
</div>

<div id="overlay">
  ### OVERLAY
</div>

* `OVERLAY(string PLACING replacement FROM offset)` - La función `overlay(string, replacement, offset)`.
* `OVERLAY(string PLACING replacement FROM offset FOR length)` - La función `overlay(string, replacement, offset, length)`.
* `OVERLAYUTF8(string PLACING replacement FROM offset)` - La función `overlayUTF8(string, replacement, offset)`.
* `OVERLAYUTF8(string PLACING replacement FROM offset FOR length)` - La función `overlayUTF8(string, replacement, offset, length)`.

<div id="operators-for-working-with-data-sets">
  ## Operadores para trabajar con conjuntos de datos
</div>

Consulte los [operadores IN](../../sql-reference/operators/in.md) y el operador [EXISTS](../../sql-reference/operators/exists.md).

<div id="in-function">
  ### función in
</div>

`a IN ...` – La función `in(a, b)`.

<div id="notin-function">
  ### función notIn
</div>

`a NOT IN ...` – La función `notIn(a, b)`.

<div id="globalin-function">
  ### función globalIn
</div>

`a GLOBAL IN ...` – La función `globalIn(a, b)`.

<div id="globalnotin-function">
  ### función globalNotIn
</div>

`a GLOBAL NOT IN ...` – La función `globalNotIn(a, b)`.

<div id="in-subquery-function">
  ### función `in` con subconsulta
</div>

`a = ANY (subquery)` – La función `in(a, subquery)`.

<div id="notin-subquery-function">
  ### notIn subconsulta function
</div>

`a != ANY (subquery)` – Igual que `a NOT IN (SELECT singleValueOrNull(*) FROM subquery)`.

<div id="in-subquery-function-1">
  ### función de subconsulta in
</div>

`a = ALL (subquery)` – Lo mismo que `a IN (SELECT singleValueOrNull(*) FROM subquery)`.

<div id="notin-subquery-function-1">
  ### función notIn en subconsultas
</div>

`a != ALL (subquery)` – La función `notIn(a, subquery)`.

**Ejemplos**

Consulta con ALL:

```sql title="Query"
SELECT number AS a FROM numbers(10) WHERE a > ALL (SELECT number FROM numbers(3, 3));
```

```text title="Response"
┌─a─┐
│ 6 │
│ 7 │
│ 8 │
│ 9 │
└───┘
```

Consulta con ANY:

```sql title="Query"
SELECT number AS a FROM numbers(10) WHERE a > ANY (SELECT number FROM numbers(3, 3));
```

```text title="Response"
┌─a─┐
│ 4 │
│ 5 │
│ 6 │
│ 7 │
│ 8 │
│ 9 │
└───┘
```

<div id="some-all-on-arrays">
  ### `SOME` / `ALL` en arrays
</div>

Además de la subconsulta form descrita anteriormente, el lado derecho de `SOME` / `ALL` puede ser una expresión de array (un literal de array, una columna de tipo array o cualquier expresión que devuelva un array). Esta es la sintaxis de cuantificador de arrays al estilo de PostgreSQL. Se reconoce durante el análisis sintáctico y se reescribe en array functions, por lo que no es necesaria ninguna reescritura manual:

| Sintaxis                                                 | Se reescribe como                  |
| -------------------------------------------------------- | ---------------------------------- |
| `expr = SOME(arr)`                                       | `has(arr, expr)`                   |
| `expr <> ALL(arr)`                                       | `NOT has(arr, expr)`               |
| `expr OP SOME(arr)` (cualquier otro operador compatible) | `arrayExists(x -> expr OP x, arr)` |
| `expr OP ALL(arr)` (cualquier otro operador compatible)  | `arrayAll(x -> expr OP x, arr)`    |

`SOME` es el cuantificador existencial (el sinónimo de SQL de `ANY`). `=` y `<>` se tratan de forma especial como `has` / `NOT has` porque tienen una implementación optimizada; la forma general recurre a las funciones de orden superior `arrayExists` / `arrayAll`.

La array form se reconoce para los operadores de comparación `=`, `==`, `!=`, `<>`, `<=>`, `<`, `<=`, `>`, `>=`, los predicados de comparación con palabras clave `IS DISTINCT FROM` y `IS NOT DISTINCT FROM`, y los predicados de búsqueda de cadenas `LIKE`, `ILIKE`, `NOT LIKE`, `NOT ILIKE` y `REGEXP`. Los predicados de comparación con palabras clave y los predicados de búsqueda de cadenas se reconocen solo para la array form, no para la subconsulta form (que se reduce a `IN`/`NOT IN`). Los operadores que no tienen significado de cuantificador sobre arrays —por ejemplo, `IN`— **no** se reescriben y conservan su significado habitual.

Los predicados de búsqueda de cadenas funcionan porque `MatchImpl` (la implementación subyacente de `LIKE` / `ILIKE` / `REGEXP`) admite un texto de entrada constante con un patrón no constante. Por ejemplo, `'abc' LIKE SOME(['a%', 'b%'])` se reescribe como `arrayExists(x -> 'abc' LIKE x, ['a%', 'b%'])`, y `'abc' NOT LIKE ALL(['x%', 'y%'])` como `arrayAll(x -> 'abc' NOT LIKE x, ['x%', 'y%'])`. Esto compara una cadena con varios patrones; si quiere hacer la comparación en una sola pasada combinada, puede seguir usando una función de búsqueda multipatrón como `multiMatchAny` (expresiones regulares) o `multiSearchAny` (substrings).

:::note `ANY` no se admite en la array form
Solo `SOME` y `ALL` aceptan un array en el lado derecho. `ANY` se excluye porque `any` también es una función de agregación, por lo que una expresión con la shape `expr = any(x)` conserva su significado de llamada a función. Use `SOME` como cuantificador de arrays.
:::

```sql title="Query"
SELECT
    3 = SOME([1, 2, 3, 4])         AS in_array,
    5 < SOME([1, 2, 6])            AS less_than_some,
    5 > ALL([1, 2, 3])             AS greater_than_all,
    'abc' LIKE SOME(['a%', 'z%'])  AS like_some;
```

```text title="Response"
┌─in_array─┬─less_than_some─┬─greater_than_all─┬─like_some─┐
│        1 │              1 │                1 │         1 │
└──────────┴────────────────┴──────────────────┴───────────┘
```

:::note el manejo de `NULL` difiere de la forma de subconsulta
Como la forma de array se reescribe en el parser (donde no están disponibles los ajustes de la consulta, como `transform_null_in`, y una columna Array por fila no puede usar la ruta `IN` null-safe del analizador), utiliza la semántica de dos valores de `has` (para `=` / `<>`) y `arrayExists` / `arrayAll` (que convierten en `0` un resultado desconocido de comparación con `NULL`). Esto puede diferir de la forma de subconsulta, cuyo manejo de `NULL` se implementa mediante `IN` / `NOT IN` y depende de `transform_null_in`:

```sql
SELECT NULL = SOME([NULL]);   -- has([NULL], NULL)                  -> 1
SELECT NULL <> ALL([NULL]);   -- NOT has([NULL], NULL)              -> 0
SELECT NULL < SOME([1]);      -- arrayExists(x -> NULL < x, [1])    -> 0
SELECT NULL > ALL([1]);       -- arrayAll(x -> NULL > x, [1])       -> 0
```

:::

<div id="operators-for-working-with-dates-and-times">
  ## Operadores para trabajar con fechas y horas
</div>

<div id="extract">
  ### EXTRACT
</div>

```sql
EXTRACT(part FROM date);
```

Extrae partes de una fecha determinada. Por ejemplo, puedes extraer el mes de una fecha o el segundo de una hora.

El parámetro `part` especifica qué parte de la fecha se debe extraer. Los siguientes valores están disponibles:

* `NANOSECOND` — El nanosegundo. Valores posibles: 0–999999999.
* `MICROSECOND` — El microsegundo. Valores posibles: 0–999999.
* `MILLISECOND` — El milisegundo. Valores posibles: 0–999.
* `SECOND` — El segundo. Valores posibles: 0–59.
* `MINUTE` — El minuto. Valores posibles: 0–59.
* `HOUR` — La hora. Valores posibles: 0–23.
* `DAY` — El día del mes. Valores posibles: 1–31.
* `WEEK` — El número de semana ISO 8601. Valores posibles: 1–53.
* `MONTH` — El número del mes. Valores posibles: 1–12.
* `QUARTER` — El trimestre. Valores posibles: 1–4.
* `YEAR` — El año.
* `EPOCH` — El Unix timestamp (segundos desde 1970-01-01 00:00:00 UTC). Nota: para `DateTime64`, la parte subsegundo se trunca.
* `DOW` — El día de la semana (compatible con PostgreSQL). 0 = domingo, 6 = sábado.
* `DOY` — El día del año. Valores posibles: 1–366.
* `ISODOW` — El día ISO de la semana. 1 = lunes, 7 = domingo.
* `ISOYEAR` — El año de numeración de semanas ISO 8601.
* `CENTURY` — El siglo. Por ejemplo, el año 2024 está en el siglo XXI.
* `DECADE` — La década (año dividido por 10). Por ejemplo, el año 2024 tiene como década 202.
* `MILLENNIUM` — El milenio. Por ejemplo, el año 2024 está en el 3.er milenio.
* `TIMEZONE_HOUR` — La parte de horas con signo del desplazamiento UTC de la zona horaria del operando. Por ejemplo, `+5:30` devuelve `5`, `-3:30` devuelve `-3`.
* `TIMEZONE_MINUTE` — La parte de minutos con signo del desplazamiento UTC de la zona horaria del operando. Por ejemplo, `+5:30` devuelve `30`, `-3:30` devuelve `-30`.

El parámetro `part` no distingue entre mayúsculas y minúsculas.

El parámetro `date` especifica el valor que se debe procesar. Se admiten los tipos [Date](../../sql-reference/data-types/date.md), [Date32](../../sql-reference/data-types/date32.md), [DateTime](../../sql-reference/data-types/datetime.md), [DateTime64](../../sql-reference/data-types/datetime64.md) e [Interval](../../sql-reference/data-types/special-data-types/interval.md). Cuando `date` es un `Interval`, la `part` solicitada debe coincidir con el tipo almacenado en el intervalo (por ejemplo, se permite `EXTRACT(DAY FROM INTERVAL 5 DAY)`; `EXTRACT(HOUR FROM INTERVAL 5 DAY)` se rechaza, porque los intervalos de ClickHouse son de un solo tipo). El resultado para un operando `Interval` es `Int64`.

Ejemplos:

```sql
SELECT EXTRACT(DAY FROM toDate('2017-06-15'));
SELECT EXTRACT(MONTH FROM toDate('2017-06-15'));
SELECT EXTRACT(YEAR FROM toDate('2017-06-15'));
SELECT EXTRACT(EPOCH FROM toDateTime('2024-01-15 12:30:45', 'UTC'));
SELECT EXTRACT(DOW FROM toDate('2024-01-15'));
SELECT EXTRACT(CENTURY FROM toDate('2024-01-01'));
SELECT EXTRACT(TIMEZONE_HOUR   FROM toDateTime('2024-01-15 12:00:00', 'Asia/Kolkata'));    -- 5
SELECT EXTRACT(TIMEZONE_MINUTE FROM toDateTime('2024-01-15 12:00:00', 'Asia/Kolkata'));    -- 30
SELECT EXTRACT(DAY   FROM INTERVAL 40 DAY);                                                -- 40
SELECT EXTRACT(MONTH FROM INTERVAL 7 MONTH);                                               -- 7
```

En el siguiente ejemplo, creamos una tabla e insertamos en ella un valor de tipo `DateTime`.

```sql
CREATE TABLE test.Orders
(
    OrderId UInt64,
    OrderName String,
    OrderDate DateTime
) ENGINE = MergeTree
ORDER BY ();
```

```sql
INSERT INTO test.Orders VALUES (1, 'Jarlsberg Cheese', toDateTime('2008-10-11 13:23:44'));
```

```sql
SELECT
    toYear(OrderDate) AS OrderYear,
    toMonth(OrderDate) AS OrderMonth,
    toDayOfMonth(OrderDate) AS OrderDay,
    toHour(OrderDate) AS OrderHour,
    toMinute(OrderDate) AS OrderMinute,
    toSecond(OrderDate) AS OrderSecond
FROM test.Orders;
```

```text
┌─OrderYear─┬─OrderMonth─┬─OrderDay─┬─OrderHour─┬─OrderMinute─┬─OrderSecond─┐
│      2008 │         10 │       11 │        13 │          23 │          44 │
└───────────┴────────────┴──────────┴───────────┴─────────────┴─────────────┘
```

Puedes ver más ejemplos en [tests](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00619_extract.sql).

<div id="interval">
  ### INTERVAL
</div>

Crea un valor de tipo [Interval](../../sql-reference/data-types/special-data-types/interval.md) que debe usarse en operaciones aritméticas con valores de tipo [Date](../../sql-reference/data-types/date.md) y [DateTime](../../sql-reference/data-types/datetime.md).

Tipos de intervalos:

* `SECOND`
* `MINUTE`
* `HOUR`
* `DAY`
* `WEEK`
* `MONTH`
* `QUARTER`
* `YEAR`

También puedes usar un literal de cadena al definir el valor `INTERVAL`. Por ejemplo, `INTERVAL 1 HOUR` es idéntico a `INTERVAL '1 hour'` o `INTERVAL '1' hour`.

:::tip
Los intervalos de distintos tipos no se pueden combinar. No puedes usar expresiones como `INTERVAL 4 DAY 1 HOUR`. Especifica los intervalos en unidades menores o iguales que la unidad más pequeña del intervalo; por ejemplo, `INTERVAL 25 HOUR`. Puedes usar operaciones consecutivas, como en el ejemplo siguiente.
:::

Ejemplos:

```sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR;
```

```text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2020-11-03 22:09:50 │                                    2020-11-08 01:09:50 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

```sql
SELECT now() AS current_date_time, current_date_time + INTERVAL '4 day' + INTERVAL '3 hour';
```

```text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2020-11-03 22:12:10 │                                    2020-11-08 01:12:10 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

```sql
SELECT now() AS current_date_time, current_date_time + INTERVAL '4' day + INTERVAL '3' hour;
```

```text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay('4')), toIntervalHour('3'))─┐
│ 2020-11-03 22:33:19 │                                        2020-11-08 01:33:19 │
└─────────────────────┴────────────────────────────────────────────────────────────┘
```

:::note
Siempre se prefieren la sintaxis `INTERVAL` o la función `addDays`. La suma o resta simple (con una sintaxis como `now() + ...`) no tiene en cuenta la configuración horaria. Por ejemplo, el horario de verano.
:::

Ejemplos:

```sql
SELECT toDateTime('2014-10-26 00:00:00', 'Asia/Istanbul') AS time, time + 60 * 60 * 24 AS time_plus_24_hours, time + toIntervalDay(1) AS time_plus_1_day;
```

```text
┌────────────────time─┬──time_plus_24_hours─┬─────time_plus_1_day─┐
│ 2014-10-26 00:00:00 │ 2014-10-26 23:00:00 │ 2014-10-27 00:00:00 │
└─────────────────────┴─────────────────────┴─────────────────────┘
```

**Véase también**

* [Interval](../../sql-reference/data-types/special-data-types/interval.md) tipo de dato
* [toInterval](/es/sql-reference/functions/type-conversion-functions#toIntervalYear) funciones de conversión de tipos

<div id="date-time-addition">
  ### Suma de Date y Time
</div>

Un valor [Date](../../sql-reference/data-types/date.md) o [Date32](../../sql-reference/data-types/date32.md) puede sumarse a un valor [Time](../../sql-reference/data-types/time.md) o [Time64](../../sql-reference/data-types/time64.md) con el operador `+`. El resultado es un [DateTime](../../sql-reference/data-types/datetime.md) o [DateTime64](../../sql-reference/data-types/datetime64.md) que representa la fecha con la hora del día indicada. La operación es conmutativa.

El tipo de resultado depende de los tipos de los operandos:

| Operando izquierdo | Operando derecho | Tipo de resultado |
| ------------------ | ---------------- | ----------------- |
| `Date`             | `Time`           | `DateTime`        |
| `Date`             | `Time64(s)`      | `DateTime64(s)`   |
| `Date32`           | `Time`           | `DateTime64(0)`   |
| `Date32`           | `Time64(s)`      | `DateTime64(s)`   |

:::note
El resultado utiliza la [zona horaria de la sesión](../../operations/settings/settings.md#session_timezone) (o la zona horaria predeterminada del servidor si no se ha configurado ninguna para la sesión). La configuración [`date_time_overflow_behavior`](../../operations/settings/settings-formats.md#date_time_overflow_behavior) controla qué ocurre cuando el resultado queda fuera del rango representable.
:::

Ejemplos:

```sql
SET use_legacy_to_time = 0;
SELECT toDate('2024-07-15') + toTime('14:30:25') AS dt, toTypeName(dt);
```

```text
┌──────────────────dt─┬─toTypeName(dt)─┐
│ 2024-07-15 14:30:25 │ DateTime       │
└─────────────────────┴────────────────┘
```

```sql
SELECT toDate('2024-07-15') + toTime64('14:30:25.123456', 6) AS dt, toTypeName(dt);
```

```text
┌─────────────────────────dt─┬─toTypeName(dt)─┐
│ 2024-07-15 14:30:25.123456 │ DateTime64(6)  │
└────────────────────────────┴────────────────┘
```

```sql
SELECT toTime64('23:59:59.999', 3) + toDate32('2024-07-15') AS dt, toTypeName(dt);
```

```text
┌──────────────────────dt─┬─toTypeName(dt)─┐
│ 2024-07-15 23:59:59.999 │ DateTime64(3)  │
└─────────────────────────┴────────────────┘
```

<div id="at-time-zone">
  ### AT TIME ZONE y AT LOCAL
</div>

Los operadores posfijos `AT TIME ZONE` y `AT LOCAL` convierten un valor `DateTime` o `DateTime64` a una zona horaria diferente. Son azúcar sintáctico de la función [`toTimeZone`](/es/sql-reference/functions/date-time-functions#totimezone) ya existente:

| Sintaxis                 | Equivalente                    |
| ------------------------ | ------------------------------ |
| `expr AT TIME ZONE zone` | `toTimeZone(expr, zone)`       |
| `expr AT LOCAL`          | `toTimeZone(expr, timeZone())` |

`zone` puede ser cualquier expresión de cadena constante que se evalúe como un nombre de zona horaria válido (por ejemplo, `'America/Denver'`, `'UTC'` o `concat('America', '/', 'Denver')`). Como `AT TIME ZONE` se convierte internamente en `toTimeZone`, se aplican las mismas reglas para los argumentos de zona horaria: las expresiones no constantes, como una referencia a una columna, requieren [`allow_nonconst_timezone_arguments = 1`](../../operations/settings/settings.md#allow_nonconst_timezone_arguments).

`AT LOCAL` usa la [zona horaria de la sesión](../../operations/settings/settings.md#session_timezone) actual (o la predeterminada del servidor si no se ha configurado ninguna zona horaria de sesión). En las tablas `Distributed`, `session_timezone` debe establecerse explícitamente; cuando está vacía, `timeZone()` es local al segmento y no puede usarse como argumento constante de `toTimeZone`, lo que provoca una excepción `ILLEGAL_COLUMN`.

:::note
A diferencia de PostgreSQL, donde `timestamp without time zone AT TIME ZONE zone` reinterpreta el valor de hora local como si estuviera en la zona indicada antes de convertirlo, ClickHouse siempre mantiene el mismo punto absoluto en el tiempo y solo cambia la etiqueta de zona horaria usada para mostrarlo. Ambas formas son equivalentes a `toTimeZone` y no alteran el timestamp subyacente.
:::

`AT TIME ZONE` tiene precedencia de operador 13 (por encima de `*`/`/`/`%` con 12 y de `+`/`-` con 11), igual que en PostgreSQL. Esto significa que `a * ts AT TIME ZONE 'tz'` se asocia como `a * (ts AT TIME ZONE 'tz')`, y `ts + interval AT TIME ZONE 'tz'` se asocia como `ts + (interval AT TIME ZONE 'tz')`. Para aplicar la conversión de zona horaria después de la aritmética, use paréntesis explícitos:

```sql
-- Explicit parens required to add first, then convert timezone
SELECT (TIMESTAMP '2001-02-16 20:38:40' + INTERVAL 1 HOUR) AT TIME ZONE 'America/Denver';
-- Equivalent to:
SELECT toTimeZone(TIMESTAMP '2001-02-16 20:38:40' + INTERVAL 1 HOUR, 'America/Denver');
```

Ejemplos:

```sql
SET session_timezone = 'UTC';

SELECT TIMESTAMP '2001-02-16 20:38:40' AT TIME ZONE 'America/Denver';
```

```text
┌─toTimeZone(toDateTime('2001-02-16 20:38:40'), 'America/Denver')─┐
│ 2001-02-16 13:38:40                                              │
└──────────────────────────────────────────────────────────────────┘
```

```sql
SELECT TIMESTAMP '2001-02-16 20:38:40' AT LOCAL;
```

```text
┌─toTimeZone(toDateTime('2001-02-16 20:38:40'), timeZone())─┐
│ 2001-02-16 20:38:40                                        │
└────────────────────────────────────────────────────────────┘
```

**Véase también**

* [`toTimeZone`](/es/sql-reference/functions/date-time-functions#totimezone)
* [`timeZone`](/es/sql-reference/functions/date-time-functions#timezone)

<div id="logical-and-operator">
  ## Operador lógico AND
</div>

Sintaxis `SELECT a AND b` — calcula la conjunción lógica entre `a` y `b` mediante la función [and](/es/sql-reference/functions/logical-functions#and).

<div id="logical-or-operator">
  ## Operador lógico OR
</div>

Sintaxis `SELECT a OR b` — calcula la disyunción lógica de `a` y `b` mediante la función [or](/es/sql-reference/functions/logical-functions#or).

<div id="logical-negation-operator">
  ## Operador de negación lógica
</div>

Sintaxis `SELECT NOT a`: calcula la negación lógica de `a` con la función [not](/es/sql-reference/functions/logical-functions#not).

<div id="conditional-operator">
  ## Operador condicional
</div>

`a ? b : c` – La función `if(a, b, c)`.

Nota:

El operador condicional calcula los valores de b y c, luego comprueba si se cumple la condición a y devuelve el valor correspondiente. Si `b` o `C` es una función [arrayJoin()](/es/sql-reference/functions/array-join), cada fila se replicará independientemente de la condición &quot;a&quot;.

<div id="conditional-expression">
  ## Expresión condicional
</div>

```sql
CASE [x]
    WHEN a THEN b
    [WHEN ... THEN ...]
    [ELSE c]
END
```

Si se especifica `x`, se usa la función `transform(x, [a, ...], [b, ...], c)`. En caso contrario, `multiIf(a, b, ..., c)`.

Si no hay una cláusula `ELSE c` en la expresión, el valor por defecto es `NULL`.

La función `transform` no funciona con `NULL`.

<div id="concatenation-operator">
  ## Operador de concatenación
</div>

`s1 || s2` – La `función concat(s1, s2).`

<div id="lambda-creation-operator">
  ## Operador de creación de lambda
</div>

`x -> expr` – La función `lambda(x, expr)`.

Los siguientes operadores no tienen prioridad, ya que son paréntesis:

<div id="array-creation-operator">
  ## Operador de creación de Array
</div>

`[x1, ...]` – La función `array(x1, ...)`.

<div id="tuple-creation-operator">
  ## Operador de creación de Tuple
</div>

`(x1, x2, ...)` – La función `tuple(x2, x2, ...)`.

<div id="associativity">
  ## Asociatividad
</div>

Todos los operadores binarios tienen asociatividad por la izquierda. Por ejemplo, `1 + 2 + 3` se transforma en `plus(plus(1, 2), 3)`.
A veces, esto no funciona como se espera. Por ejemplo, `SELECT 4 > 2 > 3` dará como resultado 0.

Por eficiencia, las funciones `and` y `or` aceptan cualquier cantidad de argumentos. Las cadenas correspondientes de operadores `AND` y `OR` se transforman en una sola llamada a estas funciones.

<div id="checking-for-null">
  ## Comprobación de `NULL`
</div>

ClickHouse admite los operadores `IS NULL` y `IS NOT NULL`.

<div id="is_null">
  ### IS NULL
</div>

* Para los valores del tipo [Nullable](../../sql-reference/data-types/nullable.md), el operador `IS NULL` devuelve:
  * `1` si el valor es `NULL`.
  * `0` en caso contrario.
* Para cualquier otro valor, el operador `IS NULL` siempre devuelve `0`.

Se puede optimizar habilitando la opción [optimize&#95;functions&#95;to&#95;subcolumns](/es/operations/settings/settings#optimize_functions_to_subcolumns). Con `optimize_functions_to_subcolumns = 1`, la función lee solo la subcolumna [null](../../sql-reference/data-types/nullable.md#finding-null) en lugar de leer y procesar todos los datos de la columna. La consulta `SELECT n IS NULL FROM table` se transforma en `SELECT n.null FROM TABLE`.

{/* */ }

```sql
SELECT x+100 FROM t_null WHERE y IS NULL
```

```text
┌─plus(x, 100)─┐
│          101 │
└──────────────┘
```

<div id="is_not_null">
  ### IS NOT NULL
</div>

* Para los valores de tipo [Nullable](../../sql-reference/data-types/nullable.md), el operador `IS NOT NULL` devuelve:
  * `0` si el valor es `NULL`.
  * `1` en caso contrario.
* Para otros valores, el operador `IS NOT NULL` siempre devuelve `1`.

{/* */ }

```sql
SELECT * FROM t_null WHERE y IS NOT NULL
```

```text
┌─x─┬─y─┐
│ 2 │ 3 │
└───┴───┘
```

Puede optimizarse habilitando la configuración [optimize&#95;functions&#95;to&#95;subcolumns](/es/operations/settings/settings#optimize_functions_to_subcolumns). Con `optimize_functions_to_subcolumns = 1`, la función solo lee la subcolumna [null](../../sql-reference/data-types/nullable.md#finding-null) en lugar de leer y procesar todos los datos de la columna. La consulta `SELECT n IS NOT NULL FROM table` se transforma en `SELECT NOT n.null FROM TABLE`.

<div id="checking-boolean-values">
  ## Comprobación de valores booleanos
</div>

ClickHouse admite los operadores `IS TRUE`, `IS FALSE`, `IS UNKNOWN`, `IS NOT TRUE`, `IS NOT FALSE` e `IS NOT UNKNOWN`.
Se utilizan con expresiones [Bool](../../sql-reference/data-types/boolean.md) y `Nullable(Bool)`.

* `expr IS TRUE` devuelve `1` solo si `expr` es `true`.
* `expr IS FALSE` devuelve `1` solo si `expr` es `false`.
* `expr IS UNKNOWN` devuelve `1` solo si `expr` es `NULL`.
* `expr IS NOT TRUE` devuelve `1` si `expr` es `false` o `NULL`.
* `expr IS NOT FALSE` devuelve `1` si `expr` es `true` o `NULL`.
* `expr IS NOT UNKNOWN` devuelve `1` si `expr` no es `NULL`.

En las expresiones booleanas, `IS UNKNOWN` equivale a `IS NULL` y `IS NOT UNKNOWN` equivale a `IS NOT NULL`.

{/* */ }

```sql
CREATE TABLE t_bool (x Nullable(Bool)) ENGINE = Memory;
INSERT INTO t_bool VALUES (true), (false), (NULL);

SELECT
    x,
    x IS TRUE,
    x IS FALSE,
    x IS UNKNOWN,
    x IS NOT TRUE,
    x IS NOT FALSE,
    x IS NOT UNKNOWN
FROM t_bool;
```