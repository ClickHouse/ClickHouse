---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 37
toc_title: Operador
---

# Operador {#operators}

ClickHouse transforma los operadores a sus funciones correspondientes en la etapa de análisis de consultas de acuerdo con su prioridad, precedencia y asociatividad.

## Operadores de acceso {#access-operators}

`a[N]` – Access to an element of an array. The `arrayElement(a, N)` función.

`a.N` – Access to a tuple element. The `tupleElement(a, N)` función.

## Operador de negación numérica {#numeric-negation-operator}

`-a` – The `negate (a)` función.

## Operadores de multiplicación y división {#multiplication-and-division-operators}

`a * b` – The `multiply (a, b)` función.

`a / b` – The `divide(a, b)` función.

`a % b` – The `modulo(a, b)` función.

## Operadores de suma y resta {#addition-and-subtraction-operators}

`a + b` – The `plus(a, b)` función.

`a - b` – The `minus(a, b)` función.

## Operadores de comparación {#comparison-operators}

`a = b` – The `equals(a, b)` función.

`a == b` – The `equals(a, b)` función.

`a != b` – The `notEquals(a, b)` función.

`a <> b` – The `notEquals(a, b)` función.

`a <= b` – The `lessOrEquals(a, b)` función.

`a >= b` – The `greaterOrEquals(a, b)` función.

`a < b` – The `less(a, b)` función.

`a > b` – The `greater(a, b)` función.

`a LIKE s` – The `like(a, b)` función.

`a NOT LIKE s` – The `notLike(a, b)` función.

`a BETWEEN b AND c` – The same as `a >= b AND a <= c`.

`a NOT BETWEEN b AND c` – The same as `a < b OR a > c`.

## Operadores para trabajar con conjuntos de datos {#operators-for-working-with-data-sets}

*Ver [IN operadores](in.md).*

`a IN ...` – The `in(a, b)` función.

`a NOT IN ...` – The `notIn(a, b)` función.

`a GLOBAL IN ...` – The `globalIn(a, b)` función.

`a GLOBAL NOT IN ...` – The `globalNotIn(a, b)` función.

## Operadores para trabajar con fechas y horas {#operators-datetime}

### EXTRACT {#operator-extract}

``` sql
EXTRACT(part FROM date);
```

Extraer partes de una fecha determinada. Por ejemplo, puede recuperar un mes a partir de una fecha determinada o un segundo a partir de una hora.

El `part` parámetro especifica qué parte de la fecha se va a recuperar. Los siguientes valores están disponibles:

-   `DAY` — The day of the month. Possible values: 1–31.
-   `MONTH` — The number of a month. Possible values: 1–12.
-   `YEAR` — The year.
-   `SECOND` — The second. Possible values: 0–59.
-   `MINUTE` — The minute. Possible values: 0–59.
-   `HOUR` — The hour. Possible values: 0–23.

El `part` El parámetro no distingue entre mayúsculas y minúsculas.

El `date` parámetro especifica la fecha o la hora a procesar. Bien [Fecha](../../sql-reference/data-types/date.md) o [FechaHora](../../sql-reference/data-types/datetime.md) tipo es compatible.

Ejemplos:

``` sql
SELECT EXTRACT(DAY FROM toDate('2017-06-15'));
SELECT EXTRACT(MONTH FROM toDate('2017-06-15'));
SELECT EXTRACT(YEAR FROM toDate('2017-06-15'));
```

En el siguiente ejemplo creamos una tabla e insertamos en ella un valor con el `DateTime` tipo.

``` sql
CREATE TABLE test.Orders
(
    OrderId UInt64,
    OrderName String,
    OrderDate DateTime
)
ENGINE = Log;
```

``` sql
INSERT INTO test.Orders VALUES (1, 'Jarlsberg Cheese', toDateTime('2008-10-11 13:23:44'));
```

``` sql
SELECT
    toYear(OrderDate) AS OrderYear,
    toMonth(OrderDate) AS OrderMonth,
    toDayOfMonth(OrderDate) AS OrderDay,
    toHour(OrderDate) AS OrderHour,
    toMinute(OrderDate) AS OrderMinute,
    toSecond(OrderDate) AS OrderSecond
FROM test.Orders;
```

``` text
┌─OrderYear─┬─OrderMonth─┬─OrderDay─┬─OrderHour─┬─OrderMinute─┬─OrderSecond─┐
│      2008 │         10 │       11 │        13 │          23 │          44 │
└───────────┴────────────┴──────────┴───────────┴─────────────┴─────────────┘
```

Puedes ver más ejemplos en [prueba](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00619_extract.sql).

### INTERVAL {#operator-interval}

Crea un [Intervalo](../../sql-reference/data-types/special-data-types/interval.md)-type valor que debe utilizarse en operaciones aritméticas con [Fecha](../../sql-reference/data-types/date.md) y [FechaHora](../../sql-reference/data-types/datetime.md)-type valores.

Tipos de intervalos:
- `SECOND`
- `MINUTE`
- `HOUR`
- `DAY`
- `WEEK`
- `MONTH`
- `QUARTER`
- `YEAR`

!!! warning "Advertencia"
    Los intervalos con diferentes tipos no se pueden combinar. No puedes usar expresiones como `INTERVAL 4 DAY 1 HOUR`. Especifique intervalos en unidades que sean más pequeñas o iguales que la unidad más pequeña del intervalo, por ejemplo, `INTERVAL 25 HOUR`. Puede usar operaciones consecutivas, como en el siguiente ejemplo.

Ejemplo:

``` sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR
```

``` text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2019-10-23 11:16:28 │                                    2019-10-27 14:16:28 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

**Ver también**

-   [Intervalo](../../sql-reference/data-types/special-data-types/interval.md) tipo de datos
-   [ToInterval](../../sql-reference/functions/type-conversion-functions.md#function-tointerval) funciones de conversión de tipo

## Operador de Negación Lógica {#logical-negation-operator}

`NOT a` – The `not(a)` función.

## Operador lógico and {#logical-and-operator}

`a AND b` – The`and(a, b)` función.

## Operador lógico or {#logical-or-operator}

`a OR b` – The `or(a, b)` función.

## Operador condicional {#conditional-operator}

`a ? b : c` – The `if(a, b, c)` función.

Nota:

El operador condicional calcula los valores de b y c, luego verifica si se cumple la condición a y luego devuelve el valor correspondiente. Si `b` o `C` es una [arrayJoin()](../../sql-reference/functions/array-join.md#functions_arrayjoin) función, cada fila se replicará independientemente de la “a” condición.

## Expresión condicional {#operator_case}

``` sql
CASE [x]
    WHEN a THEN b
    [WHEN ... THEN ...]
    [ELSE c]
END
```

Si `x` se especifica, entonces `transform(x, [a, ...], [b, ...], c)` function is used. Otherwise – `multiIf(a, b, ..., c)`.

Si no hay `ELSE c` cláusula en la expresión, el valor predeterminado es `NULL`.

El `transform` no funciona con `NULL`.

## Operador de Concatenación {#concatenation-operator}

`s1 || s2` – The `concat(s1, s2) function.`

## Operador de Creación Lambda {#lambda-creation-operator}

`x -> expr` – The `lambda(x, expr) function.`

Los siguientes operadores no tienen prioridad ya que son corchetes:

## Operador de creación de matrices {#array-creation-operator}

`[x1, ...]` – The `array(x1, ...) function.`

## Operador de creación de tupla {#tuple-creation-operator}

`(x1, x2, ...)` – The `tuple(x2, x2, ...) function.`

## Asociatividad {#associativity}

Todos los operadores binarios han dejado asociatividad. Por ejemplo, `1 + 2 + 3` se transforma a `plus(plus(1, 2), 3)`.
A veces esto no funciona de la manera que esperas. Por ejemplo, `SELECT 4 > 2 > 3` resultará en 0.

Para la eficiencia, el `and` y `or` funciones aceptan cualquier número de argumentos. Las cadenas correspondientes de `AND` y `OR` operadores se transforman en una sola llamada de estas funciones.

## Comprobación de `NULL` {#checking-for-null}

ClickHouse soporta el `IS NULL` y `IS NOT NULL` operador.

### IS NULL {#operator-is-null}

-   Para [NULL](../../sql-reference/data-types/nullable.md) valores de tipo, el `IS NULL` operador devuelve:
    -   `1` si el valor es `NULL`.
    -   `0` de lo contrario.
-   Para otros valores, el `IS NULL` operador siempre devuelve `0`.

<!-- -->

``` sql
SELECT x+100 FROM t_null WHERE y IS NULL
```

``` text
┌─plus(x, 100)─┐
│          101 │
└──────────────┘
```

### IS NOT NULL {#is-not-null}

-   Para [NULL](../../sql-reference/data-types/nullable.md) valores de tipo, el `IS NOT NULL` operador devuelve:
    -   `0` si el valor es `NULL`.
    -   `1` de lo contrario.
-   Para otros valores, el `IS NOT NULL` operador siempre devuelve `1`.

<!-- -->

``` sql
SELECT * FROM t_null WHERE y IS NOT NULL
```

``` text
┌─x─┬─y─┐
│ 2 │ 3 │
└───┴───┘
```

[Artículo Original](https://clickhouse.tech/docs/en/query_language/operators/) <!--hide-->
