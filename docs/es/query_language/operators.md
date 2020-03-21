# Operador {#operators}

Todos los operadores se transforman a sus funciones correspondientes en la etapa de análisis de consulta de acuerdo con su precedencia y asociatividad.
Los grupos de operadores se enumeran en orden de prioridad (cuanto más alto esté en la lista, más temprano estará conectado el operador a sus argumentos).

## Operadores de acceso {#access-operators}

`a[N]` – Acceso a un elemento de una matriz. El `arrayElement(a, N)` función.

`a.N` – El acceso a un elemento de tupla. El `tupleElement(a, N)` función.

## Operador de negación numérica {#numeric-negation-operator}

`-a` – El `negate (a)` función.

## Operadores de multiplicación y división {#multiplication-and-division-operators}

`a * b` – El `multiply (a, b)` función.

`a / b` – El `divide(a, b)` función.

`a % b` – El `modulo(a, b)` función.

## Operadores de suma y resta {#addition-and-subtraction-operators}

`a + b` – El `plus(a, b)` función.

`a - b` – El `minus(a, b)` función.

## Operadores de comparación {#comparison-operators}

`a = b` – El `equals(a, b)` función.

`a == b` – El `equals(a, b)` función.

`a != b` – El `notEquals(a, b)` función.

`a <> b` – El `notEquals(a, b)` función.

`a <= b` – El `lessOrEquals(a, b)` función.

`a >= b` – El `greaterOrEquals(a, b)` función.

`a < b` – El `less(a, b)` función.

`a > b` – El `greater(a, b)` función.

`a LIKE s` – El `like(a, b)` función.

`a NOT LIKE s` – El `notLike(a, b)` función.

`a BETWEEN b AND c` – Lo mismo que `a >= b AND a <= c`.

`a NOT BETWEEN b AND c` – Lo mismo que `a < b OR a > c`.

## Operadores para trabajar con conjuntos de datos {#operators-for-working-with-data-sets}

*Ver [IN operadores](select.md#select-in-operators).*

`a IN ...` – El `in(a, b)` función.

`a NOT IN ...` – El `notIn(a, b)` función.

`a GLOBAL IN ...` – El `globalIn(a, b)` función.

`a GLOBAL NOT IN ...` – El `globalNotIn(a, b)` función.

## Operadores para trabajar con fechas y horas {#operators-datetime}

### EXTRAER {#operator-extract}

``` sql
EXTRACT(part FROM date);
```

Extrae una parte de una fecha determinada. Por ejemplo, puede recuperar un mes a partir de una fecha determinada o un segundo a partir de una hora.

El `part` parámetro especifica qué parte de la fecha se va a recuperar. Los siguientes valores están disponibles:

-   `DAY` — El día del mes. Valores posibles: 1-31.
-   `MONTH` — El número de un mes. Valores posibles: 1-12.
-   `YEAR` — Año.
-   `SECOND` — Segundo. Valores posibles: 0–59.
-   `MINUTE` — Minuto. Valores posibles: 0–59.
-   `HOUR` — Hora. Valores posibles: 0–23.

El `part` El parámetro no distingue entre mayúsculas y minúsculas.

El `date` parámetro especifica la fecha o la hora a procesar. Bien [Fecha](../data_types/date.md) o [FechaHora](../data_types/datetime.md) tipo es compatible.

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

Puedes ver más ejemplos en [prueba](https://github.com/ClickHouse/ClickHouse/blob/master/dbms/tests/queries/0_stateless/00619_extract.sql).

### INTERVALO {#operator-interval}

Crea un [Intervalo](../data_types/special_data_types/interval.md)-type valor que debe utilizarse en operaciones aritméticas con [Fecha](../data_types/date.md) y [FechaHora](../data_types/datetime.md)-tipo valores.

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
    Los intervalos con diferentes tipos no se pueden combinar. No puede usar expresiones como `INTERVAL 4 DAY 1 HOUR`. Exprese los intervalos en unidades que son más pequeñas o iguales a la unidad más pequeña del intervalo, por ejemplo `INTERVAL 25 HOUR`. Puede usar operaciones consequtive como en el siguiente ejemplo.

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

-   [Intervalo](../data_types/special_data_types/interval.md) Tipo de datos
-   [ToInterval](functions/type_conversion_functions.md#function-tointerval) funciones de conversión de tipo

## Operador de Negación Lógica {#logical-negation-operator}

`NOT a` – El `not(a)` función.

## Operador lógico and {#logical-and-operator}

`a AND b` – El`and(a, b)` función.

## Operador lógico or {#logical-or-operator}

`a OR b` – El `or(a, b)` función.

## Operador condicional {#conditional-operator}

`a ? b : c` – El `if(a, b, c)` función.

Nota:

El operador condicional calcula los valores de b y c, luego verifica si se cumple la condición a y luego devuelve el valor correspondiente. Si `b` o `C` es una [arrayJoin()](functions/array_join.md#functions_arrayjoin) función, cada fila se replicará independientemente de la “a” condición.

## Expresión condicional {#operator-case}

``` sql
CASE [x]
    WHEN a THEN b
    [WHEN ... THEN ...]
    [ELSE c]
END
```

Si `x` se especifica, entonces `transform(x, [a, ...], [b, ...], c)` se utiliza la función. De lo contrario – `multiIf(a, b, ..., c)`.

Si no hay `ELSE c` cláusula en la expresión, el valor predeterminado es `NULL`.

El `transform` no funciona con `NULL`.

## Operador de Concatenación {#concatenation-operator}

`s1 || s2` – El `concat(s1, s2) function.`

## Operador de Creación Lambda {#lambda-creation-operator}

`x -> expr` – El `lambda(x, expr) function.`

Los siguientes operadores no tienen prioridad, ya que son corchetes:

## Operador de creación de matrices {#array-creation-operator}

`[x1, ...]` – El `array(x1, ...) function.`

## Operador de creación de tupla {#tuple-creation-operator}

`(x1, x2, ...)` – El `tuple(x2, x2, ...) function.`

## Asociatividad {#associativity}

Todos los operadores binarios han dejado asociatividad. Por ejemplo, `1 + 2 + 3` se transforma a `plus(plus(1, 2), 3)`.
A veces esto no funciona de la manera que usted espera. Por ejemplo, `SELECT 4 > 2 > 3` resultará en 0.

Para la eficiencia, el `and` y `or` funciones aceptan cualquier número de argumentos. Las cadenas correspondientes de `AND` y `OR` operadores se transforman en una sola llamada de estas funciones.

## Comprobación de `NULL` {#checking-for-null}

ClickHouse soporta el `IS NULL` y `IS NOT NULL` operador.

### ES NULO {#operator-is-null}

-   Para [NULO](../data_types/nullable.md) valores de tipo, el `IS NULL` operador devuelve:
    -   `1` Español `NULL`.
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

### NO ES NULO {#is-not-null}

-   Para [NULO](../data_types/nullable.md) valores de tipo, el `IS NOT NULL` operador devuelve:
    -   `0` Español `NULL`.
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

[Artículo Original](https://clickhouse.tech/docs/es/query_language/operators/) <!--hide-->
