---
machine_translated: true
---

# Funciones para trabajar con fechas y horas {#functions-for-working-with-dates-and-times}

Soporte para zonas horarias

Todas las funciones para trabajar con la fecha y la hora que tienen un uso lógico para la zona horaria pueden aceptar un segundo argumento de zona horaria opcional. Ejemplo: Asia/Ekaterimburgo. En este caso, utilizan la zona horaria especificada en lugar de la local (predeterminada).

``` sql
SELECT
    toDateTime('2016-06-15 23:00:00') AS time,
    toDate(time) AS date_local,
    toDate(time, 'Asia/Yekaterinburg') AS date_yekat,
    toString(time, 'US/Samoa') AS time_samoa
```

``` text
┌────────────────time─┬─date_local─┬─date_yekat─┬─time_samoa──────────┐
│ 2016-06-15 23:00:00 │ 2016-06-15 │ 2016-06-16 │ 2016-06-15 09:00:00 │
└─────────────────────┴────────────┴────────────┴─────────────────────┘
```

Solo se admiten las zonas horarias que difieren de UTC por un número entero de horas.

## Todos los derechos reservados. {#totimezone}

Convierta la hora o la fecha y la hora a la zona horaria especificada.

## paraAño {#toyear}

Convierte una fecha o fecha con hora en un número UInt16 que contiene el número de año (AD).

## aTrimestre {#toquarter}

Convierte una fecha o fecha con hora en un número UInt8 que contiene el número de trimestre.

## ParaMes {#tomonth}

Convierte una fecha o fecha con hora en un número UInt8 que contiene el número de mes (1-12).

## Todos los derechos reservados. {#todayofyear}

Convierte una fecha o fecha con hora en un número UInt16 que contiene el número del día del año (1-366).

## Todos los derechos reservados. {#todayofmonth}

Convierte una fecha o fecha con hora en un número UInt8 que contiene el número del día del mes (1-31).

## Todos los derechos reservados. {#todayofweek}

Convierte una fecha o fecha con hora en un número UInt8 que contiene el número del día de la semana (el lunes es 1 y el domingo es 7).

## ParaHora {#tohour}

Convierte una fecha con hora en un número UInt8 que contiene el número de la hora en el tiempo de 24 horas (0-23).
Esta función supone que si los relojes se mueven hacia adelante, es de una hora y ocurre a las 2 a.m., y si los relojes se mueven hacia atrás, es de una hora y ocurre a las 3 a.m. (lo cual no siempre es cierto, incluso en Moscú los relojes se cambiaron dos veces en un momento diferente).

## ToMinute {#tominute}

Convierte una fecha con hora en un número UInt8 que contiene el número del minuto de la hora (0-59).

## aSegundo {#tosecond}

Convierte una fecha con hora en un número UInt8 que contiene el número del segundo en el minuto (0-59).
Los segundos de salto no se contabilizan.

## Todos los derechos reservados. {#to-unix-timestamp}

Para el argumento DateTime: convierte el valor a su representación numérica interna (Unix Timestamp).
Para el argumento String: analice la fecha y hora de la cadena de acuerdo con la zona horaria (segundo argumento opcional, la zona horaria del servidor se usa de forma predeterminada) y devuelve la marca de tiempo de Unix correspondiente.
Para el argumento Date : el comportamiento no está especificado.

**Sintaxis**

``` sql
toUnixTimestamp(datetime)
toUnixTimestamp(str, [timezone])
```

**Valor devuelto**

-   Devuelve la marca de tiempo de Unix.

Tipo: `UInt32`.

**Ejemplo**

Consulta:

``` sql
SELECT toUnixTimestamp('2017-11-05 08:07:47', 'Asia/Tokyo') AS unix_timestamp
```

Resultado:

``` text
┌─unix_timestamp─┐
│     1509836867 │
└────────────────┘
```

## Todos los derechos reservados. {#tostartofyear}

Redondea una fecha o fecha con la hora hasta el primer día del año.
Devuelve la fecha.

## Todos los derechos reservados. {#tostartofisoyear}

Redondea una fecha o fecha con la hora hasta el primer día del año ISO.
Devuelve la fecha.

## Todos los derechos reservados. {#tostartofquarter}

Redondea una fecha o fecha con la hora hasta el primer día del trimestre.
El primer día del trimestre es el 1 de enero, el 1 de abril, el 1 de julio o el 1 de octubre.
Devuelve la fecha.

## Todos los derechos reservados. {#tostartofmonth}

Redondea una fecha o fecha con la hora hasta el primer día del mes.
Devuelve la fecha.

!!! attention "Atención"
    El comportamiento de analizar fechas incorrectas es específico de la implementación. ClickHouse puede devolver una fecha cero, lanzar una excepción o hacer “natural” desbordamiento.

## paraLunes {#tomonday}

Redondea una fecha o fecha con hora al lunes más cercano.
Devuelve la fecha.

## ¿Cómo puedo hacerlo?\]) {#tostartofweektmode}

Redondea una fecha o fecha con hora al domingo o lunes más cercano por modo.
Devuelve la fecha.
El argumento mode funciona exactamente igual que el argumento mode a toWeek() . Para la sintaxis de argumento único, se utiliza un valor de modo de 0.

## Todos los derechos reservados. {#tostartofday}

Redondea una fecha con el tiempo hasta el comienzo del día.

## Todos los derechos reservados. {#tostartofhour}

Redondea una fecha con el tiempo hasta el comienzo de la hora.

## Todos los derechos reservados. {#tostartofminute}

Redondea una fecha con el tiempo hasta el inicio del minuto.

## Acerca de nosotros {#tostartoffiveminute}

Redondea una fecha con el tiempo hasta el inicio del intervalo de cinco minutos.

## Acerca de Nosotros {#tostartoftenminutes}

Redondea una fecha con el tiempo hasta el inicio del intervalo de diez minutos.

## Para comenzar de quince minutos {#tostartoffifteenminutes}

Redondea la fecha con el tiempo hasta el inicio del intervalo de quince minutos.

## También puede usar una interfaz de usuario.\]) {#tostartofintervaltime-or-data-interval-x-unit-time-zone}

Esta es una generalización de otras funciones llamadas `toStartOf*`. Por ejemplo,
`toStartOfInterval(t, INTERVAL 1 year)` devuelve lo mismo que `toStartOfYear(t)`,
`toStartOfInterval(t, INTERVAL 1 month)` devuelve lo mismo que `toStartOfMonth(t)`,
`toStartOfInterval(t, INTERVAL 1 day)` devuelve lo mismo que `toStartOfDay(t)`,
`toStartOfInterval(t, INTERVAL 15 minute)` devuelve lo mismo que `toStartOfFifteenMinutes(t)` sucesivamente.

## Tiempo {#totime}

Convierte una fecha con hora en una fecha fija determinada, preservando al mismo tiempo la hora.

## Todos los derechos reservados. {#torelativeyearnum}

Convierte una fecha con hora o fecha en el número del año, a partir de un determinado punto fijo en el pasado.

## Nombre de la red inalámbrica (SSID): {#torelativequarternum}

Convierte una fecha con hora o fecha en el número del trimestre, a partir de un determinado punto fijo en el pasado.

## Nombre de la red inalámbrica (SSID): {#torelativemonthnum}

Convierte una fecha con hora o fecha en el número del mes, a partir de un determinado punto fijo en el pasado.

## Nombre de la red inalámbrica (SSID): {#torelativeweeknum}

Convierte una fecha con hora o fecha en el número de la semana, a partir de un determinado punto fijo en el pasado.

## Nombre de la red inalámbrica (SSID): {#torelativedaynum}

Convierte una fecha con hora o fecha en el número del día, a partir de un determinado punto fijo en el pasado.

## Nombre de la red inalámbrica (SSID): {#torelativehournum}

Convierte una fecha con hora o fecha en el número de la hora, a partir de un determinado punto fijo en el pasado.

## Todos los derechos reservados. {#torelativeminutenum}

Convierte una fecha con hora o fecha en el número del minuto, a partir de un cierto punto fijo en el pasado.

## Todos los derechos reservados. {#torelativesecondnum}

Convierte una fecha con hora o fecha en el número de la segunda, a partir de un cierto punto fijo en el pasado.

## AISOAño {#toisoyear}

Convierte una fecha o fecha con hora en un número UInt16 que contiene el número ISO Year.

## paraISOWeek {#toisoweek}

Convierte una fecha o fecha con hora en un número UInt8 que contiene el número de semana ISO.

## ParaSemana(fecha\[,modo\]) {#toweekdatemode}

Esta función devuelve el número de semana para la fecha o la fecha y hora. La forma de dos argumentos de toWeek() le permite especificar si la semana comienza el domingo o el lunes y si el valor de retorno debe estar en el rango de 0 a 53 o de 1 a 53. Si se omite el argumento mode, el modo predeterminado es 0.
`toISOWeek()`es una función de compatibilidad que es equivalente a `toWeek(date,3)`.
La siguiente tabla describe cómo funciona el argumento mode.

| Modo       | Primer día de la semana | Gama             | Semana 1 es la primera semana … |
|------------|-------------------------|------------------|---------------------------------|
| Cero       | Domingo                 | 0-53             | con un domingo de este año      |
| Uno        | Lunes                   | 0-53             | con 4 o más días este año       |
| Cómo hacer | Domingo                 | Sistema abierto. | con un domingo de este año      |
| Cómo hacer | Lunes                   | Sistema abierto. | con 4 o más días este año       |
| Cuatro     | Domingo                 | 0-53             | con 4 o más días este año       |
| Cinco      | Lunes                   | 0-53             | con un lunes de este año        |
| Código     | Domingo                 | Sistema abierto. | con 4 o más días este año       |
| Siete      | Lunes                   | Sistema abierto. | con un lunes de este año        |
| Ocho       | Domingo                 | Sistema abierto. | contiene 1 de enero             |
| Nueve      | Lunes                   | Sistema abierto. | contiene 1 de enero             |

Para valores de modo con un significado de “with 4 or more days this year,” semanas están numeradas según ISO 8601:1988:

-   Si la semana que contiene el 1 de enero tiene 4 o más días en el nuevo año, es la semana 1.

-   De lo contrario, es la última semana del año anterior, y la semana siguiente es la semana 1.

Para valores de modo con un significado de “contains January 1”, la semana contiene 1 de enero es la semana 1. No importa cuántos días en el nuevo año la semana contenía, incluso si contenía sólo un día.

``` sql
toWeek(date, [, mode][, Timezone])
```

**Parámetros**

-   `date` – Fecha o DateTime.
-   `mode` – Parámetro opcional, Rango de valores es \[0,9\], por defecto es 0.
-   `Timezone` – Parámetro opcional, se comporta como cualquier otra función de conversión.

**Ejemplo**

``` sql
SELECT toDate('2016-12-27') AS date, toWeek(date) AS week0, toWeek(date,1) AS week1, toWeek(date,9) AS week9;
```

``` text
┌───────date─┬─week0─┬─week1─┬─week9─┐
│ 2016-12-27 │    52 │    52 │     1 │
└────────────┴───────┴───────┴───────┘
```

## aYearWeek(fecha\[,modo\]) {#toyearweekdatemode}

Devuelve año y semana para una fecha. El año en el resultado puede ser diferente del año en el argumento de fecha para la primera y la última semana del año.

El argumento mode funciona exactamente igual que el argumento mode a toWeek() . Para la sintaxis de argumento único, se utiliza un valor de modo de 0.

`toISOYear()`es una función de compatibilidad que es equivalente a `intDiv(toYearWeek(date,3),100)`.

**Ejemplo**

``` sql
SELECT toDate('2016-12-27') AS date, toYearWeek(date) AS yearWeek0, toYearWeek(date,1) AS yearWeek1, toYearWeek(date,9) AS yearWeek9;
```

``` text
┌───────date─┬─yearWeek0─┬─yearWeek1─┬─yearWeek9─┐
│ 2016-12-27 │    201652 │    201652 │    201701 │
└────────────┴───────────┴───────────┴───────────┘
```

## ahora {#now}

Acepta cero argumentos y devuelve la hora actual en uno de los momentos de ejecución de la solicitud.
Esta función devuelve una constante, incluso si la solicitud tardó mucho en completarse.

## Hoy {#today}

Acepta cero argumentos y devuelve la fecha actual en uno de los momentos de ejecución de la solicitud.
Lo mismo que ‘toDate(now())’.

## enfermería {#yesterday}

Acepta cero argumentos y devuelve la fecha de ayer en uno de los momentos de ejecución de la solicitud.
Lo mismo que ‘today() - 1’.

## timeSlot {#timeslot}

Redondea el tiempo a la media hora.
Esta función es específica de Yandex.Metrica, ya que media hora es la cantidad mínima de tiempo para dividir una sesión en dos sesiones si una etiqueta de seguimiento muestra las páginas vistas consecutivas de un solo usuario que difieren en el tiempo en estrictamente más de esta cantidad. Esto significa que las tuplas (el ID de etiqueta, el ID de usuario y el intervalo de tiempo) se pueden usar para buscar páginas vistas que se incluyen en la sesión correspondiente.

## paraYYYYMM {#toyyyymm}

Convierte una fecha o fecha con hora en un número UInt32 que contiene el número de año y mes (YYYY \* 100 + MM).

## paraYYYYMMDD {#toyyyymmdd}

Convierte una fecha o fecha con hora en un número UInt32 que contiene el número de año y mes (AAAA \* 10000 + MM \* 100 + DD).

## paraYYYYMMDDhhmmss {#toyyyymmddhhmmss}

Convierte una fecha o fecha con hora en un número UInt64 que contiene el número de año y mes (YYYY \* 10000000000 + MM \* 100000000 + DD \* 1000000 + hh \* 10000 + mm \* 100 + ss).

## Por ejemplo, en el caso de que el usuario pueda acceder a la página de inicio de sesión, seleccione la página de inicio de sesión. {#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters}

La función agrega un intervalo de fecha / fecha y hora a una fecha / fecha y hora y luego devuelve la fecha / fecha y hora. Por ejemplo:

``` sql
WITH
    toDate('2018-01-01') AS date,
    toDateTime('2018-01-01 00:00:00') AS date_time
SELECT
    addYears(date, 1) AS add_years_with_date,
    addYears(date_time, 1) AS add_years_with_date_time
```

``` text
┌─add_years_with_date─┬─add_years_with_date_time─┐
│          2019-01-01 │      2019-01-01 00:00:00 │
└─────────────────────┴──────────────────────────┘
```

## restarAños, restarMeses, restarSemanas, restarDías, restarHoras, restarMinutos, restarSegundos, restarCuartos {#subtractyears-subtractmonths-subtractweeks-subtractdays-subtracthours-subtractminutes-subtractseconds-subtractquarters}

La función resta un intervalo de fecha / fecha y hora a una fecha / fecha y hora y luego devuelve la fecha / fecha y hora. Por ejemplo:

``` sql
WITH
    toDate('2019-01-01') AS date,
    toDateTime('2019-01-01 00:00:00') AS date_time
SELECT
    subtractYears(date, 1) AS subtract_years_with_date,
    subtractYears(date_time, 1) AS subtract_years_with_date_time
```

``` text
┌─subtract_years_with_date─┬─subtract_years_with_date_time─┐
│               2018-01-01 │           2018-01-01 00:00:00 │
└──────────────────────────┴───────────────────────────────┘
```

## dateDiff {#datediff}

Devuelve la diferencia entre dos valores Date o DateTime.

**Sintaxis**

``` sql
dateDiff('unit', startdate, enddate, [timezone])
```

**Parámetros**

-   `unit` — Unidad de tiempo, en la que se expresa el valor devuelto. [Cadena](../syntax.md#syntax-string-literal).

        Supported values:

        | unit   |
        | ------ |
        |second  |
        |minute  |
        |hour    |
        |day     |
        |week    |
        |month   |
        |quarter |
        |year    |

-   `startdate` — El primer valor de tiempo para comparar. [Fecha](../../data_types/date.md) o [FechaHora](../../data_types/datetime.md).

-   `enddate` — El segundo valor de tiempo para comparar. [Fecha](../../data_types/date.md) o [FechaHora](../../data_types/datetime.md).

-   `timezone` — Parámetro opcional. Si se especifica, se aplica a ambos `startdate` y `enddate`. Si no se especifica, las zonas horarias `startdate` y `enddate` se utilizan. Si no son lo mismo, el resultado no está especificado.

**Valor devuelto**

Diferencia entre `startdate` y `enddate` expresado en `unit`.

Tipo: `int`.

**Ejemplo**

Consulta:

``` sql
SELECT dateDiff('hour', toDateTime('2018-01-01 22:00:00'), toDateTime('2018-01-02 23:00:00'));
```

Resultado:

``` text
┌─dateDiff('hour', toDateTime('2018-01-01 22:00:00'), toDateTime('2018-01-02 23:00:00'))─┐
│                                                                                     25 │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

## timeSlots(Hora de inicio, Duración, \[, Tamaño\]) {#timeslotsstarttime-duration-size}

Para un intervalo de tiempo a partir de ‘StartTime’ y continuando por ‘Duration’ segundos, devuelve una matriz de momentos en el tiempo, que consiste en puntos de este intervalo ‘Size’ en segundos. ‘Size’ es un parámetro opcional: una constante UInt32, establecida en 1800 por defecto.
Por ejemplo, `timeSlots(toDateTime('2012-01-01 12:20:00'), 600) = [toDateTime('2012-01-01 12:00:00'), toDateTime('2012-01-01 12:30:00')]`.
Esto es necesario para buscar páginas vistas en la sesión correspondiente.

## FormatDateTime(Hora, Formato\[, Zona horaria\]) {#formatdatetime}

La función da formato a una hora según una cadena de formato dada. NB: El formato es una expresión constante, por ejemplo, no puede tener múltiples formatos para una sola columna de resultado.

Modificadores compatibles para Formato:
(“Example” columna muestra el resultado de formateo para el tiempo `2018-01-02 22:33:44`)

| Modificador      | Descripción                                                      | Ejemplo                          |
|------------------|------------------------------------------------------------------|----------------------------------|
| %C               | año dividido por 100 y truncado a entero (00-99)                 | Veinte                           |
| %d               | día del mes, cero acolchado (01-31)                              | Bienvenido                       |
| %D               | Fecha corta de MM/DD/YY, equivalente a %m/%d/%a                  | Método de codificación de datos: |
| %e               | día del mes, espacio acolchado ( 1-31)                           | Cómo hacer                       |
| Categoría        | fecha corta AAAA-MM-DD, equivalente de la onu %Y-%m-%d           | Sistema abierto.                 |
| %H               | hora en formato 24h (00-23)                                      | Veintidos                        |
| %Me              | hora en formato 12h (01-12)                                      | Diez                             |
| %j               | día del año (001-366)                                            | Categoría                        |
| Más información  | mes como un número decimal (01-12)                               | Acerca de                        |
| Más información  | minutos (00-59)                                                  | Sistema abierto.                 |
| %y               | carácter de nueva línea («»)                                     |                                  |
| %p               | Designación AM o PM                                              | PM                               |
| %R               | HH de 24 horas: Tiempo del milímetro, equivalente a %H: %M       | 22:33                            |
| %S               | segundo (00-59)                                                  | Sistema abierto.                 |
| % t              | carácter de pestaña horizontal (')                               |                                  |
| Tipo de artículo | Formato de hora ISO 8601 (HH:MM:SS), equivalente a %H:%M:%S      | 22:33:44                         |
| %u               | ISO 8601 día de la semana como número con el lunes como 1 (1-7)  | Cómo hacer                       |
| %V               | Número de semana ISO 8601 (01-53)                                | Acerca de                        |
| % w              | día de la semana como un número decimal con domingo como 0 (0-6) | Cómo hacer                       |
| %y               | Año, últimos dos dígitos (00-99)                                 | Acerca de                        |
| %Y               | Año                                                              | 2018 Nueva York                  |
| %%               | Por qué?                                                         | %                                |

[Artículo Original](https://clickhouse.tech/docs/es/query_language/functions/date_time_functions/) <!--hide-->
