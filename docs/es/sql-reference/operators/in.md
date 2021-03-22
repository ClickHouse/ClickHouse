---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

### IN Operadores {#select-in-operators}

El `IN`, `NOT IN`, `GLOBAL IN`, y `GLOBAL NOT IN` están cubiertos por separado, ya que su funcionalidad es bastante rica.

El lado izquierdo del operador es una sola columna o una tupla.

Ejemplos:

``` sql
SELECT UserID IN (123, 456) FROM ...
SELECT (CounterID, UserID) IN ((34, 123), (101500, 456)) FROM ...
```

Si el lado izquierdo es una sola columna que está en el índice, y el lado derecho es un conjunto de constantes, el sistema usa el índice para procesar la consulta.

Don't list too many values explicitly (i.e. millions). If a data set is large, put it in a temporary table (for example, see the section “External data for query processing”), luego use una subconsulta.

El lado derecho del operador puede ser un conjunto de expresiones constantes, un conjunto de tuplas con expresiones constantes (mostradas en los ejemplos anteriores) o el nombre de una tabla de base de datos o subconsulta SELECT entre paréntesis.

Si el lado derecho del operador es el nombre de una tabla (por ejemplo, `UserID IN users`), esto es equivalente a la subconsulta `UserID IN (SELECT * FROM users)`. Úselo cuando trabaje con datos externos que se envían junto con la consulta. Por ejemplo, la consulta se puede enviar junto con un conjunto de ID de usuario ‘users’ tabla temporal, que debe ser filtrada.

Si el lado derecho del operador es un nombre de tabla que tiene el motor Set (un conjunto de datos preparado que siempre está en RAM), el conjunto de datos no se volverá a crear para cada consulta.

La subconsulta puede especificar más de una columna para filtrar tuplas.
Ejemplo:

``` sql
SELECT (CounterID, UserID) IN (SELECT CounterID, UserID FROM ...) FROM ...
```

Las columnas a la izquierda y a la derecha del operador IN deben tener el mismo tipo.

El operador IN y la subconsulta pueden aparecer en cualquier parte de la consulta, incluidas las funciones agregadas y las funciones lambda.
Ejemplo:

``` sql
SELECT
    EventDate,
    avg(UserID IN
    (
        SELECT UserID
        FROM test.hits
        WHERE EventDate = toDate('2014-03-17')
    )) AS ratio
FROM test.hits
GROUP BY EventDate
ORDER BY EventDate ASC
```

``` text
┌──EventDate─┬────ratio─┐
│ 2014-03-17 │        1 │
│ 2014-03-18 │ 0.807696 │
│ 2014-03-19 │ 0.755406 │
│ 2014-03-20 │ 0.723218 │
│ 2014-03-21 │ 0.697021 │
│ 2014-03-22 │ 0.647851 │
│ 2014-03-23 │ 0.648416 │
└────────────┴──────────┘
```

Para cada día después del 17 de marzo, cuente el porcentaje de páginas vistas realizadas por los usuarios que visitaron el sitio el 17 de marzo.
Una subconsulta en la cláusula IN siempre se ejecuta una sola vez en un único servidor. No hay subconsultas dependientes.

## Procesamiento NULL {#null-processing-1}

Durante el procesamiento de la solicitud, el operador IN asume que el resultado de una operación [NULL](../syntax.md#null-literal) siempre es igual a `0`, independientemente de si `NULL` está en el lado derecho o izquierdo del operador. `NULL` Los valores no se incluyen en ningún conjunto de datos, no se corresponden entre sí y no se pueden comparar.

Aquí hay un ejemplo con el `t_null` tabla:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Ejecución de la consulta `SELECT x FROM t_null WHERE y IN (NULL,3)` da el siguiente resultado:

``` text
┌─x─┐
│ 2 │
└───┘
```

Se puede ver que la fila en la que `y = NULL` se expulsa de los resultados de la consulta. Esto se debe a que ClickHouse no puede decidir si `NULL` está incluido en el `(NULL,3)` conjunto, devuelve `0` como resultado de la operación, y `SELECT` excluye esta fila de la salida final.

``` sql
SELECT y IN (NULL, 3)
FROM t_null
```

``` text
┌─in(y, tuple(NULL, 3))─┐
│                     0 │
│                     1 │
└───────────────────────┘
```

## Subconsultas distribuidas {#select-distributed-subqueries}

Hay dos opciones para IN-s con subconsultas (similar a JOINs): normal `IN` / `JOIN` y `GLOBAL IN` / `GLOBAL JOIN`. Se diferencian en cómo se ejecutan para el procesamiento de consultas distribuidas.

!!! attention "Atención"
    Recuerde que los algoritmos descritos a continuación pueden funcionar de manera diferente dependiendo de la [configuración](../../operations/settings/settings.md) `distributed_product_mode` configuración.

Cuando se utiliza el IN normal, la consulta se envía a servidores remotos, y cada uno de ellos ejecuta las subconsultas en el `IN` o `JOIN` clausula.

Cuando se utiliza `GLOBAL IN` / `GLOBAL JOINs`, primero todas las subconsultas se ejecutan para `GLOBAL IN` / `GLOBAL JOINs`, y los resultados se recopilan en tablas temporales. A continuación, las tablas temporales se envían a cada servidor remoto, donde las consultas se ejecutan utilizando estos datos temporales.

Para una consulta no distribuida, utilice el `IN` / `JOIN`.

Tenga cuidado al usar subconsultas en el `IN` / `JOIN` para el procesamiento de consultas distribuidas.

Veamos algunos ejemplos. Supongamos que cada servidor del clúster tiene un **local\_table**. Cada servidor también tiene un **distributed\_table** mesa con el **Distribuido** tipo, que mira todos los servidores del clúster.

Para una consulta al **distributed\_table**, la consulta se enviará a todos los servidores remotos y se ejecutará en ellos usando el **local\_table**.

Por ejemplo, la consulta

``` sql
SELECT uniq(UserID) FROM distributed_table
```

se enviará a todos los servidores remotos como

``` sql
SELECT uniq(UserID) FROM local_table
```

y ejecutar en cada uno de ellos en paralelo, hasta que llegue a la etapa donde se pueden combinar resultados intermedios. Luego, los resultados intermedios se devolverán al servidor solicitante y se fusionarán en él, y el resultado final se enviará al cliente.

Ahora examinemos una consulta con IN:

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

-   Cálculo de la intersección de audiencias de dos sitios.

Esta consulta se enviará a todos los servidores remotos como

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

En otras palabras, los datos establecidos en la cláusula IN se recopilarán en cada servidor de forma independiente, solo a través de los datos que se almacenan localmente en cada uno de los servidores.

Esto funcionará correctamente y de manera óptima si está preparado para este caso y ha distribuido datos en los servidores de clúster de modo que los datos de un único ID de usuario residen completamente en un único servidor. En este caso, todos los datos necesarios estarán disponibles localmente en cada servidor. De lo contrario, el resultado será inexacto. Nos referimos a esta variación de la consulta como “local IN”.

Para corregir cómo funciona la consulta cuando los datos se distribuyen aleatoriamente entre los servidores de clúster, puede especificar **distributed\_table** dentro de una subconsulta. La consulta se vería así:

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

Esta consulta se enviará a todos los servidores remotos como

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

La subconsulta comenzará a ejecutarse en cada servidor remoto. Dado que la subconsulta utiliza una tabla distribuida, la subconsulta que se encuentra en cada servidor remoto se reenviará a cada servidor remoto como

``` sql
SELECT UserID FROM local_table WHERE CounterID = 34
```

Por ejemplo, si tiene un clúster de 100 servidores, la ejecución de toda la consulta requerirá 10.000 solicitudes elementales, lo que generalmente se considera inaceptable.

En tales casos, siempre debe usar GLOBAL IN en lugar de IN. Veamos cómo funciona para la consulta

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID GLOBAL IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

El servidor del solicitante ejecutará la subconsulta

``` sql
SELECT UserID FROM distributed_table WHERE CounterID = 34
```

y el resultado se colocará en una tabla temporal en la RAM. A continuación, la solicitud se enviará a cada servidor remoto como

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID GLOBAL IN _data1
```

y la tabla temporal `_data1` se enviará a cada servidor remoto con la consulta (el nombre de la tabla temporal está definido por la implementación).

Esto es más óptimo que usar el IN normal. Sin embargo, tenga en cuenta los siguientes puntos:

1.  Al crear una tabla temporal, los datos no se hacen únicos. Para reducir el volumen de datos transmitidos a través de la red, especifique DISTINCT en la subconsulta. (No necesita hacer esto para un IN normal.)
2.  La tabla temporal se enviará a todos los servidores remotos. La transmisión no tiene en cuenta la topología de red. Por ejemplo, si 10 servidores remotos residen en un centro de datos que es muy remoto en relación con el servidor solicitante, los datos se enviarán 10 veces a través del canal al centro de datos remoto. Intente evitar grandes conjuntos de datos cuando use GLOBAL IN.
3.  Al transmitir datos a servidores remotos, las restricciones en el ancho de banda de la red no son configurables. Puede sobrecargar la red.
4.  Intente distribuir datos entre servidores para que no necesite usar GLOBAL IN de forma regular.
5.  Si necesita utilizar GLOBAL IN con frecuencia, planifique la ubicación del clúster ClickHouse para que un único grupo de réplicas resida en no más de un centro de datos con una red rápida entre ellos, de modo que una consulta se pueda procesar completamente dentro de un único centro de datos.

También tiene sentido especificar una tabla local en el `GLOBAL IN` cláusula, en caso de que esta tabla local solo esté disponible en el servidor solicitante y desee usar datos de ella en servidores remotos.
