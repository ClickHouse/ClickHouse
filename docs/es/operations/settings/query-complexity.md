---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 59
toc_title: Restricciones en la complejidad de consultas
---

# Restricciones en la complejidad de consultas {#restrictions-on-query-complexity}

Las restricciones en la complejidad de la consulta forman parte de la configuración.
Se utilizan para proporcionar una ejecución más segura desde la interfaz de usuario.
Casi todas las restricciones solo se aplican a `SELECT`. Para el procesamiento de consultas distribuidas, las restricciones se aplican en cada servidor por separado.

ClickHouse comprueba las restricciones para las partes de datos, no para cada fila. Significa que puede exceder el valor de restricción con el tamaño de la parte de datos.

Restricciones en el “maximum amount of something” puede tomar el valor 0, lo que significa “unrestricted”.
La mayoría de las restricciones también tienen un ‘overflow_mode’ establecer, lo que significa qué hacer cuando se excede el límite.
Puede tomar uno de dos valores: `throw` o `break`. Las restricciones en la agregación (group_by_overflow_mode) también tienen el valor `any`.

`throw` – Throw an exception (default).

`break` – Stop executing the query and return the partial result, as if the source data ran out.

`any (only for group_by_overflow_mode)` – Continuing aggregation for the keys that got into the set, but don't add new keys to the set.

## Método de codificación de datos: {#settings_max_memory_usage}

La cantidad máxima de RAM que se utiliza para ejecutar una consulta en un único servidor.

En el archivo de configuración predeterminado, el máximo es de 10 GB.

La configuración no tiene en cuenta el volumen de memoria disponible ni el volumen total de memoria en la máquina.
La restricción se aplica a una sola consulta dentro de un único servidor.
Usted puede utilizar `SHOW PROCESSLIST` para ver el consumo de memoria actual para cada consulta.
Además, el consumo máximo de memoria se rastrea para cada consulta y se escribe en el registro.

El uso de memoria no se supervisa para los estados de ciertas funciones agregadas.

El uso de memoria no se realiza un seguimiento completo de los estados de las funciones agregadas `min`, `max`, `any`, `anyLast`, `argMin`, `argMax` de `String` y `Array` argumento.

El consumo de memoria también está restringido por los parámetros `max_memory_usage_for_user` y `max_memory_usage_for_all_queries`.

## Max_memory_usage_for_user {#max-memory-usage-for-user}

La cantidad máxima de RAM que se utilizará para ejecutar las consultas de un usuario en un único servidor.

Los valores predeterminados se definen en [Configuración.h](https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Settings.h#L288). De forma predeterminada, el importe no está restringido (`max_memory_usage_for_user = 0`).

Ver también la descripción de [Método de codificación de datos:](#settings_max_memory_usage).

## Todos los derechos reservados {#max-memory-usage-for-all-queries}

La cantidad máxima de RAM que se utilizará para ejecutar todas las consultas en un único servidor.

Los valores predeterminados se definen en [Configuración.h](https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Settings.h#L289). De forma predeterminada, el importe no está restringido (`max_memory_usage_for_all_queries = 0`).

Ver también la descripción de [Método de codificación de datos:](#settings_max_memory_usage).

## ¿Qué puedes encontrar en Neodigit {#max-rows-to-read}

Las siguientes restricciones se pueden verificar en cada bloque (en lugar de en cada fila). Es decir, las restricciones se pueden romper un poco.

Un número máximo de filas que se pueden leer de una tabla al ejecutar una consulta.

## ¿Qué puedes encontrar en Neodigit {#max-bytes-to-read}

Un número máximo de bytes (datos sin comprimir) que se pueden leer de una tabla al ejecutar una consulta.

## Método de codificación de datos: {#read-overflow-mode}

Qué hacer cuando el volumen de datos leídos excede uno de los límites: ‘throw’ o ‘break’. Por defecto, throw.

## Método de codificación de datos: {#settings-max-rows-to-group-by}

Un número máximo de claves únicas recibidas de la agregación. Esta configuración le permite limitar el consumo de memoria al agregar.

## Grupo_by_overflow_mode {#group-by-overflow-mode}

Qué hacer cuando el número de claves únicas para la agregación excede el límite: ‘throw’, ‘break’, o ‘any’. Por defecto, throw.
Uso de la ‘any’ valor le permite ejecutar una aproximación de GROUP BY. La calidad de esta aproximación depende de la naturaleza estadística de los datos.

## max_bytes_before_external_group_by {#settings-max_bytes_before_external_group_by}

Habilita o deshabilita la ejecución de `GROUP BY` en la memoria externa. Ver [GROUP BY en memoria externa](../../sql-reference/statements/select/group-by.md#select-group-by-in-external-memory).

Valores posibles:

-   Volumen máximo de RAM (en bytes) que puede ser utilizado por el único [GROUP BY](../../sql-reference/statements/select/group-by.md#select-group-by-clause) operación.
-   0 — `GROUP BY` en la memoria externa deshabilitada.

Valor predeterminado: 0.

## Método de codificación de datos: {#max-rows-to-sort}

Un número máximo de filas antes de ordenar. Esto le permite limitar el consumo de memoria al ordenar.

## Método de codificación de datos: {#max-bytes-to-sort}

Un número máximo de bytes antes de ordenar.

## sort_overflow_mode {#sort-overflow-mode}

Qué hacer si el número de filas recibidas antes de ordenar excede uno de los límites: ‘throw’ o ‘break’. Por defecto, throw.

## max_result_rows {#setting-max_result_rows}

Límite en el número de filas en el resultado. También se comprueba si hay subconsultas y en servidores remotos cuando se ejecutan partes de una consulta distribuida.

## max_result_bytes {#max-result-bytes}

Límite en el número de bytes en el resultado. Lo mismo que el ajuste anterior.

## result_overflow_mode {#result-overflow-mode}

Qué hacer si el volumen del resultado excede uno de los límites: ‘throw’ o ‘break’. Por defecto, throw.

Utilizar ‘break’ es similar a usar LIMIT. `Break` interrumpe la ejecución sólo en el nivel de bloque. Esto significa que la cantidad de filas devueltas es mayor que [max_result_rows](#setting-max_result_rows), múltiplo de [max_block_size](settings.md#setting-max_block_size) y depende de [max_threads](settings.md#settings-max_threads).

Ejemplo:

``` sql
SET max_threads = 3, max_block_size = 3333;
SET max_result_rows = 3334, result_overflow_mode = 'break';

SELECT *
FROM numbers_mt(100000)
FORMAT Null;
```

Resultado:

``` text
6666 rows in set. ...
```

## max_execution_time {#max-execution-time}

Tiempo máximo de ejecución de la consulta en segundos.
En este momento, no se comprueba una de las etapas de clasificación, o al fusionar y finalizar funciones agregadas.

## timeout_overflow_mode {#timeout-overflow-mode}

Qué hacer si la consulta se ejecuta más de ‘max_execution_time’: ‘throw’ o ‘break’. Por defecto, throw.

## Método de codificación de datos: {#min-execution-speed}

Velocidad de ejecución mínima en filas por segundo. Comprobado en cada bloque de datos cuando ‘timeout_before_checking_execution_speed’ expirar. Si la velocidad de ejecución es menor, se produce una excepción.

## Todos los derechos reservados {#min-execution-speed-bytes}

Un número mínimo de bytes de ejecución por segundo. Comprobado en cada bloque de datos cuando ‘timeout_before_checking_execution_speed’ expirar. Si la velocidad de ejecución es menor, se produce una excepción.

## Max_execution_speed {#max-execution-speed}

Un número máximo de filas de ejecución por segundo. Comprobado en cada bloque de datos cuando ‘timeout_before_checking_execution_speed’ expirar. Si la velocidad de ejecución es alta, la velocidad de ejecución se reducirá.

## Max_execution_speed_bytes {#max-execution-speed-bytes}

Un número máximo de bytes de ejecución por segundo. Comprobado en cada bloque de datos cuando ‘timeout_before_checking_execution_speed’ expirar. Si la velocidad de ejecución es alta, la velocidad de ejecución se reducirá.

## Tiempo de espera antes de comprobar_ejecución_velocidad {#timeout-before-checking-execution-speed}

Comprueba que la velocidad de ejecución no sea demasiado lenta (no menos de ‘min_execution_speed’), después de que el tiempo especificado en segundos haya expirado.

## Max_columns_to_read {#max-columns-to-read}

Un número máximo de columnas que se pueden leer de una tabla en una sola consulta. Si una consulta requiere leer un mayor número de columnas, produce una excepción.

## max_temporary_columns {#max-temporary-columns}

Un número máximo de columnas temporales que se deben mantener en la memoria RAM al mismo tiempo cuando se ejecuta una consulta, incluidas las columnas constantes. Si hay más columnas temporales que esto, arroja una excepción.

## max_temporary_non_const_columns {#max-temporary-non-const-columns}

Lo mismo que ‘max_temporary_columns’, pero sin contar columnas constantes.
Tenga en cuenta que las columnas constantes se forman con bastante frecuencia cuando se ejecuta una consulta, pero requieren aproximadamente cero recursos informáticos.

## max_subquery_depth {#max-subquery-depth}

Profundidad máxima de anidamiento de subconsultas. Si las subconsultas son más profundas, se produce una excepción. De forma predeterminada, 100.

## max_pipeline_depth {#max-pipeline-depth}

Profundidad máxima de la tubería. Corresponde al número de transformaciones que realiza cada bloque de datos durante el procesamiento de consultas. Contado dentro de los límites de un único servidor. Si la profundidad de la canalización es mayor, se produce una excepción. Por defecto, 1000.

## max_ast_depth {#max-ast-depth}

Profundidad máxima de anidamiento de un árbol sintáctico de consulta. Si se supera, se produce una excepción.
En este momento, no se verifica durante el análisis, sino solo después de analizar la consulta. Es decir, se puede crear un árbol sintáctico demasiado profundo durante el análisis, pero la consulta fallará. Por defecto, 1000.

## max_ast_elements {#max-ast-elements}

Un número máximo de elementos en un árbol sintáctico de consulta. Si se supera, se produce una excepción.
De la misma manera que la configuración anterior, se verifica solo después de analizar la consulta. De forma predeterminada, 50.000.

## Método de codificación de datos: {#max-rows-in-set}

Un número máximo de filas para un conjunto de datos en la cláusula IN creada a partir de una subconsulta.

## Método de codificación de datos: {#max-bytes-in-set}

Número máximo de bytes (datos sin comprimir) utilizados por un conjunto en la cláusula IN creada a partir de una subconsulta.

## set_overflow_mode {#set-overflow-mode}

Qué hacer cuando la cantidad de datos excede uno de los límites: ‘throw’ o ‘break’. Por defecto, throw.

## Método de codificación de datos: {#max-rows-in-distinct}

Un número máximo de filas diferentes al usar DISTINCT.

## Método de codificación de datos: {#max-bytes-in-distinct}

Un número máximo de bytes utilizados por una tabla hash cuando se utiliza DISTINCT.

## distinct_overflow_mode {#distinct-overflow-mode}

Qué hacer cuando la cantidad de datos excede uno de los límites: ‘throw’ o ‘break’. Por defecto, throw.

## max_rows_to_transfer {#max-rows-to-transfer}

Un número máximo de filas que se pueden pasar a un servidor remoto o guardar en una tabla temporal cuando se utiliza GLOBAL IN.

## max_bytes_to_transfer {#max-bytes-to-transfer}

Un número máximo de bytes (datos sin comprimir) que se pueden pasar a un servidor remoto o guardar en una tabla temporal cuando se utiliza GLOBAL IN.

## transfer_overflow_mode {#transfer-overflow-mode}

Qué hacer cuando la cantidad de datos excede uno de los límites: ‘throw’ o ‘break’. Por defecto, throw.

## Método de codificación de datos: {#settings-max_rows_in_join}

Limita el número de filas de la tabla hash que se utiliza al unir tablas.

Esta configuración se aplica a [SELECT … JOIN](../../sql-reference/statements/select/join.md#select-join) operaciones y la [Unir](../../engines/table-engines/special/join.md) motor de mesa.

Si una consulta contiene varias combinaciones, ClickHouse comprueba esta configuración para cada resultado intermedio.

ClickHouse puede proceder con diferentes acciones cuando se alcanza el límite. Utilice el [join_overflow_mode](#settings-join_overflow_mode) configuración para elegir la acción.

Valores posibles:

-   Entero positivo.
-   0 — Unlimited number of rows.

Valor predeterminado: 0.

## Método de codificación de datos: {#settings-max_bytes_in_join}

Limita el tamaño en bytes de la tabla hash utilizada al unir tablas.

Esta configuración se aplica a [SELECT … JOIN](../../sql-reference/statements/select/join.md#select-join) operaciones y [Unirse al motor de tabla](../../engines/table-engines/special/join.md).

Si la consulta contiene combinaciones, ClickHouse comprueba esta configuración para cada resultado intermedio.

ClickHouse puede proceder con diferentes acciones cuando se alcanza el límite. Utilizar [join_overflow_mode](#settings-join_overflow_mode) para elegir la acción.

Valores posibles:

-   Entero positivo.
-   0 — Memory control is disabled.

Valor predeterminado: 0.

## join_overflow_mode {#settings-join_overflow_mode}

Define qué acción realiza ClickHouse cuando se alcanza cualquiera de los siguientes límites de combinación:

-   [Método de codificación de datos:](#settings-max_bytes_in_join)
-   [Método de codificación de datos:](#settings-max_rows_in_join)

Valores posibles:

-   `THROW` — ClickHouse throws an exception and breaks operation.
-   `BREAK` — ClickHouse breaks operation and doesn't throw an exception.

Valor predeterminado: `THROW`.

**Ver también**

-   [Cláusula JOIN](../../sql-reference/statements/select/join.md#select-join)
-   [Unirse al motor de tabla](../../engines/table-engines/special/join.md)

## max_partitions_per_insert_block {#max-partitions-per-insert-block}

Limita el número máximo de particiones en un único bloque insertado.

-   Entero positivo.
-   0 — Unlimited number of partitions.

Valor predeterminado: 100.

**Detalles**

Al insertar datos, ClickHouse calcula el número de particiones en el bloque insertado. Si el número de particiones es mayor que `max_partitions_per_insert_block`, ClickHouse lanza una excepción con el siguiente texto:

> “Too many partitions for single INSERT block (more than” ¿Cómo puedo hacerlo? “). The limit is controlled by ‘max_partitions_per_insert_block’ setting. A large number of partitions is a common misconception. It will lead to severe negative performance impact, including slow server startup, slow INSERT queries and slow SELECT queries. Recommended total number of partitions for a table is under 1000..10000. Please note, that partitioning is not intended to speed up SELECT queries (ORDER BY key is sufficient to make range queries fast). Partitions are intended for data manipulation (DROP PARTITION, etc).”

[Artículo Original](https://clickhouse.tech/docs/en/operations/settings/query_complexity/) <!--hide-->
