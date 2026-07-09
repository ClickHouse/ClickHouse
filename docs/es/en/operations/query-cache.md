---
description: 'Guía para usar y configurar la caché de consultas en ClickHouse'
sidebar_label: 'Caché de consultas'
sidebar_position: 65
slug: /operations/query-cache
title: 'Caché de consultas'
doc_type: 'guide'
---

La caché de consultas permite ejecutar las consultas `SELECT` una sola vez y servir las ejecuciones posteriores de la misma consulta directamente desde la caché.
Según el tipo de consultas, esto puede reducir drásticamente la latencia y el consumo de recursos del servidor ClickHouse.

<div id="background-design-and-limitations">
  ## Antecedentes, diseño y limitaciones
</div>

Las cachés de consultas, por lo general, pueden considerarse transaccionalmente consistentes o inconsistentes.

* En las cachés transaccionalmente consistentes, la base de datos invalida (descarta) los resultados de consultas almacenados en caché si el resultado de la consulta `SELECT` cambia
  o puede llegar a cambiar. En ClickHouse, las operaciones que modifican los datos incluyen inserciones/actualizaciones/eliminaciones en/de tablas o merges
  de colapso. El almacenamiento en caché transaccionalmente consistente es especialmente adecuado para bases de datos OLTP, por ejemplo
  [MySQL](https://dev.mysql.com/doc/refman/5.6/en/query-cache.html) (que eliminó la caché de consultas a partir de la versión 8.0) y
  [Oracle](https://docs.oracle.com/database/121/TGDBA/tune_result_cache.htm).
* En las cachés transaccionalmente inconsistentes, se aceptan pequeñas imprecisiones en los resultados de las consultas, bajo el supuesto de que a todas las entradas de caché se les
  asigna un período de validez tras el cual expiran (p. ej., 1 minuto) y de que los datos subyacentes cambian muy poco durante ese período.
  Este enfoque es, en general, más adecuado para bases de datos OLAP. Como ejemplo de un caso en el que el almacenamiento en caché transaccionalmente inconsistente es suficiente,
  considere un informe horario de ventas en una herramienta de reporting al que varios usuarios acceden simultáneamente. Normalmente, los datos de ventas cambian
  lo bastante despacio como para que la base de datos solo tenga que calcular el informe una vez (representado por la primera consulta `SELECT`). Las consultas posteriores pueden
  servirse directamente desde la caché de consultas. En este ejemplo, un período de validez razonable podría ser de 30 min.

Tradicionalmente, el almacenamiento en caché transaccionalmente inconsistente lo proporcionan herramientas cliente o paquetes proxy (p. ej.,
[chproxy](https://www.chproxy.org/configuration/caching/)) que interactúan con la base de datos. Como resultado, la misma lógica de almacenamiento en caché y
configuración suele duplicarse. Con la caché de consultas de ClickHouse, la lógica de almacenamiento en caché pasa al lado del servidor. Esto reduce el esfuerzo de mantenimiento
y evita redundancias.

<div id="configuration-settings-and-usage">
  ## Ajustes de configuración y uso
</div>

:::note
En ClickHouse Cloud, debe usar la [configuración a nivel de consulta](/es/operations/settings/query-level) para editar la configuración de la caché de consultas. Actualmente, no se admite editar la [configuración a nivel de config](/es/operations/configuration-files).
:::

:::note
[clickhouse-local](utilities/clickhouse-local.md) ejecuta una sola consulta a la vez. Como no tiene sentido almacenar en caché los resultados de las consultas, la caché de
resultados de consultas está deshabilitada en clickhouse-local.
:::

La opción [use&#95;query&#95;cache](/es/operations/settings/settings#use_query_cache) puede usarse para controlar si una consulta específica o todas las consultas de la
sesión actual deben utilizar la caché de consultas. Por ejemplo, la primera ejecución de la consulta

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true;
```

almacenará el resultado de la consulta en la caché de consultas. Las ejecuciones posteriores de la misma consulta (también con el parámetro `use_query_cache = true`)
leerán el resultado calculado de la caché y lo devolverán de inmediato.

:::note
La opción `use_query_cache` y todas las demás opciones relacionadas con la caché de consultas solo surten efecto en sentencias `SELECT` independientes. En particular,
los resultados de `SELECT` sobre vistas creadas mediante `CREATE VIEW AS SELECT [...] SETTINGS use_query_cache = true` no se almacenan en caché a menos que la sentencia `SELECT`
se ejecute con `SETTINGS use_query_cache = true`.
:::

La forma en que se utiliza la caché puede configurarse con más detalle mediante las opciones [enable&#95;writes&#95;to&#95;query&#95;cache](/es/operations/settings/settings#enable_writes_to_query_cache)
y [enable&#95;reads&#95;from&#95;query&#95;cache](/es/operations/settings/settings#enable_reads_from_query_cache) (ambas con el valor predeterminado `true`). La primera opción
controla si los resultados de las consultas se almacenan en la caché, mientras que la segunda determina si la base de datos debe intentar recuperar resultados de consultas
de la caché. Por ejemplo, la siguiente consulta usará la caché solo de forma pasiva; es decir, intentará leer de ella, pero no almacenará en ella su
resultado:

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true, enable_writes_to_query_cache = false;
```

Para tener el máximo control, por lo general se recomienda proporcionar las opciones `use_query_cache`, `enable_writes_to_query_cache` y
`enable_reads_from_query_cache` solo en consultas específicas. También es posible habilitar el almacenamiento en caché a nivel de usuario o de perfil (p. ej., mediante `SET
use_query_cache = true`), pero debe tenerse en cuenta que, en ese caso, todas las consultas `SELECT` pueden devolver resultados almacenados en caché.

La caché de consultas puede borrarse con la sentencia `SYSTEM CLEAR QUERY CACHE`. El contenido de la caché de consultas se muestra en la tabla del sistema
[system.query&#95;cache](system-tables/query_cache.md). El número de aciertos y fallos de la caché de consultas desde el inicio de la base de datos se muestra como eventos
&quot;QueryCacheHits&quot; y &quot;QueryCacheMisses&quot; en la tabla del sistema [system.events](system-tables/events.md). Ambos contadores solo se actualizan para
consultas `SELECT` que se ejecutan con la opción `use_query_cache = true`; las demás consultas no afectan a &quot;QueryCacheMisses&quot;. El campo `query_cache_usage`
en la tabla del sistema [system.query&#95;log](system-tables/query_log.md) muestra, para cada consulta ejecutada, si el resultado de la consulta se escribió en
la caché de consultas o se leyó de ella. Las métricas `QueryCacheEntries` y `QueryCacheBytes` en la tabla del sistema
[system.metrics](system-tables/metrics.md) muestran cuántas entradas / bytes contiene actualmente la caché de consultas.

La caché de consultas existe una vez por cada proceso del servidor ClickHouse. Sin embargo, de forma predeterminada, los resultados almacenados en caché no se comparten entre usuarios. Esto puede
cambiarse (véase más abajo), pero no se recomienda hacerlo por motivos de seguridad.

Los resultados de las consultas se identifican en la caché de consultas mediante el [árbol de sintaxis abstracta (AST)](https://en.wikipedia.org/wiki/Abstract_syntax_tree) de
la consulta. Esto significa que el almacenamiento en caché no distingue entre mayúsculas y minúsculas; por ejemplo, `SELECT 1` y `select 1` se tratan como la misma consulta. Para
que la coincidencia sea más natural, todas las opciones a nivel de consulta relacionadas con la caché de consultas y el [formato de salida](settings/settings-formats.md))
se eliminan del AST.

Si la consulta se abortó debido a una excepción o a una cancelación del usuario, no se escribe ninguna entrada en la caché de consultas.

El tamaño de la caché de consultas en bytes, el número máximo de entradas de caché y el tamaño máximo de las entradas individuales de la caché (en bytes y en
registros) pueden configurarse mediante distintas [opciones de configuración del servidor](/es/operations/server-configuration-parameters/settings#query_cache).

```xml
<query_cache>
    <max_size_in_bytes>1073741824</max_size_in_bytes>
    <max_entries>1024</max_entries>
    <max_entry_size_in_bytes>1048576</max_entry_size_in_bytes>
    <max_entry_size_in_rows>30000000</max_entry_size_in_rows>
</query_cache>
```

También es posible limitar el uso de la caché para cada usuario mediante [perfiles de configuración](settings/settings-profiles.md) y [restricciones
de configuración](settings/constraints-on-settings.md). Más concretamente, puede restringir la cantidad máxima de memoria (en bytes) que un usuario puede
asignar en la caché de consultas y el número máximo de resultados de consulta almacenados. Para ello, primero defina las configuraciones
[query&#95;cache&#95;max&#95;size&#95;in&#95;bytes](/es/operations/settings/settings#query_cache_max_size_in_bytes) y
[query&#95;cache&#95;max&#95;entries](/es/operations/settings/settings#query_cache_max_entries) en un perfil de usuario en `users.xml`, y luego marque ambas configuraciones como
readonly:

```xml
<profiles>
    <default>
        <!-- The maximum cache size in bytes for user/profile 'default' -->
        <query_cache_max_size_in_bytes>10000</query_cache_max_size_in_bytes>
        <!-- The maximum number of SELECT query results stored in the cache for user/profile 'default' -->
        <query_cache_max_entries>100</query_cache_max_entries>
        <!-- Make both settings read-only so the user cannot change them -->
        <constraints>
            <query_cache_max_size_in_bytes>
                <readonly/>
            </query_cache_max_size_in_bytes>
            <query_cache_max_entries>
                <readonly/>
            <query_cache_max_entries>
        </constraints>
    </default>
</profiles>
```

Para definir cuánto tiempo debe ejecutarse como mínimo una consulta para que su resultado pueda almacenarse en caché, puede usar el ajuste
[query&#95;cache&#95;min&#95;query&#95;duration](/es/operations/settings/settings#query_cache_min_query_duration). Por ejemplo, el resultado de la consulta

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true, query_cache_min_query_duration = 5000;
```

solo se almacena en caché si la consulta tarda más de 5 segundos en ejecutarse. También es posible especificar cuántas veces debe ejecutarse una consulta antes de que su resultado quede
almacenado en caché; para ello, use la configuración [query&#95;cache&#95;min&#95;query&#95;runs](/es/operations/settings/settings#query_cache_min_query_runs).

Las entradas de la caché de consultas quedan obsoletas después de un determinado período de tiempo (time-to-live). De forma predeterminada, este período es de 60 segundos, pero se puede especificar un
valor distinto a nivel de sesión, perfil o consulta mediante la configuración [query&#95;cache&#95;ttl](/es/operations/settings/settings#query_cache_ttl). La caché de consultas elimina las entradas de forma &quot;perezosa&quot;,
es decir, cuando una entrada queda obsoleta, no se elimina inmediatamente de la caché. En su lugar, cuando se va a insertar una nueva entrada
en la caché de consultas, la base de datos comprueba si la caché tiene suficiente espacio libre para la nueva entrada. Si no es así,
la base de datos intenta eliminar todas las entradas obsoletas. Si la caché sigue sin tener suficiente espacio libre, la nueva entrada no se inserta.

Si la consulta se ejecuta a través de HTTP, ClickHouse establece los encabezados `Age` y `Expires` con la antigüedad (en segundos) y la marca de tiempo de expiración de la
entrada almacenada en caché.

Las entradas de la caché de consultas se comprimen de forma predeterminada. Esto reduce el consumo total de memoria a costa de escrituras y lecturas más lentas
en la caché de consultas. Para desactivar la compresión, use la configuración [query&#95;cache&#95;compress&#95;entries](/es/operations/settings/settings#query_cache_compress_entries).

A veces resulta útil mantener en caché varios resultados de la misma consulta. Esto se puede lograr mediante la configuración
[query&#95;cache&#95;tag](/es/operations/settings/settings#query_cache_tag), que actúa como una etiqueta (o espacio de nombres) para las entradas de la caché de consultas. La caché de consultas
considera distintos los resultados de la misma consulta con diferentes etiquetas.

Ejemplo para crear tres entradas diferentes en la caché de consultas para la misma consulta:

```sql
SELECT 1 SETTINGS use_query_cache = true; -- query_cache_tag is implicitly '' (empty string)
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'tag 1';
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'tag 2';
```

Para eliminar únicamente las entradas con la etiqueta `tag` de la caché de consultas, puede usar la sentencia `SYSTEM CLEAR QUERY CACHE TAG 'tag'`.

<div id="subquery-caching">
  ## Almacenamiento en caché de subconsultas
</div>

De forma predeterminada, `use_query_cache` en la consulta externa no se propaga a las subconsultas. Esto significa que cada subconsulta debe habilitar explícitamente el uso de la caché:

```sql
SELECT *
FROM (SELECT number FROM system.numbers LIMIT 1000 SETTINGS use_query_cache = true)
WHERE number > 500;
```

En este ejemplo, solo se almacena en caché el resultado de la subconsulta interna. La consulta externa no se almacena en caché.

Para habilitar el almacenamiento en caché de todas las subconsultas de una sola vez, use la configuración `query_cache_for_subqueries`:

```sql
SELECT *
FROM (SELECT number FROM system.numbers LIMIT 1000)
WHERE number > 500
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;
```

Para desactivar explícitamente la caché de una subconsulta concreta mientras la propagación masiva está habilitada, establezca `use_query_cache = false` en esa subconsulta:

```sql
SELECT *
FROM (SELECT number FROM system.numbers LIMIT 1000 SETTINGS use_query_cache = false)
WHERE number > 500
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;
```

Las entradas de la caché de subconsultas son visibles en [system.query&#95;cache](system-tables/query_cache.md) con `is_subquery = 1`. La configuración `query_cache_ttl` también se aplica a las entradas de la caché de subconsultas y puede establecerse para cada subconsulta.

ClickHouse lee los datos de las tablas en bloques de [max&#95;block&#95;size](/es/operations/settings/settings#max_block_size) filas. Debido al filtrado, la agregación,
etc., los bloques de resultados suelen ser mucho más pequeños que &#39;max&#95;block&#95;size&#39;, aunque también hay casos en los que son mucho más grandes. La configuración
[query&#95;cache&#95;squash&#95;partial&#95;results](/es/operations/settings/settings#query_cache_squash_partial_results) (habilitada de forma predeterminada) controla si los bloques de resultados
se compactan (si son muy pequeños) o se dividen (si son grandes) en bloques del tamaño de &#39;max&#95;block&#95;size&#39; antes de insertarse en la caché de resultados de consultas.
Esto reduce el rendimiento de las escrituras en la caché de consultas, pero mejora la tasa de compresión de las entradas de la caché y proporciona una
granularidad de bloques más natural cuando los resultados de las consultas se sirven posteriormente desde la caché de consultas.

Como resultado, la caché de consultas almacena múltiples bloques de
resultados (parciales) para cada consulta. Aunque este comportamiento es una buena opción predeterminada, puede desactivarse mediante la configuración
[query&#95;cache&#95;squash&#95;partial&#95;results](/es/operations/settings/settings#query_cache_squash_partial_results).

Además, los resultados de las consultas con funciones no deterministas no se almacenan en caché de forma predeterminada. Estas funciones incluyen:

* funciones para acceder a diccionarios: [`dictGet()`](/es/sql-reference/functions/ext-dict-functions), etc.
* [funciones definidas por el usuario](../sql-reference/statements/create/function.md) sin la etiqueta `<deterministic>true</deterministic>` en su definición
  XML,
* funciones que devuelven la fecha o la hora actual: [`now()`](../sql-reference/functions/date-time-functions.md#now),
  [`today()`](../sql-reference/functions/date-time-functions.md#today),
  [`yesterday()`](../sql-reference/functions/date-time-functions.md#yesterday), etc.,
* funciones que devuelven valores aleatorios: [`randomString()`](../sql-reference/functions/random-functions.md#randomString),
  [`fuzzBits()`](../sql-reference/functions/random-functions.md#fuzzBits), etc.,
* funciones cuyo resultado depende del tamaño, el orden o los fragmentos internos utilizados para el procesamiento de consultas:
  [`nowInBlock()`](../sql-reference/functions/date-time-functions.md#nowInBlock), etc.,
  [`rowNumberInBlock()`](../sql-reference/functions/other-functions.md#rowNumberInBlock),
  [`runningDifference()`](../sql-reference/functions/other-functions.md#runningDifference),
  [`blockSize()`](../sql-reference/functions/other-functions.md#blockSize), etc.,
* funciones que dependen del entorno: [`currentUser()`](../sql-reference/functions/other-functions.md#currentUser),
  [`queryID()`](/es/sql-reference/functions/other-functions#queryID),
  [`getMacro()`](../sql-reference/functions/other-functions.md#getMacro), etc.

Para forzar de todos modos el almacenamiento en caché de los resultados de consultas con funciones no deterministas, use la configuración
[query&#95;cache&#95;nondeterministic&#95;function&#95;handling](/es/operations/settings/settings#query_cache_nondeterministic_function_handling).

Los resultados de las consultas que involucran tablas del sistema (p. ej., [system.processes](system-tables/processes.md)&#96; o
[information&#95;schema.tables](system-tables/information_schema.md)) no se almacenan en caché de forma predeterminada. Para forzar de todos modos el almacenamiento en caché de los resultados de consultas con
tablas del sistema, use la configuración [query&#95;cache&#95;system&#95;table&#95;handling](/es/operations/settings/settings#query_cache_system_table_handling).

Por último, las entradas de la caché de consultas no se comparten entre usuarios por motivos de seguridad. Por ejemplo, el usuario A no debe poder eludir una
ROW POLICY en una tabla ejecutando la misma consulta que otro usuario B para el que no existe dicha política. Sin embargo, si es necesario, las entradas de caché pueden
marcarse como accesibles para otros usuarios (es decir, compartidas) especificando el ajuste
[query&#95;cache&#95;share&#95;between&#95;users](/es/operations/settings/settings#query_cache_share_between_users).

<div id="related-content">
  ## Contenido relacionado
</div>

* Blog: [Presentamos la caché de consultas de ClickHouse](https://clickhouse.com/blog/introduction-to-the-clickhouse-query-cache-and-design)