---
description: 'Documentación sobre CREATE VIEW'
sidebar_label: 'VIEW'
sidebar_position: 37
slug: /sql-reference/statements/create/view
title: 'CREATE VIEW'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import DeprecatedBadge from '@theme/badges/DeprecatedBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="create-view">
  # CREATE VIEW
</div>

Crea una nueva vista. Las vistas pueden ser [normales](#normal-view), [materializadas](#materialized-view), [materializadas actualizables](#refreshable-materialized-view) y [de ventana](/es/sql-reference/statements/create/view#window-view).

<div id="normal-view">
  ## Vista normal
</div>

Sintaxis:

```sql
CREATE [OR REPLACE] VIEW [IF NOT EXISTS] [db.]table_name [(alias1 [, alias2 ...])] [ON CLUSTER cluster_name]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | INVOKER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

Las vistas normales no almacenan ningún dato. Simplemente leen de otra tabla en cada acceso. En otras palabras, una vista normal no es más que una consulta guardada. Al leer de una vista, esta consulta guardada se usa como subconsulta en la cláusula [FROM](../../../sql-reference/statements/select/from.md).

Como ejemplo, supongamos que has creado una vista:

```sql
CREATE VIEW view AS SELECT ...
```

y haber escrito una consulta:

```sql
SELECT a, b, c FROM view
```

Esta consulta es completamente equivalente a usar la subconsulta:

```sql
SELECT a, b, c FROM (SELECT ...)
```

<div id="parameterized-view">
  ## Vista parametrizada
</div>

Las vistas parametrizadas son similares a las vistas normales, pero pueden crearse con parámetros que no se resuelven de forma inmediata. Estas vistas pueden usarse con funciones de tabla, que especifican el nombre de la vista como nombre de la función y los valores de los parámetros como argumentos.

```sql
CREATE VIEW view AS SELECT * FROM TABLE WHERE Column1={column1:datatype1} and Column2={column2:datatype2} ...
```

Lo anterior crea una vista para una tabla que puede usarse como función de tabla al sustituir los parámetros, como se muestra a continuación.

```sql
SELECT * FROM view(column1=value1, column2=value2 ...)
```

<div id="materialized-view">
  ## Vista materializada
</div>

```sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster_name] [TO[db.]name [(columns)]] [ENGINE = engine] [POPULATE]
[REFRESH ...]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

```sql
CREATE OR REPLACE MATERIALIZED VIEW [db.]table_name [ON CLUSTER cluster_name] [TO[db.]name [(columns)]] [ENGINE = engine] [POPULATE]
[REFRESH ...]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

`OR REPLACE` e `IF NOT EXISTS` se excluyen mutuamente: combinarlos produce un error de sintaxis.

<div id="create-or-replace-materialized-view">
  ### CREATE OR REPLACE MATERIALIZED VIEW
</div>

`CREATE OR REPLACE MATERIALIZED VIEW` reemplaza de forma atómica una vista materializada existente y su tabla de almacenamiento interna asociada (si existe). La operación requiere un motor de base de datos `Atomic` o `Replicated`.

```sql
CREATE OR REPLACE MATERIALIZED VIEW [db.]name [ON CLUSTER cluster]
[TO [db.]target_table]
[ENGINE = engine]
[POPULATE]
[REFRESH ...]
AS SELECT ...
```

Comportamientos clave:

* **Sin cláusula `TO`**: se elimina la tabla interna anterior y se crea una nueva. Los datos existentes en la tabla interna se pierden, salvo que se especifique `POPULATE`.
* **Con cláusula `TO`**: solo se reemplaza la definición de la vista; la tabla de destino y sus datos no se ven afectados.
* Es compatible con `REFRESH`, `ON CLUSTER` y todas las opciones de motor. `POPULATE` solo es compatible con bases de datos `Atomic`; se rechaza en bases de datos `Replicated` (consulta la nota sobre `POPULATE` más abajo).
* Requiere los privilegios `CREATE VIEW` y `DROP VIEW`.

:::note
`CREATE OR REPLACE MATERIALIZED VIEW` solo es compatible con motores de base de datos `Atomic` o `Replicated`. No es compatible con el motor de base de datos `Ordinary`.
:::

**Ejemplos:**

```sql
-- Create a materialized view with an inner table
CREATE OR REPLACE MATERIALIZED VIEW mv
    ENGINE = MergeTree ORDER BY x
    AS SELECT x, sum(y) AS total FROM src GROUP BY x;

-- Replace with a new definition (old inner table data is lost)
CREATE OR REPLACE MATERIALIZED VIEW mv
    ENGINE = MergeTree ORDER BY x
    AS SELECT x, count() AS cnt FROM src GROUP BY x;

-- Replace with POPULATE to backfill from existing source data
CREATE OR REPLACE MATERIALIZED VIEW mv
    ENGINE = MergeTree ORDER BY x
    POPULATE
    AS SELECT x FROM src;

-- Replace an inner-table MV with a TO-table MV (target data is preserved)
CREATE OR REPLACE MATERIALIZED VIEW mv TO target
    AS SELECT x FROM src;
```

:::tip
Aquí tienes una guía paso a paso para usar [vistas materializadas](/es/guides/developer/cascading-materialized-views.md).
:::

Las vistas materializadas almacenan datos transformados por la consulta [SELECT](../../../sql-reference/statements/select/index.md) correspondiente.

Al crear una vista materializada sin `TO [db].[table]`, debes especificar `ENGINE`, el motor de tabla para almacenar los datos.

Al crear una vista materializada con `TO [db].[table]`, tampoco puedes usar `POPULATE`.

Una vista materializada se implementa de la siguiente manera: al insertar datos en la tabla especificada en `SELECT`, una parte de los datos insertados se transforma mediante esta consulta `SELECT`, y el resultado se inserta en la vista.

:::note
Las vistas materializadas en ClickHouse usan **nombres de columna** en lugar del orden de las columnas durante la inserción en la tabla de destino. Si algunos nombres de columna no están presentes en el resultado de la consulta `SELECT`, ClickHouse usa un valor predeterminado, incluso si la columna no es [Nullable](../../data-types/nullable.md). Una práctica segura es añadir alias para cada columna al usar vistas materializadas.

Las vistas materializadas en ClickHouse se comportan más como desencadenadores de inserción. Si hay alguna agregación en la consulta de la vista, se aplica solo al lote de datos recién insertados. Cualquier cambio en los datos existentes de la tabla de origen (como update, delete, drop partition, etc.) no modifica la vista materializada.

Las vistas materializadas en ClickHouse no tienen un comportamiento determinista en caso de error. Esto significa que los bloques que ya se hayan escrito se conservarán en la tabla de destino, pero no así todos los bloques posteriores al error.

De forma predeterminada, si el envío a una de las vistas genera un error, la consulta `INSERT` falla. No se garantiza si el bloque ya ha llegado a la tabla de origen en ese momento; depende del momento en que se ejecuta el pipeline de inserción, no del error de la vista. Reintenta el `INSERT` fallido con deduplicación de inserción (`insert_deduplicate`, `deduplicate_blocks_in_dependent_materialized_views`) para obtener entrega exactly-once a la tabla de origen y a todas las vistas dependientes.

Establecer `materialized_views_ignore_errors=true` en la consulta `INSERT` solo cambia la notificación de errores: cada error de la vista se registra como una advertencia y la consulta `INSERT` finaliza correctamente. La entrega al destino de la vista que falla es parcial: los bloques procesados antes de la excepción se conservan, y el bloque que falla, junto con cualquier bloque posterior, se descartan de esa vista. Las vistas aguas abajo de ese destino solo ven los bloques que sí llegaron, por lo que su entrega también es parcial. Las vistas hermanas (y sus cadenas aguas abajo) que no lanzaron ninguna excepción se escriben por completo, y la tabla de origen se escribe como de costumbre. Como `INSERT` se notifica como correcto, el client no recibe ninguna señal de fallo y no se activa ningún reintento automático; use esta configuración solo cuando las escrituras en la tabla de origen no deban bloquearse por problemas del lado de la vista (por ejemplo, en tablas `system.*_log`).

`materialized_views_ignore_errors` es `true` de forma predeterminada para las tablas `system.*_log`.
:::

Si especifica `POPULATE`, los datos existentes de la tabla se insertan en la vista al crearla, como si se ejecutara un `CREATE TABLE ... AS SELECT ...`. De lo contrario, la consulta contiene solo los datos insertados en la tabla después de crear la vista. **No recomendamos** usar `POPULATE`, ya que los datos insertados en la tabla durante la creación de la vista no se insertarán en ella.

:::note
Dado que `POPULATE` funciona como `CREATE TABLE ... AS SELECT ...`, tiene limitaciones:

* No es compatible con base de datos `Replicated`
* No es compatible con ClickHouse Cloud

En su lugar, se puede usar un `INSERT ... SELECT` independiente.
:::

Una consulta `SELECT` puede contener `DISTINCT`, `GROUP BY`, `ORDER BY`, `LIMIT`. Tenga en cuenta que las transformaciones correspondientes se realizan de forma independiente en cada bloque de datos insertados. Por ejemplo, si se establece `GROUP BY`, los datos se agregan durante la inserción, pero solo dentro de un único paquete de datos insertados. Después, los datos no se vuelven a agregar. La excepción es cuando se usa un `ENGINE` que realiza agregación de datos por sí mismo, como `SummingMergeTree`.

Si la vista materializada usa la construcción `TO [db.]name`, puede hacer `DETACH` de la vista, ejecutar `ALTER` en la tabla de destino y luego `ATTACH` de la vista previamente separada con `DETACH`.

Tenga en cuenta que la vista materializada está influida por la configuración [optimize&#95;on&#95;insert](/es/operations/settings/settings#optimize_on_insert). Los datos se fusionan antes de insertarse en una vista.

Las vistas tienen el mismo aspecto que las tablas normales. Por ejemplo, aparecen en el resultado de la consulta `SHOW TABLES`.

Para eliminar una vista, use [DROP VIEW](../../../sql-reference/statements/drop.md#drop-view). Aunque `DROP TABLE` también funciona para las VIEW.

<div id="sql_security">
  ## SQL security
</div>

`DEFINER` y `SQL SECURITY` permiten especificar qué usuario de ClickHouse se usará al ejecutar la consulta subyacente de la vista.
`SQL SECURITY` tiene tres valores válidos: `DEFINER`, `INVOKER` o `NONE`. Puede especificar cualquier usuario existente o `CURRENT_USER` en la cláusula `DEFINER`.

La siguiente tabla explica qué permisos se requieren para cada usuario para poder consultar la vista.
Tenga en cuenta que, independientemente de la opción de SQL security, en todos los casos sigue siendo necesario tener `GRANT SELECT ON <view>` para poder leerla.

| SQL security option | View                                                                         | Materialized View                                                                                                                     |
| ------------------- | ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `DEFINER alice`     | `alice` debe tener el permiso `SELECT` sobre la tabla fuente de la vista.    | `alice` debe tener el permiso `SELECT` sobre la tabla fuente de la vista y el permiso `INSERT` sobre la tabla de destino de la vista. |
| `INVOKER`           | El usuario debe tener el permiso `SELECT` sobre la tabla fuente de la vista. | No se puede especificar `SQL SECURITY INVOKER` para vistas materializadas.                                                            |
| `NONE`              | -                                                                            | -                                                                                                                                     |

:::note
`SQL SECURITY NONE` es una opción obsoleta. Cualquier usuario con permisos para crear vistas con `SQL SECURITY NONE` podrá ejecutar cualquier consulta arbitraria.
Por lo tanto, es necesario tener `GRANT ALLOW SQL SECURITY NONE TO <user>` para crear una vista con esta opción.
:::

Si no se especifican `DEFINER`/`SQL SECURITY`, se usan los valores predeterminados:

* `SQL SECURITY`: `INVOKER` para vistas normales y `DEFINER` para vistas materializadas ([configurable mediante Settings](../../../operations/settings/settings.md#default_normal_view_sql_security))
* `DEFINER`: `CURRENT_USER` ([configurable mediante Settings](../../../operations/settings/settings.md#default_view_definer))

Si una vista se adjunta sin especificar `DEFINER`/`SQL SECURITY`, el valor predeterminado es `SQL SECURITY NONE` para la vista materializada y `SQL SECURITY INVOKER` para la vista normal.

Para cambiar la SQL security de una vista existente, use

```sql
ALTER TABLE MODIFY SQL SECURITY { DEFINER | INVOKER | NONE } [DEFINER = { user | CURRENT_USER }]
```

<div id="examples">
  ### Ejemplos
</div>

```sql
CREATE VIEW test_view
DEFINER = alice SQL SECURITY DEFINER
AS SELECT ...
```

```sql
CREATE VIEW test_view
SQL SECURITY INVOKER
AS SELECT ...
```

<div id="live-view">
  ## Live View
</div>

<DeprecatedBadge />

Esta funcionalidad está obsoleta y se eliminará en el futuro.

Para mayor comodidad, la documentación anterior se encuentra [aquí](https://pastila.nl/?00f32652/fdf07272a7b54bda7e13b919264e449f.md)

<div id="refreshable-materialized-view">
  ## Vista materializada actualizable
</div>

```sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
REFRESH [EVERY|AFTER interval [OFFSET interval]]
[RANDOMIZE FOR interval]
[DEPENDS ON [db.]name [, [db.]name [, ...]]]
[SETTINGS name = value [, name = value [, ...]]]
[APPEND]
[TO[db.]name] [(columns)] [ENGINE = engine]
[EMPTY]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

donde `interval` es una secuencia de intervalos simples:

```sql
number SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR
```

La cláusula `REFRESH` debe especificar al menos uno de `EVERY`, `AFTER` o `DEPENDS ON`. `REFRESH` sin más (sin ninguno de ellos) se rechaza. `REFRESH DEPENDS ON ...` sin `EVERY`/`AFTER` es una forma abreviada de `REFRESH AFTER 0 SECOND DEPENDS ON ...`; consulta [Dependencias de actualización](#refresh-dependencies) más abajo.

Ejecuta periódicamente la consulta correspondiente y almacena su resultado en una tabla.

* Si se especifica `APPEND`, cada actualización inserta filas en la tabla sin eliminar las existentes. La inserción no es atómica, igual que en una consulta normal `INSERT INTO ... SELECT`.
* En caso contrario, cada actualización reemplaza atómicamente el contenido anterior de la tabla.

Diferencias con las vistas materializadas normales no actualizables:

* No hay trigger de inserción. Cuando se insertan datos nuevos en la tabla especificada en `SELECT`, *no* se envían automáticamente a la vista materializada actualizable. En su lugar, la inserción de datos solo se produce durante las ejecuciones de actualización periódicas o manuales.
* No hay restricciones en la consulta `SELECT`. Se permiten funciones de tabla (p. ej., `url()`), vistas, UNION y JOIN.

:::note
Los settings de la parte `REFRESH ... SETTINGS` de la consulta son settings de actualización (p. ej., `refresh_retries`), distintos de los settings normales (p. ej., `max_threads`). Los settings normales pueden especificarse usando `SETTINGS` al final de la consulta.
:::

<div id="refresh-schedule">
  ### Programación de actualización
</div>

Ejemplos de programación de actualización:

```sql
REFRESH EVERY 1 DAY -- every day, at midnight (UTC)
REFRESH EVERY 1 MONTH -- on 1st day of every month, at midnight
REFRESH EVERY 1 MONTH OFFSET 5 DAY 2 HOUR -- on 6th day of every month, at 2:00 am
REFRESH EVERY 2 WEEK OFFSET 5 DAY 15 HOUR 10 MINUTE -- every other Saturday, at 3:10 pm
REFRESH EVERY 30 MINUTE -- at 00:00, 00:30, 01:00, 01:30, etc
REFRESH AFTER 30 MINUTE -- 30 minutes after the previous refresh completes, no alignment with time of day
-- REFRESH AFTER 1 HOUR OFFSET 1 MINUTE -- syntax error, OFFSET is not allowed with AFTER
REFRESH EVERY 1 WEEK 2 DAYS -- every 9 days, not on any particular day of the week or month;
                            -- specifically, when day number (since 1969-12-29) is divisible by 9
REFRESH EVERY 5 MONTHS -- every 5 months, different months each year (as 12 is not divisible by 5);
                       -- specifically, when month number (since 1970-01) is divisible by 5
```

`RANDOMIZE FOR` ajusta aleatoriamente el momento de cada actualización, por ejemplo:

```sql
REFRESH EVERY 1 DAY OFFSET 2 HOUR RANDOMIZE FOR 1 HOUR -- every day at random time between 01:30 and 02:30
```

Como máximo, puede haber una operación de `refresh` en ejecución a la vez para una vista determinada. Por ejemplo, si una vista con `REFRESH EVERY 1 MINUTE` tarda 2 minutos en actualizarse, simplemente se actualizará cada 2 minutos. Si después se vuelve más rápida y pasa a actualizarse en 10 segundos, volverá a actualizarse cada minuto. (En particular, no se actualizará cada 10 segundos para recuperar actualizaciones omitidas; no existe tal retraso acumulado.)

Normalmente, la primera actualización se inicia inmediatamente después de crear la vista materializada: el tiempo transcurrido desde la última actualización es infinito, así que cualquier programación indica que es momento de actualizarla. Si se especifica `EMPTY`, esta actualización inicial se omite y la primera actualización se realiza en el siguiente momento programado; por ejemplo, con `EVERY 1 HOUR`, la primera actualización tendrá lugar al final de la hora actual.

<div id="in-replicated-db">
  ### En una base de datos replicada
</div>

Si la vista materializada actualizable está en una [base de datos `Replicated`](../../../engines/database-engines/replicated.md), las réplicas se coordinan entre sí para que solo una de ellas realice la actualización en cada momento programado. Se requiere el motor de tabla [ReplicatedMergeTree](../../../engines/table-engines/mergetree-family/replication.md) para que todas las réplicas vean los datos generados por la actualización.

En modo `APPEND`, la coordinación puede desactivarse con `SETTINGS all_replicas = 1`. Esto hace que las réplicas realicen las actualizaciones de forma independiente. En este caso, no se requiere ReplicatedMergeTree.

En el modo no `APPEND`, solo se admite la actualización coordinada. Para una actualización no coordinada, use la base de datos `Atomic` y la consulta `CREATE ... ON CLUSTER` para crear vistas materializadas actualizables en todas las réplicas.

La coordinación se realiza mediante Keeper. La ruta del znode viene determinada por la configuración del servidor [default&#95;replica&#95;path](../../../operations/server-configuration-parameters/settings.md#default_replica_path).

<div id="refresh-dependencies">
  ### Dependencias de actualización
</div>

`DEPENDS ON` sincroniza las actualizaciones de distintas tablas:

```sql
CREATE MATERIALIZED VIEW dependent REFRESH EVERY 1 HOUR DEPENDS ON dependency [...]
```

La actualización de la vista dependiente solo comenzará cuando se hayan completado las actualizaciones de todas las vistas de las que depende.

Para actualizar inmediatamente después de la actualización de otra vista:

```sql
CREATE MATERIALIZED VIEW dependent REFRESH AFTER 0 SECOND DEPENDS ON dependency [...]
```

O, de forma equivalente:

```sql
CREATE MATERIALIZED VIEW dependent REFRESH DEPENDS ON dependency [...]
```

:::note
`DEPENDS ON` solo funciona entre vistas materializadas actualizables. En particular, si la vista de la que depende usa `TO <table>`, asegúrate de usar el nombre de la vista y no el de la tabla. Si la lista de `DEPENDS ON` contiene una tabla normal o una vista no actualizable, o incluye un error tipográfico, la vista nunca se actualizará y mostrará el estado `MissingDependencies` en `system.view_refreshes`. Las dependencias se pueden cambiar o eliminar con `ALTER`; consulta [Cambio de los parámetros de actualización](#changing-refresh-parameters).
:::

<div id="using-depends-on-for-consistent-propagation-latency">
  #### Uso de DEPENDS ON para una latencia de propagación uniforme
</div>

Si ambas vistas usan `REFRESH EVERY` con el mismo período, la dependencia se aplica en cada franja horaria.

P. ej., supongamos que las vistas X e Y usan `REFRESH EVERY 1 HOUR` y que Y lee de la tabla de salida de X. Sin dependencias, Y normalmente vería los datos de X de la actualización de la hora anterior. Con `DEPENDS ON X`, la actualización de Y de las 11:00 solo comenzará después de que se complete la actualización de X de las 11:00.

```text
           10:00            11:00            12:00
           │                │                │
  X:        [run]┐           [run]┐           [run]┐
                 │                │                │
  Y:             └►[run]          └►[run]          └►[run]
```

Tanto la dependencia como el dependiente pueden omitir franjas horarias de forma independiente si las actualizaciones tardan más de lo que dura el período de actualización. No se garantiza que el dependiente se actualice exactamente una vez por cada actualización de la dependencia.

```text
           10:00          11:00          12:00          13:00
           │              │              │              |
  X:        [run]┐         [run]┐         [run]┐         [run]┐
                 │              └────┐    (Y skips 12:00)     └───┐
  Y:             └►[10:00 ru------un]└►[11:00 ru---------------un]└►[13:00 run]
```

<div id="using-depends-on-for-batched-stream-processing">
  #### Uso de DEPENDS ON para el procesamiento de flujo por lotes
</div>

Si no se usa `REFRESH EVERY`, la vista dependiente X se actualiza si todas sus dependencias se han actualizado al menos una vez desde la última actualización de X. `REFRESH AFTER T` añade un retraso: la dependiente empezará a actualizarse un tiempo T después de que la dependencia complete una actualización.

Se permiten las dependencias circulares y son útiles. Considere este grafo de vistas materializadas actualizables:

1. X toma un lote de filas de algún flujo y las coloca en una tabla.
2. Luego, Y y Z leen de esa tabla, realizan distintas agregaciones y añaden los resultados a otras tablas.
3. Después de que el lote se haya procesado por completo, X toma el siguiente lote y el ciclo se repite.

```text
            source
               │
               ▼
          ┌─────────┐
     ┌───►│    X    │◄───┐
     │    └──┬───┬──┘    │
  DEPENDS    │   │    DEPENDS
    ON       ▼   ▼      ON
     │      ┌─┐ ┌─┐      │
     └──────┤Y│ │Z├──────┘
            └─┘ └─┘
```

Ejemplo completo:

```sql
CREATE TABLE current_batch (t UInt64, v Int64) ENGINE ReplicatedMergeTree ORDER BY t;
CREATE TABLE batch_log (max_t UInt64, n Int64, v_sum Int64, processed_at DateTime64) ENGINE ReplicatedMergeTree ORDER BY max_t;
CREATE TABLE stats (h UInt64, n UInt64) ENGINE ReplicatedSummingMergeTree ORDER BY h;

-- (system.numbers stands in for a data source with monotonically increasing timestamps or sequence numbers)
CREATE MATERIALIZED VIEW current_batch_v REFRESH EVERY 10 SECOND DEPENDS ON batch_log_v, stats_v TO current_batch AS SELECT number as t, number * 10 as v FROM system.numbers WHERE number > (SELECT max(max_t) FROM batch_log) LIMIT 100;

CREATE MATERIALIZED VIEW batch_log_v REFRESH DEPENDS ON current_batch_v APPEND TO batch_log AS SELECT max(t) as max_t, count() as n, sum(v) as v_sum, now64() as processed_at FROM current_batch;

CREATE MATERIALIZED VIEW stats_v REFRESH DEPENDS ON current_batch_v APPEND TO stats AS SELECT cityHash64(v) % 20 as h, count() as n FROM current_batch GROUP BY h;

-- Must trigger initial refresh manually.
SYSTEM REFRESH VIEW current_batch_v;
```

Las cadenas más largas también funcionan.

Esto solo funciona bien cuando la coordinación de actualización está habilitada; es decir, cuando las vistas están en Replicated o en una Shared database. Sin coordinación, el reinicio del servidor interrumpe el ciclo, por lo que se requiere un `SYSTEM REFRESH VIEW` manual después de cada reinicio, en lugar de hacerlo solo una vez tras crear las vistas.

<div id="refresh-settings">
  ### Configuración de actualización
</div>

Configuraciones de actualización disponibles:

* `refresh_retries` - Cuántas veces reintentar si la consulta de actualización falla con una excepción. Si todos los reintentos fallan, se omite y se pasa al siguiente momento de actualización programado. 0 significa que no hay reintentos; -1 significa reintentos infinitos. Valor predeterminado: 2.
* `refresh_retry_initial_backoff_ms` - Retraso antes del primer reintento, si `refresh_retries` no es cero. Cada reintento posterior duplica el retraso, hasta `refresh_retry_max_backoff_ms`. Valor predeterminado: 100 ms.
* `refresh_retry_max_backoff_ms` - Límite del crecimiento exponencial del retraso entre intentos de actualización. Valor predeterminado: 60000 ms (1 minuto).
* `all_replicas` - En una [base de datos Replicated](../../../engines/database-engines/replicated.md) con `APPEND`, controla si todas las réplicas se actualizan de forma independiente o si solo una réplica se actualiza en cada momento programado. No se puede cambiar después de crear la vista. Valor predeterminado: `false`.

<div id="changing-refresh-parameters">
  ### Cambio de los parámetros de actualización
</div>

Los parámetros de actualización de una vista materializada actualizable existente se modifican con [`ALTER TABLE ... MODIFY REFRESH`](../alter/view.md#alter-table--modify-refresh-statement):

```sql
ALTER TABLE [db.]name MODIFY REFRESH EVERY|AFTER ... [RANDOMIZE FOR ...] [DEPENDS ON ...] [SETTINGS ...]
```

La programación (`EVERY` o `AFTER`) es obligatoria: la instrucción siempre reemplaza *todos* los parámetros de actualización —programación, `RANDOMIZE FOR`, `DEPENDS ON` y la configuración de actualización— por lo especificado. Todo lo que se omita se restablece a su valor predeterminado (configuración) o se elimina (dependencias, aleatorización).

:::note

* Para cambiar solo la configuración de actualización (p. ej., `refresh_retries`), repita la programación existente:

  ```sql
  ALTER TABLE rmv MODIFY REFRESH EVERY 1 HOUR SETTINGS refresh_retries = 5;
  ```

* `ALTER TABLE ... MODIFY SETTING refresh_retries = ...` no se admite en las vistas materializadas; debe hacerlo mediante `MODIFY REFRESH`.

* No se admite agregar ni quitar `APPEND`.

* La configuración `all_replicas` no puede modificarse después de la creación.
  :::

Ejemplos:

```sql
-- Change the schedule, drop existing settings and dependencies.
ALTER TABLE rmv MODIFY REFRESH EVERY 30 MINUTE;

-- Change the schedule and tune retry behavior.
ALTER TABLE rmv MODIFY REFRESH EVERY 30 MINUTE
SETTINGS refresh_retries = 5,
         refresh_retry_initial_backoff_ms = 500,
         refresh_retry_max_backoff_ms = 60000;

-- Keep the dependency while changing the period.
ALTER TABLE rmv MODIFY REFRESH EVERY 6 HOUR DEPENDS ON other_rmv;

-- Drop the dependency by omitting `DEPENDS ON`.
ALTER TABLE rmv MODIFY REFRESH EVERY 6 HOUR;
```

<div id="other-operations">
  ### Otras operaciones
</div>

El estado de todas las vistas materializadas actualizables está disponible en la tabla [`system.view_refreshes`](../../../operations/system-tables/view_refreshes.md). En particular, contiene el progreso de la actualización (si está en curso), la hora de la última y de la próxima actualización, y el mensaje de excepción si alguna actualización falló.

Para detener, iniciar, forzar o cancelar actualizaciones manualmente, use [`SYSTEM STOP|START|REFRESH|WAIT|CANCEL VIEW`](../system.md#managing-refreshable-materialized-views).

Para esperar a que se complete una actualización, use [`SYSTEM WAIT VIEW`](../system.md#wait-view). En particular, resulta útil para esperar la actualización inicial después de crear una vista.

:::note
Dato curioso: la consulta de actualización puede leer de la vista que se está actualizando y ver la versión de los datos anterior a la actualización. Esto significa que puede implementar el juego de la vida de Conway: https://pastila.nl/?00021a4b/d6156ff819c83d490ad2dcec05676865#O0LGWTO7maUQIA4AcGUtlA==
:::

<div id="window-view">
  ## vista de ventana
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::info
Esta es una función experimental que puede cambiar de forma incompatible con versiones anteriores en versiones futuras. Habilite el uso de vista de ventana y la consulta `WATCH` con la configuración [allow&#95;experimental&#95;window&#95;view](/es/operations/settings/settings#allow_experimental_window_view). Introduzca el comando `set allow_experimental_window_view = 1`.
:::

```sql
CREATE WINDOW VIEW [IF NOT EXISTS] [db.]table_name [TO [db.]table_name] [INNER ENGINE engine] [ENGINE engine] [WATERMARK strategy] [ALLOWED_LATENESS interval_function] [POPULATE]
AS SELECT ...
GROUP BY time_window_function
[COMMENT 'comment']
```

vista de ventana puede agregar datos por ventana de tiempo y producir los resultados cuando la ventana esté lista para activarse. Almacena los resultados parciales de agregación en una tabla interna (o especificada) para reducir la latencia y puede enviar el resultado del procesamiento a una tabla especificada o enviar notificaciones mediante la consulta WATCH.

Crear una vista de ventana es similar a crear una `MATERIALIZED VIEW`. vista de ventana necesita un motor de almacenamiento interno para almacenar datos intermedios. El almacenamiento interno puede especificarse mediante la cláusula `INNER ENGINE`; la vista de ventana usará `AggregatingMergeTree` como motor interno predeterminado.

Al crear una vista de ventana sin `TO [db].[table]`, debe especificar `ENGINE`, el motor de tabla para almacenar datos.

<div id="time-window-functions">
  ### Funciones de ventana de tiempo
</div>

Las [funciones de ventana de tiempo](../../functions/time-window-functions.md) se utilizan para obtener los límites inferior y superior de la ventana de los registros. La vista de ventana debe utilizarse junto con una función de ventana de tiempo.

<div id="time-attributes">
  ### ATRIBUTOS DE TIEMPO
</div>

vista de ventana admite el procesamiento con **tiempo de procesamiento** y con **tiempo de evento**.

El **tiempo de procesamiento** permite que vista de ventana produzca resultados en función de la hora de la máquina local y se usa de forma predeterminada. Es la noción de tiempo más sencilla, pero no proporciona determinismo. El atributo de tiempo de procesamiento puede definirse configurando el `time_attr` de la función de ventana de tiempo como una columna de la tabla o usando la función `now()`. La siguiente consulta crea una vista de ventana con tiempo de procesamiento.

```sql
CREATE WINDOW VIEW wv AS SELECT count(number), tumbleStart(w_id) as w_start from date GROUP BY tumble(now(), INTERVAL '5' SECOND) as w_id
```

El **tiempo del evento** es el momento en que ocurrió cada evento individual en el dispositivo que lo produjo. Este tiempo suele estar incorporado en los registros cuando se generan. El procesamiento por tiempo del evento permite obtener resultados consistentes incluso en caso de eventos desordenados o tardíos. La vista de ventana admite el procesamiento por tiempo del evento mediante la sintaxis `WATERMARK`.

La vista de ventana ofrece tres estrategias de marca de agua:

* `STRICTLY_ASCENDING`: Emite una marca de agua con el valor máximo de timestamp observado hasta el momento. Las filas que tienen un timestamp inferior al timestamp máximo no se consideran tardías.
* `ASCENDING`: Emite una marca de agua con el valor máximo de timestamp observado hasta el momento menos 1. Las filas que tienen un timestamp igual o inferior al timestamp máximo no se consideran tardías.
* `BOUNDED`: WATERMARK=INTERVAL. Emite marcas de agua, que son el timestamp máximo observado menos el retraso especificado.

Las siguientes consultas son ejemplos de cómo crear una vista de ventana con `WATERMARK`:

```sql
CREATE WINDOW VIEW wv WATERMARK=STRICTLY_ASCENDING AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
CREATE WINDOW VIEW wv WATERMARK=ASCENDING AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
CREATE WINDOW VIEW wv WATERMARK=INTERVAL '3' SECOND AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
```

De forma predeterminada, la ventana se disparará cuando llegue el watermark, y los elementos que lleguen después del watermark se descartarán. vista de ventana admite el procesamiento de eventos tardíos configurando `ALLOWED_LATENESS=INTERVAL`. Un ejemplo de gestión de la tardanza es:

```sql
CREATE WINDOW VIEW test.wv TO test.dst WATERMARK=ASCENDING ALLOWED_LATENESS=INTERVAL '2' SECOND AS SELECT count(a) AS count, tumbleEnd(wid) AS w_end FROM test.mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND) AS wid;
```

Tenga en cuenta que los elementos emitidos por una activación tardía deben tratarse como resultados actualizados de un cálculo anterior. En lugar de activarse al final de las ventanas, la vista de ventana se activará inmediatamente cuando llegue el evento tardío. Por lo tanto, esto dará lugar a múltiples resultados para la misma ventana. Los usuarios deben tener en cuenta estos resultados duplicados o deduplicarlos.

Puede modificar la consulta `SELECT` especificada en la vista de ventana mediante la instrucción `ALTER TABLE ... MODIFY QUERY`. La estructura de datos resultante de la nueva consulta `SELECT` debe ser la misma que la de la consulta `SELECT` original, tanto con la cláusula `TO [db.]name` como sin ella. Tenga en cuenta que los datos de la ventana actual se perderán porque el estado intermedio no se puede reutilizar.

<div id="monitoring-new-windows">
  ### Supervisión de nuevas ventanas
</div>

vista de ventana admite la consulta [WATCH](../../../sql-reference/statements/watch.md) para supervisar los cambios, o use la sintaxis `TO` para enviar los resultados a una tabla.

```sql
WATCH [db.]window_view
[EVENTS]
[LIMIT n]
[FORMAT format]
```

Se puede especificar un `LIMIT` para establecer el número de actualizaciones que se recibirán antes de que finalice la consulta. La cláusula `EVENTS` puede usarse para obtener una forma abreviada de la consulta `WATCH` en la que, en lugar del resultado de la consulta, solo se obtiene el watermark más reciente de la consulta.

<div id="settings-1">
  ### Configuración
</div>

* `window_view_clean_interval`: El intervalo de limpieza de la vista de ventana, en segundos, para liberar datos obsoletos. El sistema conservará las ventanas que no se hayan activado por completo según la hora del sistema o la configuración de `WATERMARK`, y eliminará los demás datos.
* `window_view_heartbeat_interval`: El intervalo de heartbeat, en segundos, para indicar que la watch query sigue activa.
* `wait_for_window_view_fire_signal_timeout`: Tiempo de espera para la señal de activación de la vista de ventana en el procesamiento de event time.

<div id="example">
  ### Ejemplo
</div>

Supongamos que necesitamos contar la cantidad de logs de clics por cada 10 segundos en una tabla de logs llamada `data`, cuya estructura es:

```sql
CREATE TABLE data ( `id` UInt64, `timestamp` DateTime) ENGINE = Memory;
```

Primero, creamos una vista de ventana con una ventana tumbling de 10 segundos:

```sql
CREATE WINDOW VIEW wv as select count(id), tumbleStart(w_id) as window_start from data group by tumble(timestamp, INTERVAL '10' SECOND) as w_id
```

A continuación, usamos la consulta `WATCH` para obtener los resultados.

```sql
WATCH wv
```

Cuando los logs se insertan en la tabla `data`,

```sql
INSERT INTO data VALUES(1,now())
```

La consulta `WATCH` debería mostrar los resultados de la siguiente manera:

```text
┌─count(id)─┬────────window_start─┐
│         1 │ 2020-01-14 16:56:40 │
└───────────┴─────────────────────┘
```

Como alternativa, podemos enviar la salida a otra tabla mediante la sintaxis `TO`.

```sql
CREATE WINDOW VIEW wv TO dst AS SELECT count(id), tumbleStart(w_id) as window_start FROM data GROUP BY tumble(timestamp, INTERVAL '10' SECOND) as w_id
```

Se pueden encontrar ejemplos adicionales en las pruebas stateful de ClickHouse (allí se llaman `*window_view*`).

<div id="window-view-usage">
  ### Uso de vista de ventana
</div>

La vista de ventana es útil en los siguientes escenarios:

* **Monitoreo**: Agregue y calcule las métricas de los logs a lo largo del tiempo, y envíe los resultados a una tabla de destino. El dashboard puede usar la tabla de destino como tabla de origen.
* **Análisis**: Agregue previamente y preprocese automáticamente los datos dentro de la ventana de tiempo. Esto puede ser útil al analizar una gran cantidad de logs. El preprocesamiento elimina cálculos repetidos en múltiples consultas y reduce la latencia de las consultas.

<div id="related-content">
  ## Contenido relacionado
</div>

* Blog: [Cómo trabajar con datos de series temporales en ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
* Blog: [Creación de una solución de observabilidad con ClickHouse - Parte 2 - Trazas](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)

<div id="temporary-views">
  ## Vistas temporales
</div>

ClickHouse admite **vistas temporales** con las siguientes características (en línea con las tablas temporales, cuando corresponda):

* **Duración de la sesión**
  Una vista temporal existe solo mientras dure la sesión actual. Se elimina automáticamente cuando finaliza la sesión.

* **Sin base de datos**
  **No se puede** calificar una vista temporal con un nombre de base de datos. Existe fuera de las bases de datos (en el espacio de nombres de la sesión).

* **No replicado / sin ON CLUSTER**
  Los objetos temporales son locales a la sesión y **no pueden** crearse con `ON CLUSTER`.

* **Resolución de nombres**
  Si un objeto temporal (tabla o vista) tiene el mismo nombre que un objeto persistente y una consulta hace referencia a ese nombre **sin** una base de datos, se usa el objeto **temporal**.

* **Objeto lógico (sin almacenamiento)**
  Una vista temporal solo almacena su texto `SELECT` (usa internamente el motor `View`). No conserva datos y no admite `INSERT`.

* **Cláusula ENGINE**
  **No** es necesario especificar `ENGINE`; si se proporciona como `ENGINE = View`, se ignora o se considera equivalente a la misma vista lógica.

* **Seguridad / privilegios**
  Crear una vista temporal requiere el privilegio `CREATE TEMPORARY VIEW`, que se concede implícitamente con `CREATE VIEW`.

* **SHOW CREATE**
  Use `SHOW CREATE TEMPORARY VIEW view_name;` para mostrar el DDL de una vista temporal.

<div id="temporary-views-syntax">
  ### Sintaxis
</div>

```sql
CREATE TEMPORARY VIEW [IF NOT EXISTS] view_name AS <select_query>
```

`OR REPLACE` **no** es compatible con las vistas temporales (para mantener coherencia con las tablas temporales). Si necesita “reemplazar” una vista temporal, elimínela y vuelva a crearla.

<div id="examples">
  ### Ejemplos
</div>

Cree una tabla temporal de origen y una vista temporal sobre ella:

```sql
CREATE TEMPORARY TABLE t_src (id UInt32, val String);
INSERT INTO t_src VALUES (1, 'a'), (2, 'b');

CREATE TEMPORARY VIEW tview AS
SELECT id, upper(val) AS u
FROM t_src
WHERE id <= 2;

SELECT * FROM tview ORDER BY id;
```

Mostrar el DDL:

```sql
SHOW CREATE TEMPORARY VIEW tview;
```

Eliminarla:

```sql
DROP TEMPORARY VIEW IF EXISTS tview;  -- temporary views are dropped with TEMPORARY TABLE syntax
```

<div id="temporary-views-limitations">
  ### No permitido / limitaciones
</div>

* `CREATE OR REPLACE TEMPORARY VIEW ...` → **no se permite** (usa `DROP` + `CREATE`).
* `CREATE TEMPORARY MATERIALIZED VIEW ...` / `WINDOW VIEW` → **no se permite**.
* `CREATE TEMPORARY VIEW db.view AS ...` → **no se permite** (sin calificador de base de datos).
* `CREATE TEMPORARY VIEW view ON CLUSTER 'name' AS ...` → **no se permite** (los objetos temporales son locales a la sesión).
* `POPULATE`, `REFRESH`, `TO [db.table]`, motores internos y todas las cláusulas específicas de MV → **no aplican** a las vistas temporales.

<div id="temporary-views-distributed-notes">
  ### Notas sobre las consultas distribuidas
</div>

Una **vista** temporal es solo una definición; no hay datos que transferir. Si tu vista temporal hace referencia a **tablas** temporales (por ejemplo, `Memory`), sus datos pueden enviarse a servidores remotos durante la ejecución distribuida de consultas, igual que ocurre con las tablas temporales.

<div id="temporary-views-distributed-example">
  #### Ejemplo
</div>

```sql
-- A session-scoped, in-memory table
CREATE TEMPORARY TABLE temp_ids (id UInt64) ENGINE = Memory;

INSERT INTO temp_ids VALUES (1), (5), (42);

-- A session-scoped view over the temp table (purely logical)
CREATE TEMPORARY VIEW v_ids AS
SELECT id FROM temp_ids;

-- Replace 'test' with your cluster name.
-- GLOBAL JOIN forces ClickHouse to *ship* the small join-side (temp_ids via v_ids)
-- to every remote server that executes the left side.
SELECT count()
FROM cluster('test', system.numbers) AS n
GLOBAL ANY INNER JOIN v_ids USING (id)
WHERE n.number < 100;

```