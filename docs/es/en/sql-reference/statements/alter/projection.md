---
description: 'Documentación sobre la gestión de proyecciones'
sidebar_label: 'PROJECTION'
sidebar_position: 49
slug: /sql-reference/statements/alter/projection
title: 'Proyecciones'
doc_type: 'referencia'
---

Esta página explica qué son las proyecciones, cómo puede usarlas y las distintas opciones disponibles para gestionarlas.

<div id="overview">
  ## Descripción general de las proyecciones
</div>

Las proyecciones almacenan los datos en un formato que optimiza la ejecución de consultas; esta funcionalidad es útil para:

* Ejecutar consultas sobre una columna que no forma parte de la clave primaria
* Preagregar columnas; esto reducirá tanto el procesamiento como la IO

Puede definir una o más proyecciones para una tabla y, durante el análisis de la consulta, ClickHouse seleccionará la proyección con menos datos para escanear sin modificar la consulta proporcionada por el usuario.

:::note[Uso de disco]
Las proyecciones crearán internamente una nueva tabla oculta; esto significa que se requerirá más IO y más espacio en disco.
Por ejemplo, si la proyección define una clave primaria diferente, todos los datos de la tabla original se duplicarán.
:::

Puede ver más detalles técnicos sobre cómo funcionan internamente las proyecciones en esta [página](/es/guides/best-practices/sparse-primary-indexes.md/#option-3-projections).

<div id="examples">
  ## Uso de las proyecciones
</div>

<div id="example-filtering-without-using-primary-keys">
  ### Ejemplo de filtrado sin usar claves primarias
</div>

Creación de la tabla:

```sql
CREATE TABLE visits_order
(
   `user_id` UInt64,
   `user_name` String,
   `pages_visited` Nullable(Float64),
   `user_agent` String
)
ENGINE = MergeTree()
PRIMARY KEY user_agent
```

Con `ALTER TABLE`, podemos añadir la proyección a una tabla existente:

```sql
ALTER TABLE visits_order ADD PROJECTION user_name_projection (
    SELECT *
    ORDER BY user_name
)

ALTER TABLE visits_order MATERIALIZE PROJECTION user_name_projection
```

Inserción de datos:

```sql
INSERT INTO visits_order SELECT
    number,
    'test',
    1.5 * (number / 2),
    'Android'
FROM numbers(1, 100);
```

La proyección nos permitirá filtrar por `user_name` rápidamente, incluso si en la tabla original `user_name` no se definió como `PRIMARY_KEY`.
En el momento de la consulta, ClickHouse determina que se procesarán menos datos si se usa la proyección, ya que los datos están ordenados por `user_name`.

```sql
SELECT
    *
FROM visits_order
WHERE user_name='test'
LIMIT 2
```

Para verificar que una consulta está usando la proyección, podemos revisar la tabla `system.query_log`. En el campo `projections` aparece el nombre de la proyección utilizada, o queda vacío si no se ha usado ninguna:

```sql
SELECT query, projections FROM system.query_log WHERE query_id='<query_id>'
```

<div id="example-pre-aggregation-query">
  ### Ejemplo de consulta de preagregación
</div>

Cree la tabla con la proyección `projection_visits_by_user`:

```sql
CREATE TABLE visits
(
   `user_id` UInt64,
   `user_name` String,
   `pages_visited` Nullable(Float64),
   `user_agent` String,
   PROJECTION projection_visits_by_user
   (
       SELECT
           user_agent,
           sum(pages_visited)
       GROUP BY user_id, user_agent
   )
)
ENGINE = MergeTree()
ORDER BY user_agent
```

Inserta los datos:

```sql
INSERT INTO visits SELECT
    number,
    'test',
    1.5 * (number / 2),
    'Android'
FROM numbers(1, 100);
```

```sql
INSERT INTO visits SELECT
    number,
    'test',
    1. * (number / 2),
   'IOS'
FROM numbers(100, 500);
```

Ejecute una primera consulta con `GROUP BY` usando el campo `user_agent`.
Esta consulta no usará la proyección definida, ya que la preagregación no se corresponde.

```sql
SELECT
    user_agent,
    count(DISTINCT user_id)
FROM visits
GROUP BY user_agent
```

Para usar la proyección, puede ejecutar consultas que seleccionen parte o la totalidad de los campos de preagregación y de `GROUP BY`:

```sql
SELECT
    user_agent
FROM visits
WHERE user_id > 50 AND user_id < 150
GROUP BY user_agent
```

```sql
SELECT
    user_agent,
    sum(pages_visited)
FROM visits
GROUP BY user_agent
```

Como se mencionó anteriormente, puedes revisar la tabla `system.query_log` para comprobar si se utilizó una proyección.
El campo `projections` muestra el nombre de la proyección utilizada.
Estará vacío si no se ha utilizado ninguna proyección:

```sql
SELECT query, projections FROM system.query_log WHERE query_id='<query_id>'
```

<div id="projection-indexes">
  ### Creación y uso de índices de proyección
</div>

Creación de un [índice de proyección](../../../engines/table-engines/mergetree-family/mergetree.md#projection-index):

```sql
CREATE TABLE events
(
    `event_time` DateTime,
    `event_id` UInt64,
    `user_id` UInt64,
    `huge_string` String,
    PROJECTION order_by_user_id INDEX user_id TYPE basic
)
ENGINE = MergeTree()
ORDER BY (event_id);
```

<details markdown="1">
  <summary>Creación de una proyección con el campo `_part_offset` explícito</summary>

  Los índices de proyección también se pueden crear con la siguiente sintaxis (no recomendada):

  ```sql
  CREATE TABLE events
  (
      `event_time` DateTime,
      `event_id` UInt64,
      `user_id` UInt64,
      `huge_string` String,
      PROJECTION order_by_user_id
      (
          SELECT
              _part_offset
          ORDER BY user_id
      )
  )
  ENGINE = MergeTree()
  ORDER BY (event_id);
  ```
</details>

Inserción de algunos datos de ejemplo:

```sql
INSERT INTO events SELECT * FROM generateRandom() LIMIT 100000;
```

El campo `_part_offset` mantiene su valor tras las fusiones y las mutaciones, lo que lo hace útil para los índices secundarios. Podemos aprovecharlo en las consultas:

```sql
SELECT
    count()
FROM events
WHERE _part_starting_offset + _part_offset IN (
    SELECT _part_starting_offset + _part_offset
    FROM events
    WHERE user_id = 42
)
SETTINGS enable_shared_storage_snapshot_in_query = 1
```

<div id="example-projection-with-where">
  ### Proyección de ejemplo con cláusula WHERE
</div>

Las proyecciones pueden incluir una cláusula `WHERE` para almacenar solo un subconjunto de filas. Esto es útil cuando las consultas filtran con frecuencia por un predicado conocido: la proyección materializa solo las filas coincidentes, lo que reduce el almacenamiento y mejora el rendimiento de las consultas.

Crear una tabla y añadir una proyección filtrada:

```sql
CREATE TABLE events
(
    `event_type` String,
    `time` DateTime,
    `message` String
)
ENGINE = MergeTree()
ORDER BY time;

ALTER TABLE events ADD PROJECTION proj_pageview (
    SELECT event_type, time, message
    WHERE event_type = 'pageview'
    ORDER BY time
);

ALTER TABLE events MATERIALIZE PROJECTION proj_pageview;
```

Insertar datos:

```sql
INSERT INTO events VALUES
    ('pageview', '2024-01-01', 'homepage'),
    ('click', '2024-01-02', 'button'),
    ('pageview', '2024-01-03', 'about');
```

Cuando la cláusula `WHERE` de una consulta **implica** la cláusula `WHERE` de la proyección (es decir, todas las condiciones del filtro de la proyección también están presentes en el filtro de la consulta), el optimizador puede usar automáticamente la proyección cuando determina que resulta beneficioso:

```sql
-- This query implies the projection's WHERE, so the projection may be used:
SELECT time, message FROM events WHERE event_type = 'pageview';

-- A stricter query also implies the projection's WHERE:
SELECT time, message FROM events WHERE event_type = 'pageview' AND time > '2024-01-01';

-- This query does NOT imply the projection, so the base table is scanned:
SELECT time, message FROM events WHERE event_type = 'click';
```

La comprobación de implicación es conservadora: se basa en la coincidencia exacta de conjunciones en la forma canónica de la expresión. Puede pasar por alto algunas oportunidades de optimización válidas (p. ej., implicaciones de rango), pero nunca producirá resultados incorrectos.

<div id="manipulating-projections">
  ## Gestión de proyecciones
</div>

Están disponibles las siguientes operaciones con [proyecciones](/es/engines/table-engines/mergetree-family/mergetree.md/#projections):

<div id="add-projection">
  ### ADD PROJECTION
</div>

Utilice la siguiente sentencia para añadir una descripción de proyección a los metadatos de una tabla:

```sql
-- Normal projection (supports WHERE)
ALTER TABLE [db.]name [ON CLUSTER cluster] ADD PROJECTION [IF NOT EXISTS] name ( SELECT <COLUMN LIST EXPR> [WHERE <expr>] [ORDER BY] ) [WITH SETTINGS ( setting_name1 = setting_value1, setting_name2 = setting_value2, ...)]

-- Aggregate projection (supports WHERE)
ALTER TABLE [db.]name [ON CLUSTER cluster] ADD PROJECTION [IF NOT EXISTS] name ( SELECT <COLUMN LIST EXPR> [WHERE <expr>] [GROUP BY] ) [WITH SETTINGS ( setting_name1 = setting_value1, setting_name2 = setting_value2, ...)]
```

:::note
Cuando una proyección define una cláusula `WHERE`, solo se materializan las filas que cumplen el predicado. El optimizador puede usar esa proyección cuando la cláusula `WHERE` de la consulta implica lógicamente la cláusula `WHERE` de la proyección y esta resulta beneficiosa para el plan de consulta. Esto se aplica tanto a las proyecciones normales como a las de agregación.
:::

<div id="with-settings">
  #### Cláusula `WITH SETTINGS`
</div>

`WITH SETTINGS` define **ajustes a nivel de proyección**, que personalizan cómo la proyección almacena los datos (por ejemplo, `index_granularity` o `index_granularity_bytes`).
Estos corresponden directamente a los **ajustes de la tabla MergeTree**, pero se aplican **solo a esta proyección**.

Ejemplo:

```sql
ALTER TABLE t
ADD PROJECTION p (
    SELECT x ORDER BY x
) WITH SETTINGS (
    index_granularity = 4096,
    index_granularity_bytes = 1048576
);
```

La configuración de la proyección prevalece sobre la configuración efectiva de la tabla para la proyección, de acuerdo con las reglas de validación (p. ej., se rechazarán las sobrescrituras no válidas o incompatibles).

<div id="drop-projection">
  ### DROP PROJECTION
</div>

Utilice la siguiente sentencia para eliminar una descripción de proyección de los metadatos de una tabla y borrar los archivos de proyección del disco.
Esto se implementa como una [mutación](/es/sql-reference/statements/alter/index.md#mutations).

```sql
ALTER TABLE [db.]name [ON CLUSTER cluster] DROP PROJECTION [IF EXISTS] name
```

<div id="materialize-projection">
  ### MATERIALIZE PROJECTION
</div>

Utilice la sentencia siguiente para reconstruir la proyección `name` en la partición `partition_name`.
Esto se implementa como una [mutación](/es/sql-reference/statements/alter/index.md#mutations).

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] MATERIALIZE PROJECTION [IF EXISTS] name [IN PARTITION partition_name]
```

<div id="clear-projection">
  ### CLEAR PROJECTION
</div>

Utilice la sentencia siguiente para eliminar los archivos de proyección del disco sin eliminar la descripción.
Esto se implementa como una [mutación](/es/sql-reference/statements/alter/index.md#mutations).

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] CLEAR PROJECTION [IF EXISTS] name [IN PARTITION partition_name]
```

Los comandos `ADD`, `DROP` y `CLEAR` son ligeros en el sentido de que solo modifican metadatos o eliminan archivos.
Además, se replican y sincronizan los metadatos de las proyecciones mediante ClickHouse Keeper o ZooKeeper.

:::note
La manipulación de proyecciones solo es compatible con tablas con motor [`*MergeTree`](/es/engines/table-engines/mergetree-family/mergetree.md) (incluidas las variantes [replicadas](/es/engines/table-engines/mergetree-family/replication.md)).
:::

<div id="control-projections-merges">
  ### Control del comportamiento de las fusiones de proyecciones
</div>

Cuando se ejecuta una consulta, ClickHouse elige entre leer de la tabla original o de una de sus proyecciones.
La decisión de leer de la tabla original o de una de sus proyecciones se toma de forma individual para cada parte de la tabla.
Por lo general, ClickHouse intenta leer la menor cantidad de datos posible y emplea algunos trucos para identificar la mejor parte desde la que leer; por ejemplo, muestreando la clave primaria de una parte.
En algunos casos, las partes de la tabla de origen no tienen sus correspondientes partes de proyección.
Esto puede ocurrir, por ejemplo, porque la creación de una proyección para una tabla en SQL es &quot;perezosa&quot; de forma predeterminada: solo afecta a los datos insertados a partir de ese momento, pero deja intactas las partes existentes.

Como una de las proyecciones ya contiene los valores de agregación precalculados, ClickHouse intenta leer de las correspondientes partes de proyección para evitar volver a agregar en tiempo de ejecución de la consulta. Si una parte concreta no tiene la correspondiente parte de proyección, la ejecución de la consulta recurre a la parte original.

Pero ¿qué ocurre si las filas de la tabla original cambian de una manera no trivial debido a operaciones no triviales de fusión en segundo plano de partes de datos?
Por ejemplo, supongamos que la tabla se almacena usando el motor de tabla `ReplacingMergeTree`.
Si se detecta la misma fila en varias partes de entrada durante la fusión, solo se conservará la versión más reciente de la fila (la de la parte insertada más recientemente), mientras que todas las versiones anteriores se descartarán.

De forma similar, si la tabla se almacena usando el motor de tabla `AggregatingMergeTree`, la operación de fusión puede combinar las mismas filas en las partes de entrada (según los valores de la clave primaria) en una sola fila para actualizar estados parciales de agregación.

Antes de ClickHouse v24.8, las partes de proyección o bien quedaban silenciosamente desincronizadas con los datos principales, o bien ciertas operaciones, como las actualizaciones y eliminaciones, no podían ejecutarse en absoluto, ya que la base de datos lanzaba automáticamente una excepción si la tabla tenía proyecciones.

Desde la versión v24.8, una nueva configuración a nivel de tabla, [`deduplicate_merge_projection_mode`](/es/operations/settings/merge-tree-settings#deduplicate_merge_projection_mode), controla el comportamiento si las operaciones no triviales de fusión en segundo plano mencionadas anteriormente se producen en partes de la tabla original.

Las mutaciones de borrado son otro ejemplo de operaciones de fusión de partes que eliminan filas en las partes de la tabla original. Desde la versión v24.7, también disponemos de una configuración para controlar el comportamiento con respecto a las mutaciones de borrado activadas por eliminaciones ligeras: [`lightweight_mutation_projection_mode`](/es/operations/settings/merge-tree-settings#deduplicate_merge_projection_mode).

A continuación se muestran los valores posibles para `deduplicate_merge_projection_mode` y `lightweight_mutation_projection_mode`:

* `throw` (predeterminado): Se lanza una excepción, lo que evita que las partes de proyección queden desincronizadas.
* `drop`: Se eliminan las partes de tabla de proyección afectadas. Las consultas recurrirán a la parte de la tabla original en el caso de las partes de proyección afectadas.
* `rebuild`: La parte de proyección afectada se reconstruye para mantener la coherencia con los datos de la parte de la tabla original.

<div id="limitations">
  ## Limitaciones
</div>

No se puede usar una columna `ALIAS` en la cláusula `ORDER BY` de una proyección. Por ejemplo:

```sql
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    ab_sum UInt64 ALIAS a + 1,
--highlight-next-line
    PROJECTION p (SELECT a ORDER BY ab_sum)
)
ENGINE = MergeTree ORDER BY id;
-- Fails with UNKNOWN_IDENTIFIER
```

Las columnas `ALIAS` no se almacenan físicamente y se calculan sobre la marcha en el momento de la consulta, por lo que no están disponibles durante la escritura de la parte de proyección, cuando se evalúa la expresión de ordenación.

En su lugar, use columnas `MATERIALIZED` o inserte la expresión directamente:

```sql
-- using MATERIALIZED column
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    ab_sum UInt64 MATERIALIZED a + 1,
    PROJECTION p (SELECT a ORDER BY ab_sum)
)
ENGINE = MergeTree ORDER BY id;

-- using an inline expression
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    PROJECTION p (SELECT a ORDER BY a + 1)
)
ENGINE = MergeTree ORDER BY id;
```

<div id="see-also">
  ## Véase también
</div>

* [&quot;Control de las proyecciones durante las fusiones&quot; (entrada de blog)](https://clickhouse.com/blog/clickhouse-release-24-08#control-of-projections-during-merges)
* [&quot;Proyecciones&quot; (guía)](/es/data-modeling/projections#using-projections-to-speed-up-UK-price-paid)
* [&quot;Vistas materializadas frente a proyecciones&quot;](https://clickhouse.com/docs/managing-data/materialized-views-versus-projections)