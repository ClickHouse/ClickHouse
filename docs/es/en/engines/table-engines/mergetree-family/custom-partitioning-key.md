---
description: 'Aprenda a agregar una clave de partición personalizada a las tablas MergeTree.'
sidebar_label: 'Clave de partición personalizada'
sidebar_position: 30
slug: /engines/table-engines/mergetree-family/custom-partitioning-key
title: 'Clave de partición personalizada'
doc_type: 'guide'
---

:::note
En la mayoría de los casos no necesita una clave de partición y, en la mayoría de los demás, tampoco necesita una clave de partición más detallada que por mes, salvo en casos de uso de observabilidad, donde el particionamiento por día es habitual.

Nunca debe usar un particionamiento demasiado granular. No particione sus datos por identificadores o nombres de cliente. En su lugar, haga que un identificador o nombre de cliente sea la primera columna de la expresión `ORDER BY`.
:::

El particionamiento está disponible para las [tablas de la familia MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md), incluidas las [tablas replicadas](../../../engines/table-engines/mergetree-family/replication.md) y las [vistas materializadas](/es/sql-reference/statements/create/view#materialized-view).

Una partición es una agrupación lógica de registros de una tabla según un criterio especificado. Puede definir una partición con un criterio arbitrario, por ejemplo, por mes, por día o por tipo de evento. Cada partición se almacena por separado para simplizar la manipulación de estos datos. Al acceder a los datos, ClickHouse usa el subconjunto más pequeño posible de particiones. Las particiones mejoran el rendimiento de las consultas que incluyen una clave de partición porque ClickHouse filtrará esa partición antes de seleccionar las partes y los gránulos dentro de ella.

La partición se especifica en la cláusula `PARTITION BY expr` al [crear una tabla](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table). La clave de partición puede ser cualquier expresión basada en las columnas de la tabla. Por ejemplo, para especificar el particionamiento por mes, use la expresión `toYYYYMM(date_column)`:

```sql
CREATE TABLE visits
(
    VisitDate Date,
    Hour UInt8,
    ClientID UUID
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(VisitDate)
ORDER BY Hour;
```

La clave de partición también puede ser una tupla de expresiones (de forma similar a la [clave primaria](../../../engines/table-engines/mergetree-family/mergetree.md#primary-keys-and-indexes-in-queries)). Por ejemplo:

```sql
ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/name', 'replica1', Sign)
PARTITION BY (toMonday(StartDate), EventType)
ORDER BY (CounterID, StartDate, intHash32(UserID));
```

En este ejemplo, establecemos el particionamiento según los tipos de eventos que ocurrieron durante la semana actual.

De forma predeterminada, no se admite una clave de partición de coma flotante. Para usarla, habilite la configuración [allow&#95;floating&#95;point&#95;partition&#95;key](../../../operations/settings/merge-tree-settings.md#allow_floating_point_partition_key).

Al insertar datos nuevos en una tabla, estos se almacenan como una parte independiente (fragmento) ordenada por la clave primaria. Entre 10 y 15 minutos después de la inserción, las partes de la misma partición se fusionan en una sola parte.

:::info
Una fusión solo funciona con partes de datos que tienen el mismo valor en la expresión de particionamiento. Esto significa que **no debe crear particiones excesivamente granulares** (más de unas mil particiones). De lo contrario, la consulta `SELECT` tendrá un rendimiento deficiente debido a una cantidad excesiva de archivos en el sistema de archivos y de descriptores de archivo abiertos.
:::

Use la tabla [system.parts](../../../operations/system-tables/parts.md) para ver las partes de la tabla y las particiones. Por ejemplo, supongamos que tenemos una tabla `visits` con particionamiento por mes. Ejecutemos la consulta `SELECT` para la tabla `system.parts`:

```sql
SELECT
    partition,
    name,
    active
FROM system.parts
WHERE table = 'visits'
```

```text
┌─partition─┬─name──────────────┬─active─┐
│ 201901    │ 201901_1_3_1      │      0 │
│ 201901    │ 201901_1_9_2_11   │      1 │
│ 201901    │ 201901_8_8_0      │      0 │
│ 201901    │ 201901_9_9_0      │      0 │
│ 201902    │ 201902_4_6_1_11   │      1 │
│ 201902    │ 201902_10_10_0_11 │      1 │
│ 201902    │ 201902_11_11_0_11 │      1 │
└───────────┴───────────────────┴────────┘
```

La columna `partition` contiene los nombres de las particiones. Hay dos particiones en este ejemplo: `201901` y `201902`. Puede usar el valor de esta columna para especificar el nombre de la partición en las consultas [ALTER ... PARTITION](../../../sql-reference/statements/alter/partition.md).

La columna `name` contiene los nombres de las partes de datos de la partición. Puede usar esta columna para especificar el nombre de la parte en la consulta [ALTER ATTACH PART](/es/sql-reference/statements/alter/partition#attach-partitionpart).

Desglosemos el nombre de la parte: `201901_1_9_2_11`:

* `201901` es el nombre de la partición.
* `1` es el número mínimo del bloque de datos.
* `9` es el número máximo del bloque de datos.
* `2` es el nivel del fragmento (la profundidad del árbol de fusiones del que se forma).
* `11` es la versión de la mutación (si una parte fue modificada por una mutación)

:::info
Las partes de las tablas de tipo antiguo tienen el nombre: `20190117_20190123_2_2_0` (fecha mínima - fecha máxima - número mínimo de bloque - número máximo de bloque - nivel).
:::

La columna `active` muestra el estado de la parte. `1` es activa; `0` es inactiva. Las partes inactivas son, por ejemplo, las partes de origen que permanecen después de fusionarse en una parte más grande. Las partes de datos corruptas también se indican como inactivas.

Como puede ver en el ejemplo, hay varias partes separadas de la misma partición (por ejemplo, `201901_1_3_1` y `201901_1_9_2`). Esto significa que estas partes aún no se han fusionado. ClickHouse fusiona periódicamente las partes de datos insertadas, aproximadamente 15 minutos después de la inserción. Además, puede realizar una fusión no programada mediante la consulta [OPTIMIZE](../../../sql-reference/statements/optimize.md). Ejemplo:

```sql
OPTIMIZE TABLE visits PARTITION 201902;
```

```text
┌─partition─┬─name─────────────┬─active─┐
│ 201901    │ 201901_1_3_1     │      0 │
│ 201901    │ 201901_1_9_2_11  │      1 │
│ 201901    │ 201901_8_8_0     │      0 │
│ 201901    │ 201901_9_9_0     │      0 │
│ 201902    │ 201902_4_6_1     │      0 │
│ 201902    │ 201902_4_11_2_11 │      1 │
│ 201902    │ 201902_10_10_0   │      0 │
│ 201902    │ 201902_11_11_0   │      0 │
└───────────┴──────────────────┴────────┘
```

Las partes inactivas se eliminarán aproximadamente 10 minutos después de la fusión.

Otra forma de ver un conjunto de partes y particiones es acceder al directorio de la tabla: `/var/lib/clickhouse/data/<database>/<table>/`. Por ejemplo:

```bash
/var/lib/clickhouse/data/default/visits$ ls -l
total 40
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  1 16:48 201901_1_3_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201901_1_9_2_11
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 15:52 201901_8_8_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 15:52 201901_9_9_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201902_10_10_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201902_11_11_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:19 201902_4_11_2_11
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 12:09 201902_4_6_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  1 16:48 detached
```

Las carpetas &#39;201901&#95;1&#95;1&#95;0&#39;, &#39;201901&#95;1&#95;7&#95;1&#39;, etc., son los directorios de las partes. Cada parte corresponde a una partición y contiene datos solo de un mes determinado (la tabla de este ejemplo está particionada por mes).

El directorio `detached` contiene partes que se separaron de la tabla mediante la consulta [DETACH](/es/sql-reference/statements/detach). Las partes corruptas también se mueven a este directorio, en lugar de eliminarse. El servidor no usa las partes del directorio `detached`. Puede agregar, eliminar o modificar los datos de este directorio en cualquier momento; el servidor no tendrá constancia de ello hasta que ejecute la consulta [ATTACH](/es/sql-reference/statements/alter/partition#attach-partitionpart).

Tenga en cuenta que, en un servidor en ejecución, no puede cambiar manualmente el conjunto de partes ni sus datos en el sistema de archivos, ya que el servidor no tendrá constancia de ello. En el caso de las tablas no replicadas, puede hacerlo cuando el servidor esté detenido, pero no se recomienda. En las tablas replicadas, el conjunto de partes no puede modificarse en ningún caso.

ClickHouse le permite realizar operaciones con las particiones: eliminarlas, copiarlas de una tabla a otra o crear una copia de seguridad. Consulte la lista de todas las operaciones en la sección [Manipulaciones con particiones y partes](/es/sql-reference/statements/alter/partition).

<div id="group-by-optimisation-using-partition-key">
  ## Optimización de Group By mediante la clave de partición
</div>

Para algunas combinaciones de la clave de partición de la tabla y la clave de Group By de la consulta, puede ser posible ejecutar la agregación de cada partición de forma independiente.
Así, no será necesario fusionar al final los datos agregados parcialmente de todos los hilos de ejecución,
porque tenemos la garantía de que cada valor de la clave de Group By no puede aparecer en los conjuntos de trabajo de dos hilos distintos.

El ejemplo típico es:

```sql
CREATE TABLE session_log
(
    UserID UInt64,
    SessionID UUID
)
ENGINE = MergeTree
PARTITION BY sipHash64(UserID) % 16
ORDER BY tuple();

SELECT
    UserID,
    COUNT()
FROM session_log
GROUP BY UserID;
```

:::note
El rendimiento de una consulta de este tipo depende en gran medida de la estructura de la tabla. Por eso, esta optimización no está habilitada de forma predeterminada.
:::

Los factores clave para lograr un buen rendimiento son:

* el número de particiones implicadas en la consulta debe ser lo bastante grande (más de `max_threads / 2`); de lo contrario, la consulta infrautilizará la máquina
* las particiones no deben ser demasiado pequeñas, para que el procesamiento por lotes no termine degenerando en un procesamiento fila por fila
* las particiones deben tener tamaños comparables, para que todos los hilos realicen aproximadamente la misma cantidad de trabajo

:::info
Se recomienda aplicar alguna función hash a las columnas de la cláusula `partition by` para distribuir los datos de forma uniforme entre las particiones.
:::

Los ajustes relevantes son:

* `allow_aggregate_partitions_independently` - controla si se habilita el uso de la optimización
* `force_aggregate_partitions_independently` - fuerza su uso cuando es aplicable desde el punto de vista de la corrección, pero la lógica interna que evalúa su conveniencia lo desactiva
* `max_number_of_partitions_for_independent_aggregation` - límite estricto del número máximo de particiones que puede tener la tabla