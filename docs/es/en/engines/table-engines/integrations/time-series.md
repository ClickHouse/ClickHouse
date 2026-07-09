---
description: 'Un motor de tabla que almacena series temporales, es decir, un conjunto de valores asociados
  a marcas de tiempo y tag (o labels).'
sidebar_label: 'TimeSeries'
sidebar_position: 60
slug: /engines/table-engines/special/time_series
title: 'Motor de tabla TimeSeries'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="timeseries-table-engine">
  # Motor de tabla TimeSeries
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

Un motor de tabla que almacena series temporales, es decir, un conjunto de valores asociados a marcas de tiempo y tags (o labels):

```sql
metric_name1[tag1=value1, tag2=value2, ...] = {timestamp1: value1, timestamp2: value2, ...}
metric_name2[...] = ...
```

:::info
Esta es una funcionalidad experimental que puede cambiar de formas incompatibles con versiones anteriores en futuras versiones.
Habilite el uso del motor de tabla TimeSeries
con la opción [allow&#95;experimental&#95;time&#95;series&#95;table](/es/operations/settings/settings#allow_experimental_time_series_table).
Ejecute el comando `set allow_experimental_time_series_table = 1`.
:::

<div id="syntax">
  ## Sintaxis
</div>

```sql
CREATE TABLE name [(columns)] ENGINE=TimeSeries
[SETTINGS var1=value1, ...]
[SAMPLES db.samples_table_name | [SAMPLES INNER COLUMNS (...)] [SAMPLES INNER ENGINE engine(arguments)]]
[TAGS db.tags_table_name | [TAGS INNER COLUMNS (...)] [TAGS INNER ENGINE engine(arguments)]]
[METRICS db.metrics_table_name | [METRICS INNER COLUMNS (...)] [METRICS INNER ENGINE engine(arguments)]]
```

:::note
La palabra clave `SAMPLES` tiene el alias `DATA`, que se mantiene por compatibilidad con versiones anteriores.
:::

<div id="usage">
  ## Uso
</div>

Es más fácil empezar con la configuración predeterminada (se puede crear una tabla `TimeSeries` sin especificar una lista de columnas):

```sql
CREATE TABLE my_table ENGINE=TimeSeries
```

Esta tabla puede usarse con los siguientes protocolos (se debe asignar un puerto en la configuración del servidor):

* [prometheus remote-write](/es/interfaces/prometheus#remote-write)
* [prometheus remote-read](/es/interfaces/prometheus#remote-read)

<div id="outer-columns">
  ### Columnas externas
</div>

Las columnas de una tabla TimeSeries se generan automáticamente. Son columnas externas: no almacenan datos, solo proporcionan la interfaz para SELECT/INSERT. Los datos reales se almacenan en las [tablas de destino](#target-tables). A continuación se muestra la lista de columnas externas:

| Name            | Type                                              | Description                                                                                                                                                                                                                                                                             |
| --------------- | ------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `metric_name`   | `String`                                          | El nombre de la métrica                                                                                                                                                                                                                                                                 |
| `tags`          | `Map(String, String)`                             | Mapa de tags (labels) de la serie temporal                                                                                                                                                                                                                                              |
| `time_series`   | `Array(Tuple(DateTime64(3), Float64))` by default | Array de pares (marca de tiempo, valor) de una serie temporal. Los types del elemento de marca de tiempo y del elemento escalar de la tupla pueden derivarse de la declaración `INNER COLUMNS` de `samples` (consulte [Especificación de columnas externas](#specifying-outer-columns)) |
| `metric_family` | `String`                                          | El nombre de la familia de métricas (para los metadatos de métricas)                                                                                                                                                                                                                    |
| `type`          | `String`                                          | El tipo de la métrica (p. ej., &quot;counter&quot;, &quot;gauge&quot;)                                                                                                                                                                                                                  |
| `unit`          | `String`                                          | La unidad de la métrica                                                                                                                                                                                                                                                                 |
| `help`          | `String`                                          | La descripción de la métrica                                                                                                                                                                                                                                                            |

Ejemplo:

```sql
INSERT INTO my_table (metric_name, tags, time_series) VALUES
    ('cpu_usage', {'job': 'node_exporter', 'instance': 'host1:9100'},
     [(toDateTime64('2024-01-01 00:00:00', 3), 0.5), (toDateTime64('2024-01-01 00:01:00', 3), 0.7)])
```

Se permite que `metric_name` esté vacío al insertar; esto significa que el nombre de la métrica se especifica en `tags` en `__name__`, por ejemplo:

```sql
INSERT INTO my_table (tags, time_series) VALUES
    ({'__name__': 'cpu_usage', 'job': 'test'},
     [(toDateTime64('2024-01-01 00:00:00', 3), 0.5)])
```

Para insertar los metadatos de las métricas, inserte datos en las columnas `metric_family`, `type`, `unit` y `help`:

```sql
INSERT INTO my_table (metric_name, tags, time_series, metric_family, type, unit, help) VALUES
    ('http_requests_total', {'method': 'GET'}, [(now64(), 100.0)],
     'http_requests_total', 'counter', 'requests', 'Total HTTP requests')
```

<div id="specifying-outer-columns">
  ### Especificación de las columnas externas
</div>

La columna externa `time_series` puede incluirse explícitamente en una sentencia `CREATE TABLE` para sobrescribir su tipo predeterminado `Array(Tuple(DateTime64(3), Float64))`. ClickHouse extrae la marca de tiempo y los tipos escalares de la tupla y los propaga a la tabla interna Samples:

```sql
CREATE TABLE my_table (time_series Array(Tuple(UInt32, Float32))) ENGINE=TimeSeries
```

Esto equivale a declarar directamente en la cláusula `INNER COLUMNS` de samples los tipos de las columnas de marca de tiempo y valor:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp UInt32, value Float32)
```

Si ambas formas se utilizan en la misma sentencia `CREATE TABLE`, los tipos declarados deben coincidir.

<div id="target-tables">
  ## Tablas de destino
</div>

Una tabla `TimeSeries` no tiene datos propios; todo se almacena en sus tablas de destino.
Esto es similar al funcionamiento de una [vista materializada](../../../sql-reference/statements/create/view#materialized-view),
con la diferencia de que una vista materializada tiene una única tabla de destino,
mientras que una tabla `TimeSeries` tiene tres tablas de destino llamadas [sample](#samples-table), [tag](#tags-table) y [metric](#metrics-table).

Las tablas de destino pueden especificarse explícitamente en la consulta `CREATE TABLE`
o el motor de tabla `TimeSeries` puede generar automáticamente tablas de destino internas.

Las filas insertadas en una tabla `TimeSeries` se transforman, se dividen en bloques y se insertan en estas tres tablas de destino.

Las tablas de destino son las siguientes:

<div id="samples-table">
  ### Tabla de samples
</div>

La tabla *samples* contiene series temporales asociadas a un identificador.

La tabla *samples* debe tener las columnas siguientes:

| Nombre      | ¿Obligatorio? | Tipo predeterminado | Tipos posibles        | Descripción                                                   |
| ----------- | ------------- | ------------------- | --------------------- | ------------------------------------------------------------- |
| `id`        | [x]           | `UUID`              | cualquiera            | Identifica una combinación de nombres de métricas y tag |
| `timestamp` | [x]           | `DateTime64(3)`     | `DateTime64(X)`       | Un punto en el tiempo                                         |
| `value`     | [x]           | `Float64`           | `Float32` o `Float64` | Un valor asociado al `timestamp`                              |

<div id="tags-table">
  ### Tabla de tags
</div>

La tabla *tags* contiene identificadores calculados para cada combinación de nombre de métrica y tags.

La tabla *tags* debe tener las siguientes columnas:

| Nombre               | ¿Obligatorio? | Tipo predeterminado                   | Tipos posibles                                                                                                        | Descripción                                                                                                                                                                                            |
| -------------------- | ------------- | ------------------------------------- | --------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `id`                 | [x]           | `UUID`                                | cualquiera (debe coincidir con el tipo de `id` de la tabla [samples](#samples-table))                                 | Un `id` identifica una combinación de nombre de métrica y tags. La expresión DEFAULT especifica cómo calcular dicho identificador                                                                      |
| `metric_name`        | [x]           | `LowCardinality(String)`              | `String` o `LowCardinality(String)`                                                                                   | El nombre de una métrica                                                                                                                                                                               |
| `<tag_value_column>` | [ ]           | `String`                              | `String` o `LowCardinality(String)` o `LowCardinality(Nullable(String))`                                              | El valor de un tag específico; el nombre del tag y el de la columna correspondiente se especifican en la configuración [tags&#95;to&#95;columns](#settings)                                            |
| `tags`               | [x]           | `Map(LowCardinality(String), String)` | `Map(String, String)` o `Map(LowCardinality(String), String)` o `Map(LowCardinality(String), LowCardinality(String))` | Mapa de tags que excluye el tag `__name__`, que contiene el nombre de una métrica, y los tags cuyos nombres se enumeran en la configuración [tags&#95;to&#95;columns](#settings)                       |
| `all_tags`           | [ ]           | `Map(String, String)`                 | `Map(String, String)` o `Map(LowCardinality(String), String)` o `Map(LowCardinality(String), LowCardinality(String))` | Columna efímera; cada fila es un mapa de todos los tags, excluyendo únicamente el tag `__name__`, que contiene el nombre de una métrica. El único propósito de esta columna es usarse al calcular `id` |
| `min_time`           | [ ]           | `Nullable(DateTime64(3))`             | `DateTime64(X)` o `Nullable(DateTime64(X))`                                                                           | Marca de tiempo mínima de la serie temporal con ese `id`. La columna se crea si [store&#95;min&#95;time&#95;and&#95;max&#95;time](#settings) es `true`                                                  |
| `max_time`           | [ ]           | `Nullable(DateTime64(3))`             | `DateTime64(X)` o `Nullable(DateTime64(X))`                                                                           | Marca de tiempo máxima de la serie temporal con ese `id`. La columna se crea si [store&#95;min&#95;time&#95;and&#95;max&#95;time](#settings) es `true`                                                  |

<div id="metrics-table">
  ### Tabla de métricas
</div>

La tabla *metrics* contiene información sobre las métricas recopiladas, sus tipos y sus descripciones.

La tabla *metrics* debe tener las siguientes columnas:

| Nombre               | ¿Obligatorio? | Tipo predeterminado      | Tipos posibles                       | Descripción                                                                                                                                                                     |
| -------------------- | ------------- | ------------------------ | ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `metric_family_name` | [x]           | `String`                 | `String` or `LowCardinality(String)` | El nombre de una familia de métricas                                                                                                                                            |
| `type`               | [x]           | `LowCardinality(String)` | `String` or `LowCardinality(String)` | El tipo de una familia de métricas; uno de &quot;counter&quot;, &quot;gauge&quot;, &quot;summary&quot;, &quot;stateset&quot;, &quot;histogram&quot;, &quot;gaugehistogram&quot; |
| `unit`               | [x]           | `LowCardinality(String)` | `String` or `LowCardinality(String)` | La unidad utilizada en una métrica                                                                                                                                              |
| `help`               | [x]           | `String`                 | `String` or `LowCardinality(String)` | La descripción de una métrica                                                                                                                                                   |

<div id="creation">
  ## Creación
</div>

Hay varias formas de crear una tabla con el motor de tabla `TimeSeries`.
La instrucción más sencilla

```sql
CREATE TABLE my_table ENGINE=TimeSeries
```

en realidad creará la siguiente tabla (puedes comprobarlo ejecutando `SHOW CREATE TABLE my_table`):

```sql
CREATE TABLE my_table
(
    `metric_name` String,
    `tags` Map(String, String),
    `time_series` Array(Tuple(DateTime64(3), Float64)),
    `metric_family` String,
    `type` String,
    `unit` String,
    `help` String
)
ENGINE = TimeSeries
SAMPLES INNER COLUMNS
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
SAMPLES INNER ENGINE = MergeTree ORDER BY (id, timestamp)
TAGS INNER COLUMNS
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
TAGS INNER ENGINE = AggregatingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1
METRICS INNER COLUMNS
(
    `metric_family_name` String,
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
METRICS INNER ENGINE = ReplacingMergeTree ORDER BY metric_family_name
```

Así, las columnas se generaron automáticamente y, además, hay tres tablas de destino internas con sus propias definiciones de columnas
almacenadas en las cláusulas `INNER COLUMNS`.

Las tablas de destino internas tienen nombres como `.inner_id.samples.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`,
`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, `.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
y cada tabla de destino tiene su propio conjunto de columnas:

```sql
CREATE TABLE default.`.inner_id.samples.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp)
```

```sql
CREATE TABLE default.`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
ENGINE = AggregatingMergeTree
PRIMARY KEY metric_name
ORDER BY (metric_name, id)
SETTINGS allow_dimensions_outside_sorting_key = 1
```

```sql
CREATE TABLE default.`.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `metric_family_name` String,
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
ENGINE = ReplacingMergeTree
ORDER BY metric_family_name
```

<div id="create-as">
  ## Crear una tabla AS a partir de una tabla existente
</div>

La sentencia `CREATE TABLE new_table AS existing_table` copia de `existing_table`:

* `SETTINGS`
* `INNER COLUMNS` para cada tipo
* `INNER ENGINE` para cada tipo

Esta sentencia no se permite si `existing_table` tiene destinos externos.
La lista de columnas externa se vuelve a generar y no se copia.

<div id="adjusting-column-types">
  ## Ajuste de los tipos de las columnas
</div>

Puede ajustar los tipos de las columnas en las tablas de destino internas mediante la cláusula `INNER COLUMNS`. Por ejemplo, para almacenar marcas de tiempo en microsegundos y valores como `Float32`:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp DateTime64(6), value Float32)
```

La misma cláusula puede usarse para especificar códecs y otros atributos de columna:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp DateTime64(3) CODEC(DoubleDelta))
```

<div id="id-column">
  ## La columna `id`
</div>

La columna `id` contiene identificadores; cada uno se calcula a partir de una combinación de un nombre de métrica y tag.
El tipo y la expresión `DEFAULT` utilizados para generar los identificadores se pueden personalizar mediante la cláusula `TAGS INNER COLUMNS`:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
TAGS INNER COLUMNS (id UInt64 DEFAULT sipHash64(metric_name, all_tags))
```

El tipo de la columna `id` debe ser `UUID`, `UInt64`, `UInt128` o `FixedString(16)`. Si no se proporciona ninguna expresión `DEFAULT`, ClickHouse la elegirá automáticamente en función del tipo de `id`. Los tipos de `id` declarados en las tablas internas `samples` y `tags` deben coincidir.

La configuración `id_generator` ofrece la misma posibilidad de personalización sin usar la cláusula `INNER COLUMNS`:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SETTINGS id_generator = 'sipHash64(metric_name, all_tags)'
```

Si este ajuste está establecido, se usa para generar `id` incluso si el `DEFAULT` de la columna contiene una expresión diferente.

<div id="tags-and-all-tags">
  ## Las columnas `tags` y `all_tags`
</div>

Hay dos columnas que contienen mapas de tags: `tags` y `all_tags`. En este ejemplo significan lo mismo; sin embargo, pueden ser distintas
si se usa la configuración `tags_to_columns`. Esta configuración permite especificar que un tag concreto se almacene en una columna independiente en lugar de hacerlo
en un mapa dentro de la columna `tags`:

```sql
CREATE TABLE my_table
ENGINE = TimeSeries 
SETTINGS tags_to_columns = {'instance': 'instance', 'job': 'job'}
```

Esta sentencia agregará las columnas `instance` y `job` a la tabla de destino interna [tags](#tags-table).
En este caso, la columna `tags` no contendrá los tags `instance` y `job`,
pero la columna `all_tags` sí las contendrá. La columna `all_tags` es efímera y su único propósito es servir en la expresión DEFAULT
de la columna `id`.

<div id="inner-table-engines">
  ## Motores de tabla de las tablas de destino internas
</div>

De forma predeterminada, las tablas de destino internas usan los siguientes motores de tabla:

* la tabla [samples](#samples-table) usa [MergeTree](../mergetree-family/mergetree);
* la tabla [tags](#tags-table) usa [AggregatingMergeTree](../mergetree-family/aggregatingmergetree) porque los mismos datos suelen insertarse varias veces en esta tabla, por lo que necesitamos una forma
  de eliminar los duplicados, y también porque es necesario realizar la agregación de las columnas `min_time` y `max_time`;
* la tabla [metrics](#metrics-table) usa [ReplacingMergeTree](../mergetree-family/replacingmergetree) porque los mismos datos suelen insertarse varias veces en esta tabla, por lo que necesitamos una forma
  de eliminar los duplicados.

También se pueden usar otros motores de tabla para las tablas de destino internas si así se especifica:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES ENGINE=ReplicatedMergeTree
TAGS ENGINE=ReplicatedAggregatingMergeTree
METRICS ENGINE=ReplicatedReplacingMergeTree
```

La tabla [tags](#tags-table) mantiene las columnas de tags (y los Maps `tags`/`all_tags`) fuera de su clave de ordenación,
algo que `AggregatingMergeTree` rechaza de forma predeterminada (consulte [`allow_dimensions_outside_sorting_key`](../mergetree-family/aggregatingmergetree)).
Esto es seguro aquí porque esas columnas dependen funcionalmente de `id`, que forma parte de la clave de ordenación, por lo que todas las
filas que un merge en segundo plano colapsa comparten los mismos valores. Cuando se genera la tabla interna de tags o su
engine se especifica Inline, como arriba, `TimeSeries` establece `allow_dimensions_outside_sorting_key = 1` en ella automáticamente;
en una tabla [externa](#external-target-tables) de agregación de tags creada manualmente, debe configurarlo usted mismo.

<div id="external-target-tables">
  ## Tablas de destino externas
</div>

Es posible hacer que una tabla `TimeSeries` utilice una tabla creada manualmente:

```sql
CREATE TABLE samples_for_my_table
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp);

CREATE TABLE tags_for_my_table ...

CREATE TABLE metrics_for_my_table ...

CREATE TABLE my_table ENGINE=TimeSeries SAMPLES samples_for_my_table TAGS tags_for_my_table METRICS metrics_for_my_table;
```

Los tipos de las columnas de las tablas externas (`id`, `timestamp`, `value` y las `<tag_value_column>` incluidas en [`tags_to_columns`](#settings)) deben coincidir con los que la tabla `TimeSeries` generaría internamente en caso contrario (consulte [Samples table](#samples-table), [Tags table](#tags-table) y [Metrics table](#metrics-table) para conocer las restricciones de tipos). Las incompatibilidades de tipos se notifican en el momento de `CREATE`.

La expresión del generador de id para un destino externo de tags se resuelve en el momento de INSERT en el siguiente orden: la configuración [`id_generator`](#settings) (si se ha establecido), luego el `DEFAULT` declarado en la columna `id` de la tabla externa (si existe) y, por último, el generador canónico derivado del tipo de `id`. Por lo tanto, la configuración prevalece sobre cualquier `DEFAULT` declarado en la tabla externa; consulte [La columna `id`](#id-column) para obtener más información.

<div id="altering-settings">
  ## Modificar la configuración
</div>

Se pueden cambiar dos configuraciones después de `CREATE`:

* `id_generator`
* `filter_by_min_time_and_max_time`

```sql
ALTER TABLE my_table MODIFY SETTING id_generator = 'sipHash64(metric_name, all_tags)';
ALTER TABLE my_table MODIFY SETTING filter_by_min_time_and_max_time = 0;
```

Ten en cuenta que cambiar `id_generator` cuando ya hay datos en la tabla Tags puede producir IDs distintos para la misma combinación de métrica+tag: las filas antiguas conservan sus IDs anteriores y las filas nuevas usan el nuevo generador.

Los demás ajustes no se pueden cambiar con `ALTER ... MODIFY SETTING` porque se incorporan al esquema de las tablas internas en el momento de `CREATE`.

<div id="settings">
  ## Configuración
</div>

A continuación se muestra una lista de opciones de configuración que se pueden especificar al definir una tabla `TimeSeries`:

| Nombre                               | Tipo      | Predeterminado           | Descripción                                                                                                                                                                                                                                                                                   |
| ------------------------------------ | --------- | ------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id_generator`                       | Expresión | depende del tipo de `id` | Expresión que calcula el identificador (huella) de una serie temporal a partir de sus tags. Si no se establece, se usa la expresión predeterminada de la columna `id`. Si la expresión predeterminada de la columna `id` tampoco está establecida, la expresión se selecciona automáticamente |
| `tags_to_columns`                    | Map       | {}                       | Map que especifica qué tags deben colocarse en columnas independientes en la tabla [tags](#tags-table). Sintaxis: `{'tag1': 'column1', 'tag2' : column2, ...}`                                                                                                                                |
| `use_all_tags_column_to_generate_id` | Bool      | true                     | Al generar una expresión para calcular el identificador de una serie temporal, este indicador permite usar la columna `all_tags` en dicho cálculo                                                                                                                                             |
| `store_min_time_and_max_time`        | Bool      | true                     | Si se establece en true, la tabla almacenará `min_time` y `max_time` para cada serie temporal                                                                                                                                                                                                 |
| `aggregate_min_time_and_max_time`    | Bool      | true                     | Al crear una tabla `tags` interna de destino, este indicador permite usar `SimpleAggregateFunction(min, Nullable(DateTime64(3)))` en lugar de solo `Nullable(DateTime64(3))` como tipo de la columna `min_time`, y lo mismo para la columna `max_time`                                        |
| `filter_by_min_time_and_max_time`    | Bool      | true                     | Si se establece en true, la tabla usará las columnas `min_time` y `max_time` para filtrar series temporales                                                                                                                                                                                   |

<div id="functions">
  # Funciones
</div>

Aquí tienes una lista de funciones que aceptan una tabla `TimeSeries` como argumento:

* [timeSeriesSamples](../../../sql-reference/table-functions/timeSeriesSamples.md)
* [timeSeriesTags](../../../sql-reference/table-functions/timeSeriesTags.md)
* [timeSeriesMetrics](../../../sql-reference/table-functions/timeSeriesMetrics.md)