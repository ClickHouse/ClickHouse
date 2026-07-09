---
description: 'Sustituye todas las filas con la misma clave primaria (o, más exactamente, con
  la misma [clave de ordenación](../../../engines/table-engines/mergetree-family/mergetree.md))
  por una sola fila (dentro de una única parte de datos) que almacena una combinación
  de estados de funciones de agregación.'
sidebar_label: 'AggregatingMergeTree'
sidebar_position: 60
slug: /engines/table-engines/mergetree-family/aggregatingmergetree
title: 'Motor de tabla AggregatingMergeTree'
doc_type: 'reference'
---

Este motor hereda de [MergeTree](/es/engines/table-engines/mergetree-family/mergetree) y modifica la lógica de fusión de las partes de datos. ClickHouse sustituye todas las filas con la misma clave primaria (o, más exactamente, con la misma [clave de ordenación](../../../engines/table-engines/mergetree-family/mergetree.md)) por una sola fila (dentro de una única parte de datos) que almacena una combinación de estados de funciones de agregación.

Puede usar tablas `AggregatingMergeTree` para la agregación incremental de datos, incluidas las vistas materializadas agregadas.

Puede ver un ejemplo de cómo usar AggregatingMergeTree y las funciones de agregación en el siguiente video:

<div class="vimeo-container">
  <iframe width="1030" height="579" src="https://www.youtube.com/embed/pryhI4F_zqQ" title="Estados de agregación en ClickHouse" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />
</div>

El motor procesa todas las columnas con los siguientes tipos:

* [`AggregateFunction`](../../../sql-reference/data-types/aggregatefunction.md)
* [`SimpleAggregateFunction`](../../../sql-reference/data-types/simpleaggregatefunction.md)

Conviene usar `AggregatingMergeTree` si reduce el número de filas en varios órdenes de magnitud.

<div id="creating-a-table">
  ## Crear una tabla
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = AggregatingMergeTree()
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[TTL expr]
[SETTINGS name=value, ...]
```

Para obtener una descripción de los parámetros de la solicitud, consulte la [descripción de la solicitud](../../../sql-reference/statements/create/table.md).

**Cláusulas de la consulta**

Al crear una tabla `AggregatingMergeTree`, se requieren las mismas [cláusulas](../../../engines/table-engines/mergetree-family/mergetree.md) que al crear una tabla `MergeTree`.

<details markdown="1">
  <summary>Método obsoleto para crear una tabla</summary>

  :::note
  No utilice este método en proyectos nuevos y, si es posible, migre los proyectos antiguos al método descrito anteriormente.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] AggregatingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
  ```

  Todos los parámetros tienen el mismo significado que en `MergeTree`.
</details>

<div id="select-and-insert">
  ## SELECT e INSERT
</div>

Para insertar datos, use la consulta [INSERT SELECT](../../../sql-reference/statements/insert-into.md) con funciones de agregación `-State-`.
Al seleccionar datos de una tabla `AggregatingMergeTree`, use la cláusula `GROUP BY` y las mismas funciones de agregación que al insertar los datos, pero con el sufijo `-Merge`.

En los resultados de una consulta `SELECT`, los valores del tipo `AggregateFunction` tienen una representación binaria específica de la implementación en todos los formatos de salida de ClickHouse. Por ejemplo, si exporta datos en formato `TabSeparated` con una consulta `SELECT`, este volcado puede volver a cargarse mediante una consulta `INSERT`.

<div id="example-of-an-aggregated-materialized-view">
  ## Ejemplo de una vista materializada agregada
</div>

En el siguiente ejemplo, se supone que tiene una base de datos llamada `test`. Créela si aún no existe con el comando que aparece a continuación:

```sql
CREATE DATABASE test;
```

Ahora, cree la tabla `test.visits`, que contiene los datos sin procesar:

```sql
CREATE TABLE test.visits
 (
    StartDate DateTime64 NOT NULL,
    CounterID UInt64,
    Sign Nullable(Int32),
    UserID Nullable(Int32)
) ENGINE = MergeTree ORDER BY (StartDate, CounterID);
```

A continuación, necesitas una tabla `AggregatingMergeTree` que almacenará `AggregationFunction`s para llevar un seguimiento del número total de visitas y del número de usuarios únicos.

Crea una vista materializada `AggregatingMergeTree` que supervise la tabla `test.visits` y utilice el tipo [`AggregateFunction`](/es/sql-reference/data-types/aggregatefunction):

```sql
CREATE TABLE test.agg_visits (
    StartDate DateTime64 NOT NULL,
    CounterID UInt64,
    Visits AggregateFunction(sum, Nullable(Int32)),
    Users AggregateFunction(uniq, Nullable(Int32))
)
ENGINE = AggregatingMergeTree() ORDER BY (StartDate, CounterID);
```

Cree una vista materializada que pueble `test.agg_visits` a partir de `test.visits`:

```sql
CREATE MATERIALIZED VIEW test.visits_mv TO test.agg_visits
AS SELECT
    StartDate,
    CounterID,
    sumState(Sign) AS Visits,
    uniqState(UserID) AS Users
FROM test.visits
GROUP BY StartDate, CounterID;
```

Inserte datos en la tabla `test.visits`:

```sql
INSERT INTO test.visits (StartDate, CounterID, Sign, UserID)
 VALUES (1667446031000, 1, 3, 4), (1667446031000, 1, 6, 3);
```

Los datos se insertan tanto en `test.visits` como en `test.agg_visits`.

Para obtener los datos agregados, ejecute una consulta como `SELECT ... GROUP BY ...` en la vista materializada `test.visits_mv`:

```sql
SELECT
    StartDate,
    sumMerge(Visits) AS Visits,
    uniqMerge(Users) AS Users
FROM test.visits_mv
GROUP BY StartDate
ORDER BY StartDate;
```

```text
┌───────────────StartDate─┬─Visits─┬─Users─┐
│ 2022-11-03 03:27:11.000 │      9 │     2 │
└─────────────────────────┴────────┴───────┘
```

Añade otro par de registros a `test.visits`, pero esta vez intenta usar una marca de tiempo diferente para uno de los registros:

```sql
INSERT INTO test.visits (StartDate, CounterID, Sign, UserID)
 VALUES (1669446031000, 2, 5, 10), (1667446031000, 3, 7, 5);
```

Ejecute de nuevo la consulta `SELECT`, que devolverá el siguiente resultado:

```text
┌───────────────StartDate─┬─Visits─┬─Users─┐
│ 2022-11-03 03:27:11.000 │     16 │     3 │
│ 2022-11-26 07:00:31.000 │      5 │     1 │
└─────────────────────────┴────────┴───────┘
```

En algunos casos, puede que desee evitar la preagregación de filas en el momento de la inserción para trasladar el costo de la agregación del momento de la inserción
al momento de la fusión. Normalmente, es necesario incluir las columnas que no forman parte de la agregación en la cláusula `GROUP BY`
de la definición de la vista materializada para evitar un error. Sin embargo, puede utilizar la función [`initializeAggregation`](/es/sql-reference/functions/other-functions#initializeAggregation)
con la configuración `optimize_on_insert = 0` (está activada de forma predeterminada) para conseguirlo. En este caso, el uso de `GROUP BY`
ya no es necesario:

```sql
CREATE MATERIALIZED VIEW test.visits_mv TO test.agg_visits
AS SELECT
    StartDate,
    CounterID,
    initializeAggregation('sumState', Sign) AS Visits,
    initializeAggregation('uniqState', UserID) AS Users
FROM test.visits;
```

:::note
Al usar `initializeAggregation`, se crea un estado de agregación para cada fila individual, sin agrupar.
Cada fila de origen produce una fila en la vista materializada, y la agregación real se realiza más tarde, cuando
`AggregatingMergeTree` fusiona partes. Esto solo es así si `optimize_on_insert = 0`.
:::

<div id="tuple-element-aggregation">
  ## Agregación de elementos de Tuple
</div>

Cuando la configuración `allow_tuple_element_aggregation` está habilitada, las columnas `Tuple` se aplanan de forma recursiva para que cada elemento hoja participe en la agregación de manera independiente. Esto significa que las sub-columnas `AggregateFunction` o `SimpleAggregateFunction` dentro de un `Tuple` se agregan según sus respectivas funciones, como si fueran columnas de nivel superior.

Las sub-columnas que pertenecen a un `Tuple` de la clave de ordenación se excluyen de la agregación. Las sub-columnas no agregadas se tratan como columnas ordinarias (se conserva su primer valor).

:::note
Esta configuración es inmutable y debe especificarse al crear la tabla.
:::

```sql
CREATE TABLE agg_tuples
(
    key UInt32,
    metrics Tuple(
        total_visits SimpleAggregateFunction(sum, UInt64),
        unique_users SimpleAggregateFunction(max, UInt64)
    )
) ENGINE = AggregatingMergeTree()
ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO agg_tuples VALUES (1, (100, 5));
INSERT INTO agg_tuples VALUES (1, (200, 8));
INSERT INTO agg_tuples VALUES (2, (50, 3));

OPTIMIZE TABLE agg_tuples FINAL;

SELECT key, metrics.total_visits, metrics.unique_users FROM agg_tuples ORDER BY key;
```

```text
┌─key─┬─metrics.total_visits─┬─metrics.unique_users─┐
│   1 │                  300 │                    8 │
│   2 │                   50 │                    3 │
└─────┴──────────────────────┴──────────────────────┘
```

`total_visits` se agrega mediante `sum` (100 + 200 = 300), mientras que `unique_users` se agrega mediante `max` (max(5, 8) = 8).

<div id="related-content">
  ## Contenido relacionado
</div>

* Blog: [Uso de combinadores de agregación en ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)