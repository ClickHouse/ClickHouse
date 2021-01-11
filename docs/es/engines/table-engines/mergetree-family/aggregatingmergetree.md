---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 35
toc_title: "Agregaci\xF3nMergeTree"
---

# Aggregatingmergetree {#aggregatingmergetree}

El motor hereda de [Método de codificación de datos:](mergetree.md#table_engines-mergetree), alterando la lógica para la fusión de partes de datos. ClickHouse reemplaza todas las filas con la misma clave principal (o más exactamente, con la misma [clave de clasificación](mergetree.md)) con una sola fila (dentro de una parte de datos) que almacena una combinación de estados de funciones agregadas.

Usted puede utilizar `AggregatingMergeTree` tablas para la agregación de datos incrementales, incluidas las vistas materializadas agregadas.

El motor procesa todas las columnas con los siguientes tipos:

-   [AggregateFunction](../../../sql-reference/data-types/aggregatefunction.md)
-   [SimpleAggregateFunction](../../../sql-reference/data-types/simpleaggregatefunction.md)

Es apropiado usar `AggregatingMergeTree` si reduce el número de filas por pedidos.

## Creación de una tabla {#creating-a-table}

``` sql
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

Para obtener una descripción de los parámetros de solicitud, consulte [descripción de la solicitud](../../../sql-reference/statements/create.md).

**Cláusulas de consulta**

Al crear un `AggregatingMergeTree` mesa de la misma [clausula](mergetree.md) se requieren, como al crear un `MergeTree` tabla.

<details markdown="1">

<summary>Método obsoleto para crear una tabla</summary>

!!! attention "Atención"
    No use este método en proyectos nuevos y, si es posible, cambie los proyectos antiguos al método descrito anteriormente.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] AggregatingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
```

Todos los parámetros tienen el mismo significado que en `MergeTree`.
</details>

## SELECCIONAR e INSERTAR {#select-and-insert}

Para insertar datos, utilice [INSERT SELECT](../../../sql-reference/statements/insert-into.md) consulta con funciones agregadas -State-.
Al seleccionar datos de `AggregatingMergeTree` mesa, uso `GROUP BY` cláusula y las mismas funciones agregadas que al insertar datos, pero usando `-Merge` sufijo.

En los resultados de `SELECT` consulta, los valores de `AggregateFunction` tipo tiene representación binaria específica de la implementación para todos los formatos de salida de ClickHouse. Si volcar datos en, por ejemplo, `TabSeparated` formato con `SELECT` consulta entonces este volcado se puede cargar de nuevo usando `INSERT` consulta.

## Ejemplo de una vista materializada agregada {#example-of-an-aggregated-materialized-view}

`AggregatingMergeTree` vista materializada que mira el `test.visits` tabla:

``` sql
CREATE MATERIALIZED VIEW test.basic
ENGINE = AggregatingMergeTree() PARTITION BY toYYYYMM(StartDate) ORDER BY (CounterID, StartDate)
AS SELECT
    CounterID,
    StartDate,
    sumState(Sign)    AS Visits,
    uniqState(UserID) AS Users
FROM test.visits
GROUP BY CounterID, StartDate;
```

Insertar datos en el `test.visits` tabla.

``` sql
INSERT INTO test.visits ...
```

Los datos se insertan tanto en la tabla como en la vista `test.basic` que realizará la agregación.

Para obtener los datos agregados, necesitamos ejecutar una consulta como `SELECT ... GROUP BY ...` de la vista `test.basic`:

``` sql
SELECT
    StartDate,
    sumMerge(Visits) AS Visits,
    uniqMerge(Users) AS Users
FROM test.basic
GROUP BY StartDate
ORDER BY StartDate;
```

[Artículo Original](https://clickhouse.tech/docs/en/operations/table_engines/aggregatingmergetree/) <!--hide-->
