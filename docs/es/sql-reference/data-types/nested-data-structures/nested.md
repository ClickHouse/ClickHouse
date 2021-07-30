---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 57
toc_title: Anidado (Nombre1 Tipo1, Nombre2 Tipo2, ...)
---

# Nested(name1 Type1, Name2 Type2, …) {#nestedname1-type1-name2-type2}

A nested data structure is like a table inside a cell. The parameters of a nested data structure – the column names and types – are specified the same way as in a [CREATE TABLE](../../../sql-reference/statements/create.md) consulta. Cada fila de la tabla puede corresponder a cualquier número de filas en una estructura de datos anidada.

Ejemplo:

``` sql
CREATE TABLE test.visits
(
    CounterID UInt32,
    StartDate Date,
    Sign Int8,
    IsNew UInt8,
    VisitID UInt64,
    UserID UInt64,
    ...
    Goals Nested
    (
        ID UInt32,
        Serial UInt32,
        EventTime DateTime,
        Price Int64,
        OrderID String,
        CurrencyID UInt32
    ),
    ...
) ENGINE = CollapsingMergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192, Sign)
```

Este ejemplo declara la `Goals` estructura de datos anidada, que contiene datos sobre conversiones (objetivos alcanzados). Cada fila en el ‘visits’ la tabla puede corresponder a cero o cualquier número de conversiones.

Solo se admite un único nivel de anidamiento. Las columnas de estructuras anidadas que contienen matrices son equivalentes a matrices multidimensionales, por lo que tienen un soporte limitado (no hay soporte para almacenar estas columnas en tablas con el motor MergeTree).

En la mayoría de los casos, cuando se trabaja con una estructura de datos anidada, sus columnas se especifican con nombres de columna separados por un punto. Estas columnas forman una matriz de tipos coincidentes. Todas las matrices de columnas de una sola estructura de datos anidados tienen la misma longitud.

Ejemplo:

``` sql
SELECT
    Goals.ID,
    Goals.EventTime
FROM test.visits
WHERE CounterID = 101500 AND length(Goals.ID) < 5
LIMIT 10
```

``` text
┌─Goals.ID───────────────────────┬─Goals.EventTime───────────────────────────────────────────────────────────────────────────┐
│ [1073752,591325,591325]        │ ['2014-03-17 16:38:10','2014-03-17 16:38:48','2014-03-17 16:42:27']                       │
│ [1073752]                      │ ['2014-03-17 00:28:25']                                                                   │
│ [1073752]                      │ ['2014-03-17 10:46:20']                                                                   │
│ [1073752,591325,591325,591325] │ ['2014-03-17 13:59:20','2014-03-17 22:17:55','2014-03-17 22:18:07','2014-03-17 22:18:51'] │
│ []                             │ []                                                                                        │
│ [1073752,591325,591325]        │ ['2014-03-17 11:37:06','2014-03-17 14:07:47','2014-03-17 14:36:21']                       │
│ []                             │ []                                                                                        │
│ []                             │ []                                                                                        │
│ [591325,1073752]               │ ['2014-03-17 00:46:05','2014-03-17 00:46:05']                                             │
│ [1073752,591325,591325,591325] │ ['2014-03-17 13:28:33','2014-03-17 13:30:26','2014-03-17 18:51:21','2014-03-17 18:51:45'] │
└────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────┘
```

Es más fácil pensar en una estructura de datos anidados como un conjunto de múltiples matrices de columnas de la misma longitud.

El único lugar donde una consulta SELECT puede especificar el nombre de una estructura de datos anidada completa en lugar de columnas individuales es la cláusula ARRAY JOIN. Para obtener más información, consulte “ARRAY JOIN clause”. Ejemplo:

``` sql
SELECT
    Goal.ID,
    Goal.EventTime
FROM test.visits
ARRAY JOIN Goals AS Goal
WHERE CounterID = 101500 AND length(Goals.ID) < 5
LIMIT 10
```

``` text
┌─Goal.ID─┬──────Goal.EventTime─┐
│ 1073752 │ 2014-03-17 16:38:10 │
│  591325 │ 2014-03-17 16:38:48 │
│  591325 │ 2014-03-17 16:42:27 │
│ 1073752 │ 2014-03-17 00:28:25 │
│ 1073752 │ 2014-03-17 10:46:20 │
│ 1073752 │ 2014-03-17 13:59:20 │
│  591325 │ 2014-03-17 22:17:55 │
│  591325 │ 2014-03-17 22:18:07 │
│  591325 │ 2014-03-17 22:18:51 │
│ 1073752 │ 2014-03-17 11:37:06 │
└─────────┴─────────────────────┘
```

No puede realizar SELECT para toda una estructura de datos anidados. Solo puede enumerar explícitamente columnas individuales que forman parte de él.

Para una consulta INSERT, debe pasar todas las matrices de columnas de componentes de una estructura de datos anidada por separado (como si fueran matrices de columnas individuales). Durante la inserción, el sistema comprueba que tienen la misma longitud.

Para una consulta DESCRIBE, las columnas de una estructura de datos anidada se enumeran por separado de la misma manera.

La consulta ALTER para elementos en una estructura de datos anidados tiene limitaciones.

[Artículo Original](https://clickhouse.tech/docs/en/data_types/nested_data_structures/nested/) <!--hide-->
