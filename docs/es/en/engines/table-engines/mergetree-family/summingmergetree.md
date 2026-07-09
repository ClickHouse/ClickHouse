---
description: 'SummingMergeTree hereda del motor MergeTree. Su característica principal
  es la capacidad de sumar automáticamente datos numéricos durante las fusiones de partes.'
sidebar_label: 'SummingMergeTree'
sidebar_position: 50
slug: /engines/table-engines/mergetree-family/summingmergetree
title: 'Motor de tabla SummingMergeTree'
doc_type: 'reference'
---

El motor hereda de [MergeTree](/es/engines/table-engines/mergetree-family/mergetree). La diferencia es que, al fusionar partes de datos en las tablas `SummingMergeTree`, ClickHouse reemplaza todas las filas con la misma clave primaria (o, más precisamente, con la misma [clave de ordenación](../../../engines/table-engines/mergetree-family/mergetree.md)) por una sola fila que contiene los valores sumados de las columnas con tipo de dato numérico. Si la clave de ordenación está compuesta de forma que un único valor de clave corresponde a un gran número de filas, esto reduce significativamente el volumen de almacenamiento y acelera la selección de datos.

Recomendamos usar este motor junto con `MergeTree`. Almacene los datos completos en una tabla `MergeTree` y utilice `SummingMergeTree` para almacenar datos agregados, por ejemplo, al preparar informes. Este enfoque evitará que pierda datos valiosos debido a una clave primaria mal definida.

<div id="creating-a-table">
  ## Crear una tabla
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = SummingMergeTree([columns])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Para obtener una descripción de los parámetros de la solicitud, consulte la [descripción de la solicitud](../../../sql-reference/statements/create/table.md).

<div id="parameters-of-summingmergetree">
  ### Parámetros de SummingMergeTree
</div>

<div id="columns">
  #### Columnas
</div>

`columns` - una tupla con los nombres de las columnas cuyos valores se sumarán. Parámetro opcional.
Las columnas deben ser de tipo numérico y no deben estar en la partición ni en la clave de ordenación.

Si no se especifica `columns`, ClickHouse suma los valores de todas las columnas de tipo de dato numérico que no estén en la clave de ordenación.

<div id="query-clauses">
  ### Cláusulas de consulta
</div>

Al crear una tabla `SummingMergeTree`, se requieren las mismas [cláusulas](../../../engines/table-engines/mergetree-family/mergetree.md) que al crear una tabla `MergeTree`.

<details markdown="1">
  <summary>Método obsoleto para crear una tabla</summary>

  :::note
  No utilice este método en proyectos nuevos y, si es posible, cambie los proyectos antiguos al método descrito anteriormente.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] SummingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [columns])
  ```

  Todos los parámetros, excepto `columns`, tienen el mismo significado que en `MergeTree`.

  * `columns` — tupla con los nombres de las columnas cuyos valores se sumarán. Es un parámetro opcional. Para ver su descripción, consulte el texto anterior.
</details>

<div id="usage-example">
  ## Ejemplo de uso
</div>

Considere la siguiente tabla:

```sql
CREATE TABLE summtt
(
    key UInt32,
    value UInt32
)
ENGINE = SummingMergeTree()
ORDER BY key
```

Inserte datos en ella:

```sql
INSERT INTO summtt VALUES(1,1),(1,2),(2,1)
```

ClickHouse puede sumar todas las filas de manera incompleta ([ver más abajo](#data-processing)), por lo que usamos una función de agregación `sum` y la cláusula `GROUP BY` en la consulta.

```sql
SELECT key, sum(value) FROM summtt GROUP BY key
```

```text
┌─key─┬─sum(value)─┐
│   2 │          1 │
│   1 │          3 │
└─────┴────────────┘
```

<div id="data-processing">
  ## Procesamiento de datos
</div>

Cuando se insertan datos en una tabla, se guardan tal cual. ClickHouse fusiona periódicamente las partes de datos insertadas, y es entonces cuando las filas con la misma clave primaria se suman y se reemplazan por una sola en cada parte de datos resultante.

ClickHouse puede fusionar las partes de datos de modo que distintas partes de datos resultantes puedan contener filas con la misma clave primaria; es decir, la suma será incompleta. Por lo tanto, en una consulta se deben usar una función de agregación [sum()](/es/sql-reference/aggregate-functions/reference/sum) y la cláusula `GROUP BY`, como se describe en el ejemplo anterior.

<div id="common-rules-for-summation">
  ### Reglas comunes para la suma
</div>

Se suman los valores de las columnas con tipo de dato numérico. El conjunto de columnas se define mediante el parámetro `columns`.

Si los valores eran 0 en todas las columnas de la suma, la fila se elimina.

Si una columna no está en la clave primaria y no se suma, se selecciona un valor arbitrario de entre los existentes.

Los valores no se suman en las columnas de la clave primaria.

<div id="the-summation-in-the-aggregatefunction-columns">
  ### La suma en las columnas de AggregateFunction
</div>

Para las columnas de [tipo AggregateFunction](../../../sql-reference/data-types/aggregatefunction.md), ClickHouse se comporta como el motor [AggregatingMergeTree](../../../engines/table-engines/mergetree-family/aggregatingmergetree.md), y realiza la agregación de acuerdo con la función.

<div id="nested-structures">
  ### Estructuras anidadas
</div>

Una tabla puede tener estructuras de datos anidadas que se procesan de una forma especial.

Si el nombre de una tabla anidada termina en `Map` y contiene al menos dos columnas que cumplen los siguientes criterios:

* la primera columna es numérica `(*Int*, Date, DateTime)` o una cadena `(String, FixedString)`, llamémosla `key`,
* las demás columnas son aritméticas `(*Int*, Float32/64)`, llamémoslas `(values...)`,

entonces esta tabla anidada se interpreta como una asociación de `key => (values...)`, y al fusionar sus filas, los elementos de dos conjuntos de datos se fusionan por `key` con la suma de los `(values...)` correspondientes.

Ejemplos:

```text
DROP TABLE IF EXISTS nested_sum;
CREATE TABLE nested_sum
(
    date Date,
    site UInt32,
    hitsMap Nested(
        browser String,
        imps UInt32,
        clicks UInt32
    )
) ENGINE = SummingMergeTree
PRIMARY KEY (date, site);

INSERT INTO nested_sum VALUES ('2020-01-01', 12, ['Firefox', 'Opera'], [10, 5], [2, 1]);
INSERT INTO nested_sum VALUES ('2020-01-01', 12, ['Chrome', 'Firefox'], [20, 1], [1, 1]);
INSERT INTO nested_sum VALUES ('2020-01-01', 12, ['IE'], [22], [0]);
INSERT INTO nested_sum VALUES ('2020-01-01', 10, ['Chrome'], [4], [3]);

OPTIMIZE TABLE nested_sum FINAL; -- emulate merge 

SELECT * FROM nested_sum;
┌───────date─┬─site─┬─hitsMap.browser───────────────────┬─hitsMap.imps─┬─hitsMap.clicks─┐
│ 2020-01-01 │   10 │ ['Chrome']                        │ [4]          │ [3]            │
│ 2020-01-01 │   12 │ ['Chrome','Firefox','IE','Opera'] │ [20,11,22,5] │ [1,3,0,1]      │
└────────────┴──────┴───────────────────────────────────┴──────────────┴────────────────┘

SELECT
    site,
    browser,
    impressions,
    clicks
FROM
(
    SELECT
        site,
        sumMap(hitsMap.browser, hitsMap.imps, hitsMap.clicks) AS imps_map
    FROM nested_sum
    GROUP BY site
)
ARRAY JOIN
    imps_map.1 AS browser,
    imps_map.2 AS impressions,
    imps_map.3 AS clicks;

┌─site─┬─browser─┬─impressions─┬─clicks─┐
│   12 │ Chrome  │          20 │      1 │
│   12 │ Firefox │          11 │      3 │
│   12 │ IE      │          22 │      0 │
│   12 │ Opera   │           5 │      1 │
│   10 │ Chrome  │           4 │      3 │
└──────┴─────────┴─────────────┴────────┘
```

Al solicitar datos, utilice la función [sumMap(key, value)](../../../sql-reference/aggregate-functions/reference/sumMappedArrays.md) para la agregación de `Map`.

En una estructura de datos anidada, no es necesario especificar sus columnas en la tupla de columnas para la suma.

<div id="tuple-element-aggregation">
  ### Agregación de elementos de Tuple
</div>

Cuando la configuración `allow_tuple_element_aggregation` está habilitada, las columnas `Tuple` se aplanan de forma recursiva para que cada elemento hoja participe en la suma de manera independiente. Esto permite almacenar múltiples métricas en una sola columna `Tuple` y hacer que se sumen elemento por elemento durante las fusiones.

A las subcolumnas aplanadas se les aplican las mismas reglas que a las columnas normales:

* Solo se suman las subcolumnas numéricas.
* Las subcolumnas que pertenecen a un `Tuple` de la clave de ordenación o de la clave de partición se excluyen de la suma.
* Si se especifica `columns`, solo se suman las subcolumnas de las columnas `Tuple` incluidas en la lista.
* Si todas las subcolumnas numéricas de una fila son cero después de la suma, la fila se elimina.

:::note
Esta configuración es inmutable y debe especificarse al crear la tabla.
:::

```sql
CREATE TABLE summing_tuples
(
    key UInt32,
    metrics Tuple(
        impressions UInt64,
        clicks UInt64,
        nested Tuple(
            conversions UInt64
        )
    )
) ENGINE = SummingMergeTree()
ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO summing_tuples VALUES (1, (100, 10, (1)));
INSERT INTO summing_tuples VALUES (1, (200, 20, (3)));

OPTIMIZE TABLE summing_tuples FINAL;

SELECT key, metrics.impressions, metrics.clicks, metrics.nested.conversions FROM summing_tuples;
```

```text
┌─key─┬─metrics.impressions─┬─metrics.clicks─┬─metrics.nested.conversions─┐
│   1 │                 300 │             30 │                          4 │
└─────┴─────────────────────┴────────────────┴────────────────────────────┘
```

<div id="related-content">
  ## Contenido relacionado
</div>

* Blog: [Uso de los combinadores de agregación en ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)