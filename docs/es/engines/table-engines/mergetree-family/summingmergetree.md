---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 34
toc_title: SummingMergeTree
---

# SummingMergeTree {#summingmergetree}

El motor hereda de [Método de codificación de datos:](mergetree.md#table_engines-mergetree). La diferencia es que al fusionar partes de datos para `SummingMergeTree` ClickHouse reemplaza todas las filas con la misma clave primaria (o más exactamente, con la misma [clave de clasificación](mergetree.md)) con una fila que contiene valores resumidos para las columnas con el tipo de datos numérico. Si la clave de ordenación está compuesta de manera que un solo valor de clave corresponde a un gran número de filas, esto reduce significativamente el volumen de almacenamiento y acelera la selección de datos.

Recomendamos usar el motor junto con `MergeTree`. Almacenar datos completos en `MergeTree` mesa, y el uso `SummingMergeTree` para el almacenamiento de datos agregados, por ejemplo, al preparar informes. Tal enfoque evitará que pierda datos valiosos debido a una clave primaria compuesta incorrectamente.

## Creación de una tabla {#creating-a-table}

``` sql
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

Para obtener una descripción de los parámetros de solicitud, consulte [descripción de la solicitud](../../../sql-reference/statements/create.md).

**Parámetros de SummingMergeTree**

-   `columns` - una tupla con los nombres de las columnas donde se resumirán los valores. Parámetro opcional.
    Las columnas deben ser de tipo numérico y no deben estar en la clave principal.

    Si `columns` no especificado, ClickHouse resume los valores de todas las columnas con un tipo de datos numérico que no están en la clave principal.

**Cláusulas de consulta**

Al crear un `SummingMergeTree` mesa de la misma [clausula](mergetree.md) se requieren, como al crear un `MergeTree` tabla.

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
) ENGINE [=] SummingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [columns])
```

Todos los parámetros excepto `columns` el mismo significado que en `MergeTree`.

-   `columns` — tuple with names of columns values of which will be summarized. Optional parameter. For a description, see the text above.

</details>

## Ejemplo de uso {#usage-example}

Considere la siguiente tabla:

``` sql
CREATE TABLE summtt
(
    key UInt32,
    value UInt32
)
ENGINE = SummingMergeTree()
ORDER BY key
```

Insertar datos:

``` sql
INSERT INTO summtt Values(1,1),(1,2),(2,1)
```

ClickHouse puede sumar todas las filas no completamente ([ver abajo](#data-processing)), entonces usamos una función agregada `sum` y `GROUP BY` cláusula en la consulta.

``` sql
SELECT key, sum(value) FROM summtt GROUP BY key
```

``` text
┌─key─┬─sum(value)─┐
│   2 │          1 │
│   1 │          3 │
└─────┴────────────┘
```

## Procesamiento de datos {#data-processing}

Cuando los datos se insertan en una tabla, se guardan tal cual. ClickHouse combina las partes insertadas de los datos periódicamente y esto es cuando las filas con la misma clave principal se suman y se reemplazan con una para cada parte resultante de los datos.

ClickHouse can merge the data parts so that different resulting parts of data cat consist rows with the same primary key, i.e. the summation will be incomplete. Therefore (`SELECT`) una función agregada [resumir()](../../../sql-reference/aggregate-functions/reference.md#agg_function-sum) y `GROUP BY` cláusula se debe utilizar en una consulta como se describe en el ejemplo anterior.

### Reglas comunes para la suma {#common-rules-for-summation}

Se resumen los valores de las columnas con el tipo de datos numérico. El conjunto de columnas está definido por el parámetro `columns`.

Si los valores eran 0 en todas las columnas para la suma, se elimina la fila.

Si la columna no está en la clave principal y no se resume, se selecciona un valor arbitrario entre los existentes.

Los valores no se resumen para las columnas de la clave principal.

### La suma en las columnas de función agregada {#the-summation-in-the-aggregatefunction-columns}

Para columnas de [Tipo AggregateFunction](../../../sql-reference/data-types/aggregatefunction.md) ClickHouse se comporta como [AgregaciónMergeTree](aggregatingmergetree.md) agregación del motor según la función.

### Estructuras anidadas {#nested-structures}

La tabla puede tener estructuras de datos anidadas que se procesan de una manera especial.

Si el nombre de una tabla anidada termina con `Map` y contiene al menos dos columnas que cumplen los siguientes criterios:

-   la primera columna es numérica `(*Int*, Date, DateTime)` o una cadena `(String, FixedString)`, vamos a llamarlo `key`,
-   las otras columnas son aritméticas `(*Int*, Float32/64)`, vamos a llamarlo `(values...)`,

entonces esta tabla anidada se interpreta como una asignación de `key => (values...)`, y al fusionar sus filas, los elementos de dos conjuntos de datos se fusionan por `key` con una suma de los correspondientes `(values...)`.

Ejemplos:

``` text
[(1, 100)] + [(2, 150)] -> [(1, 100), (2, 150)]
[(1, 100)] + [(1, 150)] -> [(1, 250)]
[(1, 100)] + [(1, 150), (2, 150)] -> [(1, 250), (2, 150)]
[(1, 100), (2, 150)] + [(1, -100)] -> [(2, 150)]
```

Al solicitar datos, utilice el [sumMap(clave, valor)](../../../sql-reference/aggregate-functions/reference.md) función para la agregación de `Map`.

Para la estructura de datos anidados, no necesita especificar sus columnas en la tupla de columnas para la suma.

[Artículo Original](https://clickhouse.tech/docs/en/operations/table_engines/summingmergetree/) <!--hide-->
