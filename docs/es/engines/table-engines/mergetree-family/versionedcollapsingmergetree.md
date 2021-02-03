---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 37
toc_title: VersionedCollapsingMergeTree
---

# VersionedCollapsingMergeTree {#versionedcollapsingmergetree}

Este motor:

-   Permite la escritura rápida de estados de objetos que cambian continuamente.
-   Elimina los estados de objetos antiguos en segundo plano. Esto reduce significativamente el volumen de almacenamiento.

Vea la sección [Derrumbar](#table_engines_versionedcollapsingmergetree) para más detalles.

El motor hereda de [Método de codificación de datos:](mergetree.md#table_engines-mergetree) y agrega la lógica para colapsar filas al algoritmo para fusionar partes de datos. `VersionedCollapsingMergeTree` tiene el mismo propósito que [ColapsarMergeTree](collapsingmergetree.md) pero usa un algoritmo de colapso diferente que permite insertar los datos en cualquier orden con múltiples hilos. En particular, el `Version` columna ayuda a contraer las filas correctamente, incluso si se insertan en el orden incorrecto. En contraste, `CollapsingMergeTree` sólo permite la inserción estrictamente consecutiva.

## Creación de una tabla {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = VersionedCollapsingMergeTree(sign, version)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Para obtener una descripción de los parámetros de consulta, consulte [descripción de la consulta](../../../sql-reference/statements/create.md).

**Parámetros del motor**

``` sql
VersionedCollapsingMergeTree(sign, version)
```

-   `sign` — Name of the column with the type of row: `1` es una “state” fila, `-1` es una “cancel” fila.

    El tipo de datos de columna debe ser `Int8`.

-   `version` — Name of the column with the version of the object state.

    El tipo de datos de columna debe ser `UInt*`.

**Cláusulas de consulta**

Al crear un `VersionedCollapsingMergeTree` mesa, la misma [clausula](mergetree.md) se requieren como al crear un `MergeTree` tabla.

<details markdown="1">

<summary>Método obsoleto para crear una tabla</summary>

!!! attention "Atención"
    No utilice este método en nuevos proyectos. Si es posible, cambie los proyectos antiguos al método descrito anteriormente.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] VersionedCollapsingMergeTree(date-column [, samp#table_engines_versionedcollapsingmergetreeling_expression], (primary, key), index_granularity, sign, version)
```

Todos los parámetros excepto `sign` y `version` el mismo significado que en `MergeTree`.

-   `sign` — Name of the column with the type of row: `1` es una “state” fila, `-1` es una “cancel” fila.

    Column Data Type — `Int8`.

-   `version` — Name of the column with the version of the object state.

    El tipo de datos de columna debe ser `UInt*`.

</details>

## Derrumbar {#table_engines_versionedcollapsingmergetree}

### Datos {#data}

Considere una situación en la que necesite guardar datos que cambien continuamente para algún objeto. Es razonable tener una fila para un objeto y actualizar la fila siempre que haya cambios. Sin embargo, la operación de actualización es costosa y lenta para un DBMS porque requiere volver a escribir los datos en el almacenamiento. La actualización no es aceptable si necesita escribir datos rápidamente, pero puede escribir los cambios en un objeto secuencialmente de la siguiente manera.

Utilice el `Sign` columna al escribir la fila. Si `Sign = 1` significa que la fila es un estado de un objeto (llamémoslo el “state” fila). Si `Sign = -1` indica la cancelación del estado de un objeto con los mismos atributos (llamémoslo el “cancel” fila). También use el `Version` columna, que debe identificar cada estado de un objeto con un número separado.

Por ejemplo, queremos calcular cuántas páginas visitaron los usuarios en algún sitio y cuánto tiempo estuvieron allí. En algún momento escribimos la siguiente fila con el estado de la actividad del usuario:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

En algún momento después registramos el cambio de actividad del usuario y lo escribimos con las siguientes dos filas.

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

La primera fila cancela el estado anterior del objeto (usuario). Debe copiar todos los campos del estado cancelado excepto `Sign`.

La segunda fila contiene el estado actual.

Debido a que solo necesitamos el último estado de actividad del usuario, las filas

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

se puede eliminar, colapsando el estado no válido (antiguo) del objeto. `VersionedCollapsingMergeTree` hace esto mientras fusiona las partes de datos.

Para averiguar por qué necesitamos dos filas para cada cambio, vea [Algoritmo](#table_engines-versionedcollapsingmergetree-algorithm).

**Notas sobre el uso**

1.  El programa que escribe los datos debe recordar el estado de un objeto para cancelarlo. El “cancel” cadena debe ser una copia de la “state” con lo opuesto `Sign`. Esto aumenta el tamaño inicial de almacenamiento, pero permite escribir los datos rápidamente.
2.  Las matrices de largo crecimiento en columnas reducen la eficiencia del motor debido a la carga para escribir. Cuanto más sencillos sean los datos, mejor será la eficiencia.
3.  `SELECT` Los resultados dependen en gran medida de la coherencia del historial de cambios de objetos. Sea preciso al preparar los datos para insertarlos. Puede obtener resultados impredecibles con datos incoherentes, como valores negativos para métricas no negativas, como la profundidad de la sesión.

### Algoritmo {#table_engines-versionedcollapsingmergetree-algorithm}

Cuando ClickHouse combina partes de datos, elimina cada par de filas que tienen la misma clave principal y versión y diferentes `Sign`. El orden de las filas no importa.

Cuando ClickHouse inserta datos, ordena filas por la clave principal. Si el `Version` la columna no está en la clave principal, ClickHouse la agrega a la clave principal implícitamente como el último campo y la usa para ordenar.

## Selección de datos {#selecting-data}

ClickHouse no garantiza que todas las filas con la misma clave principal estén en la misma parte de datos resultante o incluso en el mismo servidor físico. Esto es cierto tanto para escribir los datos como para la posterior fusión de las partes de datos. Además, ClickHouse procesa `SELECT` consultas con múltiples subprocesos, y no puede predecir el orden de las filas en el resultado. Esto significa que la agregación es necesaria si hay una necesidad de obtener completamente “collapsed” datos de un `VersionedCollapsingMergeTree` tabla.

Para finalizar el colapso, escriba una consulta con un `GROUP BY` cláusula y funciones agregadas que representan el signo. Por ejemplo, para calcular la cantidad, use `sum(Sign)` en lugar de `count()`. Para calcular la suma de algo, use `sum(Sign * x)` en lugar de `sum(x)` y agregar `HAVING sum(Sign) > 0`.

Los agregados `count`, `sum` y `avg` se puede calcular de esta manera. El agregado `uniq` se puede calcular si un objeto tiene al menos un estado no colapsado. Los agregados `min` y `max` no se puede calcular porque `VersionedCollapsingMergeTree` no guarda el historial de valores de estados colapsados.

Si necesita extraer los datos con “collapsing” pero sin agregación (por ejemplo, para verificar si hay filas presentes cuyos valores más nuevos coinciden con ciertas condiciones), puede usar el `FINAL` modificador para el `FROM` clausula. Este enfoque es ineficiente y no debe usarse con tablas grandes.

## Ejemplo de uso {#example-of-use}

Datos de ejemplo:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

Creación de la tabla:

``` sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews UInt8,
    Duration UInt8,
    Sign Int8,
    Version UInt8
)
ENGINE = VersionedCollapsingMergeTree(Sign, Version)
ORDER BY UserID
```

Insertar los datos:

``` sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1, 1)
```

``` sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1, 1),(4324182021466249494, 6, 185, 1, 2)
```

Usamos dos `INSERT` consultas para crear dos partes de datos diferentes. Si insertamos los datos con una sola consulta, ClickHouse crea una parte de datos y nunca realizará ninguna fusión.

Obtener los datos:

``` sql
SELECT * FROM UAct
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 │
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

¿Qué vemos aquí y dónde están las partes colapsadas?
Creamos dos partes de datos usando dos `INSERT` consulta. El `SELECT` la consulta se realizó en dos subprocesos, y el resultado es un orden aleatorio de filas.
No se produjo el colapso porque las partes de datos aún no se han fusionado. ClickHouse fusiona partes de datos en un punto desconocido en el tiempo que no podemos predecir.

Es por eso que necesitamos agregación:

``` sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration,
    Version
FROM UAct
GROUP BY UserID, Version
HAVING sum(Sign) > 0
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │       2 │
└─────────────────────┴───────────┴──────────┴─────────┘
```

Si no necesitamos agregación y queremos forzar el colapso, podemos usar el `FINAL` modificador para el `FROM` clausula.

``` sql
SELECT * FROM UAct FINAL
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

Esta es una forma muy ineficiente de seleccionar datos. No lo use para mesas grandes.

[Artículo Original](https://clickhouse.tech/docs/en/operations/table_engines/versionedcollapsingmergetree/) <!--hide-->
