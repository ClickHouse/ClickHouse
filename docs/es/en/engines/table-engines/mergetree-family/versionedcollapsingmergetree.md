---
description: 'Permite escribir rápidamente estados de objetos que cambian constantemente
  y eliminar en segundo plano los estados antiguos de los objetos.'
sidebar_label: 'VersionedCollapsingMergeTree'
sidebar_position: 80
slug: /engines/table-engines/mergetree-family/versionedcollapsingmergetree
title: 'motor de tabla VersionedCollapsingMergeTree'
doc_type: 'reference'
---

Este motor:

* Permite escribir rápidamente estados de objetos que cambian constantemente.
* Elimina en segundo plano estados antiguos de objetos. Esto reduce significativamente el espacio de almacenamiento.

Consulte la sección [Collapsing](#table_engines_versionedcollapsingmergetree) para más detalles.

El motor hereda de [MergeTree](/es/engines/table-engines/mergetree-family/mergetree) y añade la lógica de colapso de filas al algoritmo de fusión de partes de datos. `VersionedCollapsingMergeTree` cumple el mismo propósito que [CollapsingMergeTree](../../../engines/table-engines/mergetree-family/collapsingmergetree.md), pero utiliza un algoritmo de colapso diferente que permite insertar datos en cualquier orden usando varios hilos. En particular, la columna `Version` ayuda a colapsar correctamente las filas, incluso si se insertan en un orden incorrecto. En cambio, `CollapsingMergeTree` solo permite una inserción estrictamente consecutiva.

<div id="creating-a-table">
  ## Crear una tabla
</div>

```sql
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

Para ver una descripción de los parámetros de consulta, consulte la [descripción de la consulta](../../../sql-reference/statements/create/table.md).

<div id="engine-parameters">
  ### Parámetros del motor
</div>

```sql
VersionedCollapsingMergeTree(sign, version)
```

| Parámetro | Descripción                                                                                                              | Tipo                                                                                                                                                                                                                                                                                         |
| --------- | ------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sign`    | Nombre de la columna que indica el tipo de fila: `1` es una fila &quot;estado&quot;, `-1` es una fila &quot;cancelación&quot;. | [`Int8`](/es/sql-reference/data-types/int-uint)                                                                                                                                                                                                                                                 |
| `version` | Nombre de la columna que contiene la versión del estado del objeto.                                                      | [`Int*`](/es/sql-reference/data-types/int-uint), [`UInt*`](/es/sql-reference/data-types/int-uint), [`Date`](/es/sql-reference/data-types/date), [`Date32`](/es/sql-reference/data-types/date32), [`DateTime`](/es/sql-reference/data-types/datetime) o [`DateTime64`](/es/sql-reference/data-types/datetime64) |

<div id="query-clauses">
  ### Cláusulas de consulta
</div>

Al crear una tabla `VersionedCollapsingMergeTree`, se requieren las mismas [cláusulas](../../../engines/table-engines/mergetree-family/mergetree.md) que al crear una tabla `MergeTree`.

<details markdown="1">
  <summary>Método obsoleto para crear una tabla</summary>

  :::note
  No utilice este método en proyectos nuevos. Si es posible, cambie los proyectos antiguos al método descrito anteriormente.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] VersionedCollapsingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, sign, version)
  ```

  Todos los parámetros, excepto `sign` y `version`, tienen el mismo significado que en `MergeTree`.

  * `sign` — Nombre de la columna con el tipo de fila: `1` es una fila de estado, `-1` es una fila de cancelación.

    Tipo de dato de la columna — `Int8`.

  * `version` — Nombre de la columna con la versión del estado del objeto.

    El tipo de dato de la columna debe ser `UInt*`.
</details>

<div id="table_engines_versionedcollapsingmergetree">
  ## Collapsing
</div>

<div id="data">
  ### Datos
</div>

Considere una situación en la que necesita guardar datos que cambian continuamente para un objeto. Es razonable tener una fila por objeto y actualizarla cada vez que haya cambios. Sin embargo, la operación de actualización es costosa y lenta para un SGBD, porque requiere reescribir los datos en el almacenamiento. La actualización no es aceptable si necesita escribir datos rápidamente, pero puede escribir los cambios de un objeto de forma secuencial, como se muestra a continuación.

Use la columna `Sign` al escribir la fila. Si `Sign = 1`, significa que la fila representa el estado de un objeto (llamémosla la fila de &quot;estado&quot;). Si `Sign = -1`, indica la cancelación del estado de un objeto con los mismos atributos (llamémosla la fila de &quot;cancelación&quot;). Utilice también la columna `Version`, que debe identificar cada estado de un objeto con un número distinto.

Por ejemplo, queremos calcular cuántas páginas visitaron los usuarios en un sitio y cuánto tiempo permanecieron allí. En un momento determinado, escribimos la siguiente fila con el estado de la actividad del usuario:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

Más adelante, registramos el cambio en la actividad del usuario y lo escribimos en las dos filas siguientes.

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

La primera fila anula el estado anterior del objeto (usuario). Debe copiar todos los campos del estado anulado, excepto `Sign`.

La segunda fila contiene el estado actual.

Como solo necesitamos el último estado de la actividad del usuario, las filas

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

puede eliminarse, con lo que se colapsa el estado no válido (antiguo) del objeto. `VersionedCollapsingMergeTree` hace esto al fusionar las partes de datos.

Para averiguar por qué se necesitan dos filas para cada cambio, consulte [Algorithm](#table_engines-versionedcollapsingmergetree-algorithm).

**Notas sobre el uso**

1. El programa que escribe los datos debe recordar el estado de un objeto para poder cancelarlo. La cadena &quot;Cancelación&quot; debe contener copias de los campos de la clave primaria, la versión de la cadena &quot;estado&quot; y el `Sign` opuesto. Esto aumenta el tamaño inicial del almacenamiento, pero permite escribir los datos rápidamente.
2. Los arrays largos y crecientes en las columnas reducen la eficiencia del motor debido a la carga de escritura. Cuanto más sencillos sean los datos, mayor será la eficiencia.
3. Los resultados de `SELECT` dependen en gran medida de la consistencia del historial de cambios del objeto. Sea preciso al preparar los datos para insertar. Con datos incoherentes, puede obtener resultados impredecibles, como valores negativos en métricas no negativas, como la profundidad de la sesión.

<div id="table_engines-versionedcollapsingmergetree-algorithm">
  ### Algoritmo
</div>

Cuando ClickHouse fusiona partes de datos, elimina cada par de filas que tienen la misma clave primaria y la misma versión, pero un `Sign` distinto. El orden de las filas no importa.

Cuando ClickHouse inserta datos, ordena las filas por la clave primaria. Si la columna `Version` no forma parte de la clave primaria, ClickHouse la añade implícitamente a la clave primaria como último campo y la usa para ordenar.

<div id="selecting-data">
  ## Selección de datos
</div>

ClickHouse no garantiza que todas las filas con la misma clave primaria estén en la misma parte de datos resultante, ni siquiera en el mismo servidor físico. Esto se aplica tanto a la escritura de los datos como a la posterior fusión de las partes de datos. Además, ClickHouse procesa las consultas `SELECT` con múltiples hilos y no puede predecir el orden de las filas en el resultado. Esto significa que se requiere agregación si necesita obtener datos completamente &quot;colapsados&quot; de una tabla `VersionedCollapsingMergeTree`.

Para completar el colapso, escriba una consulta con una cláusula `GROUP BY` y funciones de agregación que tengan en cuenta el signo. Por ejemplo, para calcular la cantidad, use `sum(Sign)` en lugar de `count()`. Para calcular la suma de algún valor, use `sum(Sign * x)` en lugar de `sum(x)` y añada `HAVING sum(Sign) > 0`.

Las funciones de agregación `count`, `sum` y `avg` pueden calcularse de esta forma. La función de agregación `uniq` puede calcularse si un objeto tiene al menos un estado no colapsado. Las funciones de agregación `min` y `max` no pueden calcularse porque `VersionedCollapsingMergeTree` no guarda el historial de valores de los estados colapsados.

Si necesita extraer los datos con &quot;colapso&quot; pero sin agregación (por ejemplo, para comprobar si hay filas cuyos valores más recientes coinciden con determinadas condiciones), puede usar el modificador `FINAL` para la cláusula `FROM`. Este enfoque es ineficiente y no debe utilizarse con tablas grandes.

<div id="example-of-use">
  ## Ejemplo de uso
</div>

Datos de ejemplo:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 |
│ 4324182021466249494 │         5 │      146 │   -1 │       1 |
│ 4324182021466249494 │         6 │      185 │    1 │       2 |
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

Crear la tabla:

```sql
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

Inserción de los datos:

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1, 1)
```

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1, 1),(4324182021466249494, 6, 185, 1, 2)
```

Usamos dos consultas `INSERT` para crear dos partes de datos distintas. Si insertamos los datos con una sola consulta, ClickHouse crea una única parte de datos y nunca realizará ninguna fusión.

Obtener los datos:

```sql
SELECT * FROM UAct
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 │
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

¿Qué vemos aquí y dónde están las partes colapsadas?
Creamos dos partes de datos con dos consultas `INSERT`. La consulta `SELECT` se ejecutó en dos hilos, y el resultado es un orden aleatorio de las filas.
El colapso no se produjo porque las partes de datos aún no se han fusionado. ClickHouse fusiona las partes de datos en algún momento que no podemos predecir.

Por eso necesitamos agregación:

```sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration,
    Version
FROM UAct
GROUP BY UserID, Version
HAVING sum(Sign) > 0
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │       2 │
└─────────────────────┴───────────┴──────────┴─────────┘
```

Si no necesitamos agregación y queremos forzar el colapso, podemos usar el modificador `FINAL` en la cláusula `FROM`.

```sql
SELECT * FROM UAct FINAL
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
```

Esta es una forma muy ineficiente de seleccionar datos. No la utilices con tablas grandes.