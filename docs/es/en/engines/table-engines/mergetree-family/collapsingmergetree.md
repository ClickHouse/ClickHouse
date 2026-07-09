---
description: 'Hereda de MergeTree, pero añade lógica para colapsar filas durante el
  proceso de fusión.'
keywords: ['updates', 'collapsing']
sidebar_label: 'CollapsingMergeTree'
sidebar_position: 70
slug: /engines/table-engines/mergetree-family/collapsingmergetree
title: 'Motor de tabla CollapsingMergeTree'
doc_type: 'guide'
---

<div id="description">
  ## Descripción
</div>

El motor `CollapsingMergeTree` hereda de [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md)
y añade lógica para colapsar filas durante el proceso de fusión.
El motor de tabla `CollapsingMergeTree` elimina (colapsa) de forma asíncrona
pares de filas si todos los campos de una clave de ordenación (`ORDER BY`) son equivalentes, salvo el campo especial `Sign`,
que puede tener un valor de `1` o de `-1`.
Las filas que no tienen un par con el valor opuesto de `Sign` se conservan.

Para obtener más información, consulte la sección [Collapsing](#table_engine-collapsingmergetree-collapsing) de este documento.

:::note
Este motor puede reducir significativamente el volumen de almacenamiento,
lo que aumenta la eficiencia de las consultas `SELECT`.
:::

<div id="parameters">
  ## Parámetros
</div>

Todos los parámetros de este motor de tabla, excepto el parámetro `Sign`,
tienen el mismo significado que en [`MergeTree`](/es/engines/table-engines/mergetree-family/mergetree).

* `Sign` — El nombre de la columna que indica el tipo de fila, donde `1` es una fila de &quot;estado&quot; y `-1` es una fila de &quot;cancelación&quot;. Tipo: [Int8](/es/sql-reference/data-types/int-uint).

<div id="creating-a-table">
  ## Crear una tabla
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) 
ENGINE = CollapsingMergeTree(Sign)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

<details markdown="1">
  <summary>Método obsoleto para crear una tabla</summary>

  :::note
  No se recomienda usar el método que aparece a continuación en proyectos nuevos.
  Si es posible, recomendamos actualizar los proyectos antiguos para que utilicen el nuevo método.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) 
  ENGINE [=] CollapsingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, Sign)
  ```

  `Sign` — Nombre de una columna que indica el tipo de fila, donde `1` es una fila de &quot;estado&quot; y `-1` es una fila de &quot;cancelación&quot;. [Int8](/es/sql-reference/data-types/int-uint).
</details>

* Para ver una descripción de los parámetros de consulta, consulte la [descripción de la consulta](../../../sql-reference/statements/create/table.md).
* Al crear una tabla `CollapsingMergeTree`, se requieren las mismas [cláusulas de consulta](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) que al crear una tabla `MergeTree`.

<div id="table_engine-collapsingmergetree-collapsing">
  ## Collapsing
</div>

<div id="data">
  ### Datos
</div>

Considera la situación en la que necesitas guardar datos que cambian continuamente para un objeto determinado.
Puede parecer lógico tener una fila por objeto y actualizarla cada vez que cambie algo;
sin embargo, las operaciones de actualización son costosas y lentas para el SGBD, porque requieren reescribir los datos en el almacenamiento.
Si necesitamos escribir datos rápidamente, realizar un gran número de actualizaciones no es una opción aceptable,
pero siempre podemos escribir secuencialmente los cambios de un objeto.
Para ello, usamos la columna especial `Sign`.

* Si `Sign` = `1`, significa que la fila es una fila de &quot;estado&quot;: *una fila que contiene campos que representan el estado válido actual*.
* Si `Sign` = `-1`, significa que la fila es una fila de &quot;cancelación&quot;: *una fila que se usa para cancelar el estado de un objeto con los mismos atributos*.

Por ejemplo, queremos calcular cuántas páginas visitaron los usuarios en un sitio web y durante cuánto tiempo lo hicieron.
En un momento determinado, escribimos la siguiente fila con el estado de la actividad del usuario:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Más adelante, registramos el cambio en la actividad del usuario y lo escribimos en las dos filas siguientes:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

La primera fila cancela el estado anterior del objeto (que en este caso representa a un usuario).
Debe copiar todos los campos de la clave de ordenación de la fila &quot;cancelada&quot;, excepto `Sign`.
La segunda fila contiene el estado actual.

Como solo necesitamos el último estado de la actividad del usuario, la fila original de &quot;estado&quot; y la fila de &quot;cancelación&quot;
que insertamos pueden eliminarse, como se muestra a continuación, colapsando el estado no válido (antiguo) de un objeto:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │ -- old "state" row can be deleted
│ 4324182021466249494 │         5 │      146 │   -1 │ -- "cancel" row can be deleted
│ 4324182021466249494 │         6 │      185 │    1 │ -- new "state" row remains
└─────────────────────┴───────────┴──────────┴──────┘
```

`CollapsingMergeTree` lleva a cabo precisamente este comportamiento de *collapsing* durante la fusión de las partes de datos.

:::note
La razón por la que se necesitan dos filas para cada cambio
se explica con más detalle en el apartado [Algorithm](#table_engine-collapsingmergetree-collapsing-algorithm).
:::

**Las particularidades de este enfoque**

1. El programa que escribe los datos debe recordar el estado de un objeto para poder cancelarlo. La fila de &quot;cancelación&quot; debe contener copias de los campos de la clave de ordenación del &quot;estado&quot; y el `Sign` opuesto. Esto aumenta el tamaño inicial del almacenamiento, pero permite escribir los datos rápidamente.
2. Los arrays largos y en crecimiento en las columnas reducen la eficiencia del motor debido al aumento de la carga de escritura. Cuanto más simples sean los datos, mayor será la eficiencia.
3. Los resultados de `SELECT` dependen en gran medida de la consistencia del historial de cambios del objeto. Sea cuidadoso al preparar los datos para insertarlos. Los datos inconsistentes pueden producir resultados impredecibles. Por ejemplo, valores negativos para métricas no negativas, como la profundidad de la sesión.

<div id="table_engine-collapsingmergetree-collapsing-algorithm">
  ### Algoritmo
</div>

Cuando ClickHouse fusiona [partes](/es/concepts/glossary#parts) de datos,
cada grupo de filas consecutivas con la misma clave de ordenación (`ORDER BY`) se reduce a no más de dos filas:
la fila &quot;estado&quot; con `Sign` = `1` y la fila &quot;cancelación&quot; con `Sign` = `-1`.
En otras palabras, en ClickHouse las entradas se colapsan.

Para cada parte de datos resultante, ClickHouse guarda:

|    |                                                                                                                                                                                              |
| -- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1. | La primera fila &quot;cancelación&quot; y la última fila &quot;estado&quot;, si el número de filas &quot;estado&quot; y &quot;cancelación&quot; coincide y la última fila es una fila &quot;estado&quot;. |
| 2. | La última fila &quot;estado&quot;, si hay más filas &quot;estado&quot; que filas &quot;cancelación&quot;.                                                                                           |
| 3. | La primera fila &quot;cancelación&quot;, si hay más filas &quot;cancelación&quot; que filas &quot;estado&quot;.                                                                                         |
| 4. | Ninguna fila, en todos los demás casos.                                                                                                                                                      |

Además, cuando hay al menos dos filas &quot;estado&quot; más que filas &quot;cancelación&quot;,
o al menos dos filas &quot;cancelación&quot; más que filas &quot;estado&quot;, la fusión continúa.
Sin embargo, ClickHouse trata esta situación como un error lógico y la registra en el registro del servidor.
Este error puede producirse si los mismos datos se insertan más de una vez.
Por tanto, el colapsado no debería cambiar los resultados del cálculo de estadísticas.
Los cambios se colapsan gradualmente para que, al final, solo quede el último estado de casi todos los objetos.

La columna `Sign` es necesaria porque el algoritmo de fusión no garantiza
que todas las filas con la misma clave de ordenación estén en la misma parte de datos resultante, ni siquiera en el mismo servidor físico.
ClickHouse procesa las consultas `SELECT` con múltiples hilos, y no puede predecir el orden de las filas en el resultado.

La agregación es necesaria si se necesita obtener datos completamente &quot;colapsados&quot; de la tabla `CollapsingMergeTree`.
Para completar el colapsado, escriba una consulta con la cláusula `GROUP BY` y funciones de agregación que tengan en cuenta el signo.
Por ejemplo, para calcular la cantidad, use `sum(Sign)` en lugar de `count()`.
Para calcular la suma de algún valor, use `sum(Sign * x)` junto con `HAVING sum(Sign) > 0` en lugar de `sum(x)`
como en el [ejemplo](#example-of-use) siguiente.

Las funciones de agregación `count`, `sum` y `avg` podrían calcularse de esta manera.
La función de agregación `uniq` podría calcularse si un objeto tiene al menos un estado no colapsado.
Las funciones de agregación `min` y `max` no podrían calcularse
porque `CollapsingMergeTree` no guarda el historial de los estados colapsados.

:::note
Si necesita extraer datos sin agregación
(por ejemplo, para comprobar si hay filas cuyos valores más recientes coinciden con determinadas condiciones),
puede usar el modificador [`FINAL`](../../../sql-reference/statements/select/from.md#final-modifier) para la cláusula `FROM`. Fusionará los datos antes de devolver el resultado.
Para CollapsingMergeTree, solo se devuelve la fila del estado más reciente para cada clave.
:::

<div id="examples">
  ## Ejemplos
</div>

<div id="example-of-use">
  ### Ejemplo de uso
</div>

Con los siguientes datos de ejemplo:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Vamos a crear una tabla `UAct` con `CollapsingMergeTree`:

```sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews UInt8,
    Duration UInt8,
    Sign Int8
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY UserID
```

A continuación, insertaremos algunos datos:

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1)
```

```sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1),(4324182021466249494, 6, 185, 1)
```

Usamos dos consultas `INSERT` para crear dos partes de datos distintas.

:::note
Si insertamos los datos con una sola consulta, ClickHouse crea solo una parte de datos y no realizará ninguna fusión.
:::

Podemos seleccionar los datos con:

```sql
SELECT * FROM UAct
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Echemos un vistazo a los datos devueltos anteriormente y veamos si se produjo el colapso...
Con dos consultas `INSERT`, creamos dos partes de datos.
La consulta `SELECT` se ejecutó en dos hilos y obtuvimos las filas en un orden aleatorio.
Sin embargo, el colapso **no se produjo** porque todavía no se había realizado ninguna fusión de las partes de datos
y ClickHouse fusiona las partes de datos en segundo plano en un momento desconocido que no podemos predecir.

Por lo tanto, necesitamos una agregación,
que realizamos con la función de agregación [`sum`](/es/sql-reference/aggregate-functions/reference/sum)
y la cláusula [`HAVING`](/es/sql-reference/statements/select/having):

```sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration
FROM UAct
GROUP BY UserID
HAVING sum(Sign) > 0
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

Si no necesitamos agregación y queremos forzar el colapsado, también podemos usar el modificador `FINAL` en la cláusula `FROM`.

```sql
SELECT * FROM UAct FINAL
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

:::note
Esta forma de seleccionar los datos es menos eficiente y no se recomienda cuando se trabaja con grandes volúmenes de datos analizados (millones de filas).
:::

<div id="example-of-another-approach">
  ### Ejemplo de otro enfoque
</div>

La idea de este enfoque es que las fusiones solo tienen en cuenta los campos clave.
En la fila &quot;cancelación&quot;, por tanto, podemos especificar valores negativos
que compensen la versión anterior de la fila al sumar, sin usar la columna `Sign`.

Para este ejemplo, utilizaremos los datos de muestra que aparecen a continuación:

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │        -5 │     -146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Para este enfoque, es necesario cambiar los tipos de datos de `PageViews` y `Duration` para poder almacenar valores negativos.
Por ello, cambiamos el tipo de estas columnas de `UInt8` a `Int16` al crear nuestra tabla `UAct` con
`collapsingMergeTree`:

```sql
CREATE TABLE UAct
(
    UserID UInt64,
    PageViews Int16,
    Duration Int16,
    Sign Int8
)
ENGINE = CollapsingMergeTree(Sign)
ORDER BY UserID
```

Probemos este enfoque insertando datos en nuestra tabla.

No obstante, para ejemplos o tablas pequeñas, es aceptable:

```sql
INSERT INTO UAct VALUES(4324182021466249494,  5,  146,  1);
INSERT INTO UAct VALUES(4324182021466249494, -5, -146, -1);
INSERT INTO UAct VALUES(4324182021466249494,  6,  185,  1);

SELECT * FROM UAct FINAL;
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

```sql
SELECT
    UserID,
    sum(PageViews) AS PageViews,
    sum(Duration) AS Duration
FROM UAct
GROUP BY UserID
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

```sql
SELECT COUNT() FROM UAct
```

```text
┌─count()─┐
│       3 │
└─────────┘
```

```sql
OPTIMIZE TABLE UAct FINAL;

SELECT * FROM UAct
```

```text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```