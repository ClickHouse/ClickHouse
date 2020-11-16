---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: ColapsarMergeTree
---

# ColapsarMergeTree {#table_engine-collapsingmergetree}

El motor hereda de [Método de codificación de datos:](mergetree.md) y agrega la lógica de las filas que colapsan al algoritmo de fusión de partes de datos.

`CollapsingMergeTree` elimina de forma asincrónica (colapsa) pares de filas si todos los campos de una clave de ordenación (`ORDER BY`) son equivalentes excepto el campo particular `Sign` que puede tener `1` y `-1` valor. Las filas sin un par se mantienen. Para más detalles, consulte el [Derrumbar](#table_engine-collapsingmergetree-collapsing) sección del documento.

El motor puede reducir significativamente el volumen de almacenamiento y aumentar la eficiencia de `SELECT` consulta como consecuencia.

## Creación de una tabla {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = CollapsingMergeTree(sign)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Para obtener una descripción de los parámetros de consulta, consulte [descripción de la consulta](../../../sql-reference/statements/create.md).

**CollapsingMergeTree Parámetros**

-   `sign` — Name of the column with the type of row: `1` es una “state” fila, `-1` es una “cancel” fila.

    Column data type — `Int8`.

**Cláusulas de consulta**

Al crear un `CollapsingMergeTree` mesa, la misma [cláusulas de consulta](mergetree.md#table_engine-mergetree-creating-a-table) se requieren, como al crear un `MergeTree` tabla.

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
) ENGINE [=] CollapsingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, sign)
```

Todos los parámetros excepto `sign` el mismo significado que en `MergeTree`.

-   `sign` — Name of the column with the type of row: `1` — “state” fila, `-1` — “cancel” fila.

    Column Data Type — `Int8`.

</details>

## Derrumbar {#table_engine-collapsingmergetree-collapsing}

### Datos {#data}

Considere la situación en la que necesita guardar datos que cambian continuamente para algún objeto. Parece lógico tener una fila para un objeto y actualizarla en cualquier cambio, pero la operación de actualización es costosa y lenta para DBMS porque requiere la reescritura de los datos en el almacenamiento. Si necesita escribir datos rápidamente, la actualización no es aceptable, pero puede escribir los cambios de un objeto secuencialmente de la siguiente manera.

Utilice la columna en particular `Sign`. Si `Sign = 1` significa que la fila es un estado de un objeto, llamémoslo “state” fila. Si `Sign = -1` significa la cancelación del estado de un objeto con los mismos atributos, llamémoslo “cancel” fila.

Por ejemplo, queremos calcular cuántas páginas revisaron los usuarios en algún sitio y cuánto tiempo estuvieron allí. En algún momento escribimos la siguiente fila con el estado de la actividad del usuario:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

En algún momento después registramos el cambio de actividad del usuario y lo escribimos con las siguientes dos filas.

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

La primera fila cancela el estado anterior del objeto (usuario). Debe copiar los campos clave de ordenación del estado cancelado exceptuando `Sign`.

La segunda fila contiene el estado actual.

Como solo necesitamos el último estado de actividad del usuario, las filas

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │         5 │      146 │   -1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

se puede eliminar colapsando el estado no válido (antiguo) de un objeto. `CollapsingMergeTree` hace esto mientras se fusionan las partes de datos.

Por qué necesitamos 2 filas para cada cambio leído en el [Algoritmo](#table_engine-collapsingmergetree-collapsing-algorithm) apartado.

**Propiedades peculiares de tal enfoque**

1.  El programa que escribe los datos debe recordar el estado de un objeto para poder cancelarlo. “Cancel” debe contener copias de los campos de clave de ordenación “state” y lo opuesto `Sign`. Aumenta el tamaño inicial de almacenamiento, pero permite escribir los datos rápidamente.
2.  Las matrices de largo crecimiento en columnas reducen la eficiencia del motor debido a la carga para escribir. Los datos más sencillos, mayor será la eficiencia.
3.  El `SELECT` Los resultados dependen en gran medida de la consistencia del historial de cambios de objetos. Sea preciso al preparar los datos para insertarlos. Puede obtener resultados impredecibles en datos incoherentes, por ejemplo, valores negativos para métricas no negativas, como la profundidad de la sesión.

### Algoritmo {#table_engine-collapsingmergetree-collapsing-algorithm}

Cuando ClickHouse combina partes de datos, cada grupo de filas consecutivas tiene la misma clave de ordenación (`ORDER BY`) se reduce a no más de dos filas, una con `Sign = 1` (“state” fila) y otro con `Sign = -1` (“cancel” fila). En otras palabras, las entradas colapsan.

Para cada parte de datos resultante, ClickHouse guarda:

1.  El primero “cancel” y el último “state” si el número de “state” y “cancel” y la última fila es una “state” fila.
2.  El último “state” fila, si hay más “state” filas que “cancel” filas.
3.  El primero “cancel” fila, si hay más “cancel” filas que “state” filas.
4.  Ninguna de las filas, en todos los demás casos.

También cuando hay al menos 2 más “state” filas que “cancel” filas, o al menos 2 más “cancel” filas entonces “state” fila, la fusión continúa, pero ClickHouse trata esta situación como un error lógico y la registra en el registro del servidor. Este error puede producirse si se insertan los mismos datos más de una vez.

Por lo tanto, el colapso no debe cambiar los resultados del cálculo de las estadísticas.
Los cambios colapsaron gradualmente para que al final solo quedara el último estado de casi todos los objetos.

El `Sign` se requiere porque el algoritmo de fusión no garantiza que todas las filas con la misma clave de clasificación estén en la misma parte de datos resultante e incluso en el mismo servidor físico. Proceso de ClickHouse `SELECT` consultas con múltiples hilos, y no puede predecir el orden de las filas en el resultado. La agregación es necesaria si hay una necesidad de obtener completamente “collapsed” datos de `CollapsingMergeTree` tabla.

Para finalizar el colapso, escriba una consulta con `GROUP BY` cláusula y funciones agregadas que representan el signo. Por ejemplo, para calcular la cantidad, use `sum(Sign)` en lugar de `count()`. Para calcular la suma de algo, use `sum(Sign * x)` en lugar de `sum(x)` y así sucesivamente, y también añadir `HAVING sum(Sign) > 0`.

Los agregados `count`, `sum` y `avg` podría calcularse de esta manera. El agregado `uniq` podría calcularse si un objeto tiene al menos un estado no colapsado. Los agregados `min` y `max` no se pudo calcular porque `CollapsingMergeTree` no guarda el historial de valores de los estados colapsados.

Si necesita extraer datos sin agregación (por ejemplo, para comprobar si hay filas presentes cuyos valores más recientes coinciden con ciertas condiciones), puede utilizar el `FINAL` modificador para el `FROM` clausula. Este enfoque es significativamente menos eficiente.

## Ejemplo de uso {#example-of-use}

Datos de ejemplo:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Creación de la tabla:

``` sql
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

Inserción de los datos:

``` sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1)
```

``` sql
INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1),(4324182021466249494, 6, 185, 1)
```

Usamos dos `INSERT` consultas para crear dos partes de datos diferentes. Si insertamos los datos con una consulta, ClickHouse crea una parte de datos y nunca realizará ninguna fusión.

Obtener los datos:

``` sql
SELECT * FROM UAct
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

¿Qué vemos y dónde está colapsando?

Con dos `INSERT` consultas, hemos creado 2 partes de datos. El `SELECT` la consulta se realizó en 2 hilos, y obtuvimos un orden aleatorio de filas. No se ha producido un colapso porque todavía no se había fusionado las partes de datos. ClickHouse fusiona parte de datos en un momento desconocido que no podemos predecir.

Por lo tanto, necesitamos agregación:

``` sql
SELECT
    UserID,
    sum(PageViews * Sign) AS PageViews,
    sum(Duration * Sign) AS Duration
FROM UAct
GROUP BY UserID
HAVING sum(Sign) > 0
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

Si no necesitamos agregación y queremos forzar el colapso, podemos usar `FINAL` modificador para `FROM` clausula.

``` sql
SELECT * FROM UAct FINAL
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

Esta forma de seleccionar los datos es muy ineficiente. No lo use para mesas grandes.

## Ejemplo de otro enfoque {#example-of-another-approach}

Datos de ejemplo:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │    1 │
│ 4324182021466249494 │        -5 │     -146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

La idea es que las fusiones tengan en cuenta solo los campos clave. Y en el “Cancel” línea podemos especificar valores negativos que igualan la versión anterior de la fila al sumar sin usar la columna Sign. Para este enfoque, es necesario cambiar el tipo de datos `PageViews`,`Duration` para almacenar valores negativos de UInt8 -\> Int16.

``` sql
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

Vamos a probar el enfoque:

``` sql
insert into UAct values(4324182021466249494,  5,  146,  1);
insert into UAct values(4324182021466249494, -5, -146, -1);
insert into UAct values(4324182021466249494,  6,  185,  1);

select * from UAct final; // avoid using final in production (just for a test or small tables)
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

``` sql
SELECT
    UserID,
    sum(PageViews) AS PageViews,
    sum(Duration) AS Duration
FROM UAct
GROUP BY UserID
```text
┌──────────────UserID─┬─PageViews─┬─Duration─┐
│ 4324182021466249494 │         6 │      185 │
└─────────────────────┴───────────┴──────────┘
```

``` sqk
select count() FROM UAct
```

``` text
┌─count()─┐
│       3 │
└─────────┘
```

``` sql
optimize table UAct final;

select * FROM UAct
```

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

[Artículo Original](https://clickhouse.tech/docs/en/operations/table_engines/collapsingmergetree/) <!--hide-->
