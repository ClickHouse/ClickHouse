---
description: 'Documentación sobre el tipo de dato AggregateFunction en ClickHouse, que
almacena estados intermedios de funciones de agregación'
keywords: ['AggregateFunction', 'Tipo']
sidebar_label: 'AggregateFunction'
sidebar_position: 46
slug: /sql-reference/data-types/aggregatefunction
title: 'Tipo AggregateFunction'
doc_type: 'reference'
---

<div id="description">
  ## Descripción
</div>

Todas las [funciones de agregación](/es/sql-reference/aggregate-functions) de ClickHouse tienen
un estado intermedio propio de la implementación que puede serializarse como un
tipo de datos `AggregateFunction` y almacenarse en una tabla. Esto suele hacerse por
medio de una [vista materializada](../../sql-reference/statements/create/view.md).

Hay dos [combinadores](/es/sql-reference/aggregate-functions/combinators) de funciones de agregación
que se usan habitualmente con el tipo `AggregateFunction`:

* El combinador de funciones de agregación [`-State`](/es/sql-reference/aggregate-functions/combinators#-state), que, al añadirse al nombre de una función de agregación,
  genera estados intermedios de `AggregateFunction`.
* El combinador de funciones de agregación [`-Merge`](/es/sql-reference/aggregate-functions/combinators#-merge), que se usa para obtener el resultado final de una agregación
  a partir de los estados intermedios.

<div id="syntax">
  ## Sintaxis
</div>

```sql
AggregateFunction(aggregate_function_name, types_of_arguments...)
```

**Parámetros**

* `aggregate_function_name` - El nombre de una función de agregación. Si la función
  es paramétrica, también deben especificarse sus parámetros.
* `types_of_arguments` - Los tipos de los argumentos de la función de agregación.

por ejemplo:

```sql
CREATE TABLE t
(
    column1 AggregateFunction(uniq, UInt64),
    column2 AggregateFunction(anyIf, String, UInt8),
    column3 AggregateFunction(quantiles(0.5, 0.9), UInt64)
) ENGINE = ...
```

<div id="usage">
  ## Uso
</div>

<div id="data-insertion">
  ### Inserción de datos
</div>

Para insertar datos en una tabla con columnas de tipo `AggregateFunction`, puede
utilizar `INSERT SELECT` con funciones de agregación y el
combinador de funciones de agregación
[`-State`](/es/sql-reference/aggregate-functions/combinators#-state).

Por ejemplo, para insertar datos en columnas de tipo `AggregateFunction(uniq, UInt64)` y
`AggregateFunction(quantiles(0.5, 0.9), UInt64)`, usaría las siguientes
funciones de agregación con combinadores.

```sql
uniqState(UserID)
quantilesState(0.5, 0.9)(SendTiming)
```

A diferencia de las funciones `uniq` y `quantiles`, `uniqState` y `quantilesState`
(con el combinador `-State` añadido) devuelven el estado, en lugar del valor final.
En otras palabras, devuelven un valor del tipo `AggregateFunction`.

En los resultados de la consulta `SELECT`, los valores de tipo `AggregateFunction` tienen
representaciones binarias específicas de la implementación en todos los formatos de salida
de ClickHouse.

Hay una configuración especial de nivel de sesión, `aggregate_function_input_format`, que permite construir el estado a partir de los valores de entrada.
Admite los siguientes formatos:

* `state` - cadena binaria con el estado serializado (el valor predeterminado).
  Si vuelca datos, por ejemplo, en el formato `TabSeparated` con una
  consulta `SELECT`, este volcado puede volver a cargarse mediante la consulta `INSERT`.
* `value` - el formato esperará un único valor del argumento de la función de agregación o, en el caso de varios argumentos, una tupla con ellos; se deserializará para formar el estado correspondiente
* `array` - el formato esperará un `Array` de valores, como se describe en la opción `value` anterior; todos los elementos del array se agregarán para formar el estado

<div id="data-selection">
  ### Selección de datos
</div>

Al seleccionar datos de una tabla `AggregatingMergeTree`, use la cláusula `GROUP BY`
y las mismas funciones de agregación que usó al insertar los datos, pero con el
combinador [`-Merge`](/es/sql-reference/aggregate-functions/combinators#-merge).

Una función de agregación con el combinador `-Merge` aplicado toma un conjunto de
estados, los combina y devuelve el resultado de la agregación completa de los datos.

Por ejemplo, las dos consultas siguientes devuelven el mismo resultado:

```sql
SELECT uniq(UserID) FROM table

SELECT uniqMerge(state) FROM (SELECT uniqState(UserID) AS state FROM table GROUP BY RegionID)
```

<div id="usage-example">
  ## Ejemplo de uso
</div>

Consulte la descripción del motor [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md).

<div id="related-content">
  ## Contenido relacionado
</div>

* Blog: [Uso de los combinadores de agregación en ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
* [MergeState](/es/sql-reference/aggregate-functions/combinators#-mergestate)
  combinador.
* [State](/es/sql-reference/aggregate-functions/combinators#-state) combinador.