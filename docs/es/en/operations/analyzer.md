---
description: 'Página que detalla el analizador de consultas de ClickHouse'
keywords: ['analyzer']
sidebar_label: 'Analizador'
slug: /operations/analyzer
title: 'Analizador'
doc_type: 'reference'
---

En la versión `24.3` de ClickHouse, el nuevo analizador de consultas se habilitó de forma predeterminada.
Puede obtener más información sobre su funcionamiento [aquí](/es/guides/developer/understanding-query-execution-with-the-analyzer#analyzer).

<div id="known-incompatibilities">
  ## Incompatibilidades conocidas
</div>

A pesar de corregir una gran cantidad de errores e introducir nuevas optimizaciones, también incorpora algunos cambios incompatibles en el comportamiento de ClickHouse. Lea los siguientes cambios para determinar cómo reescribir sus consultas para adaptarlas al analizador.

<div id="invalid-queries-are-no-longer-optimized">
  ### Las consultas no válidas ya no se optimizan
</div>

La infraestructura anterior de planificación de consultas aplicaba optimizaciones a nivel de AST antes del paso de validación de la consulta.
Las optimizaciones podían reescribir la consulta inicial para que fuera válida y ejecutable.

En el analizador, la validación de consultas se realiza antes del paso de optimización.
Esto significa que las consultas no válidas que antes podían ejecutarse ahora ya no se admiten.
En esos casos, la consulta debe corregirse manualmente.

<div id="example-1">
  #### Ejemplo 1
</div>

La siguiente consulta usa la columna `number` en la lista de proyección cuando, después de la agregación, solo está disponible `toString(number)`.
En el analizador anterior, `GROUP BY toString(number)` se optimizaba a `GROUP BY number,`, lo que hacía que la consulta fuera válida.

```sql
SELECT number
FROM numbers(1)
GROUP BY toString(number)
```

<div id="example-2">
  #### Ejemplo 2
</div>

El mismo problema se produce en esta consulta. La columna `number` se usa después de la agregación con otra clave.
El analizador de consultas anterior corregía esta consulta moviendo el filtro `number > 5` de la cláusula `HAVING` a la cláusula `WHERE`.

```sql
SELECT
    number % 2 AS n,
    sum(number)
FROM numbers(10)
GROUP BY n
HAVING number > 5
```

Para corregir la consulta, debes mover a la sección `WHERE` todas las condiciones que se aplican a columnas no agregadas, para ajustarte a la sintaxis SQL estándar:

```sql
SELECT
    number % 2 AS n,
    sum(number)
FROM numbers(10)
WHERE number > 5
GROUP BY n
```

<div id="create-view-with-invalid-query">
  ### `CREATE VIEW` con una consulta no válida
</div>

El analizador siempre realiza la comprobación de tipos.
Anteriormente, era posible crear una `VIEW` con una consulta `SELECT` no válida.
En ese caso, fallaba al ejecutar el primer `SELECT` o `INSERT` (en el caso de `MATERIALIZED VIEW`).

Ya no es posible crear una `VIEW` de esta forma.

<div id="example-view">
  #### Ejemplo
</div>

```sql
CREATE TABLE source (data String)
ENGINE=MergeTree
ORDER BY tuple();

CREATE VIEW some_view
AS SELECT JSONExtract(data, 'test', 'DateTime64(3)')
FROM source;
```

<div id="known-incompatibilities-of-the-join-clause">
  ### Incompatibilidades conocidas de la cláusula `JOIN`
</div>

<div id="join-using-column-from-projection">
  #### `JOIN` usando una columna de una proyección
</div>

De forma predeterminada, no se puede usar un alias de la lista `SELECT` como clave de `JOIN USING`.

Una nueva configuración, `analyzer_compatibility_join_using_top_level_identifier`, cuando está habilitada, cambia el comportamiento de `JOIN USING` para que dé prioridad a la resolución de identificadores basada en expresiones de la lista de proyección de la consulta `SELECT`, en lugar de usar directamente las columnas de la tabla de la izquierda.

Por ejemplo:

```sql
SELECT a + 1 AS b, t2.s
FROM VALUES('a UInt64, b UInt64', (1, 1)) AS t1
JOIN VALUES('b UInt64, s String', (1, 'one'), (2, 'two')) t2
USING (b);
```

Con `analyzer_compatibility_join_using_top_level_identifier` establecido en `true`, la condición de join se interpreta como `t1.a + 1 = t2.b`, de acuerdo con el comportamiento de las versiones anteriores.
El resultado será `2, 'two'`.
Cuando la configuración es `false`, la condición de join pasa a ser, por defecto, `t1.b = t2.b`, y la consulta devolverá `2, 'one'`.
Si `b` no está presente en `t1`, la consulta fallará con un error.

<div id="changes-in-behavior-with-join-using-and-aliasmaterialized-columns">
  #### Cambios de comportamiento con `JOIN USING` y columnas `ALIAS`/`MATERIALIZED`
</div>

En el analizador, usar `*` en una consulta `JOIN USING` que involucre columnas `ALIAS` o `MATERIALIZED` incluirá esas columnas en el resultado de forma predeterminada.

Por ejemplo:

```sql
CREATE TABLE t1 (id UInt64, payload ALIAS sipHash64(id)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t1 VALUES (1), (2);

CREATE TABLE t2 (id UInt64, payload ALIAS sipHash64(id)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t2 VALUES (2), (3);

SELECT * FROM t1
FULL JOIN t2 USING (payload);
```

En el analizador, el resultado de esta consulta incluirá la columna `payload` junto con `id` de ambas tablas.
En cambio, el analizador anterior solo incluía estas columnas `ALIAS` si se habilitaban opciones específicas (`asterisk_include_alias_columns` o `asterisk_include_materialized_columns`),
y las columnas podían aparecer en un orden distinto.

Para garantizar resultados coherentes y previsibles, especialmente al migrar consultas antiguas al analizador, es aconsejable especificar las columnas explícitamente en la cláusula `SELECT` en lugar de usar `*`.

<div id="handling-of-type-modifiers-for-columns-in-using-clause">
  #### Manejo de los modificadores de tipo de las columnas en la cláusula `USING`
</div>

En la nueva versión del analizador, se han estandarizado las reglas para determinar el supertipo común de las columnas especificadas en la cláusula `USING`, con el fin de producir resultados más predecibles,
especialmente al trabajar con modificadores de tipo como `LowCardinality` y `Nullable`.

* `LowCardinality(T)` y `T`: cuando una columna de tipo `LowCardinality(T)` se combina con una columna de tipo `T`, el supertipo común resultante será `T`, descartando de forma efectiva el modificador `LowCardinality`.
* `Nullable(T)` y `T`: cuando una columna de tipo `Nullable(T)` se combina con una columna de tipo `T`, el supertipo común resultante será `Nullable(T)`, lo que garantiza que se conserve la capacidad de admitir NULL.

Por ejemplo:

```sql
SELECT id, toTypeName(id)
FROM VALUES('id LowCardinality(String)', ('a')) AS t1
FULL OUTER JOIN VALUES('id String', ('b')) AS t2
USING (id);
```

En esta consulta, el supertipo común de `id` se establece como `String`, descartando el modificador `LowCardinality` de `t1`.

<div id="projection-column-names-changes">
  ### Cambios en los nombres de las columnas de la proyección
</div>

Durante el cálculo de los nombres de la proyección, no se sustituyen los alias.

```sql
SELECT
    1 + 1 AS x,
    x + 1
SETTINGS enable_analyzer = 0
FORMAT PrettyCompact

   ┌─x─┬─plus(plus(1, 1), 1)─┐
1. │ 2 │                   3 │
   └───┴─────────────────────┘

SELECT
    1 + 1 AS x,
    x + 1
SETTINGS enable_analyzer = 1
FORMAT PrettyCompact

   ┌─x─┬─plus(x, 1)─┐
1. │ 2 │          3 │
   └───┴────────────┘
```

<div id="incompatible-function-arguments-types">
  ### Tipos incompatibles en los argumentos de funciones
</div>

En el analizador, la inferencia de tipos se realiza durante el análisis de la consulta inicial.
Este cambio implica que las comprobaciones de tipos se realizan antes de la evaluación de cortocircuito; por lo tanto, los argumentos de la función `if` siempre deben tener un supertipo común.

Por ejemplo, la siguiente consulta falla con `There is no supertype for types Array(UInt8), String because some of them are Array and some of them are not`:

```sql
SELECT toTypeName(if(0, [2, 3, 4], 'String'))
```

<div id="heterogeneous-clusters">
  ### Clústeres heterogéneos
</div>

El analizador cambia de forma significativa el protocolo de comunicación entre los servidores del clúster. Por lo tanto, es imposible ejecutar consultas distribuidas en servidores con valores distintos de la configuración `enable_analyzer`.

<div id="mutations-are-interpreted-by-previous-analyzer">
  ### Las mutaciones se interpretan con el analizador anterior
</div>

Las mutaciones siguen usando el analizador anterior.
Esto significa que algunas funcionalidades nuevas de ClickHouse SQL no pueden usarse en las mutaciones. Por ejemplo, la cláusula `QUALIFY`.
El estado puede consultarse [aquí](https://github.com/ClickHouse/ClickHouse/issues/61563).

<div id="unsupported-features">
  ### Funcionalidades no compatibles
</div>

A continuación se muestra la lista de funcionalidades que el analizador no admite actualmente:

* Índice Annoy.
* Índice Hypothesis. Trabajo en curso [aquí](https://github.com/ClickHouse/ClickHouse/pull/48381).
* Window view no es compatible. No está previsto ofrecer compatibilidad en el futuro.

<div id="cloud-migration">
  ## Migración a Cloud
</div>

Estamos habilitando el nuevo analizador de consultas en todas las instancias donde actualmente está deshabilitado para dar soporte a nuevas optimizaciones funcionales y de rendimiento. Este cambio impone reglas de ámbito de SQL más estrictas, por lo que los clientes deberán actualizar manualmente las consultas que no las cumplan.

<div id="migration-workflow">
  ### Flujo de trabajo de migración
</div>

1. Identifique la consulta filtrando `system.query_log` por `normalized_query_hash`:

```sql
SELECT query 
FROM clusterAllReplicas(default, system.query_log)
WHERE normalized_query_hash='{hash}' 
LIMIT 1 
SETTINGS skip_unavailable_shards=1
```

2. Ejecuta la consulta con el analizador activado añadiendo esta configuración.

```sql
SETTINGS
    enable_analyzer=1,
    analyzer_compatibility_join_using_top_level_identifier=1
```

3. Refactoriza y verifica los resultados de la consulta para confirmar que coinciden con la salida generada cuando el analizador está deshabilitado.

Consulta las incompatibilidades más frecuentes detectadas durante las pruebas internas.

<div id="unknown-expression-identifier">
  ### Identificador de expresión desconocido
</div>

Error: `Unknown expression identifier ... in scope ... (UNKNOWN_IDENTIFIER)`. Código de excepción: 47

Causa: Las consultas que dependen de comportamientos heredados permisivos y no estándar, como hacer referencia a alias calculados en filtros, proyecciones ambiguas de subconsultas o un alcance &quot;dinámico&quot; de las CTE, ahora se identifican correctamente como no válidas y se rechazan de inmediato.

Solución: Actualice sus patrones SQL de la siguiente manera:

* Lógica de filtrado: Mueva la lógica de WHERE a HAVING si filtra por resultados, o duplique la expresión en WHERE si filtra por datos de origen.
* Alcance de la subconsulta: Seleccione explícitamente todas las columnas que necesita la consulta externa.
* Claves de JOIN: Use ON con expresiones completas en lugar de USING si la clave es un alias.
* En las consultas externas, haga referencia al alias de la propia subconsulta/CTE, no a las tablas que contiene.

<div id="non-aggregated-columns-in-group-by">
  ### Columnas no agregadas en GROUP BY
</div>

Error: `Column ... is not under aggregate function and not in GROUP BY keys (NOT_AN_AGGREGATE)`. código de excepción: 215

Causa: El analizador anterior permitía seleccionar columnas que no estaban presentes en la cláusula GROUP BY (a menudo tomando un valor arbitrario). El analizador se ajusta al SQL estándar: cada columna seleccionada debe ser una agregación o una clave de agrupación.

Solución: Envuelva la columna en `any()`, `argMax()` o añádala al GROUP BY.

```sql
/* ORIGINAL QUERY */
-- device_id is ambiguous
SELECT user_id, device_id FROM table GROUP BY user_id

/* FIXED QUERY */
SELECT user_id, any(device_id) FROM table GROUP BY user_id
-- OR
SELECT user_id, device_id FROM table GROUP BY user_id, device_id
```

<div id="duplicate-cte-names">
  ### Nombres de CTE duplicados
</div>

Error: `CTE with name ... already exists (MULTIPLE_EXPRESSIONS_FOR_ALIAS)`. Código de excepción: 179

Causa: El analizador anterior permitía definir varias expresiones de tabla comunes (WITH ...) con el mismo nombre, de modo que una ocultaba a la anterior. El analizador no permite esta ambigüedad.

Solución: Cambie el nombre de los CTE duplicados para que cada uno sea único.

```sql
/* ORIGINAL QUERY */
WITH 
  data AS (SELECT 1 AS id), 
  data AS (SELECT 2 AS id) -- Redefined
SELECT * FROM data;

/* FIXED QUERY */
WITH 
  raw_data AS (SELECT 1 AS id), 
  processed_data AS (SELECT 2 AS id)
SELECT * FROM processed_data;
```

<div id="ambiguous-column-identifiers">
  ### Identificadores de columna ambiguos
</div>

Error: `JOIN [JOIN TYPE] ambiguous identifier ... (AMBIGUOUS_IDENTIFIER)` Código de excepción: 207

Causa: La consulta hace referencia a un nombre de columna presente en varias tablas dentro de un JOIN sin especificar la tabla de origen. El analizador anterior a menudo infería la columna según la lógica interna; el analizador requiere un nombre explícito.

Solución: Especifique la columna por completo con table&#95;alias.column&#95;name.

```sql
/* ORIGINAL QUERY */
SELECT table1.ID AS ID FROM table1, table2 WHERE ID...

/* FIXED QUERY */
SELECT table1.ID AS ID_RENAMED FROM table1, table2 WHERE ID_RENAMED...
```

<div id="invalid-usage-of-final">
  ### Uso no válido de FINAL
</div>

Error: `Table expression modifiers FINAL are not supported for subquery...` o `Storage ... doesn't support FINAL` (`UNSUPPORTED_METHOD`). Códigos de excepción: 1, 181

Causa: FINAL es un modificador del almacenamiento de tablas (específicamente [Shared]ReplacingMergeTree). El analizador rechaza FINAL cuando se aplica a:

* Subconsultas o tablas derivadas (p. ej., FROM (SELECT ...) FINAL).
* Motores de tabla que no lo admiten (p. ej., SharedMergeTree).

Solución: Aplique FINAL solo a la tabla de origen dentro de la subconsulta, o elimínelo si el motor no lo admite.

```sql
/* ORIGINAL QUERY */
SELECT * FROM (SELECT * FROM my_table) AS subquery FINAL ...

/* FIXED QUERY */
SELECT * FROM (SELECT * FROM my_table FINAL) AS subquery ...
```

<div id="countdistinct-case-insensitivity">
  ### Insensibilidad a mayúsculas y minúsculas de la función `countDistinct()`
</div>

Error: `Function with name countdistinct does not exist (UNKNOWN_FUNCTION)`. Código de excepción: 46

Causa: Los nombres de las funciones distinguen entre mayúsculas y minúsculas o se asignan de forma estricta en el analizador. `countdistinct` (todo en minúsculas) ya no se reconoce automáticamente.

Solución: Use la forma estándar `countDistinct` (camelCase) o `uniq`, específico de ClickHouse.