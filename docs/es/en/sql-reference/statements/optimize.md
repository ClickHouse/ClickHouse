---
description: 'Documentación de Optimize'
sidebar_label: 'OPTIMIZE'
sidebar_position: 47
slug: /sql-reference/statements/optimize
title: 'Sentencia OPTIMIZE'
doc_type: 'reference'
---

Esta consulta intenta iniciar una fusión no programada de partes de datos en tablas. Ten en cuenta que, por lo general, recomendamos no usar `OPTIMIZE TABLE ... FINAL` (consulta esta [documentación](/es/optimize/avoidoptimizefinal)), ya que su caso de uso está pensado para tareas de administración, no para las operaciones diarias.

:::note
`OPTIMIZE` no puede corregir el error `Too many parts`.
:::

**Sintaxis**

```sql
OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL | FORCE] [DEDUPLICATE [BY expression]]
```

```sql
OPTIMIZE TABLE [db.]name DRY RUN PARTS 'part_name1', 'part_name2' [, ...] [DEDUPLICATE [BY expression]] [CLEANUP]
```

La consulta `OPTIMIZE` es compatible con la familia [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) (incluidas las [vistas materializadas](/es/sql-reference/statements/create/view#materialized-view)) y con los motores [Buffer](../../engines/table-engines/special/buffer.md). No es compatible con otros motores de tabla.

Cuando `OPTIMIZE` se usa con la familia [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication.md) de motores de tabla, ClickHouse crea una tarea de fusión y espera a que se ejecute en todas las réplicas (si la configuración [alter&#95;sync](/es/operations/settings/settings#alter_sync) está establecida en `2`) o en la réplica actual (si la configuración [alter&#95;sync](/es/operations/settings/settings#alter_sync) está establecida en `1`).

* Si `OPTIMIZE` no realiza una fusión por cualquier motivo, no se lo notifica al cliente. Para habilitar las notificaciones, use la configuración [optimize&#95;throw&#95;if&#95;noop](/es/operations/settings/settings#optimize_throw_if_noop).
* Si especifica una `PARTITION`, solo se optimiza la partición indicada. [Cómo establecer la expresión de partición](alter/partition.md#how-to-set-partition-expression).
* Si especifica `FINAL` o `FORCE`, la optimización se realiza incluso cuando todos los datos ya están en una sola parte. Puede controlar este comportamiento con [optimize&#95;skip&#95;merged&#95;partitions](/es/operations/settings/settings#optimize_skip_merged_partitions). Además, la fusión se fuerza incluso si se están realizando fusiones concurrentes.
* Si especifica `DEDUPLICATE`, se eliminarán las filas completamente idénticas (salvo que se especifique una cláusula BY) comparando todas las columnas; esto solo tiene sentido para el motor MergeTree.

Puede especificar cuánto tiempo (en segundos) se debe esperar a que las réplicas inactivas ejecuten consultas `OPTIMIZE` mediante la configuración [replication&#95;wait&#95;for&#95;inactive&#95;replica&#95;timeout](/es/operations/settings/settings#replication_wait_for_inactive_replica_timeout).

:::note
Si `alter_sync` está establecido en `2` y algunas réplicas permanecen inactivas durante más tiempo del especificado por la configuración `replication_wait_for_inactive_replica_timeout`, se genera una excepción `UNFINISHED`.
:::

<div id="dry-run">
  ## DRY RUN
</div>

La cláusula `DRY RUN` simula una fusión de las partes especificadas sin hacer commit del resultado. La parte fusionada se escribe en una ubicación temporal, se valida y luego se descarta. Las partes originales y los datos de la tabla permanecen sin cambios.

Esto es útil para:

* Probar la corrección de la fusión entre versiones de ClickHouse.
* Reproducir de forma determinista bugs relacionados con la fusión.
* Realizar benchmarks del rendimiento de la fusión.

`DRY RUN` solo es compatible con tablas de la familia [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md). Se requiere la palabra clave `PARTS` con una lista de nombres de partes. Todas las partes especificadas deben existir, estar activas y pertenecer a la misma partición.

`DRY RUN` es incompatible con `FINAL` y `PARTITION`. Puede combinarse con `DEDUPLICATE` (con especificación opcional de columnas) y `CLEANUP` (para tablas `ReplacingMergeTree`).

**Sintaxis**

```sql
OPTIMIZE TABLE [db.]name DRY RUN PARTS 'part_name1', 'part_name2' [, ...] [DEDUPLICATE [BY expression]] [CLEANUP]
```

De forma predeterminada, la parte fusionada resultante se valida de forma similar a la consulta [`CHECK TABLE`](/es/sql-reference/statements/check-table). Este comportamiento está controlado por la configuración [optimize&#95;dry&#95;run&#95;check&#95;part](/es/operations/settings/settings#optimize_dry_run_check_part) (habilitada de forma predeterminada). Si se deshabilita, se omite la validación, lo que puede ser útil para evaluar el rendimiento de la propia fusión.

**Ejemplo**

```sql
CREATE TABLE dry_run_example (key UInt64, value String) ENGINE = MergeTree ORDER BY key;

INSERT INTO dry_run_example VALUES (1, 'a'), (2, 'b');
INSERT INTO dry_run_example VALUES (1, 'c'), (4, 'd');

-- Simulate merging using two parts
OPTIMIZE TABLE dry_run_example DRY RUN PARTS 'all_1_1_0', 'all_2_2_0';

-- Simulate merging with deduplication
OPTIMIZE TABLE dry_run_example DRY RUN PARTS 'all_1_1_0', 'all_2_2_0' DEDUPLICATE;

-- Parts and data remain unchanged after DRY RUN
SELECT name, rows FROM system.parts
WHERE database = currentDatabase() AND table = 'dry_run_example' AND active
ORDER BY name;
```

```response
┌─name────────┬─rows─┐
│ all_1_1_0   │    2 │
│ all_2_2_0   │    2 │
└─────────────┴──────┘
```

<div id="by-expression">
  ## Expresión BY
</div>

Si desea realizar la deduplicación sobre un conjunto personalizado de columnas en lugar de sobre todas ellas, puede especificar explícitamente una lista de columnas o usar cualquier combinación de expresiones [`*`](../../sql-reference/statements/select/index.md#asterisk), [`COLUMNS`](/es/sql-reference/statements/select#select-clause) o [`EXCEPT`](/es/sql-reference/statements/select/except-modifier). La lista de columnas especificada explícitamente o expandida implícitamente debe incluir todas las columnas indicadas en la expresión de ordenación de filas (tanto la clave primaria como la clave de ordenación) y en la expresión de particionamiento (clave de partición).

:::note
Tenga en cuenta que `*` se comporta igual que en `SELECT`: las columnas [MATERIALIZED](/es/sql-reference/statements/create/view#materialized-view) y [ALIAS](../../sql-reference/statements/create/table.md#alias) no se usan en la expansión.

Además, es un error especificar una lista vacía de columnas, escribir una expresión que dé como resultado una lista vacía de columnas o deduplicar por una columna `ALIAS`.
:::

**Sintaxis**

```sql
OPTIMIZE TABLE table DEDUPLICATE; -- all columns
OPTIMIZE TABLE table DEDUPLICATE BY *; -- excludes MATERIALIZED and ALIAS columns
OPTIMIZE TABLE table DEDUPLICATE BY colX,colY,colZ;
OPTIMIZE TABLE table DEDUPLICATE BY * EXCEPT colX;
OPTIMIZE TABLE table DEDUPLICATE BY * EXCEPT (colX, colY);
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex');
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex') EXCEPT colX;
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex') EXCEPT (colX, colY);
```

**Ejemplos**

Considere la siguiente tabla:

```sql title="Query"
CREATE TABLE example (
    primary_key Int32,
    secondary_key Int32,
    value UInt32,
    partition_key UInt32,
    materialized_value UInt32 MATERIALIZED 12345,
    aliased_value UInt32 ALIAS 2,
    PRIMARY KEY primary_key
) ENGINE=MergeTree
PARTITION BY partition_key
ORDER BY (primary_key, secondary_key);
```

```sql title="Query"
INSERT INTO example (primary_key, secondary_key, value, partition_key)
VALUES (0, 0, 0, 0), (0, 0, 0, 0), (1, 1, 2, 2), (1, 1, 2, 3), (1, 1, 3, 3);
```

```sql title="Query"
SELECT * FROM example;
```

```sql title="Response"

┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
│           1 │             1 │     3 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

Todos los ejemplos a continuación se ejecutan sobre este estado con 5 filas.

<div id="deduplicate">
  #### `DEDUPLICATE`
</div>

Cuando no se especifican las columnas para la deduplicación, se tienen en cuenta todas. La fila solo se elimina si todos los valores de todas las columnas son iguales a los valores correspondientes de la fila anterior:

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
│           1 │             1 │     3 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

<div id="deduplicate-by-">
  #### `DEDUPLICATE BY *`
</div>

Cuando las columnas se especifican de forma implícita, la tabla se deduplica usando todas las columnas que no son `ALIAS` ni `MATERIALIZED`. Teniendo en cuenta la tabla anterior, estas son las columnas `primary_key`, `secondary_key`, `value` y `partition_key`:

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY *;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
│           1 │             1 │     3 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

<div id="deduplicate-by--except">
  #### `DEDUPLICATE BY * EXCEPT`
</div>

Elimina duplicados usando todas las columnas que no sean `ALIAS` ni `MATERIALIZED`, y excluyendo explícitamente `value`: las columnas `primary_key`, `secondary_key` y `partition_key`.

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY * EXCEPT value;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

<div id="deduplicate-by-list-of-columns">
  #### `DEDUPLICATE BY <list of columns>`
</div>

Elimine duplicados explícitamente según las columnas `primary_key`, `secondary_key` y `partition_key`:

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY primary_key, secondary_key, partition_key;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

<div id="deduplicate-by-columnsregex">
  #### `DEDUPLICATE BY COLUMNS(<regex>)`
</div>

Elimina duplicados en todas las columnas que coincidan con una expresión regular: las columnas `primary_key`, `secondary_key` y `partition_key`:

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY COLUMNS('.*_key');
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```