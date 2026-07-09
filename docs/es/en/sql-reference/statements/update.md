---
description: 'Las actualizaciones ligeras simplifican la actualización de datos en la base de datos mediante partes de parche.'
keywords: ['update']
sidebar_label: 'UPDATE'
sidebar_position: 39
slug: /sql-reference/statements/update
title: 'La sentencia UPDATE ligera'
doc_type: 'referencia'
---

import BetaBadge from '@theme/badges/BetaBadge';

<BetaBadge />

:::note
Las actualizaciones ligeras se encuentran actualmente en beta.
Si tiene algún problema, abra un issue en el [repositorio de ClickHouse](https://github.com/clickhouse/clickhouse/issues).
:::

La actualización ligera `UPDATE` actualiza las filas de una tabla `[db.]table` que coinciden con la expresión `filter_expr`.
Se llama &quot;actualización ligera&quot; para diferenciarla de la consulta [`ALTER TABLE ... UPDATE`](/es/sql-reference/statements/alter/update), que es un proceso pesado que reescribe columnas completas en las partes de datos.
Solo está disponible para la familia de motores de tabla [`MergeTree`](/es/engines/table-engines/mergetree-family/mergetree).

```sql
UPDATE [db.]table [ON CLUSTER cluster] SET column1 = expr1 [, ...] [IN PARTITION partition_expr] WHERE filter_expr;
```

`filter_expr` debe ser de tipo `UInt8`. Esta consulta actualiza los valores de las columnas especificadas con los valores de las expresiones correspondientes en las filas en las que `filter_expr` toma un valor distinto de cero.
Los valores se convierten al tipo de columna mediante el operador `CAST`. No se admite la actualización de columnas usadas en el cálculo de las claves primaria o de partición.

<div id="examples">
  ## Ejemplos
</div>

```sql
UPDATE hits SET Title = 'Updated Title' WHERE EventDate = today();

UPDATE wikistat SET hits = hits + 1, time = now() WHERE path = 'ClickHouse';
```

<div id="lightweight-update-does-not-update-data-immediately">
  ## Las actualizaciones ligeras no actualizan los datos de inmediato
</div>

La actualización ligera se implementa mediante **partes de parche**: un tipo especial de parte de datos que contiene únicamente las columnas y filas actualizadas.
Una actualización ligera crea partes de parche, pero no modifica físicamente de inmediato los datos originales almacenados.
El proceso de actualización es similar al de una consulta `INSERT ... SELECT ...`, pero la consulta `UPDATE` espera a que se complete la creación de la parte de parche antes de finalizar.

Los valores actualizados quedan:

* **Inmediatamente visibles** en las consultas `SELECT` mediante la aplicación de parches
* **Materializados físicamente** solo durante fusiones y mutaciones posteriores
* **Eliminados automáticamente** una vez que todas las partes activas tienen los parches materializados

<div id="lightweight-update-requirements">
  ## Requisitos de las actualizaciones ligeras
</div>

Las actualizaciones ligeras son compatibles con los motores [`MergeTree`](/es/engines/table-engines/mergetree-family/mergetree), [`ReplacingMergeTree`](/es/engines/table-engines/mergetree-family/replacingmergetree), [`CollapsingMergeTree`](/es/engines/table-engines/mergetree-family/collapsingmergetree), [`VersionedCollapsingMergeTree`](https://clickhouse.com/docs/engines/table-engines/mergetree-family/versionedcollapsingmergetree), así como con sus versiones [`Replicated`](/es/engines/table-engines/mergetree-family/replication.md) y [`Shared`](/es/cloud/reference/shared-merge-tree).

Para usar actualizaciones ligeras, debe estar habilitada la materialización de las columnas `_block_number` y `_block_offset` mediante la configuración de la tabla [`enable_block_number_column`](/es/operations/settings/merge-tree-settings#enable_block_number_column) y [`enable_block_offset_column`](/es/operations/settings/merge-tree-settings#enable_block_offset_column).

<div id="lightweight-delete">
  ## Eliminaciones ligeras
</div>

Una consulta de [eliminación ligera `DELETE`](/es/sql-reference/statements/delete) puede ejecutarse como una actualización ligera `UPDATE` en lugar de una mutación `ALTER UPDATE`. La implementación de la eliminación ligera `DELETE` se controla con la configuración [`lightweight_delete_mode`](/es/operations/settings/settings#lightweight_delete_mode).

<div id="performance-considerations">
  ## Consideraciones de rendimiento
</div>

**Ventajas de las actualizaciones ligeras:**

* La latencia de la actualización es comparable a la de la consulta `INSERT ... SELECT ...`
* Solo se escriben las columnas y los valores actualizados, no columnas completas en las partes de datos
* No es necesario esperar a que terminen las fusiones/mutaciones que estén en ejecución, por lo que la latencia de una actualización es predecible
* Es posible ejecutar actualizaciones ligeras en paralelo

**Posibles impactos en el rendimiento:**

* Añaden sobrecarga a las consultas `SELECT` que necesitan aplicar parches
* Los [índices de omisión](/es/engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-data_skipping-indexes) no se usarán para las columnas de las partes de datos que tengan parches por aplicar. Las [proyecciones](/es/engines/table-engines/mergetree-family/mergetree.md/#projections) no se usarán si la tabla tiene partes de parche, incluso en las partes de datos que no tengan parches por aplicar.
* Las actualizaciones pequeñas demasiado frecuentes pueden provocar un error de &quot;too many parts&quot;. Se recomienda agrupar varias actualizaciones en una sola consulta, por ejemplo, incluyendo los ID de las actualizaciones en una única cláusula `IN` dentro de la cláusula `WHERE`
* Las actualizaciones ligeras están diseñadas para actualizar pequeñas cantidades de filas (hasta aproximadamente el 10% de la tabla). Si necesita actualizar una cantidad mayor, se recomienda usar la mutación [`ALTER TABLE ... UPDATE`](/es/sql-reference/statements/alter/update)

<div id="concurrent-operations">
  ## Operaciones concurrentes
</div>

Las actualizaciones ligeras, a diferencia de las mutaciones pesadas, no esperan a que finalicen las fusiones/mutaciones que se estén ejecutando en ese momento.
La consistencia de las actualizaciones ligeras concurrentes se controla mediante las opciones de configuración [`update_sequential_consistency`](/es/operations/settings/settings#update_sequential_consistency) y [`update_parallel_mode`](/es/operations/settings/settings#update_parallel_mode).

<div id="update-permissions">
  ## Permisos de `UPDATE`
</div>

`UPDATE` requiere el privilegio `ALTER UPDATE`. Para habilitar las sentencias `UPDATE` en una tabla específica para un usuario determinado, ejecute:

```sql
GRANT ALTER UPDATE ON db.table TO username;
```

<div id="details-of-the-implementation">
  ## Detalles de la implementación
</div>

Las partes de parche son iguales a las partes normales, pero contienen solo las columnas actualizadas y varias columnas del sistema:

* `_part` - el nombre de la parte original
* `_part_offset` - el número de fila en la parte original
* `_block_number` - el número de bloque de la fila en la parte original
* `_block_offset` - el desplazamiento del bloque de la fila en la parte original
* `_data_version` - la versión de los datos actualizados (número de bloque asignado para la consulta `UPDATE`)

En promedio, esto añade unos 40 bytes de sobrecarga por fila actualizada en las partes de parche (datos sin comprimir).
Las columnas del sistema ayudan a encontrar las filas de la parte original que deben actualizarse.
Las columnas del sistema están relacionadas con las [columnas virtuales](/es/engines/table-engines/mergetree-family/mergetree.md/#virtual-columns) de la parte original, que se añaden durante la lectura si deben aplicarse partes de parche.
Las partes de parche se ordenan por `_part` y `_part_offset`.

Las partes de parche pertenecen a particiones distintas de la parte original.
El ID de partición de la parte de parche es `patch-<hash of column names in patch part>-<original_partition_id>`.
Por lo tanto, las partes de parche con columnas diferentes se almacenan en particiones distintas.
Por ejemplo, tres actualizaciones `SET x = 1 WHERE <cond>`, `SET y = 1 WHERE <cond>` y `SET x = 1, y = 1 WHERE <cond>` crearán tres partes de parche en tres particiones diferentes.

Las partes de parche pueden fusionarse entre sí para reducir la cantidad de parches aplicados en las consultas `SELECT` y disminuir la sobrecarga. La fusión de partes de parche usa el algoritmo de fusión [de reemplazo](/es/engines/table-engines/mergetree-family/replacingmergetree) con `_data_version` como columna de versión.
Por lo tanto, las partes de parche siempre almacenan la versión más reciente de cada fila actualizada en la parte.

Las actualizaciones ligeras no esperan a que terminen las fusiones y mutaciones que ya están en ejecución, y siempre usan un snapshot actual de las partes de datos para ejecutar una actualización y generar una parte de parche.
Por eso, puede haber dos casos al aplicar partes de parche.

Por ejemplo, si leemos la parte `A`, necesitamos aplicar la parte de parche `X`:

* si `X` contiene la propia parte `A`. Esto ocurre si `A` no estaba participando en una fusión cuando se ejecutó `UPDATE`.
* si `X` contiene las partes `B` y `C`, que están cubiertas por la parte `A`. Esto ocurre si había una fusión (`B`, `C`) -&gt; `A` en ejecución cuando se ejecutó `UPDATE`.

Para estos dos casos, respectivamente, hay dos formas de aplicar partes de parche:

* Usar la fusión por las columnas ordenadas `_part`, `_part_offset`.
* Usar join por las columnas `_block_number`, `_block_offset`.

El modo join es más lento y requiere más memoria que el modo de fusión, pero se usa con menos frecuencia.

<div id="related-content">
  ## Contenido relacionado
</div>

* [`ALTER UPDATE`](/es/sql-reference/statements/alter/update) - Operaciones `UPDATE` costosas
* [Eliminación ligera `DELETE`](/es/sql-reference/statements/delete) - Operaciones de eliminación ligera `DELETE`
* [`APPLY PATCHES`](/es/sql-reference/statements/alter/apply-patches) - Forzar la materialización física de los parches en las partes de datos (operación de mutación)