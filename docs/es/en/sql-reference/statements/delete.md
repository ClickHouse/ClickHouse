---
description: 'La eliminación ligera simplifica el proceso de eliminar datos de la base de datos.'
keywords: ['delete']
sidebar_label: 'DELETE'
sidebar_position: 36
slug: /sql-reference/statements/delete
title: 'La sentencia DELETE de eliminación ligera'
doc_type: 'reference'
---

La sentencia `DELETE` de eliminación ligera elimina filas de la tabla `[db.]table` que coinciden con la expresión `expr`. Solo está disponible para la familia de motores de tabla *MergeTree.

```sql
DELETE FROM [db.]table [ON CLUSTER cluster] [IN PARTITION partition_expr] WHERE expr;
```

Se denomina &quot;eliminación ligera `DELETE`&quot; para diferenciarla del comando [ALTER TABLE ... DELETE](/sql-reference/statements/alter/delete), que es un proceso más costoso.

<div id="examples">
  ## Ejemplos
</div>

```sql
-- Deletes all rows from the `hits` table where the `Title` column contains the text `hello`
DELETE FROM hits WHERE Title LIKE '%hello%';
```

## La eliminación ligera `DELETE` no elimina los datos de inmediato

La eliminación ligera `DELETE` se implementa como una [mutación](/sql-reference/statements/alter#mutations) que marca las filas como eliminadas, pero no las elimina físicamente de inmediato.

De forma predeterminada, las sentencias `DELETE` esperan a que termine el marcado de las filas como eliminadas antes de devolver el control. Esto puede tardar mucho tiempo si el volumen de datos es grande. Como alternativa, puede ejecutarla de forma asíncrona en segundo plano mediante la configuración [`lightweight_deletes_sync`](/operations/settings/settings#lightweight_deletes_sync). Si está deshabilitada, la sentencia `DELETE` devolverá el control inmediatamente, pero los datos pueden seguir siendo visibles para las consultas hasta que finalice la mutación en segundo plano.

La mutación no elimina físicamente las filas marcadas como eliminadas; esto solo ocurrirá durante el siguiente merge. Como resultado, es posible que, durante un período no especificado, los datos no se eliminen realmente del almacenamiento y solo queden marcados como eliminados.

Si necesita garantizar que sus datos se eliminen del almacenamiento en un tiempo predecible, considere usar la configuración de la tabla [`min_age_to_force_merge_seconds`](/operations/settings/merge-tree-settings#min_age_to_force_merge_seconds). O bien, puede usar el comando [ALTER TABLE ... DELETE](/sql-reference/statements/alter/delete). Tenga en cuenta que eliminar datos mediante `ALTER TABLE ... DELETE` puede consumir una cantidad significativa de recursos, ya que recrea todas las partes afectadas.

<div id="deleting-large-amounts-of-data">
  ## Eliminar grandes cantidades de datos
</div>

Las eliminaciones de gran volumen pueden afectar negativamente al rendimiento de ClickHouse. Si intenta eliminar todas las filas de una tabla, considere usar el comando [`TRUNCATE TABLE`](/sql-reference/statements/truncate).

Si prevé eliminaciones frecuentes, considere usar una [clave de partición personalizada](/engines/table-engines/mergetree-family/custom-partitioning-key). A continuación, puede usar el comando [`ALTER TABLE ... DROP PARTITION`](/sql-reference/statements/alter/partition#drop-partitionpart) para eliminar rápidamente todas las filas asociadas a esa partición.

## Limitaciones de la eliminación ligera `DELETE`

### Eliminaciones ligeras con `DELETE` y proyecciones

De forma predeterminada, `DELETE` no funciona en tablas con proyecciones. Esto se debe a que una operación `DELETE` puede afectar a las filas de una proyección. Sin embargo, existe una [configuración de MergeTree](/operations/settings/merge-tree-settings), `lightweight_mutation_projection_mode`, para cambiar este comportamiento.

## Consideraciones de rendimiento al usar la eliminación ligera {#performance-considerations-when-using-lightweight-delete}

**Eliminar grandes volúmenes de datos con la eliminación ligera puede afectar negativamente al rendimiento de las consultas SELECT.**

Lo siguiente también puede afectar negativamente al rendimiento de la eliminación ligera:

* Una condición `WHERE` compleja en una consulta `DELETE`.
* Si la cola de mutaciones está llena de muchas otras mutaciones, esto puede provocar problemas de rendimiento, ya que todas las mutaciones de una tabla se ejecutan secuencialmente.
* La tabla afectada tiene un número muy elevado de partes de datos.
* Tener una gran cantidad de datos en partes compactas. En una parte compacta, todas las columnas se almacenan en un solo archivo.

<div id="delete-permissions">
  ## Permisos de DELETE
</div>

`DELETE` requiere el privilegio `ALTER DELETE`. Para permitir las sentencias `DELETE` en una tabla específica para un usuario determinado, ejecute el siguiente comando:

```sql
GRANT ALTER DELETE ON db.table to username;
```

## Cómo funcionan internamente las eliminaciones ligeras `DELETE` en ClickHouse

1. **Se aplica una &quot;máscara&quot; a las filas afectadas**

   Cuando se ejecuta una consulta `DELETE FROM table ...`, ClickHouse guarda una máscara en la que cada fila queda marcada como &quot;existente&quot; o &quot;eliminada&quot;. Esas filas &quot;eliminadas&quot; se omiten en las consultas posteriores. Sin embargo, las filas solo se eliminan realmente más adelante, durante merges posteriores. Escribir esta máscara es mucho más ligero que lo que hace una consulta `ALTER TABLE ... DELETE`.

   La máscara se implementa como una columna oculta del sistema llamada `_row_exists`, que almacena `True` para todas las filas visibles y `False` para las eliminadas. Esta columna solo está presente en una parte si se eliminaron algunas filas de esa parte. Esta columna no existe cuando una parte tiene todos los valores iguales a `True`.

2. **Las consultas `SELECT` se transforman para incluir la máscara**

   Cuando se usa una columna enmascarada en una consulta, la consulta `SELECT ... FROM table WHERE condition` se amplía internamente con el predicado sobre `_row_exists` y se transforma en:

   ```sql
   SELECT ... FROM table PREWHERE _row_exists WHERE condition
   ```

   En tiempo de ejecución, se lee la columna `_row_exists` para determinar qué filas no deben devolverse. Si hay muchas filas eliminadas, ClickHouse puede determinar qué gránulos pueden omitirse por completo al leer el resto de las columnas.

3. **Las consultas `DELETE` se transforman en consultas `ALTER TABLE ... UPDATE`**

   `DELETE FROM table WHERE condition` se traduce en una mutación `ALTER TABLE table UPDATE _row_exists = 0 WHERE condition`.

   Internamente, esta mutación se ejecuta en dos pasos:

   1. Se ejecuta un comando `SELECT count() FROM table WHERE condition` para cada parte individual a fin de determinar si la parte está afectada.

   2. En función de los comandos anteriores, las partes afectadas se mutan y se crean enlaces duros para las partes no afectadas. En el caso de las partes wide, la columna `_row_exists` de cada fila se actualiza y los archivos del resto de las columnas se enlazan mediante enlaces duros. En el caso de las partes compact, todas las columnas se reescriben porque se almacenan juntas en un único archivo.

   Como se ve en los pasos anteriores, la eliminación ligera `DELETE`, al usar la técnica de enmascaramiento, mejora el rendimiento frente a `ALTER TABLE ... DELETE` tradicional porque no reescribe todos los archivos de columnas de las partes afectadas.

<div id="related-content">
  ## Contenido relacionado
</div>

* Blog: [Cómo gestionar actualizaciones y eliminaciones en ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)