---
description: 'Documentación sobre columnas'
sidebar_label: 'COLUMN'
sidebar_position: 37
slug: /sql-reference/statements/alter/column
title: 'Modificación de columnas'
doc_type: 'reference'
---

Un conjunto de consultas que permite modificar la estructura de la tabla.

Sintaxis:

```sql
ALTER [TEMPORARY] TABLE [db].name [ON CLUSTER cluster] ADD|DROP|RENAME|CLEAR|COMMENT|{MODIFY|ALTER}|MATERIALIZE COLUMN ...
```

En la consulta, especifique una lista de una o más acciones separadas por comas.
Cada acción es una operación sobre una columna.

Se admiten las siguientes acciones:

* [ADD COLUMN](#add-column) — Agrega una nueva columna a la tabla.
* [DROP COLUMN](#drop-column) — Elimina la columna.
* [RENAME COLUMN](#rename-column) — Cambia el nombre de una columna existente.
* [CLEAR COLUMN](#clear-column) — Restablece los valores de la columna.
* [COMMENT COLUMN](#comment-column) — Agrega un comentario de texto a la columna.
* [MODIFY COLUMN](#modify-column) — Cambia el tipo de la columna, la expresión predeterminada, el TTL y la configuración de columna.
* [MODIFY COLUMN REMOVE](#modify-column-remove) — Elimina una de las propiedades de la columna.
* [MODIFY COLUMN MODIFY SETTING](#modify-column-modify-setting) - Cambia la configuración de columna.
* [MODIFY COLUMN RESET SETTING](#modify-column-reset-setting) - Restablece la configuración de columna.
* [MODIFY COLUMN ADD ENUM VALUES](#modify-column-add-enum-values) - Agrega nuevos valores a Enum.
* [MATERIALIZE COLUMN](#materialize-column) — Materializa la columna en las partes en las que falta la columna.
  Estas acciones se describen en detalle a continuación.

<div id="add-column">
  ## ADD COLUMN
</div>

```sql
ADD COLUMN [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after | FIRST]
```

Agrega una nueva columna a la tabla con el `name`, `type`, [`codec`](../create/table.md/#column_compression_codec) y `default_expr` especificados (consulte la sección [Expresiones predeterminadas](/es/sql-reference/statements/create/table#default_values)).

Si se incluye la cláusula `IF NOT EXISTS`, la consulta no devolverá un error si la columna ya existe. Si especifica `AFTER name_after` (el nombre de otra columna), la columna se agrega después de la columna especificada en la lista de columnas de la tabla. Si desea agregar una columna al principio de la tabla, use la cláusula `FIRST`. De lo contrario, la columna se agrega al final de la tabla. En una cadena de acciones, `name_after` puede ser el nombre de una columna agregada en una de las acciones anteriores.

Agregar una columna solo cambia la estructura de la tabla, sin realizar ninguna acción sobre los datos. Los datos no aparecen en el disco después de `ALTER`. Si faltan datos de una columna al leer la tabla, se completan con valores predeterminados (ejecutando la expresión predeterminada si existe, o usando ceros o cadenas vacías). La columna aparece en el disco después de la fusión de partes de datos (consulte [MergeTree](/es/engines/table-engines/mergetree-family/mergetree.md)).

Este enfoque permite completar la consulta `ALTER` al instante, sin aumentar el volumen de los datos antiguos.

Ejemplo:

```sql
ALTER TABLE alter_test ADD COLUMN Added1 UInt32 FIRST;
ALTER TABLE alter_test ADD COLUMN Added2 UInt32 AFTER NestedColumn;
ALTER TABLE alter_test ADD COLUMN Added3 UInt32 AFTER ToDrop;
DESC alter_test FORMAT TSV;
```

```text
Added1  UInt32
CounterID       UInt32
StartDate       Date
UserID  UInt32
VisitID UInt32
NestedColumn.A  Array(UInt8)
NestedColumn.S  Array(String)
Added2  UInt32
ToDrop  UInt32
Added3  UInt32
```

<div id="drop-column">
  ## DROP COLUMN
</div>

```sql
DROP COLUMN [IF EXISTS] name
```

Elimina la columna con el nombre `name`. Si se especifica la cláusula `IF EXISTS`, la consulta no devolverá ningún error si la columna no existe.

Elimina datos del sistema de archivos. Como elimina archivos completos, la consulta se completa casi al instante.

:::tip
No se puede eliminar una columna si una [vista materializada](/es/sql-reference/statements/create/view) hace referencia a ella. De lo contrario, devuelve un error.
:::

Ejemplo:

```sql
ALTER TABLE visits DROP COLUMN browser
```

<div id="rename-column">
  ## RENAME COLUMN
</div>

```sql
RENAME COLUMN [IF EXISTS] name to new_name
```

Cambia el nombre de la columna `name` a `new_name`. Si se especifica la cláusula `IF EXISTS`, la consulta no devolverá un error si la columna no existe. Como el cambio de nombre no implica modificar los datos subyacentes, la consulta se completa casi al instante.

**NOTA**: Las columnas especificadas en la expresión de clave de la tabla (ya sea con `ORDER BY` o `PRIMARY KEY`) no se pueden renombrar. Si intenta cambiar estas columnas, se producirá `SQL Error [524]`.

Ejemplo:

```sql
ALTER TABLE visits RENAME COLUMN webBrowser TO browser
```

<div id="clear-column">
  ## CLEAR COLUMN
</div>

```sql
CLEAR COLUMN [IF EXISTS] name IN PARTITION partition_name
```

Restablece todos los datos de una columna en una partición especificada. Obtenga más información sobre cómo establecer el nombre de la partición en la sección [Cómo establecer la expresión de partición](../alter/partition.md/#how-to-set-partition-expression).

Si se especifica la cláusula `IF EXISTS`, la consulta no devolverá un error si la columna no existe.

Ejemplo:

```sql
ALTER TABLE visits CLEAR COLUMN browser IN PARTITION tuple()
```

<div id="comment-column">
  ## COMMENT COLUMN
</div>

```sql
COMMENT COLUMN [IF EXISTS] name 'Text comment'
```

Añade un comentario a la columna. Si se especifica la cláusula `IF EXISTS`, la consulta no devolverá ningún error si la columna no existe.

Cada columna puede tener un comentario. Si la columna ya tiene un comentario, el nuevo comentario sobrescribe el anterior.

Los comentarios se almacenan en la columna `comment_expression` que devuelve la consulta [DESCRIBE TABLE](/es/sql-reference/statements/describe-table.md).

Ejemplo:

```sql
ALTER TABLE visits COMMENT COLUMN browser 'This column shows the browser used for accessing the site.'
```

<div id="modify-column">
  ## MODIFY COLUMN
</div>

```sql
MODIFY COLUMN [IF EXISTS] name
    [type] [default_expr] [codec] [TTL] [settings] [AFTER name_after | FIRST]
    | ADD ENUM VALUES ( 'name' [= number] [, ...] )
ALTER COLUMN [IF EXISTS] name
    TYPE [type] [default_expr] [codec] [TTL] [settings] [AFTER name_after | FIRST]
    | ADD ENUM VALUES ( 'name' [= number] [, ...] )
```

Esta consulta cambia las propiedades de la columna `name`:

* Tipo

* Expresión predeterminada

* Códec de compresión

* TTL

* Configuración a nivel de columna

* Valores de Enum para los tipos Enum/Enum8/Enum16

Para ver ejemplos de modificación de códecs de compresión de columnas, consulte [Códecs de compresión de columnas](../create/table.md/#column_compression_codec).

Para ver ejemplos de modificación del TTL de columnas, consulte [TTL de columna](/es/engines/table-engines/mergetree-family/mergetree.md/#mergetree-column-ttl).

Para ver ejemplos de modificación de la configuración a nivel de columna, consulte [Configuración a nivel de columna](/es/engines/table-engines/mergetree-family/mergetree.md/#column-level-settings).

Si se especifica la cláusula `IF EXISTS`, la consulta no devolverá ningún error si la columna no existe.

Al cambiar el tipo, los valores se convierten como si se les hubieran aplicado las funciones [toType](/es/sql-reference/functions/type-conversion-functions.md). Si solo se cambia la expresión predeterminada, la consulta no realiza ninguna operación compleja y se completa casi al instante.

Ejemplo:

```sql
ALTER TABLE visits MODIFY COLUMN browser Array(String)
```

Cambiar el tipo de columna es la única operación compleja, ya que modifica el contenido de los archivos de datos. En tablas grandes, esto puede llevar mucho tiempo.

La consulta también puede cambiar el orden de las columnas mediante la cláusula `FIRST | AFTER`; consulta la descripción de [ADD COLUMN](#add-column), pero en este caso el tipo de columna es obligatorio.

Ejemplo:

```sql
CREATE TABLE users (
    c1 Int16,
    c2 String
) ENGINE = MergeTree
ORDER BY c1;

DESCRIBE users;
┌─name─┬─type───┬
│ c1   │ Int16  │
│ c2   │ String │
└──────┴────────┴

ALTER TABLE users MODIFY COLUMN c2 String FIRST;

DESCRIBE users;
┌─name─┬─type───┬
│ c2   │ String │
│ c1   │ Int16  │
└──────┴────────┴

ALTER TABLE users ALTER COLUMN c2 TYPE String AFTER c1;

DESCRIBE users;
┌─name─┬─type───┬
│ c1   │ Int16  │
│ c2   │ String │
└──────┴────────┴
```

La consulta `ALTER` es atómica. En las tablas MergeTree también se ejecuta sin bloqueos.

La consulta `ALTER` para cambiar columnas se replica. Las instrucciones se guardan en ZooKeeper y luego cada réplica las aplica. Todas las consultas `ALTER` se ejecutan en el mismo orden. La consulta espera a que se completen las acciones correspondientes en las demás réplicas. Sin embargo, una consulta para cambiar columnas en una tabla replicada puede interrumpirse, y todas las acciones se realizarán de forma asíncrona.

:::note
Tenga mucho cuidado al cambiar una columna de Nullable a Non-Nullable. Asegúrese de que no tenga valores NULL; de lo contrario, causará problemas al leerla. En ese caso, la solución alternativa sería cancelar la mutación y volver a cambiar la columna al tipo Nullable.
:::

<div id="modify-column-remove">
  ## MODIFY COLUMN REMOVE
</div>

Elimina una de las propiedades de la columna: `DEFAULT`, `ALIAS`, `MATERIALIZED`, `CODEC`, `COMMENT`, `TTL`, `SETTINGS`.

Sintaxis:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name REMOVE property;
```

**Ejemplo**

Eliminar TTL:

```sql
ALTER TABLE table_with_ttl MODIFY COLUMN column_ttl REMOVE TTL;
```

**Véase también**

* [REMOVE TTL](ttl.md).

<div id="modify-column-modify-setting">
  ## MODIFY COLUMN MODIFY SETTING
</div>

Modifica la configuración de una columna.

Sintaxis:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name MODIFY SETTING name=value,...;
```

**Ejemplo**

Modifique el valor de `max_compress_block_size` de la columna a `1MB`:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name MODIFY SETTING max_compress_block_size = 1048576;
```

<div id="modify-column-reset-setting">
  ## MODIFY COLUMN RESET SETTING
</div>

Restablece una configuración de columna y también elimina la declaración de esa configuración en la expresión de columna de la consulta CREATE de la tabla.

Sintaxis:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name RESET SETTING name,...;
```

**Ejemplo**

Restablecer la configuración de columna `max_compress_block_size` a su valor predeterminado:

```sql
ALTER TABLE table_name MODIFY COLUMN column_name RESET SETTING max_compress_block_size;
```

<div id="modify-column-add-enum-values">
  ## MODIFY COLUMN ADD ENUM VALUES
</div>

Agrega nuevos valores a una columna de tipo `Enum`, `Enum8`, `Enum16`, `Nullable(Enum)`, `Nullable(Enum8)` o `Nullable(Enum16)`

Sintaxis:

```sql
ALTER TABLE table_name MODIFY COLUMN enum_column_name ADD ENUM VALUES ('EnumName' [= number], ...);
```

**Ejemplo**

Añada dos valores a la columna `enum_column_name`:

```sql
ALTER TABLE table_name MODIFY COLUMN enum_column_name ADD ENUM VALUES ('Hundred' = 100, 'HundredOne');
```

<div id="materialize-column">
  ## MATERIALIZE COLUMN
</div>

Materializa una columna con una expresión de valor `DEFAULT` o `MATERIALIZED`. Al añadir una columna materializada con `ALTER TABLE table_name ADD COLUMN column_name MATERIALIZED`, las filas existentes sin valores materializados no se rellenan automáticamente. La sentencia `MATERIALIZE COLUMN` puede utilizarse para reescribir los datos existentes de una columna después de añadir o actualizar una expresión `DEFAULT` o `MATERIALIZED` (lo que solo actualiza los metadatos, pero no modifica los datos existentes). Ten en cuenta que materializar una columna en la clave de ordenación es una operación no válida, ya que podría alterar el orden de clasificación.
Se implementa como una [mutación](/es/sql-reference/statements/alter/index.md#mutations).

En las columnas con una expresión de valor `MATERIALIZED` nueva o actualizada, se reescriben todas las filas existentes.

En las columnas con una expresión de valor `DEFAULT` nueva o actualizada, el comportamiento depende de la versión de ClickHouse:

* En ClickHouse &lt; v24.2, se reescriben todas las filas existentes.
* ClickHouse &gt;= v24.2 distingue si el valor de una fila en una columna con una expresión de valor `DEFAULT` se especificó explícitamente al insertarla o no; es decir, si se calculó a partir de la expresión de valor `DEFAULT`. Si el valor se especificó explícitamente, ClickHouse lo mantiene tal cual. Si el valor se calculó, ClickHouse lo cambia por la expresión de valor `MATERIALIZED` nueva o actualizada.

Sintaxis:

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] MATERIALIZE COLUMN col [IN PARTITION partition | IN PARTITION ID 'partition_id'];
```

* Si especifica una PARTITION, se materializará una columna que solo contendrá la partición especificada.

**Ejemplo**

```sql
DROP TABLE IF EXISTS tmp;
SET mutations_sync = 2;
CREATE TABLE tmp (x Int64) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY tuple();
INSERT INTO tmp SELECT * FROM system.numbers LIMIT 5;
ALTER TABLE tmp ADD COLUMN s String MATERIALIZED toString(x);

ALTER TABLE tmp MATERIALIZE COLUMN s;

SELECT groupArray(x), groupArray(s) FROM (select x,s from tmp order by x);

┌─groupArray(x)─┬─groupArray(s)─────────┐
│ [0,1,2,3,4]   │ ['0','1','2','3','4'] │
└───────────────┴───────────────────────┘

ALTER TABLE tmp MODIFY COLUMN s String MATERIALIZED toString(round(100/x));

INSERT INTO tmp SELECT * FROM system.numbers LIMIT 5,5;

SELECT groupArray(x), groupArray(s) FROM tmp;

┌─groupArray(x)─────────┬─groupArray(s)──────────────────────────────────┐
│ [0,1,2,3,4,5,6,7,8,9] │ ['0','1','2','3','4','20','17','14','12','11'] │
└───────────────────────┴────────────────────────────────────────────────┘

ALTER TABLE tmp MATERIALIZE COLUMN s;

SELECT groupArray(x), groupArray(s) FROM tmp;

┌─groupArray(x)─────────┬─groupArray(s)─────────────────────────────────────────┐
│ [0,1,2,3,4,5,6,7,8,9] │ ['inf','100','50','33','25','20','17','14','12','11'] │
└───────────────────────┴───────────────────────────────────────────────────────┘
```

**Véase también**

* [MATERIALIZED](/es/sql-reference/statements/create/view#materialized-view).

<div id="limitations">
  ## Limitaciones
</div>

La consulta `ALTER` permite crear y eliminar elementos individuales (columnas) en estructuras de datos anidadas, pero no estructuras de datos anidadas completas. Para agregar una estructura de datos anidada, puede añadir columnas con un nombre como `name.nested_name` y el tipo `Array(T)`. Una estructura de datos anidada equivale a varias columnas de tipo array con un nombre que comparte el mismo prefijo antes del punto.

El cambio de nombre de columnas con puntos en sus nombres tiene compatibilidad parcial. Los puntos están reservados para el acceso a sub-columnas [Nested](/es/sql-reference/data-types/nested-data-structures/nested), por lo que el prefijo (nombre padre) debe seguir siendo el mismo. Solo puede cambiarse el sufijo (nombre de la sub-columna). Por ejemplo, `a.b` puede renombrarse a `a.c`, pero no se permite renombrar `a.b` a `b.d` porque cambia el prefijo padre de Nested.

No se admite la eliminación de columnas de la clave primaria o de la clave de muestreo (columnas que se usan en la expresión `ENGINE`). Cambiar el tipo de las columnas incluidas en la clave primaria solo es posible si ese cambio no provoca modificaciones en los datos (por ejemplo, se permite agregar valores a un Enum o cambiar un tipo de `DateTime` a `UInt32`).

Si la consulta `ALTER` no es suficiente para realizar los cambios que necesita en la tabla, puede crear una tabla nueva, copiar los datos en ella mediante la consulta [INSERT SELECT](/es/sql-reference/statements/insert-into.md/#inserting-the-results-of-select), luego intercambiar las tablas mediante la consulta [RENAME](/es/sql-reference/statements/rename.md/#rename-table) y eliminar la tabla antigua.

La consulta `ALTER` bloquea todas las lecturas y escrituras de la tabla. En otras palabras, si un `SELECT` de larga duración se está ejecutando en el momento de la consulta `ALTER`, la consulta `ALTER` esperará a que finalice. Al mismo tiempo, todas las consultas nuevas sobre la misma tabla esperarán mientras este `ALTER` se esté ejecutando.

En el caso de las tablas que no almacenan datos por sí mismas (como [Merge](/es/sql-reference/statements/alter/index.md) y [Distributed](/es/sql-reference/statements/alter/index.md)), `ALTER` solo cambia la estructura de la tabla y no la estructura de las tablas subyacentes. Por ejemplo, al ejecutar ALTER para una tabla `Distributed`, también deberá ejecutar `ALTER` para las tablas en todos los servidores remotos.