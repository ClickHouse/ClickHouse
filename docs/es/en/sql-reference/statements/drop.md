---
description: 'Documentación de las sentencias DROP'
sidebar_label: 'DROP'
sidebar_position: 44
slug: /sql-reference/statements/drop
title: 'Sentencias DROP'
doc_type: 'reference'
---

Elimina una entidad existente. Si se especifica la cláusula `IF EXISTS`, estas consultas no generan ningún error si la entidad no existe. Si se especifica el modificador `SYNC`, la entidad se elimina inmediatamente.

<div id="drop-database">
  ## DROP DATABASE
</div>

Elimina todas las tablas de la base de datos `db` y, a continuación, elimina la propia base de datos `db`.

Sintaxis:

```sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster] [SYNC]
```

<div id="drop-table">
  ## DROP TABLE
</div>

Elimina una o varias tablas.

:::tip
Para deshacer la eliminación de una tabla, consulta [UNDROP TABLE](/es/sql-reference/statements/undrop.md)
:::

Sintaxis:

```sql
DROP [TEMPORARY] TABLE [IF EXISTS] [IF EMPTY]  [db1.]name_1[, [db2.]name_2, ...] [ON CLUSTER cluster] [SYNC]
```

Limitaciones:

* Si se especifica la cláusula `IF EMPTY`, el servidor comprueba si la tabla está vacía solo en la réplica que recibió la consulta.
* Eliminar varias tablas a la vez no es una operación atómica; es decir, si falla la eliminación de una tabla, las tablas siguientes no se eliminarán.

<div id="drop-dictionary">
  ## DROP DICTIONARY
</div>

Elimina el diccionario.

Sintaxis:

```sql
DROP DICTIONARY [IF EXISTS] [db.]name [SYNC]
```

<div id="drop-user">
  ## DROP USER
</div>

Elimina un usuario.

Sintaxis:

```sql
DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-role">
  ## DROP ROLE
</div>

Elimina un rol. El rol eliminado se revoca de todas las entidades a las que estaba asignado.

Sintaxis:

```sql
DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-row-policy">
  ## DROP ROW POLICY
</div>

Elimina una ROW POLICY. La ROW POLICY eliminada se revoca para todas las entidades a las que se había asignado.

Sintaxis:

```sql
DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-masking-policy">
  ## DROP MASKING POLICY
</div>

Elimina una política de enmascaramiento.

Sintaxis:

```sql
DROP MASKING POLICY [IF EXISTS] name ON [database.]table [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-quota">
  ## DROP QUOTA
</div>

Elimina una QUOTA. La QUOTA eliminada se revoca para todas las entidades a las que se había asignado.

Sintaxis:

```sql
DROP QUOTA [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-settings-profile">
  ## DROP SETTINGS PROFILE
</div>

Elimina un perfil de configuración. El perfil de configuración eliminado se revoca para todas las entidades a las que estaba asignado.

Sintaxis:

```sql
DROP [SETTINGS] PROFILE [IF EXISTS] name [,...] [ON CLUSTER cluster_name] [FROM access_storage_type]
```

<div id="drop-view">
  ## DROP VIEW
</div>

Elimina una vista. Las vistas también se pueden eliminar con el comando `DROP TABLE`, pero `DROP VIEW` comprueba que `[db.]name` sea una vista.

Sintaxis:

```sql
DROP VIEW [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]
```

<div id="drop-function">
  ## DROP FUNCTION
</div>

Elimina una función definida por el usuario creada con [CREATE FUNCTION](./create/function.md).
Las funciones del sistema no pueden eliminarse.

**Sintaxis**

```sql
DROP FUNCTION [IF EXISTS] function_name [on CLUSTER cluster]
```

**Ejemplo**

```sql
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
DROP FUNCTION linear_equation;
```

<div id="drop-named-collection">
  ## DROP NAMED COLLECTION
</div>

Elimina una colección nombrada.

**Sintaxis**

```sql
DROP NAMED COLLECTION [IF EXISTS] name [on CLUSTER cluster]
```

**Ejemplo**

```sql
CREATE NAMED COLLECTION foobar AS a = '1', b = '2';
DROP NAMED COLLECTION foobar;
```