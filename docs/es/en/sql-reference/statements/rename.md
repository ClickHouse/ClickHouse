---
description: 'Documentación de la sentencia RENAME'
sidebar_label: 'RENAME'
sidebar_position: 48
slug: /sql-reference/statements/rename
title: 'Sentencia RENAME'
doc_type: 'reference'
---

Renombra bases de datos, tablas o diccionarios. Se pueden renombrar varias entidades en una sola consulta.
Ten en cuenta que la consulta `RENAME` con varias entidades es una operación no atómica. Para intercambiar los nombres de las entidades de forma atómica, usa la sentencia [EXCHANGE](./exchange.md).

**Sintaxis**

```sql
RENAME [DATABASE|TABLE|DICTIONARY] name TO new_name [,...] [ON CLUSTER cluster]
```

<div id="rename-database">
  ## RENAME DATABASE
</div>

Cambia el nombre de bases de datos.

**Sintaxis**

```sql
RENAME DATABASE atomic_database1 TO atomic_database2 [,...] [ON CLUSTER cluster]
```

<div id="rename-table">
  ## RENAME TABLE
</div>

Cambia el nombre de una o más tablas.

Renombrar tablas es una operación ligera. Si especifica una base de datos distinta después de `TO`, la tabla se moverá a esa base de datos. Sin embargo, los directorios de las bases de datos deben estar en el mismo sistema de archivos. De lo contrario, se devuelve un error.
Si renombra varias tablas en una sola consulta, la operación no es atómica. Puede ejecutarse parcialmente, y las consultas en otras sesiones pueden devolver el error `Table ... does not exist ...`.

**Sintaxis**

```sql
RENAME TABLE [db1.]name1 TO [db2.]name2 [,...] [ON CLUSTER cluster]
```

**Ejemplo**

```sql
RENAME TABLE table_A TO table_A_bak, table_B TO table_B_bak;
```

Y puedes usar una consulta SQL más simple:

```sql
RENAME table_A TO table_A_bak, table_B TO table_B_bak;
```

<div id="rename-dictionary">
  ## RENAME DICTIONARY
</div>

Cambia el nombre de uno o varios diccionarios. Esta consulta puede utilizarse para mover diccionarios entre bases de datos.

**Sintaxis**

```sql
RENAME DICTIONARY [db0.]dict_A TO [db1.]dict_B [,...] [ON CLUSTER cluster]
```

**Véase también**

* [Diccionarios](./create/dictionary/overview.md)