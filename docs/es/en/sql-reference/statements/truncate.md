---
description: 'Documentación de las sentencias TRUNCATE'
sidebar_label: 'TRUNCATE'
sidebar_position: 52
slug: /sql-reference/statements/truncate
title: 'Sentencias TRUNCATE'
doc_type: 'reference'
---

La sentencia `TRUNCATE` en ClickHouse se utiliza para eliminar rápidamente todos los datos de una tabla o base de datos, conservando su estructura.

<div id="truncate-table">
  ## TRUNCATE TABLE
</div>

```sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]
```

<br />

| Parámetro            | Descripción                                                                                                                                                                |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `IF EXISTS`          | Evita un error si la tabla no existe. Si se omite, la consulta devuelve un error.                                                                                          |
| `db.name`            | Nombre opcional de la base de datos.                                                                                                                                       |
| `ON CLUSTER cluster` | Ejecuta el comando en todo el clúster especificado.                                                                                                                        |
| `SYNC`               | Hace que el truncado sea síncrono en todas las réplicas cuando se usan tablas replicadas. Si se omite, el truncado se realiza de forma asíncrona de manera predeterminada. |

Puede usar la configuración [alter&#95;sync](/es/operations/settings/settings#alter_sync) para definir la espera hasta que las acciones se ejecuten en las réplicas.

Puede especificar cuánto tiempo (en segundos) esperar a que las réplicas inactivas ejecuten consultas `TRUNCATE` con la configuración [replication&#95;wait&#95;for&#95;inactive&#95;replica&#95;timeout](/es/operations/settings/settings#replication_wait_for_inactive_replica_timeout).

:::note
Si `alter_sync` está configurado en `2` y algunas réplicas permanecen inactivas durante más tiempo del especificado por la configuración `replication_wait_for_inactive_replica_timeout`, se lanza una excepción `UNFINISHED`.
:::

La consulta `TRUNCATE TABLE` **no es compatible** con los siguientes motores de tabla:

* [`View`](../../engines/table-engines/special/view.md)
* [`File`](../../engines/table-engines/special/file.md)
* [`URL`](../../engines/table-engines/special/url.md)
* [`Buffer`](../../engines/table-engines/special/buffer.md)
* [`Null`](../../engines/table-engines/special/null.md)

<div id="truncate-all-tables">
  ## TRUNCATE TODAS LAS TABLAS
</div>

```sql
TRUNCATE [ALL] TABLES FROM [IF EXISTS] db [LIKE | ILIKE | NOT LIKE '<pattern>'] [ON CLUSTER cluster]
```

<br />

| Parámetro                               | Descripción                                                |
| --------------------------------------- | ---------------------------------------------------------- |
| `ALL`                                   | Elimina los datos de todas las tablas de la base de datos. |
| `IF EXISTS`                             | Evita un error si la base de datos no existe.              |
| `db`                                    | Nombre de la base de datos.                                |
| `LIKE \| ILIKE \| NOT LIKE '<pattern>'` | Filtra las tablas según el patrón.                         |
| `ON CLUSTER cluster`                    | Ejecuta el comando en todo el clúster.                     |

Elimina todos los datos de todas las tablas de una base de datos.

<div id="truncate-database">
  ## TRUNCATE DATABASE
</div>

```sql
TRUNCATE DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```

<br />

| Parámetro            | Descripción                                         |
| -------------------- | --------------------------------------------------- |
| `IF EXISTS`          | Evita un error si la base de datos no existe.       |
| `db`                 | El nombre de la base de datos.                      |
| `ON CLUSTER cluster` | Ejecuta el comando en todo el clúster especificado. |

Elimina todas las tablas de una base de datos, pero conserva la propia base de datos. Cuando se omite la cláusula `IF EXISTS`, la consulta devuelve un error si la base de datos no existe.

:::note
`TRUNCATE DATABASE` no es compatible con bases de datos `Replicated`. En su lugar, simplemente haz `DROP` y `CREATE` de la base de datos.
:::