---
description: 'Documentación de ALTER'
sidebar_label: 'ALTER'
sidebar_position: 35
slug: /sql-reference/statements/alter/
title: 'ALTER'
doc_type: 'reference'
---

La mayoría de las consultas `ALTER TABLE` modifican la configuración de la tabla o sus datos:

| Modificador                                                                 |
| --------------------------------------------------------------------------- |
| [COLUMN](/es/sql-reference/statements/alter/column.md)                         |
| [PARTITION](/es/sql-reference/statements/alter/partition.md)                   |
| [DELETE](/es/sql-reference/statements/alter/delete.md)                         |
| [UPDATE](/es/sql-reference/statements/alter/update.md)                         |
| [ORDER BY](/es/sql-reference/statements/alter/order-by.md)                     |
| [INDEX](/es/sql-reference/statements/alter/skipping-index.md)                  |
| [CONSTRAINT](/es/sql-reference/statements/alter/constraint.md)                 |
| [TTL](/es/sql-reference/statements/alter/ttl.md)                               |
| [STATISTICS](/es/sql-reference/statements/alter/statistics.md)                 |
| [APPLY DELETED MASK](/es/sql-reference/statements/alter/apply-deleted-mask.md) |
| [APPLY PATCHES](/es/sql-reference/statements/alter/apply-patches.md)           |

:::note
La mayoría de las consultas `ALTER TABLE` solo se admiten para tablas [*MergeTree](/es/engines/table-engines/mergetree-family/index.md), [Merge](/es/engines/table-engines/special/merge.md) y [Distributed](/es/engines/table-engines/special/distributed.md).
:::

Estas sentencias `ALTER` manipulan vistas:

| Sentencia                                                               | Descripción                                                                                 |
| ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| [ALTER TABLE ... MODIFY QUERY](/es/sql-reference/statements/alter/view.md) | Modifica la estructura de una [vista materializada](/es/sql-reference/statements/create/view). |

Estas sentencias `ALTER` modifican entidades relacionadas con el control de acceso basado en roles:

| Sentencia                                                               |
| ----------------------------------------------------------------------- |
| [USER](/es/sql-reference/statements/alter/user.md)                         |
| [ROLE](/es/sql-reference/statements/alter/role.md)                         |
| [QUOTA](/es/sql-reference/statements/alter/quota.md)                       |
| [ROW POLICY](/es/sql-reference/statements/alter/row-policy.md)             |
| [SETTINGS PROFILE](/es/sql-reference/statements/alter/settings-profile.md) |

| Sentencia                                                                     | Descripción                                                                                                    |
| ----------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| [ALTER TABLE ... MODIFY COMMENT](/es/sql-reference/statements/alter/comment.md)  | Agrega, modifica o elimina comentarios en la tabla, independientemente de si se habían establecido antes o no. |
| [ALTER NAMED COLLECTION](/es/sql-reference/statements/alter/named-collection.md) | Modifica las [Named Collections](/es/operations/named-collections.md).                                            |

<div id="mutations">
  ## Mutaciones
</div>

Las consultas `ALTER` destinadas a manipular datos de tablas se implementan mediante un mecanismo llamado &quot;mutaciones&quot;, en particular [ALTER TABLE ... DELETE](/es/sql-reference/statements/alter/delete.md) y [ALTER TABLE ... UPDATE](/es/sql-reference/statements/alter/update.md). Son procesos asíncronos en segundo plano similares a las fusiones en las tablas [MergeTree](/es/engines/table-engines/mergetree-family/index.md), que producen nuevas versiones &quot;mutadas&quot; de las partes.

En las tablas `*MergeTree`, las mutaciones se ejecutan **reescribiendo partes de datos completas**.
No hay atomicidad: las partes se sustituyen por partes mutadas en cuanto están listas, y una consulta `SELECT` que haya comenzado a ejecutarse durante una mutación verá datos de partes que ya han sido mutadas junto con datos de partes que todavía no han sido mutadas.

Las mutaciones se ordenan totalmente según su orden de creación y se aplican a cada parte en ese orden. Las mutaciones también están parcialmente ordenadas con las consultas `INSERT INTO`: los datos insertados en la tabla antes de que se enviara la mutación serán mutados, y los datos insertados después no lo serán. Tenga en cuenta que las mutaciones no bloquean las inserciones de ninguna manera.

Una consulta de mutación devuelve el resultado inmediatamente después de que se añade la entrada de mutación (en el caso de las tablas replicadas, a ZooKeeper; en las tablas no replicadas, al sistema de archivos). La mutación en sí se ejecuta de forma asíncrona usando la configuración del perfil del sistema. Para seguir el progreso de las mutaciones, puede usar la tabla [`system.mutations`](/es/operations/system-tables/mutations). Una mutación enviada correctamente seguirá ejecutándose incluso si se reinician los servidores ClickHouse. No hay forma de revertir la mutación una vez enviada, pero si la mutación se queda bloqueada por algún motivo, puede cancelarse con la consulta [`KILL MUTATION`](/es/sql-reference/statements/kill.md/#kill-mutation).

Las entradas de las mutaciones finalizadas no se eliminan de inmediato (el número de entradas conservadas lo determina el parámetro del motor de almacenamiento `finished_mutations_to_keep`). Las entradas de mutación más antiguas se eliminan.

<div id="synchronicity-of-alter-queries">
  ## Sincronía de las consultas ALTER
</div>

Para las tablas no replicadas, todas las consultas `ALTER` se realizan de forma síncrona. Para las tablas replicadas, la consulta solo añade instrucciones para las acciones correspondientes en `ZooKeeper`, y las acciones en sí se ejecutan lo antes posible. Sin embargo, la consulta puede esperar a que estas acciones se completen en todas las réplicas.

Para las consultas `ALTER` que crean mutaciones (p. ej., entre otras, `UPDATE`, `DELETE`, `MATERIALIZE INDEX`, `MATERIALIZE PROJECTION`, `MATERIALIZE COLUMN`, `APPLY DELETED MASK`, `APPLY PATCHES`, `CLEAR STATISTIC`, `MATERIALIZE STATISTIC`), la sincronía viene determinada por la configuración [mutations&#95;sync](/es/operations/settings/settings.md/#mutations_sync).

Para otras consultas `ALTER` que solo modifican los metadatos, puede usar la configuración [alter&#95;sync](/es/operations/settings/settings#alter_sync) para configurar la espera.

Puede especificar cuánto tiempo (en segundos) se debe esperar a que las réplicas inactivas ejecuten todas las consultas `ALTER` con la configuración [replication&#95;wait&#95;for&#95;inactive&#95;replica&#95;timeout](/es/operations/settings/settings#replication_wait_for_inactive_replica_timeout).

:::note
Para todas las consultas `ALTER`, si `alter_sync = 2` y algunas réplicas permanecen inactivas durante más tiempo del especificado en la configuración `replication_wait_for_inactive_replica_timeout`, se lanza una excepción `UNFINISHED`.
:::

<div id="related-content">
  ## Contenido relacionado
</div>

* Blog: [Cómo gestionar actualizaciones y eliminaciones en ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)