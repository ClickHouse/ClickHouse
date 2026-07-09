---
description: 'Tabla del sistema que muestra los watches de ZooKeeper activos actualmente registrados en
  este servidor de ClickHouse.'
keywords: ['tabla del sistema', 'zookeeper_watches']
slug: /operations/system-tables/zookeeper_watches
title: 'system.zookeeper_watches'
doc_type: 'reference'
---

<div id="description">
  ## Descripción
</div>

Muestra los [watches](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html#ch_zkWatches) activos en este momento que este servidor de ClickHouse ha registrado en nodos de ZooKeeper (incluidos los ZooKeepers auxiliares). Cada fila representa un watch.

<div id="columns">
  ## Columnas
</div>

* `zookeeper_name` ([String](../../sql-reference/data-types/string.md)) — Nombre de la conexión de ZooKeeper (`default` para la conexión principal o el nombre auxiliar).
* `create_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Hora a la que se creó el watch.
* `create_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Hora a la que se creó el watch con precisión de microsegundos.
* `path` ([String](../../sql-reference/data-types/string.md)) — ruta de ZooKeeper que se está vigilando.
* `session_id` ([Int64](../../sql-reference/data-types/int-uint.md)) — ID de la sesión de la conexión que registró el watch.
* `request_xid` ([Int64](../../sql-reference/data-types/int-uint.md)) — XID de la solicitud que creó el watch.
* `op_num` ([Enum](../../sql-reference/data-types/enum.md)) — Tipo de la solicitud que creó el watch.
* `watch_type` ([Enum8](../../sql-reference/data-types/enum.md)) — Tipo de watch. Posibles valores:
  * `Children` — vigila cambios en la lista de nodos hijo (establecido por operaciones `List`).
  * `Exists` — vigila la creación o eliminación de nodos.
  * `Data` — vigila cambios en los datos del nodo (establecido por operaciones `Get`).

Ejemplo:

```sql
SELECT * FROM system.zookeeper_watches FORMAT Vertical;
```

```text
Row 1:
──────
zookeeper_name:           default
create_time:              2026-03-16 12:00:00
create_time_microseconds: 2026-03-16 12:00:00.123456
path:                     /clickhouse/task_queue/ddl
session_id:               106662742089334927
request_xid:              10858
op_num:                   List
watch_type:               Children
```

**Véase también**

* [ZooKeeper](../../operations/tips.md#zookeeper)
* [Guía de ZooKeeper](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html)