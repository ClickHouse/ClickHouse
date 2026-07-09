---
description: 'Системная таблица, показывающая активные наблюдения ZooKeeper,
  зарегистрированные на этом сервере ClickHouse.'
keywords: ['системная таблица', 'zookeeper_watches']
slug: /operations/system-tables/zookeeper_watches
title: 'system.zookeeper_watches'
doc_type: 'reference'
---

<div id="description">
  ## Описание
</div>

Показывает активные [наблюдения](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html#ch_zkWatches), которые этот сервер ClickHouse в данный момент зарегистрировал на узлах ZooKeeper (включая дополнительные экземпляры ZooKeeper). Каждая строка соответствует одному наблюдению.

<div id="columns">
  ## Столбцы
</div>

* `zookeeper_name` ([String](../../sql-reference/data-types/string.md)) — Имя подключения ZooKeeper (`default` для основного подключения или вспомогательное имя).
* `create_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Время создания наблюдения.
* `create_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Время создания наблюдения с точностью до микросекунд.
* `path` ([String](../../sql-reference/data-types/string.md)) — Путь ZooKeeper, за которым установлено наблюдение.
* `session_id` ([Int64](../../sql-reference/data-types/int-uint.md)) — Идентификатор сеанса подключения, которое зарегистрировало наблюдение.
* `request_xid` ([Int64](../../sql-reference/data-types/int-uint.md)) — XID запроса, создавшего наблюдение.
* `op_num` ([Enum](../../sql-reference/data-types/enum.md)) — Тип запроса, создавшего наблюдение.
* `watch_type` ([Enum8](../../sql-reference/data-types/enum.md)) — Тип наблюдения. Возможные значения:
  * `Children` — наблюдение за изменениями в списке дочерних узлов (задаётся операциями `List`).
  * `Exists` — наблюдение за созданием или удалением узла.
  * `Data` — наблюдение за изменениями данных узла (задаётся операциями `Get`).

Пример:

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

**См. также**

* [ZooKeeper](../../operations/tips.md#zookeeper)
* [Руководство по ZooKeeper](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html)