---
description: 'جدول نظامي يعرض مراقبات ZooKeeper النشطة حاليًا والمسجّلة على
  خادم ClickHouse هذا.'
keywords: ['جدول نظامي', 'zookeeper_watches']
slug: /operations/system-tables/zookeeper_watches
title: 'system.zookeeper_watches'
doc_type: 'مرجع'
---

<div id="description">
  ## الوصف
</div>

يعرض [المراقبات](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html#ch_zkWatches) النشطة حاليًا التي سجّلها خادم ClickHouse هذا على عُقد ZooKeeper (بما في ذلك مثيلات ZooKeeper الإضافية). يمثّل كل صف مراقبة واحدة.

<div id="columns">
  ## الأعمدة
</div>

* `zookeeper_name` ([String](../../sql-reference/data-types/string.md)) — اسم اتصال ZooKeeper (`default` للاتصال الرئيسي أو الاسم الإضافي).
* `create_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — الوقت الذي أُنشئت فيه المراقبة.
* `create_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — الوقت الذي أُنشئت فيه المراقبة بدقة الميكروثانية.
* `path` ([String](../../sql-reference/data-types/string.md)) — مسار ZooKeeper الذي تجري مراقبته.
* `session_id` ([Int64](../../sql-reference/data-types/int-uint.md)) — معرّف الجلسة للاتصال الذي سجّل المراقبة.
* `request_xid` ([Int64](../../sql-reference/data-types/int-uint.md)) — XID الخاص بالطلب الذي أنشأ المراقبة.
* `op_num` ([Enum](../../sql-reference/data-types/enum.md)) — نوع الطلب الذي أنشأ المراقبة.
* `watch_type` ([Enum8](../../sql-reference/data-types/enum.md)) — نوع المراقبة. القيم الممكنة:
  * `Children` — مراقبة التغييرات في قائمة العُقد الفرعية (تُعيَّن بواسطة عمليات `List`).
  * `Exists` — مراقبة إنشاء العُقدة أو حذفها.
  * `Data` — مراقبة التغييرات في بيانات العُقدة (تُعيَّن بواسطة عمليات `Get`).

مثال:

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

**راجع أيضًا**

* [ZooKeeper](../../operations/tips.md#zookeeper)
* [دليل ZooKeeper](https://zookeeper.apache.org/doc/r3.3.3/zookeeperProgrammers.html)