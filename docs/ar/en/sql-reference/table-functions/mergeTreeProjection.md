---
description: 'يمثل محتويات أحد الإسقاطات في جداول MergeTree.
  ويمكن استخدامه للاستبطان.'
sidebar_label: 'mergeTreeProjection'
sidebar_position: 77
slug: /sql-reference/table-functions/mergeTreeProjection
title: 'mergeTreeProjection'
doc_type: 'reference'
---

يمثل محتويات أحد الإسقاطات في جداول MergeTree. ويمكن استخدامه للاستبطان.

<div id="syntax">
  ## الصياغة
</div>

```sql
mergeTreeProjection(database, table, projection)
```

<div id="arguments">
  ## الوسيطات
</div>

| الوسيط       | الوصف                                         |
| ------------ | --------------------------------------------- |
| `database`   | اسم قاعدة البيانات التي تُقرأ منها الإسقاطات. |
| `table`      | اسم الجدول الذي تُقرأ منه الإسقاطات.          |
| `projection` | الإسقاط المراد القراءة منه.                   |

<div id="returned_value">
  ## القيمة المُعادة
</div>

كائن جدول بأعمدة يوفّرها الإسقاط المحدد.

<div id="usage-example">
  ## مثال استخدام
</div>

```sql
CREATE TABLE test
(
    `user_id` UInt64,
    `item_id` UInt64,
    PROJECTION order_by_item_id
    (
        SELECT _part_offset
        ORDER BY item_id
    )
)
ENGINE = MergeTree
ORDER BY user_id;

INSERT INTO test SELECT number, 100 - number FROM numbers(5);
```

```sql
SELECT *, _part_offset FROM mergeTreeProjection(currentDatabase(), test, order_by_item_id);
```

```text
   ┌─item_id─┬─_parent_part_offset─┬─_part_offset─┐
1. │      96 │                   4 │            0 │
2. │      97 │                   3 │            1 │
3. │      98 │                   2 │            2 │
4. │      99 │                   1 │            3 │
5. │     100 │                   0 │            4 │
   └─────────┴─────────────────────┴──────────────┘
```

```sql
DESCRIBE mergeTreeProjection(currentDatabase(), test, order_by_item_id) SETTINGS describe_compact_output = 1;
```

```text
   ┌─name────────────────┬─type───┐
1. │ item_id             │ UInt64 │
2. │ _parent_part_offset │ UInt64 │
   └─────────────────────┴────────┘
```