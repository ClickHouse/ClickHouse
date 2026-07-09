---
description: 'MergeTree テーブル内の特定のプロジェクションの内容を表します。
  イントロスペクションに使用できます。'
sidebar_label: 'mergeTreeProjection'
sidebar_position: 77
slug: /sql-reference/table-functions/mergeTreeProjection
title: 'mergeTreeProjection'
doc_type: 'reference'
---

MergeTree テーブル内の特定のプロジェクションの内容を表します。イントロスペクションに使用できます。

<div id="syntax">
  ## 構文
</div>

```sql
mergeTreeProjection(database, table, projection)
```

<div id="arguments">
  ## 引数
</div>

| 引数           | 説明                       |
| ------------ | ------------------------ |
| `database`   | プロジェクションを読み取る対象のデータベース名。 |
| `table`      | プロジェクションを読み取る対象のテーブル名。   |
| `projection` | 読み取る対象のプロジェクション。         |

<div id="returned_value">
  ## 戻り値
</div>

指定されたプロジェクションによって提供されるカラムを含むテーブルオブジェクト。

<div id="usage-example">
  ## 使用例
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