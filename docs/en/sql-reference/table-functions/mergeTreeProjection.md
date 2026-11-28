---
description: 'Represents the contents of some projection in MergeTree tables.
  It can be used for introspection.'
sidebar_label: 'mergeTreeProjection'
sidebar_position: 77
slug: /sql-reference/table-functions/mergeTreeProjection
title: 'mergeTreeProjection'
doc_type: 'reference'
---

# mergeTreeProjection Table Function

Represents the contents of some projection in MergeTree tables. It can be used for introspection.

## Syntax {#syntax}

```sql
mergeTreeProjection(database, table, projection)
```

## Arguments {#arguments}

| Argument     | Description                                |
|--------------|--------------------------------------------|
| `database`   | The database name to read projection from. |
| `table`      | The table name to read projection from.    |
| `projection` | The projection to read from.               |

## Returned value {#returned_value}

A table object with columns provided by given projection.

## Usage Example {#usage-example}

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
