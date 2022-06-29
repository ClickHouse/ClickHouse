---
sidebar_position: 50
sidebar_label: VIEW
---

# ALTER TABLE … MODIFY QUERY Statement

You can modify `SELECT` query that was specified when a [materialized view](../create/view.md#materialized) was created with the `ALTER TABLE … MODIFY QUERY` statement. Use it when the materialized view was created without the `TO [db.]name` clause. The `allow_experimental_alter_materialized_view_structure` setting must be enabled. 

If a materialized view uses the `TO [db.]name` construction, you must [DETACH](../detach.md) the view, run [ALTER TABLE](index.md) query for the target table, and then [ATTACH](../attach.md) the previously detached (`DETACH`) view.

**Example**

```sql
CREATE TABLE src_table (`a` UInt32) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW mv (`a` UInt32) ENGINE = MergeTree ORDER BY a AS SELECT a FROM src_table; 
INSERT INTO src_table (a) VALUES (1), (2);
SELECT * FROM mv;
```
```text
┌─a─┐
│ 1 │
│ 2 │
└───┘
```
```sql
ALTER TABLE mv MODIFY QUERY SELECT a * 2 as a FROM src_table;
INSERT INTO src_table (a) VALUES (3), (4);
SELECT * FROM mv;
```
```text
┌─a─┐
│ 6 │
│ 8 │
└───┘
┌─a─┐
│ 1 │
│ 2 │
└───┘
```

## ALTER LIVE VIEW Statement

`ALTER LIVE VIEW ... REFRESH` statement refreshes a [Live view](../create/view.md#live-view). See [Force Live View Refresh](../create/view.md#live-view-alter-refresh).
