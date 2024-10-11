---
slug: /zh/sql-reference/statements/alter/view
sidebar_position: 50
sidebar_label: VIEW
---

# ALTER TABLE ... MODIFY QUERY 语句 {#alter-modify-query}

当使用`ALTER TABLE ... MODIFY QUERY`语句创建一个[物化视图](../create/view.md#materialized)时，可以修改`SELECT`查询。当物化视图在没有  `TO [db.]name` 的情况下创建时使用它。必须启用 `allow_experimental_alter_materialized_view_structure`设置。

如果一个物化视图使用`TO [db.]name`，你必须先 [DETACH](../detach.mdx) 视图。用[ALTER TABLE](index.md)修改目标表，然后 [ATTACH](../attach.mdx)之前分离的(`DETACH`)视图。

**示例**

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

## ALTER LIVE VIEW 语句 {#alter-live-view}

`ALTER LIVE VIEW ... REFRESH` 语句刷新一个 [实时视图](../create/view.md#live-view). 参见 [强制实时视图刷新](../create/view.md#live-view-alter-refresh).
