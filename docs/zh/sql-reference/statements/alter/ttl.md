---
toc_priority: 44
toc_title: TTL
---

# 表的 TTL 操作 {#manipulations-with-table-ttl}

## 修改 MODIFY TTL {#modify-ttl}

你能修改 [表 TTL](../../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) ，命令语法如下所示：

``` sql
ALTER TABLE table_name MODIFY TTL ttl_expression;
```

## 移除 REMOVE TTL {#remove-ttl}

TTL 属性可以用下列命令从表中移除:

```sql
ALTER TABLE table_name REMOVE TTL
```

**示例**

创建一个表，带有 `TTL` 属性如下所示:

```sql
CREATE TABLE table_with_ttl
(
    event_time DateTime,
    UserID UInt64,
    Comment String
)
ENGINE MergeTree()
ORDER BY tuple()
TTL event_time + INTERVAL 3 MONTH;
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO table_with_ttl VALUES (now(), 1, 'username1');

INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
```

运行命令 `OPTIMIZE` 强制清理 `TTL`:

```sql
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```
第二行记录被从表中删除掉了.

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
└───────────────────────┴─────────┴──────────────┘
```

现在用下面的命令，把表的  `TTL` 移除掉:

```sql
ALTER TABLE table_with_ttl REMOVE TTL;
```

重新插入上面的数据，并尝试再次运行  `OPTIMIZE` 命令清理   `TTL` 属性  :

```sql
INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```

可以看得到 `TTL` 这个属性已经没了，并且可以看得到第二行记录并没有被删除:

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
│   2020-08-11 12:44:57 │       2 │    username2 │
└───────────────────────┴─────────┴──────────────┘
```

**更多参考**

- 关于 [TTL 表达式](../../../sql-reference/statements/create/table.md#ttl-expression).
- 修改列 [with TTL](../../../sql-reference/statements/alter/column.md#alter_modify-column).
