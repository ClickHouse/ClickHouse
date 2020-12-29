---
toc_priority: 44
toc_title: TTL
---

# Manipulations with Table TTL {#manipulations-with-table-ttl}

## MODIFY TTL {#modify-ttl}

You can change [table TTL](../../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) with a request of the following form:

``` sql
ALTER TABLE table_name MODIFY TTL ttl_expression;
```

## REMOVE TTL {#remove-ttl}

Removes TTL-property from the specified column.

Syntax:

```sql
ALTER TABLE table_name MODIFY column_name REMOVE TTL 
```

**Example**

Requests and results:

Consider the table:

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

Trigger `TTL` works clearly with `OPTIMIZE` query. Make this to start the background cleaning using TTL:

```sql
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```
As a result you see that the second line was deleted.

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
└───────────────────────┴─────────┴──────────────┘
```

```sql
ALTER TABLE table_with_ttl REMOVE TTL;
INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```

Now TTL-property was removed and there is nothing to be deleted.

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
│   2020-08-11 12:44:57 │       2 │    username2 │
└───────────────────────┴─────────┴──────────────┘
```

### See Also

- More about the [TTL-expression](../../../sql-reference/statements/create/table#ttl-expression).
- Modify column [with TTL](../../../sql-reference/statements/alter/column#alter_modify-column).
