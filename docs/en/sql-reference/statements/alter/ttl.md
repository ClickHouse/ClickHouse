---
toc_priority: 44
toc_title: TTL
---

# Manipulations with Table TTL {#manipulations-with-table-ttl}

You can change [table TTL](../../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) with a request of the following form:

``` sql
ALTER TABLE table_name MODIFY TTL ttl_expression;
```

## ALTER TABLE REMOVE TTL

Removes ttl-property from the specified column.

Syntax:

```sql
ALTER TABLE table_name MODIFY column_name REMOVE TTL 
```

**Example**

Request

```sql
ALTER TABLE r_no_prop_table MODIFY COLUMN some_column REMOVE TTL;
```

Result

```text

```

### See Also

- More about the [TTL-expression](../../sql-reference/statements/create/table/#ttl-expression).
- Modify column [with TTL](../../sql-reference/statements/alter/column/#alter_modify-column).
