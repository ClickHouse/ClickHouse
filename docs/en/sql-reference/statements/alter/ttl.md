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

## REMOVE TTL {remove-ttl}

Removes TTL-property from the specified column.

Syntax:

```sql
ALTER TABLE table_name MODIFY column_name REMOVE TTL 
```

**Example**

<!--Попробовать: 
посмотреть установленные TTL, 
показать состояние таблицы до, 
удалить TTL 
и показать состояние после.-->

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
