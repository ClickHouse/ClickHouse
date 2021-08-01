---
toc_priority: 49
toc_title: EXCHANGE
---

# EXCHANGE Statement {#exchange}

Exchanges names of two tables or dictionaries in an atomic query.

!!! note "Note"
    An `EXCHANGE` query is supported by [Atomic](../../engines/database-engines/atomic.md) database engine only.

**Syntax**

```sql
EXCHANGE TABLES|DICTIONARIES [db0.]name_A AND [db1.]name_B
```

## EXCHANGE TABLES {#exchange_tables}

Exchanges names of two tables in an atomic query.

**Syntax**

```sql
EXCHANGE TABLES [db0.]table_A AND [db1.]table_B
```

## EXCHANGE DICTIONARIES {#exchange_dictionaries}

Exchanges names of two dictionaries in an atomic query.

**Syntax**

```sql
EXCHANGE DICTIONARIES [db0.]dict_A AND [db1.]dict_B
```

**See Also**

-   [Dictionaries](../../sql-reference/dictionaries/index.md)
