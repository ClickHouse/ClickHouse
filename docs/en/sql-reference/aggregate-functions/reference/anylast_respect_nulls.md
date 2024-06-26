---
slug: /en/sql-reference/aggregate-functions/reference/anylast_respect_nulls
sidebar_position: 106
---

# anyLast_respect_nulls

Selects the last value encountered, irregardless of whether it is `NULL` or not.

**Syntax**

```sql
anyLast_respect_nulls(column)
```

**Parameters**
- `column`: The column name. 

**Returned value**

- The last value encountered, irregardless of whether it is `NULL` or not.

**Example**

Query:

```sql
CREATE TABLE any_last_nulls (city Nullable(String)) ENGINE=Log;

INSERT INTO any_last_nulls (city) VALUES ('Amsterdam'),(NULL),('New York'),('Tokyo'),('Valencia'),(NULL);

SELECT anyLast(city), anyLast_respect_nulls(city) FROM any_last_nulls;
```

```response
┌─anyLast(city)─┬─anyLast_respect_nulls(city)─┐
│ Valencia      │ ᴺᵁᴸᴸ                        │
└───────────────┴─────────────────────────────┘
```