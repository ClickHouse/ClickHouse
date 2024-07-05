---
slug: /en/sql-reference/aggregate-functions/reference/anylast
sidebar_position: 105
---

# anyLast

Selects the last value encountered. The result is just as indeterminate as for the [any](../../../sql-reference/aggregate-functions/reference/any.md) function.

**Syntax**

```sql
anyLast(column)
```

**Parameters**
- `column`: The column name. 

**Returned value**

- The last value encountered.

**Example**

Query:

```sql
CREATE TABLE any_last_nulls (city Nullable(String)) ENGINE=Log;

INSERT INTO any_last_nulls (city) VALUES ('Amsterdam'),(NULL),('New York'),('Tokyo'),('Valencia'),(NULL);

SELECT anyLast(city) FROM any_last_nulls;
```

```response
┌─anyLast(city)─┐
│ Valencia      │
└───────────────┘
```