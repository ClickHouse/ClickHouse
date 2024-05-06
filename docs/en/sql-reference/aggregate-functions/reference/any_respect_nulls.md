---
slug: /en/sql-reference/aggregate-functions/reference/any_respect_nulls
sidebar_position: 103
---

# any_respect_nulls

Selects the first encountered value of a column, irregardless of whether it is a `NULL` value or not.

Alias: `any_value_respect_nulls`, `first_value_repect_nulls`.

**Syntax**

```sql
any_respect_nulls(column)
```

**Parameters**
- `column`: The column name. 

**Returned value**

- The last value encountered, irregardless of whether it is a `NULL` value or not.

**Example**

Query:

```sql
CREATE TABLE any_nulls (city Nullable(String)) ENGINE=Log;

INSERT INTO any_nulls (city) VALUES (NULL), ('Amsterdam'), ('New York'), ('Tokyo'), ('Valencia'), (NULL);

SELECT any(city), any_respect_nulls(city) FROM any_nulls;
```

```response
┌─any(city)─┬─any_respect_nulls(city)─┐
│ Amsterdam │ ᴺᵁᴸᴸ                    │
└───────────┴─────────────────────────┘
```

**See Also**
- [any](../reference/any.md)
