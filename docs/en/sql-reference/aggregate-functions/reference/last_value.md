---
slug: /en/sql-reference/aggregate-functions/reference/last_value
sidebar_position: 8
---

# first_value

Selects the last encountered value, similar to `anyLast`, but could accept NULL.


## examples

```sql
insert into test_data (a,b) values (1,null), (2,3), (4, 5), (6,null)
```

### example1
The NULL value is ignored at default.
```sql
select last_value(b) from test_data
```

```text
┌─last_value_ignore_nulls(b)─┐
│                          5 │
└────────────────────────────┘
```

### example2
The NULL value is ignored.
```sql
select last_value(b) ignore nulls from test_data
```

```text
┌─last_value_ignore_nulls(b)─┐
│                          5 │
└────────────────────────────┘
```

### example3
The NULL value is accepted.
```sql
select last_value(b) respect nulls from test_data
```

```text
┌─last_value_respect_nulls(b)─┐
│                        ᴺᵁᴸᴸ │
└─────────────────────────────┘
```


