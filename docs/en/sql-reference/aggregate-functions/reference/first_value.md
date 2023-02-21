---
slug: /en/sql-reference/aggregate-functions/reference/first_value
sidebar_position: 7
---

# first_value

Selects the first encountered value, similar to `any`, but could accept NULL.

## examples

```sq;
insert into test_data (a,b) values (1,null), (2,3), (4, 5), (6,null)
```

### example1
The NULL value is ignored at default.
```sql
select first_value(b) from test_data
```

```text
┌─first_value_ignore_nulls(b)─┐
│                           3 │
└─────────────────────────────┘

```

### example2
The NULL value is ignored.
```sql
select first_value(b) ignore nulls sfrom test_data
```

```text
┌─first_value_ignore_nulls(b)─┐
│                           3 │
└─────────────────────────────┘

```

### example3
The NULL value is accepted.
```sql
select first_value(b) respect nulls from test_data
```

```text

┌─first_value_respect_nulls(b)─┐
│                         ᴺᵁᴸᴸ │
└──────────────────────────────┘
```


