---
toc_priority: 38
toc_title: FUNCTION
---

# CREATE FUNCTION {#create-function}

Creates a user defined function from a lambda expression.

**Syntax**

```sql
CREATE FUNCTION {function_name} as ({parameters}) -> {function core}
```

Errors are caused by

-   Creating a function with a name that already exists;
-   Recursive function.
-   An identifier not listed in function parameters is used inside a function.

**Example**


Query:

```sql
CREATE FUNCTION plus_one as (a) -> a + 1;
SELECT number, plus_one(number) FROM numbers(3);
```

Result:

``` text
┌─number─┬─plus_one─┐
│      0 │        1 │
│      1 │        2 │
│      2 │        3 │
└────────┴──────────┘
```

-   [DROP](../../../sql-reference/statements/drop.md#drop-function)
