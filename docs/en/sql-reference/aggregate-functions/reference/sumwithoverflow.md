---
slug: /en/sql-reference/aggregate-functions/reference/sumwithoverflow
sidebar_position: 200
---

# sumWithOverflow

Computes the sum of the numbers, using the same data type for the result as for the input parameters. If the sum exceeds the maximum value for this data type, it is calculated with overflow.

Only works for numbers.

**Syntax**

```sql
sumWithOverflow(num)
```

**Parameters**
- `num`: Column of numeric values. [(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal*](../../data-types/decimal.md).

**Returned value**

- The sum of the values. [(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal*](../../data-types/decimal.md).

**Example**

First we create a table `employees` and insert some fictional employee data into it. For this example we will select `salary` as `UInt16` such that a sum of these values may produce an overflow.

Query:

```sql
CREATE TABLE employees
(
    `id` UInt32,
    `name` String,
    `monthly_salary` UInt16
)
ENGINE = Log
```

```sql
SELECT
    sum(monthly_salary) AS no_overflow,
    sumWithOverflow(monthly_salary) AS overflow,
    toTypeName(no_overflow),
    toTypeName(overflow)
FROM employees
```

We query for the total amount of the employee salaries using the `sum` and `sumWithOverflow` functions and show their types using the `toTypeName` function.
For the `sum` function the resulting type is `UInt64`, big enough to contain the sum, whilst for `sumWithOverflow` the resulting type remains as `UInt16`.  

Query:

```sql
SELECT 
    sum(monthly_salary) AS no_overflow,
    sumWithOverflow(monthly_salary) AS overflow,
    toTypeName(no_overflow),
    toTypeName(overflow),    
FROM employees;
```

Result:


```response
   ┌─no_overflow─┬─overflow─┬─toTypeName(no_overflow)─┬─toTypeName(overflow)─┐
1. │      118700 │    53164 │ UInt64                  │ UInt16               │
   └─────────────┴──────────┴─────────────────────────┴──────────────────────┘
```