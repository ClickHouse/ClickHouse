---
slug: /en/sql-reference/aggregate-functions/reference/sum
sidebar_position: 195
---

# sum

Calculates the sum. Only works for numbers.

**Syntax**

```sql
sum(num)
```

**Parameters**
- `num`: Column of numeric values. [(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal*](../../data-types/decimal.md).

**Returned value**

- The sum of the values. [(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal*](../../data-types/decimal.md).

**Example**

First we create a table `employees` and insert some fictional employee data into it.

Query:

```sql
CREATE TABLE employees
(
    `id` UInt32,
    `name` String,
    `salary` UInt32
)
ENGINE = Log
```

```sql
INSERT INTO employees VALUES
    (87432, 'John Smith', 45680),
    (59018, 'Jane Smith', 72350),
    (20376, 'Ivan Ivanovich', 58900),
    (71245, 'Anastasia Ivanovna', 89210);
```

We query for the total amount of the employee salaries using the `sum` function. 

Query:

```sql
SELECT sum(salary) FROM employees;
```

Result:


```response
   ┌─sum(salary)─┐
1. │      266140 │
   └─────────────┘
```
