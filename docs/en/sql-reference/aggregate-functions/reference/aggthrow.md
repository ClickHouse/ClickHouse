---
slug: /en/sql-reference/aggregate-functions/reference/aggthrow
sidebar_position: 101
---

# aggThrow

This function can be used for the purpose of testing exception safety. It will throw an exception on creation with the specified probability.

**Syntax**

```sql
aggThrow(throw_prob)
```

**Arguments**

- `throw_prob` — Probability to throw on creation. [Float64](../../data-types/float.md).

**Returned value**

- An exception: `Code: 503. DB::Exception: Aggregate function aggThrow has thrown exception successfully`.

**Example**

Query:

```sql
SELECT number % 2 AS even, aggThrow(number) FROM numbers(10) GROUP BY even;
```

Result:

```response
Received exception:
Code: 503. DB::Exception: Aggregate function aggThrow has thrown exception successfully: While executing AggregatingTransform. (AGGREGATE_FUNCTION_THROW)
```
