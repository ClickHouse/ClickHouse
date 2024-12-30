---
slug: /ja/sql-reference/aggregate-functions/reference/aggthrow
sidebar_position: 101
---

# aggThrow

この関数は例外の安全性をテストするために使用できます。指定された確率で生成時に例外をスローします。

**構文**

```sql
aggThrow(throw_prob)
```

**引数**

- `throw_prob` — 生成時にスローする確率。[Float64](../../data-types/float.md)。

**戻り値**

- 例外: `Code: 503. DB::Exception: Aggregate function aggThrow has thrown exception successfully`.

**例**

クエリ:

```sql
SELECT number % 2 AS even, aggThrow(number) FROM numbers(10) GROUP BY even;
```

結果:

```response
例外を受信:
Code: 503. DB::Exception: Aggregate function aggThrow has thrown exception successfully: While executing AggregatingTransform. (AGGREGATE_FUNCTION_THROW)
```
