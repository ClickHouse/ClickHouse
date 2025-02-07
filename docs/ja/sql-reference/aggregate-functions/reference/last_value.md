---
slug: /ja/sql-reference/aggregate-functions/reference/last_value
sidebar_position: 160
---

# last_value

`anyLast` に似た最後に遭遇した値を選択しますが、NULL を受け入れることができます。
主に[ウィンドウ関数](../../window-functions/index.md)と共に使用されます。
ウィンドウ関数を使用しない場合、ソースストリームが順序付けされていない場合、結果はランダムになります。

## 例

```sql
CREATE TABLE test_data
(
    a Int64,
    b Nullable(Int64)
)
ENGINE = Memory;

INSERT INTO test_data (a, b) Values (1,null), (2,3), (4, 5), (6,null)
```

### 例1
デフォルトで NULL 値は無視されます。
```sql
select last_value(b) from test_data
```

```text
┌─last_value_ignore_nulls(b)─┐
│                          5 │
└────────────────────────────┘
```

### 例2
NULL 値が無視されます。
```sql
select last_value(b) ignore nulls from test_data
```

```text
┌─last_value_ignore_nulls(b)─┐
│                          5 │
└────────────────────────────┘
```

### 例3
NULL 値が受け入れられます。
```sql
select last_value(b) respect nulls from test_data
```

```text
┌─last_value_respect_nulls(b)─┐
│                        ᴺᵁᴸᴸ │
└─────────────────────────────┘
```

### 例4
`ORDER BY` を使ったサブクエリによる安定化した結果。
```sql
SELECT
    last_value_respect_nulls(b),
    last_value(b)
FROM
(
    SELECT *
    FROM test_data
    ORDER BY a ASC
)
```

```text
┌─last_value_respect_nulls(b)─┬─last_value(b)─┐
│                        ᴺᵁᴸᴸ │             5 │
└─────────────────────────────┴───────────────┘
```
