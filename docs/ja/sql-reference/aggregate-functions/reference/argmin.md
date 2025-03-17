---
slug: /ja/sql-reference/aggregate-functions/reference/argmin
sidebar_position: 110
---

# argMin

`val` の最小値に対応する `arg` の値を計算します。最大となる `val` が同じ値を持つ複数の行がある場合、返される `arg` は非決定的です。`arg` と `min` の両方は[集計関数](/docs/ja/sql-reference/aggregate-functions/index.md)として動作し、処理中に[`Null` をスキップします](/docs/ja/sql-reference/aggregate-functions/index.md#null-processing) 。 `Null` でない値が利用可能な場合、`Null` でない値を返します。

**構文**

``` sql
argMin(arg, val)
```

**引数**

- `arg` — 引数。
- `val` — 値。

**戻り値**

- 最小の `val` 値に対応する `arg` 値。

型：`arg` の型と一致。

**例**

入力テーブル:

``` text
┌─user─────┬─salary─┐
│ director │   5000 │
│ manager  │   3000 │
│ worker   │   1000 │
└──────────┴────────┘
```

クエリ:

``` sql
SELECT argMin(user, salary) FROM salary
```

結果:

``` text
┌─argMin(user, salary)─┐
│ worker               │
└──────────────────────┘
```

**拡張例**

```sql
CREATE TABLE test
(
    a Nullable(String),
    b Nullable(Int64)
)
ENGINE = Memory AS
SELECT *
FROM VALUES((NULL, 0), ('a', 1), ('b', 2), ('c', 2), (NULL, NULL), ('d', NULL));

select * from test;
┌─a────┬────b─┐
│ ᴺᵁᴸᴸ │    0 │
│ a    │    1 │
│ b    │    2 │
│ c    │    2 │
│ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │
│ d    │ ᴺᵁᴸᴸ │
└──────┴──────┘

SELECT argMin(a, b), min(b) FROM test;
┌─argMin(a, b)─┬─min(b)─┐
│ a            │      0 │ -- argMin = a は最初の `NULL` でない値であり、min(b) は別の行から来ています！
└──────────────┴────────┘

SELECT argMin(tuple(a), b) FROM test;
┌─argMin(tuple(a), b)─┐
│ (NULL)              │ -- `Tuple` に含まれる唯一の `NULL` 値は `NULL` ではないので、集計関数はその `NULL` 値によってその行をスキップしません
└─────────────────────┘

SELECT (argMin((a, b), b) as t).1 argMinA, t.2 argMinB from test;
┌─argMinA─┬─argMinB─┐
│ ᴺᵁᴸᴸ    │       0 │ -- `Tuple` を使用して、最大の b に対応するすべての (すなわち tuple(*)) カラムを取得できます
└─────────┴─────────┘

SELECT argMin(a, b), min(b) FROM test WHERE a IS NULL and b IS NULL;
┌─argMin(a, b)─┬─min(b)─┐
│ ᴺᵁᴸᴸ         │   ᴺᵁᴸᴸ │ -- すべての集約行はフィルターのために少なくとも1つの `NULL` 値を含むため、すべての行がスキップされ、その結果 `NULL` になります
└──────────────┴────────┘

SELECT argMin(a, (b, a)), min(tuple(b, a)) FROM test;
┌─argMin(a, tuple(b, a))─┬─min(tuple(b, a))─┐
│ d                      │ (NULL,NULL)      │ -- 'd' は最小の `NULL` でない値です
└────────────────────────┴──────────────────┘

SELECT argMin((a, b), (b, a)), min(tuple(b, a)) FROM test;
┌─argMin(tuple(a, b), tuple(b, a))─┬─min(tuple(b, a))─┐
│ (NULL,NULL)                      │ (NULL,NULL)      │ -- `Tuple` が `NULL` をスキップしないように許可するため、argMin は (NULL,NULL) を返し、min(tuple(b, a)) はこのデータセットの最小値になります
└──────────────────────────────────┴──────────────────┘

SELECT argMin(a, tuple(b)) FROM test;
┌─argMin(a, tuple(b))─┐
│ d                   │ -- `Tuple` は `NULL` な値を持つ行をスキップしないように `min` に使用できます。
└─────────────────────┘
```

**参照**

- [Tuple](/docs/ja/sql-reference/data-types/tuple.md)
