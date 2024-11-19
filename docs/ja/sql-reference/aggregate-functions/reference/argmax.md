---
slug: /ja/sql-reference/aggregate-functions/reference/argmax
sidebar_position: 109
---

# argMax

`val` の最大値に対応する `arg` の値を計算します。同じ `val` で最大値を持つ複数の行がある場合、どの関連する `arg` が返されるかは決定的ではありません。`arg` と `max` の両方の部分は[集約関数](/docs/ja/sql-reference/aggregate-functions/index.md)として動作し、処理中に[`Null` をスキップします](/docs/ja/sql-reference/aggregate-functions/index.md#null-processing)が、`Null` 値が利用可能でない場合は `Null` 以外の値を返します。

**構文**

``` sql
argMax(arg, val)
```

**引数**

- `arg` — 引数。
- `val` — 値。

**返される値**

- 最大の `val` に対応する `arg` の値。

型: `arg` の型に一致します。

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
SELECT argMax(user, salary) FROM salary;
```

結果:

``` text
┌─argMax(user, salary)─┐
│ director             │
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
FROM VALUES(('a', 1), ('b', 2), ('c', 2), (NULL, 3), (NULL, NULL), ('d', NULL));

select * from test;
┌─a────┬────b─┐
│ a    │    1 │
│ b    │    2 │
│ c    │    2 │
│ ᴺᵁᴸᴸ │    3 │
│ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │
│ d    │ ᴺᵁᴸᴸ │
└──────┴──────┘

SELECT argMax(a, b), max(b) FROM test;
┌─argMax(a, b)─┬─max(b)─┐
│ b            │      3 │ -- argMax = 'b' は Null 以外の最初の値だから、max(b) は別の行からです！
└──────────────┴────────┘

SELECT argMax(tuple(a), b) FROM test;
┌─argMax(tuple(a), b)─┐
│ (NULL)              │ -- `Tuple` 内に唯一 `NULL` を含む場合、`NULL` にはならないので、集約関数はその `NULL` が原因で行をスキップしません
└─────────────────────┘

SELECT (argMax((a, b), b) as t).1 argMaxA, t.2 argMaxB FROM test;
┌─argMaxA─┬─argMaxB─┐
│ ᴺᵁᴸᴸ    │       3 │ -- タプルを使用して max(b) に対応する両方の (すべての - tuple(*)) カラムを取得できます
└─────────┴─────────┘

SELECT argMax(a, b), max(b) FROM test WHERE a IS NULL AND b IS NULL;
┌─argMax(a, b)─┬─max(b)─┐
│ ᴺᵁᴸᴸ         │   ᴺᵁᴸᴸ │ -- フィルターにより集計されたすべての行に少なくとも1つの `NULL` が含まれ、そのためすべての行がスキップされ、結果は `NULL` になります
└──────────────┴────────┘

SELECT argMax(a, (b,a)) FROM test;
┌─argMax(a, tuple(b, a))─┐
│ c                      │ -- b=2 の行が2つあり、`Max` に `Tuple` を使用することで最初の `arg` 以外を取得できます
└────────────────────────┘

SELECT argMax(a, tuple(b)) FROM test;
┌─argMax(a, tuple(b))─┐
│ b                   │ -- `Tuple` を `Max` で使用して、`Max` の中で Null をスキップしないようにできます
└─────────────────────┘
```

**関連項目**

- [Tuple](/docs/ja/sql-reference/data-types/tuple.md)
