---
slug: /ja/sql-reference/statements/select/where
sidebar_label: WHERE
---

# WHERE 句

`WHERE` 句は、`SELECT` の [FROM](../../../sql-reference/statements/select/from.md) 句から取得したデータをフィルタリングするために使用します。

`WHERE` 句がある場合、`UInt8` 型の式を含める必要があります。これは通常、比較および論理演算子を使用した式です。この式が `0` と評価される行は、以降の変換や結果から除外されます。

`WHERE` 式は、基になるテーブルエンジンがインデックスやパーティションプルーニングをサポートしている場合、その能力を使用するために評価されます。

:::note    
[PREWHERE](../../../sql-reference/statements/select/prewhere.md) と呼ばれるフィルタリングの最適化があります。
:::

[NULL](../../../sql-reference/syntax.md#null-literal) をテストする必要がある場合は、[IS NULL](../../operators/index.md#operator-is-null) や [IS NOT NULL](../../operators/index.md#is-not-null) 演算子、または [isNull](../../../sql-reference/functions/functions-for-nulls.md#isnull) と [isNotNull](../../../sql-reference/functions/functions-for-nulls.md#isnotnull) 関数を使用してください。そうでないと、`NULL` を含む式は決して通過しません。

**例**

3 の倍数であり、かつ 10 より大きい数を見つけるには、[numbers テーブル](../../../sql-reference/table-functions/numbers.md)で次のクエリを実行します:

``` sql
SELECT number FROM numbers(20) WHERE (number > 10) AND (number % 3 == 0);
```

結果:

``` text
┌─number─┐
│     12 │
│     15 │
│     18 │
└────────┘
```

`NULL` 値を含むクエリ:

``` sql
CREATE TABLE t_null(x Int8, y Nullable(Int8)) ENGINE=MergeTree() ORDER BY x;
INSERT INTO t_null VALUES (1, NULL), (2, 3);

SELECT * FROM t_null WHERE y IS NULL;
SELECT * FROM t_null WHERE y != 0;
```

結果:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
┌─x─┬─y─┐
│ 2 │ 3 │
└───┴───┘
```
