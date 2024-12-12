---
slug: /ja/sql-reference/aggregate-functions/reference/count
sidebar_position: 120
---

# count

行数またはNULL以外の値を数えます。

ClickHouseは`count`に対して以下の構文をサポートしています:

- `count(expr)` または `COUNT(DISTINCT expr)`。
- `count()` または `COUNT(*)`。`count()`構文はClickHouse固有のものです。

**引数**

この関数は以下を受け取れます:

- パラメータを一つも取らない。
- 一つの[式](../../../sql-reference/syntax.md#syntax-expressions)。

**戻り値**

- 関数がパラメータなしで呼び出された場合、行数を数えます。
- [式](../../../sql-reference/syntax.md#syntax-expressions)が渡された場合、その式がNULL以外を返した回数を数えます。式が[Nullable](../../../sql-reference/data-types/nullable.md)型の値を返す場合でも、`count`の結果は`Nullable`にはなりません。式がすべての行で`NULL`を返した場合、関数は0を返します。

いずれの場合も、戻り値の型は[UInt64](../../../sql-reference/data-types/int-uint.md)です。

**詳細**

ClickHouseは`COUNT(DISTINCT …)`構文をサポートします。この構文の動作は、[count_distinct_implementation](../../../operations/settings/settings.md#count_distinct_implementation)設定によって決まります。これは、操作を実行するために使用される[uniq\*](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq)関数を定義します。デフォルトは[uniqExact](../../../sql-reference/aggregate-functions/reference/uniqexact.md#agg_function-uniqexact)関数です。

`SELECT count() FROM table`クエリは、デフォルトでMergeTreeのメタデータを使用して最適化されます。行レベルのセキュリティを使用する必要がある場合、[optimize_trivial_count_query](../../../operations/settings/settings.md#optimize-trivial-count-query)設定を使用して最適化を無効にします。

ただし、`SELECT count(nullable_column) FROM table`クエリは、[optimize_functions_to_subcolumns](../../../operations/settings/settings.md#optimize-functions-to-subcolumns)設定を有効にすることで最適化できます。`optimize_functions_to_subcolumns = 1`の設定では、全体のカラムデータを読み込んで処理するのではなく、[null](../../../sql-reference/data-types/nullable.md#finding-null)サブカラムのみを読み取ります。このクエリは`SELECT count(n) FROM table`から`SELECT sum(NOT n.null) FROM table`に変換されます。

**COUNT(DISTINCT expr)のパフォーマンス向上**

`COUNT(DISTINCT expr)`クエリが遅い場合、並列化を改善するために[`GROUP BY`](../../../sql-reference/statements/select/group-by.md)句を追加することを検討してください。また、[projection](../../../sql-reference/statements/alter/projection.md)を使用して、`COUNT(DISTINCT target_col)`と一緒に使用する対象カラムにインデックスを作成することもできます。

**例**

例1:

``` sql
SELECT count() FROM t
```

``` text
┌─count()─┐
│       5 │
└─────────┘
```

例2:

``` sql
SELECT name, value FROM system.settings WHERE name = 'count_distinct_implementation'
```

``` text
┌─name──────────────────────────┬─value─────┐
│ count_distinct_implementation │ uniqExact │
└───────────────────────────────┴───────────┘
```

``` sql
SELECT count(DISTINCT num) FROM t
```

``` text
┌─uniqExact(num)─┐
│              3 │
└────────────────┘
```

この例は、`count(DISTINCT num)`が`count_distinct_implementation`設定の値に従って`uniqExact`関数によって実行されることを示しています。
