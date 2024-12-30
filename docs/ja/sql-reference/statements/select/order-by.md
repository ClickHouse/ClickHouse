---
slug: /ja/sql-reference/statements/select/order-by
sidebar_label: ORDER BY
---

# ORDER BY 句

`ORDER BY` 句は次のいずれかを含みます：

- 式のリスト、例：`ORDER BY visits, search_phrase`、
- `SELECT` 句内のカラムを指す数字のリスト、例：`ORDER BY 2, 1`、または
- `ALL`、これは `SELECT` 句のすべてのカラムを意味します。例：`ORDER BY ALL`。

カラム番号によるソートを無効にするには、設定 [enable_positional_arguments](../../../operations/settings/settings.md#enable-positional-arguments) = 0 を設定します。
`ALL` によるソートを無効にするには、設定 [enable_order_by_all](../../../operations/settings/settings.md#enable-order-by-all) = 0 を設定します。

`ORDER BY` 句にはソートの方向を決定するために `DESC` （降順）または `ASC` （昇順）修飾子が付けられることがあります。
明示的なソート順指定がない限り、デフォルトで `ASC` が使用されます。
ソートの方向は、リスト全体ではなく単一の式に適用されます。例：`ORDER BY Visits DESC, SearchPhrase`。
また、ソートは大文字小文字を区別して行われます。

ソート式の値が同一の行は、任意で非決定論的な順序で返されます。
`SELECT` ステートメントで `ORDER BY` 句が省略された場合、行の順序も任意で非決定論的です。

## 特殊値のソート

`NaN` と `NULL` のソート順には2つのアプローチがあります：

- デフォルトまたは `NULLS LAST` 修飾子を使用した場合：最初に値、その後 `NaN`、最後に `NULL`。
- `NULLS FIRST` 修飾子を使用した場合：最初に `NULL`、その後 `NaN`、最後に他の値。

### 例

以下のテーブルについて：

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    2 │
│ 1 │  nan │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │  nan │
│ 7 │ ᴺᵁᴸᴸ │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

クエリ `SELECT * FROM t_null_nan ORDER BY y NULLS FIRST` を実行すると：

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 7 │ ᴺᵁᴸᴸ │
│ 1 │  nan │
│ 6 │  nan │
│ 2 │    2 │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

浮動小数点数がソートされる際、NaNは他の値とは別に扱われます。ソートの順序にかかわらず、NaNは最後に来ます。つまり、昇順のソートではすべての他の数値よりも大きいかのように扱われ、降順のソートでは残りの数値よりも小さいかのように扱われます。

## 照合サポート

[String](../../../sql-reference/data-types/string.md) 値によるソートの際、照合（比較）を指定できます。例：`ORDER BY SearchPhrase COLLATE 'tr'` - トルコ語アルファベットを使用した大文字小文字を区別しない昇順のキーワードによるソート。`COLLATE` は ORDER BY 内の各式について独立して指定可能です。`ASC` または `DESC` が指定されている場合は、`COLLATE` はその後に指定されます。`COLLATE` を使用する場合、ソートは常に大文字小文字を区別しません。

照合は [LowCardinality](../../../sql-reference/data-types/lowcardinality.md)、[Nullable](../../../sql-reference/data-types/nullable.md)、[Array](../../../sql-reference/data-types/array.md)、および [Tuple](../../../sql-reference/data-types/tuple.md) 内でサポートされています。

通常のバイトによるソートよりも効率が低下するため、わずかな行の最終的なソートにのみ `COLLATE` の使用をお勧めします。

## 照合の例

[String](../../../sql-reference/data-types/string.md) 値のみに関する例：

入力テーブル：

``` text
┌─x─┬─s────┐
│ 1 │ bca  │
│ 2 │ ABC  │
│ 3 │ 123a │
│ 4 │ abc  │
│ 5 │ BCA  │
└───┴──────┘
```

クエリ：

```sql
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

結果：

``` text
┌─x─┬─s────┐
│ 3 │ 123a │
│ 4 │ abc  │
│ 2 │ ABC  │
│ 1 │ bca  │
│ 5 │ BCA  │
└───┴──────┘
```

[Nullable](../../../sql-reference/data-types/nullable.md) を用いた例：

入力テーブル：

``` text
┌─x─┬─s────┐
│ 1 │ bca  │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │ ABC  │
│ 4 │ 123a │
│ 5 │ abc  │
│ 6 │ ᴺᵁᴸᴸ │
│ 7 │ BCA  │
└───┴──────┘
```

クエリ：

```sql
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

結果：

``` text
┌─x─┬─s────┐
│ 4 │ 123a │
│ 5 │ abc  │
│ 3 │ ABC  │
│ 1 │ bca  │
│ 7 │ BCA  │
│ 6 │ ᴺᵁᴸᴸ │
│ 2 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

[Array](../../../sql-reference/data-types/array.md) を用いた例：

入力テーブル：

``` text
┌─x─┬─s─────────────┐
│ 1 │ ['Z']         │
│ 2 │ ['z']         │
│ 3 │ ['a']         │
│ 4 │ ['A']         │
│ 5 │ ['z','a']     │
│ 6 │ ['z','a','a'] │
│ 7 │ ['']          │
└───┴───────────────┘
```

クエリ：

```sql
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

結果：

``` text
┌─x─┬─s─────────────┐
│ 7 │ ['']          │
│ 3 │ ['a']         │
│ 4 │ ['A']         │
│ 2 │ ['z']         │
│ 5 │ ['z','a']     │
│ 6 │ ['z','a','a'] │
│ 1 │ ['Z']         │
└───┴───────────────┘
```

[LowCardinality](../../../sql-reference/data-types/lowcardinality.md) string を用いた例：

入力テーブル：

```text
┌─x─┬─s───┐
│ 1 │ Z   │
│ 2 │ z   │
│ 3 │ a   │
│ 4 │ A   │
│ 5 │ za  │
│ 6 │ zaa │
│ 7 │     │
└───┴─────┘
```

クエリ：

```sql
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

結果：

```text
┌─x─┬─s───┐
│ 7 │     │
│ 3 │ a   │
│ 4 │ A   │
│ 2 │ z   │
│ 1 │ Z   │
│ 5 │ za  │
│ 6 │ zaa │
└───┴─────┘
```

[Tuple](../../../sql-reference/data-types/tuple.md) を用いた例：

```text
┌─x─┬─s───────┐
│ 1 │ (1,'Z') │
│ 2 │ (1,'z') │
│ 3 │ (1,'a') │
│ 4 │ (2,'z') │
│ 5 │ (1,'A') │
│ 6 │ (2,'Z') │
│ 7 │ (2,'A') │
└───┴─────────┘
```

クエリ：

```sql
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

結果：

```text
┌─x─┬─s───────┐
│ 3 │ (1,'a') │
│ 5 │ (1,'A') │
│ 2 │ (1,'z') │
│ 1 │ (1,'Z') │
│ 7 │ (2,'A') │
│ 4 │ (2,'z') │
│ 6 │ (2,'Z') │
└───┴─────────┘
```

## 実装の詳細

小さな [LIMIT](../../../sql-reference/statements/select/limit.md) が `ORDER BY` に追加されている場合、より少ないRAMを使用します。そうでない場合、ソートするデータの量に比例したメモリ量を消費します。分散クエリ処理の場合、[GROUP BY](../../../sql-reference/statements/select/group-by.md) が省略されている場合、ソートはリモートサーバーで部分的に行われ、結果は要求サーバーでマージされます。つまり、分散ソートの際、ソートするデータ量は1台のサーバーのメモリを超えることがあります。

RAMが不足している場合、外部メモリ（ディスク上の一時ファイルの作成）でソートを行うことができます。この目的のために、`max_bytes_before_external_sort` 設定を使用してください。0（デフォルト）に設定されている場合、外部ソートは無効です。有効にされている場合、ソートするデータの量が指定されたバイト数に達すると、収集されたデータがソートされ、一時ファイルに書き込まれます。すべてのデータが読み取られた後、ソートされたすべてのファイルがマージされ、結果が出力されます。ファイルは `/var/lib/clickhouse/tmp/` ディレクトリに書き込まれ（デフォルトで、`tmp_path` パラメータを使用してこの設定を変更することも可能です）。

クエリの実行により、`max_bytes_before_external_sort` よりも多くのメモリを使用することがあります。このため、この設定は `max_memory_usage` よりもかなり小さい値を持たなければなりません。たとえば、サーバーに128GBのRAMがあり、単一のクエリを実行する必要がある場合、`max_memory_usage` を100GBに設定し、`max_bytes_before_external_sort` を80GBに設定します。

外部ソートはRAM内でのソートほど効果的ではありません。

## データ読み取りの最適化

 `ORDER BY` 式にテーブルソートキーと一致するプレフィクスがある場合、[optimize_read_in_order](../../../operations/settings/settings.md#optimize_read_in_order) 設定を使用してクエリを最適化できます。

 `optimize_read_in_order` 設定が有効になっている場合、ClickHouseサーバーはテーブルインデックスを使用して、`ORDER BY` キーの順序でデータを読み取ります。これにより、指定された [LIMIT](../../../sql-reference/statements/select/limit.md) の場合にすべてのデータを読み取ることを避けることができます。そのため、大量のデータに対する小さなリミット付きのクエリがより迅速に処理されます。

この最適化は `ASC` および `DESC` の両方で動作し、[GROUP BY](../../../sql-reference/statements/select/group-by.md) 句や [FINAL](../../../sql-reference/statements/select/from.md#select-from-final) 修飾子と一緒に使用することはできません。

`optimize_read_in_order` 設定が無効な場合、ClickHouseサーバーは `SELECT` クエリを処理する際にテーブルインデックスを使用しません。

`ORDER BY` 句、大きな `LIMIT` と巨額のレコードを読み込む必要がある [WHERE](../../../sql-reference/statements/select/where.md) 条件を持つクエリを実行する場合、`optimize_read_in_order` を手動で無効化することを検討してください。

次のテーブルエンジンで最適化がサポートされています：

- [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md)（[マテリアライズドビュー](../../../sql-reference/statements/create/view.md#materialized-view) を含む）、
- [Merge](../../../engines/table-engines/special/merge.md)、
- [Buffer](../../../engines/table-engines/special/buffer.md)

`MaterializedView` エンジンのテーブルでは、ビューとして `SELECT ... FROM merge_tree_table ORDER BY pk` のような最適化が機能します。ただし、ビュークエリに `ORDER BY` 句がない場合の `SELECT ... FROM view ORDER BY pk` のようなクエリではサポートされていません。

## ORDER BY Expr WITH FILL 修飾子

この修飾子は [LIMIT ... WITH TIES 修飾子](../../../sql-reference/statements/select/limit.md#limit-with-ties) と組み合わせて使用することもできます。

`WITH FILL` 修飾子は、`ORDER BY expr` にオプションの `FROM expr`、`TO expr`、および `STEP expr` パラメータと共に設定できます。
指定の `expr` カラムのすべての欠落値が順次埋められ、他のカラムはデフォルト値で埋められます。

複数のカラムを埋めるには、`ORDER BY` セクションの各フィールド名の後にオプションのパラメータとともに `WITH FILL` 修飾子を追加します。

``` sql
ORDER BY expr [WITH FILL] [FROM const_expr] [TO const_expr] [STEP const_numeric_expr] [STALENESS const_numeric_expr], ... exprN [WITH FILL] [FROM expr] [TO expr] [STEP numeric_expr] [STALENESS numeric_expr]
[INTERPOLATE [(col [AS expr], ... colN [AS exprN])]]
```

`WITH FILL` は、Numeric（すべての種類のfloat、decimal、int）またはDate/DateTime型のフィールドに適用可能です。`String` フィールドに適用される場合、欠落値は空文字列で補充されます。
`FROM const_expr` が定義されていない場合、埋め込みのシーケンスは `ORDER BY` の `expr` フィールドの最小値を使用します。
`TO const_expr` が定義されていない場合、埋め込みのシーケンスは `ORDER BY` の `expr` フィールドの最大値を使用します。
`STEP const_numeric_expr` が定義されていると、`const_numeric_expr` は数値型の場合はそのまま解釈され、Date型では日数、DateTime型では秒数として解釈されます。また、時間や日付の間隔を表す [INTERVAL](https://clickhouse.com/docs/ja/sql-reference/data-types/special-data-types/interval/) データ型もサポートしています。
`STEP const_numeric_expr` が省略されている場合、埋め込みのシーケンスは数値型では `1.0`、Date型では `1 day`、DateTime型では `1 second` を使用します。
`STALENESS const_numeric_expr` が定義されている場合、クエリは元のデータにおける前の行との差が `const_numeric_expr` を超えるまで行を生成します。
`INTERPOLATE` は `ORDER BY WITH FILL` に参加していないカラムに適用可能です。そのようなカラムは、`expr` を適用することで前のフィールド値に基づいて埋められます。`expr` が存在しない場合、前の値を繰り返します。省略されたリストは、すべての許可されたカラムを含む結果となります。

`WITH FILL` を使用しないクエリの例：

``` sql
SELECT n, source FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n;
```

結果：

``` text
┌─n─┬─source───┐
│ 1 │ original │
│ 4 │ original │
│ 7 │ original │
└───┴──────────┘
```

`WITH FILL` 修飾子を適用した後の同じクエリ：

``` sql
SELECT n, source FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5;
```

結果：

``` text
┌───n─┬─source───┐
│   0 │          │
│ 0.5 │          │
│   1 │ original │
│ 1.5 │          │
│   2 │          │
│ 2.5 │          │
│   3 │          │
│ 3.5 │          │
│   4 │ original │
│ 4.5 │          │
│   5 │          │
│ 5.5 │          │
│   7 │ original │
└─────┴──────────┘
```

複数フィールドの場合 `ORDER BY field2 WITH FILL, field1 WITH FILL` と記述すると、埋め込みの順序は `ORDER BY` 句内のフィールドの順序に従います。

例：

``` sql
SELECT
    toDate((number * 10) * 86400) AS d1,
    toDate(number * 86400) AS d2,
    'original' AS source
FROM numbers(10)
WHERE (number % 3) = 1
ORDER BY
    d2 WITH FILL,
    d1 WITH FILL STEP 5;
```

結果：

``` text
┌───d1───────┬───d2───────┬─source───┐
│ 1970-01-11 │ 1970-01-02 │ original │
│ 1970-01-01 │ 1970-01-03 │          │
│ 1970-01-01 │ 1970-01-04 │          │
│ 1970-02-10 │ 1970-01-05 │ original │
│ 1970-01-01 │ 1970-01-06 │          │
│ 1970-01-01 │ 1970-01-07 │          │
│ 1970-03-12 │ 1970-01-08 │ original │
└────────────┴────────────┴──────────┘
```

フィールド `d1` は埋められず、デフォルト値を使用します。なぜなら `d2` 値に対する反復値がないため、シーケンスを `d1` に対して適切に計算することはできません。

次のクエリでは `ORDER BY` 内のフィールドが変更されています：

``` sql
SELECT
    toDate((number * 10) * 86400) AS d1,
    toDate(number * 86400) AS d2,
    'original' AS source
FROM numbers(10)
WHERE (number % 3) = 1
ORDER BY
    d1 WITH FILL STEP 5,
    d2 WITH FILL;
```

結果：

``` text
┌───d1───────┬───d2───────┬─source───┐
│ 1970-01-11 │ 1970-01-02 │ original │
│ 1970-01-16 │ 1970-01-01 │          │
│ 1970-01-21 │ 1970-01-01 │          │
│ 1970-01-26 │ 1970-01-01 │          │
│ 1970-01-31 │ 1970-01-01 │          │
│ 1970-02-05 │ 1970-01-01 │          │
│ 1970-02-10 │ 1970-01-05 │ original │
│ 1970-02-15 │ 1970-01-01 │          │
│ 1970-02-20 │ 1970-01-01 │          │
│ 1970-02-25 │ 1970-01-01 │          │
│ 1970-03-02 │ 1970-01-01 │          │
│ 1970-03-07 │ 1970-01-01 │          │
│ 1970-03-12 │ 1970-01-08 │ original │
└────────────┴────────────┴──────────┘
```

次のクエリは、カラム `d1` に1日あたりデータを埋めるために `INTERVAL` データ型を使用しています：

``` sql
SELECT
    toDate((number * 10) * 86400) AS d1,
    toDate(number * 86400) AS d2,
    'original' AS source
FROM numbers(10)
WHERE (number % 3) = 1
ORDER BY
    d1 WITH FILL STEP INTERVAL 1 DAY,
    d2 WITH FILL;
```

結果：
```
┌─────────d1─┬─────────d2─┬─source───┐
│ 1970-01-11 │ 1970-01-02 │ original │
│ 1970-01-12 │ 1970-01-01 │          │
│ 1970-01-13 │ 1970-01-01 │          │
│ 1970-01-14 │ 1970-01-01 │          │
│ 1970-01-15 │ 1970-01-01 │          │
│ 1970-01-16 │ 1970-01-01 │          │
│ 1970-01-17 │ 1970-01-01 │          │
│ 1970-01-18 │ 1970-01-01 │          │
│ 1970-01-19 │ 1970-01-01 │          │
│ 1970-01-20 │ 1970-01-01 │          │
│ 1970-01-21 │ 1970-01-01 │          │
│ 1970-01-22 │ 1970-01-01 │          │
│ 1970-01-23 │ 1970-01-01 │          │
│ 1970-01-24 │ 1970-01-01 │          │
│ 1970-01-25 │ 1970-01-01 │          │
│ 1970-01-26 │ 1970-01-01 │          │
│ 1970-01-27 │ 1970-01-01 │          │
│ 1970-01-28 │ 1970-01-01 │          │
│ 1970-01-29 │ 1970-01-01 │          │
│ 1970-01-30 │ 1970-01-01 │          │
│ 1970-01-31 │ 1970-01-01 │          │
│ 1970-02-01 │ 1970-01-01 │          │
│ 1970-02-02 │ 1970-01-01 │          │
│ 1970-02-03 │ 1970-01-01 │          │
│ 1970-02-04 │ 1970-01-01 │          │
│ 1970-02-05 │ 1970-01-01 │          │
│ 1970-02-06 │ 1970-01-01 │          │
│ 1970-02-07 │ 1970-01-01 │          │
│ 1970-02-08 │ 1970-01-01 │          │
│ 1970-02-09 │ 1970-01-01 │          │
│ 1970-02-10 │ 1970-01-05 │ original │
│ 1970-02-11 │ 1970-01-01 │          │
│ 1970-02-12 │ 1970-01-01 │          │
│ 1970-02-13 │ 1970-01-01 │          │
│ 1970-02-14 │ 1970-01-01 │          │
│ 1970-02-15 │ 1970-01-01 │          │
│ 1970-02-16 │ 1970-01-01 │          │
│ 1970-02-17 │ 1970-01-01 │          │
│ 1970-02-18 │ 1970-01-01 │          │
│ 1970-02-19 │ 1970-01-01 │          │
│ 1970-02-20 │ 1970-01-01 │          │
│ 1970-02-21 │ 1970-01-01 │          │
│ 1970-02-22 │ 1970-01-01 │          │
│ 1970-02-23 │ 1970-01-01 │          │
│ 1970-02-24 │ 1970-01-01 │          │
│ 1970-02-25 │ 1970-01-01 │          │
│ 1970-02-26 │ 1970-01-01 │          │
│ 1970-02-27 │ 1970-01-01 │          │
│ 1970-02-28 │ 1970-01-01 │          │
│ 1970-03-01 │ 1970-01-01 │          │
│ 1970-03-02 │ 1970-01-01 │          │
│ 1970-03-03 │ 1970-01-01 │          │
│ 1970-03-04 │ 1970-01-01 │          │
│ 1970-03-05 │ 1970-01-01 │          │
│ 1970-03-06 │ 1970-01-01 │          │
│ 1970-03-07 │ 1970-01-01 │          │
│ 1970-03-08 │ 1970-01-01 │          │
│ 1970-03-09 │ 1970-01-01 │          │
│ 1970-03-10 │ 1970-01-01 │          │
│ 1970-03-11 │ 1970-01-01 │          │
│ 1970-03-12 │ 1970-01-08 │ original │
└────────────┴────────────┴──────────┘
```

`STALENESS` を使用しないクエリの例：

``` sql
SELECT number as key, 5 * number value, 'original' AS source
FROM numbers(16) WHERE key % 5 == 0
ORDER BY key WITH FILL;
```

結果：

``` text
    ┌─key─┬─value─┬─source───┐
 1. │   0 │     0 │ original │
 2. │   1 │     0 │          │
 3. │   2 │     0 │          │
 4. │   3 │     0 │          │
 5. │   4 │     0 │          │
 6. │   5 │    25 │ original │
 7. │   6 │     0 │          │
 8. │   7 │     0 │          │
 9. │   8 │     0 │          │
10. │   9 │     0 │          │
11. │  10 │    50 │ original │
12. │  11 │     0 │          │
13. │  12 │     0 │          │
14. │  13 │     0 │          │
15. │  14 │     0 │          │
16. │  15 │    75 │ original │
    └─────┴───────┴──────────┘
```

`STALENESS 3` を適用した同じクエリ：

``` sql
SELECT number as key, 5 * number value, 'original' AS source
FROM numbers(16) WHERE key % 5 == 0
ORDER BY key WITH FILL STALENESS 3;
```

結果：

``` text
    ┌─key─┬─value─┬─source───┐
 1. │   0 │     0 │ original │
 2. │   1 │     0 │          │
 3. │   2 │     0 │          │
 4. │   5 │    25 │ original │
 5. │   6 │     0 │          │
 6. │   7 │     0 │          │
 7. │  10 │    50 │ original │
 8. │  11 │     0 │          │
 9. │  12 │     0 │          │
10. │  15 │    75 │ original │
11. │  16 │     0 │          │
12. │  17 │     0 │          │
    └─────┴───────┴──────────┘
```

`INTERPOLATE` を使用しないクエリの例：

``` sql
SELECT n, source, inter FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source, number as inter
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5;
```

結果：

``` text
┌───n─┬─source───┬─inter─┐
│   0 │          │     0 │
│ 0.5 │          │     0 │
│   1 │ original │     1 │
│ 1.5 │          │     0 │
│   2 │          │     0 │
│ 2.5 │          │     0 │
│   3 │          │     0 │
│ 3.5 │          │     0 │
│   4 │ original │     4 │
│ 4.5 │          │     0 │
│   5 │          │     0 │
│ 5.5 │          │     0 │
│   7 │ original │     7 │
└─────┴──────────┴───────┘
```

`INTERPOLATE` を適用した同じクエリ：

``` sql
SELECT n, source, inter FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source, number as inter
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5 INTERPOLATE (inter AS inter + 1);
```

結果：

``` text
┌───n─┬─source───┬─inter─┐
│   0 │          │     0 │
│ 0.5 │          │     0 │
│   1 │ original │     1 │
│ 1.5 │          │     2 │
│   2 │          │     3 │
│ 2.5 │          │     4 │
│   3 │          │     5 │
│ 3.5 │          │     6 │
│   4 │ original │     4 │
│ 4.5 │          │     5 │
│   5 │          │     6 │
│ 5.5 │          │     7 │
│   7 │ original │     7 │
└─────┴──────────┴───────┘
```

## ソートプレフィックスでグループ化されたフィルの充填

特定のカラムで同一の値を持つ行を独立して埋めることは有用です。 - 良い例は時系列データの欠落値の埋め込みです。
以下のような時系列テーブルがあるとします：

``` sql
CREATE TABLE timeseries
(
    `sensor_id` UInt64,
    `timestamp` DateTime64(3, 'UTC'),
    `value` Float64
)
ENGINE = Memory;

SELECT * FROM timeseries;

┌─sensor_id─┬───────────────timestamp─┬─value─┐
│       234 │ 2021-12-01 00:00:03.000 │     3 │
│       432 │ 2021-12-01 00:00:01.000 │     1 │
│       234 │ 2021-12-01 00:00:07.000 │     7 │
│       432 │ 2021-12-01 00:00:05.000 │     5 │
└───────────┴─────────────────────────┴───────┘
```

そして、1秒間隔で各センサーごとに欠落値を埋めたいとします。
達成する方法は `sensor_id` カラムを `timestamp` カラムを填埋するためのソートプレフィックスとして使用することです。

```
SELECT *
FROM timeseries
ORDER BY
    sensor_id,
    timestamp WITH FILL
INTERPOLATE ( value AS 9999 )

┌─sensor_id─┬───────────────timestamp─┬─value─┐
│       234 │ 2021-12-01 00:00:03.000 │     3 │
│       234 │ 2021-12-01 00:00:04.000 │  9999 │
│       234 │ 2021-12-01 00:00:05.000 │  9999 │
│       234 │ 2021-12-01 00:00:06.000 │  9999 │
│       234 │ 2021-12-01 00:00:07.000 │     7 │
│       432 │ 2021-12-01 00:00:01.000 │     1 │
│       432 │ 2021-12-01 00:00:02.000 │  9999 │
│       432 │ 2021-12-01 00:00:03.000 │  9999 │
│       432 │ 2021-12-01 00:00:04.000 │  9999 │
│       432 │ 2021-12-01 00:00:05.000 │     5 │
└───────────┴─────────────────────────┴───────┘
```

ここで、`value` カラムは埋められた行をより目立たせるために `9999` で補間されました。
この動作は設定 `use_with_fill_by_sorting_prefix` によって制御されます（デフォルトで有効）。

## 関連コンテンツ

- ブログ: [ClickHouseにおける時系列データの操作](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
