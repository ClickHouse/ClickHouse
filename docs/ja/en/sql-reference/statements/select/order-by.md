---
description: 'ORDER BY 句のドキュメント'
sidebar_label: 'ORDER BY'
slug: /sql-reference/statements/select/order-by
title: 'ORDER BY 句'
doc_type: 'reference'
---

`ORDER BY` 句には、次のいずれかを指定できます。

* 式のリスト (例: `ORDER BY visits, search_phrase`)
* `SELECT` 句内のカラムを参照する番号のリスト (例: `ORDER BY 2, 1`)
* `SELECT` 句のすべてのカラムを意味する `ALL` (例: `ORDER BY ALL`)

カラム番号によるソートを無効にするには、設定 [enable&#95;positional&#95;arguments](/ja/operations/settings/settings#enable_positional_arguments) = 0 を指定します。
`ALL` によるソートを無効にするには、設定 [enable&#95;order&#95;by&#95;all](/ja/operations/settings/settings#enable_order_by_all) = 0 を指定します。

`ORDER BY` 句には、ソート方向を決定する `DESC` (降順) または `ASC` (昇順) の 修飾子 を付けることができます。
明示的にソート順を指定しない場合、デフォルトでは `ASC` が使用されます。
ソート方向はリスト全体ではなく、個々の式に適用されます。例: `ORDER BY Visits DESC, SearchPhrase`
また、ソートでは大文字と小文字が区別されます。

ソート式の値が同一の行は、任意かつ非決定論的な順序で返されます。
`SELECT` ステートメントで `ORDER BY` 句を省略した場合も、行の順序は任意かつ非決定論的です。

<div id="sorting-of-special-values">
  ## 特殊な値のソート
</div>

`NaN` と `NULL` のソート順には、2 つの方式があります。

* デフォルト、または `NULLS LAST` 修飾子を使用する場合: まず値、次に `NaN`、最後に `NULL`。
* `NULLS FIRST` 修飾子を使用する場合: まず `NULL`、次に `NaN`、最後にその他の値。

<div id="example">
  ### 例
</div>

このテーブルでは

```text
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

クエリ `SELECT * FROM t_null_nan ORDER BY y NULLS FIRST` を実行すると、次の結果が表示されます。

```text
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

浮動小数点数をソートする場合、NaN は他の値とは区別して扱われます。ソート順にかかわらず、NaN は末尾に配置されます。つまり、昇順ソートでは他のすべての数値より大きいものとして扱われ、降順ソートでは残りの数値より小さいものとして扱われます。

<div id="collation-support">
  ## Collation のサポート
</div>

[String](../../../sql-reference/data-types/string.md) 型の値をソートする場合は、照合順序 (collation) を指定できます。例: `ORDER BY SearchPhrase COLLATE 'tr'` - 文字列が UTF-8 でエンコードされていることを前提に、トルコ語アルファベットを使用して、キーワードを大文字と小文字を区別せず昇順にソートします。`COLLATE` は、ORDER BY 内の各 expression ごとに個別に指定することも、省略することもできます。`ASC` または `DESC` を指定する場合は、その後に `COLLATE` を指定します。`COLLATE` を使用すると、ソートでは常に大文字と小文字が区別されません。

Collate は [LowCardinality](../../../sql-reference/data-types/lowcardinality.md)、[Nullable](../../../sql-reference/data-types/nullable.md)、[Array](../../../sql-reference/data-types/array.md)、[Tuple](../../../sql-reference/data-types/tuple.md) でサポートされています。

`COLLATE` を使用したソートは、通常のバイト単位のソートより効率が低いため、少数の行に対する最終ソートにのみ使用することを推奨します。

<div id="collation-examples">
  ## Collationの例
</div>

[String](../../../sql-reference/data-types/string.md) 型の値のみを使った例：

入力テーブル：

```text
┌─x─┬─s────┐
│ 1 │ bca  │
│ 2 │ ABC  │
│ 3 │ 123a │
│ 4 │ abc  │
│ 5 │ BCA  │
└───┴──────┘
```

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```text title="Response"
┌─x─┬─s────┐
│ 3 │ 123a │
│ 4 │ abc  │
│ 2 │ ABC  │
│ 1 │ bca  │
│ 5 │ BCA  │
└───┴──────┘
```

[Nullable](../../../sql-reference/data-types/nullable.md) を使った例:

入力テーブル:

```text
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

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```text title="Response"
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

[Array](../../../sql-reference/data-types/array.md)の例:

入力テーブル:

```text
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

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```text title="Response"
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

[LowCardinality](../../../sql-reference/data-types/lowcardinality.md) 文字列を使った例:

入力テーブル:

```response
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

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```response title="Response"
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

[Tuple](../../../sql-reference/data-types/tuple.md)を使った例:

```response title="Response"
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

```sql title="Query"
SELECT * FROM collate_test ORDER BY s ASC COLLATE 'en';
```

```response title="Response"
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

<div id="implementation-details">
  ## 実装の詳細
</div>

`ORDER BY` に加えて十分に小さい [LIMIT](../../../sql-reference/statements/select/limit.md) が指定されている場合、使用する RAM を抑えられます。そうでない場合、消費されるメモリ量はソート対象データの量に比例します。分散クエリ処理では、[GROUP BY](/ja/sql-reference/statements/select/group-by) が省略されていると、ソートはリモートサーバー上で部分的に実行され、その結果はリクエスト元のサーバーでマージされます。つまり、分散ソートでは、ソート対象データ量が単一サーバーのメモリ量を上回ることがあります。

RAM が不足している場合は、外部メモリでソートを実行できます (ディスク上に一時ファイルを作成します) 。この目的には設定 `max_bytes_before_external_sort` を使用します。これが 0 (デフォルト) に設定されている場合、外部ソートは無効です。有効な場合、ソート対象データ量が指定したバイト数に達すると、収集されたデータがソートされ、一時ファイルにダンプされます。すべてのデータを読み込んだ後、ソート済みのすべてのファイルがマージされ、結果が出力されます。ファイルは config の `/var/lib/clickhouse/tmp/` ディレクトリに書き込まれます (デフォルトではこの場所ですが、`tmp_path` パラメーターを使ってこの設定を変更できます) 。また、クエリがメモリ制限を超えた場合にのみディスクへのスピルを使用することもできます。つまり、`max_bytes_ratio_before_external_sort=0.6` を設定すると、クエリがメモリ制限 (ユーザー/サーバー) の `60%` に達した時点でのみディスクへのスピルが有効になります。

クエリの実行時には、`max_bytes_before_external_sort` より多くのメモリを使用する場合があります。このため、この設定の値は `max_memory_usage` よりも十分に小さくする必要があります。たとえば、サーバーに 128 GB の RAM があり、単一のクエリを実行する必要がある場合は、`max_memory_usage` を 100 GB、`max_bytes_before_external_sort` を 80 GB に設定します。

外部ソートは、RAM 内でのソートに比べて大幅に効率が低下します。

<div id="optimization-of-data-reading">
  ## データ読み取りの最適化
</div>

`ORDER BY` 式にテーブルのソートキーと一致するプレフィックスがある場合、[optimize&#95;read&#95;in&#95;order](../../../operations/settings/settings.md#optimize_read_in_order) 設定を使用してクエリを最適化できます。

`optimize_read_in_order` 設定が有効な場合、ClickHouse serverはテーブルの索引を使用し、`ORDER BY` キーの順序でデータを読み取ります。これにより、[LIMIT](../../../sql-reference/statements/select/limit.md) が指定されている場合に、すべてのデータを読み取らずに済みます。そのため、少ない LIMIT で大量のデータを対象とするクエリをより高速に処理できます。

この最適化は `ASC` と `DESC` の両方で機能しますが、[GROUP BY](/ja/sql-reference/statements/select/group-by) 句および [FINAL](/ja/sql-reference/statements/select/from#final-modifier) 修飾子とは併用できません。

`optimize_read_in_order` 設定が無効な場合、ClickHouse serverは `SELECT` クエリの処理中にテーブルの索引を使用しません。

`ORDER BY` 句があり、`LIMIT` が大きく、目的のデータが見つかるまでに大量のレコードを読み取る必要がある [WHERE](../../../sql-reference/statements/select/where.md) 条件を含むクエリを実行する場合は、`optimize_read_in_order` を手動で無効にすることを検討してください。

この最適化は、次のテーブルエンジンでサポートされています。

* [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) ([materialized views](/ja/sql-reference/statements/create/view#materialized-view) を含む)
* [Merge](../../../engines/table-engines/special/merge.md)
* [Buffer](../../../engines/table-engines/special/buffer.md)

`MaterializedView` エンジンのテーブルでは、この最適化は `SELECT ... FROM merge_tree_table ORDER BY pk` のようなビューで機能します。ただし、ビューのクエリに `ORDER BY` 句がない場合、`SELECT ... FROM view ORDER BY pk` のようなクエリではサポートされません。

<div id="order-by-expr-with-fill-modifier">
  ## ORDER BY Expr WITH FILL 修飾子
</div>

この修飾子は、[LIMIT ... WITH TIES 修飾子](/ja/sql-reference/statements/select/limit#limit--with-ties-modifier) と組み合わせることもできます。

`WITH FILL` 修飾子は、`ORDER BY expr` の後に、任意の `FROM expr`、`TO expr`、`STEP expr` パラメータを付けて指定できます。
`expr` カラムの欠けている値はすべて順次補完され、他のカラムにはデフォルト値が設定されます。

複数のカラムを補完するには、`ORDER BY` セクションの各フィールド名の後に、任意のパラメータ付きで `WITH FILL` 修飾子を追加します。

```sql title="Query"
ORDER BY expr [WITH FILL] [FROM const_expr] [TO const_expr] [STEP const_numeric_expr] [STALENESS const_numeric_expr], ... exprN [WITH FILL] [FROM expr] [TO expr] [STEP numeric_expr] [STALENESS numeric_expr]
[INTERPOLATE [(col [AS expr], ... colN [AS exprN])]]
```

`WITH FILL` は、Numeric (float、decimal、int の各型) または Date/DateTime 型のフィールドに適用できます。`String` フィールドに適用した場合、欠損値は空文字列で補完されます。
`FROM const_expr` が定義されていない場合、補完する数列には `ORDER BY` の `expr` フィールドの最小値が使用されます。
`TO const_expr` が定義されていない場合、補完する数列には `ORDER BY` の `expr` フィールドの最大値が使用されます。
`STEP const_numeric_expr` が定義されている場合、`const_numeric_expr` は numeric type では `as is`、Date type では `days`、DateTime type では `seconds` として解釈されます。また、時間および日付のインターバルを表す [INTERVAL](/ja/sql-reference/data-types/special-data-types/interval/) data type もサポートしています。
`STEP const_numeric_expr` が省略されている場合、補完する数列には numeric type では `1.0`、Date type では `1 day`、DateTime type では `1 second` が使用されます。
`STALENESS const_numeric_expr` が定義されている場合、クエリは元のデータ内で直前の行との差分が `const_numeric_expr` を超えるまで行を生成します。
`INTERPOLATE` は、`ORDER BY WITH FILL` に含まれないカラムに適用できます。これらのカラムは、前のフィールドの値に `expr` を適用して補完されます。`expr` がない場合は、直前の値がそのまま繰り返されます。リストを省略すると、対象となるすべてのカラムが含まれます。

`WITH FILL` を使用しないクエリの例:

```sql title="Query"
SELECT n, source FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n;
```

```text title="Response"
┌─n─┬─source───┐
│ 1 │ original │
│ 4 │ original │
│ 7 │ original │
└───┴──────────┘
```

`WITH FILL` 修飾子を適用した後の同じクエリ：

```sql title="Query"
SELECT n, source FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5;
```

```text title="Response"
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

複数のフィールドを指定した `ORDER BY field2 WITH FILL, field1 WITH FILL` の場合、補完の順序は `ORDER BY` 句内のフィールドの並びに従います。

例:

```sql title="Query"
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

```text title="Response"
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

フィールド `d1` は補完されず、代わりにデフォルト値が使用されます。これは、`d2` の値に繰り返しがないため、`d1` の数列を適切に計算できないからです。

`ORDER BY` のフィールドを変更した次のクエリ:

```sql title="Query"
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

```text title="Response"
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

次のクエリでは、カラム `d1` で補完される各データに 1 日の `INTERVAL` データ型を使用します:

```sql title="Query"
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

```response title="Response"
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

`STALENESS` を指定しないクエリの例:

```sql title="Query"
SELECT number AS key, 5 * number value, 'original' AS source
FROM numbers(16) WHERE key % 5 == 0
ORDER BY key WITH FILL;
```

```text title="Response"
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

同じクエリに `STALENESS 3` を適用した場合:

```sql title="Query"
SELECT number AS key, 5 * number value, 'original' AS source
FROM numbers(16) WHERE key % 5 == 0
ORDER BY key WITH FILL STALENESS 3;
```

```text title="Response"
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

`INTERPOLATE` を使用しないクエリの例:

```sql title="Query"
SELECT n, source, inter FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source, number AS inter
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5;
```

```text title="Response"
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

`INTERPOLATE` 適用後の同じクエリ:

```sql title="Query"
SELECT n, source, inter FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source, number AS inter
   FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 5.51 STEP 0.5 INTERPOLATE (inter AS inter + 1);
```

```text title="Response"
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

<div id="filling-grouped-by-sorting-prefix">
  ## ソートプレフィックスでグループ化した補完
</div>

特定のカラムで同じ値を持つ行を、それぞれ独立して補完すると便利な場合があります。時系列の欠損値補完は、その代表的な例です。
次のような時系列テーブルがあるとします。

```sql
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

また、各センサーごとに、欠損値を1秒間隔で個別に補完したいとします。
これを実現するには、補完対象のカラム `timestamp` のソートプレフィックスとして `sensor_id` カラムを使用します:

```sql
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

ここでは、補完された行をより目立たせるために、`value` カラムをあえて `9999` で補間しています。
この動作は、`use_with_fill_by_sorting_prefix` の設定で制御されます (デフォルトで有効) 。

<div id="related-content">
  ## 関連コンテンツ
</div>

* ブログ: [ClickHouseで時系列データを扱う方法](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)