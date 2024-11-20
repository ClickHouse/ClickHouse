---
slug: /ja/sql-reference/statements/select/group-by
sidebar_label: GROUP BY
---

# GROUP BY句

`GROUP BY`句は`SELECT`クエリを集計モードに切り替え、以下のように機能します:

- `GROUP BY`句には式のリスト（または長さ1のリストと見なされる単一の式）が含まれます。このリストは「グループ化キー」として機能し、それぞれの個々の式は「キー表現」と呼ばれます。
- [SELECT](../../../sql-reference/statements/select/index.md)、[HAVING](../../../sql-reference/statements/select/having.md)、および[ORDER BY](../../../sql-reference/statements/select/order-by.md)句内のすべての式は、キー表現に基づいて計算されるか、非キー式（プレーンカラムを含む）上の[集約関数](../../../sql-reference/aggregate-functions/index.md)によって計算される**必要があります**。言い換えれば、テーブルから選択された各カラムは、キー表現または集約関数のいずれかの中で使用される必要があり、両方で使用されることはありません。
- 集約された`SELECT`クエリの結果は、ソーステーブル内の「グループ化キー」のユニーク値の数だけ行を含みます。この処理は通常、行数を桁違いに減少させますが、必ずしもそうとは限りません：すべての「グループ化キー」値が異なっている場合、行数は同じままになります。

カラム名の代わりにカラム番号でテーブル内のデータをグループ化したい場合は、設定[enable_positional_arguments](../../../operations/settings/settings.md#enable-positional-arguments)を有効にします。

:::note
テーブルに対して集計を実行する追加の方法があります。クエリが集計関数内にテーブルカラムのみを含む場合、`GROUP BY句`を省略でき、キーの空の集合で集計が仮定されます。このようなクエリは常に1行のみを返します。
:::

## NULLの処理

グループ化において、ClickHouseは[NULL](../../../sql-reference/syntax.md#null-literal)を値として解釈し、`NULL==NULL`です。これは他の多くの文脈での`NULL`処理とは異なります。

これが何を意味するかを示す例を以下に示します。

次のようなテーブルがあると仮定します:

``` text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

クエリ`SELECT sum(x), y FROM t_null_big GROUP BY y`の結果は次のとおりです:

``` text
┌─sum(x)─┬────y─┐
│      4 │    2 │
│      3 │    3 │
│      5 │ ᴺᵁᴸᴸ │
└────────┴──────┘
```

`y = NULL`に対する`GROUP BY`が`x`を合計したことがわかります。まるで`NULL`がこの値であるかのように。

`GROUP BY`に複数のキーを渡す場合、結果は選択のすべての組み合わせを提供します。`NULL`が特定の値であるかのように。

## ROLLUP修飾子

`ROLLUP`修飾子は、`GROUP BY`リスト内の順序に基づいてキー表現の小計を計算するために使用されます。小計行は結果テーブルの後に追加されます。

小計は逆順で計算されます：最初にリストの最後のキー表現に対して小計が計算され、次に前のキー表現に対して計算されるという具合に、最初のキー表現に至るまでです。

小計行では、既に「グループ化」されたキー表現の値が`0`または空の行に設定されます。

:::note
[HAVING](../../../sql-reference/statements/select/having.md)句が小計の結果に影響を与える可能性があることを理解してください。
:::

**例**

テーブル`t`を考えます:

```text
┌─year─┬─month─┬─day─┐
│ 2019 │     1 │   5 │
│ 2019 │     1 │  15 │
│ 2020 │     1 │   5 │
│ 2020 │     1 │  15 │
│ 2020 │    10 │   5 │
│ 2020 │    10 │  15 │
└──────┴───────┴─────┘
```

クエリ:

```sql
SELECT year, month, day, count(*) FROM t GROUP BY ROLLUP(year, month, day);
```
`GROUP BY`セクションに3つのキー表現があるため、結果には右から左に「ロールアップ」された4つの小計テーブルが含まれます:

- `GROUP BY year, month, day`;
- `GROUP BY year, month`（そして`day`カラムはゼロで満たされています）;
- `GROUP BY year`（そして今や`month, day`カラムはどちらもゼロで満たされています）;
- そして合計（そして3つのキー表現のカラムはすべてゼロです）。

```text
┌─year─┬─month─┬─day─┬─count()─┐
│ 2020 │    10 │  15 │       1 │
│ 2020 │     1 │   5 │       1 │
│ 2019 │     1 │   5 │       1 │
│ 2020 │     1 │  15 │       1 │
│ 2019 │     1 │  15 │       1 │
│ 2020 │    10 │   5 │       1 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     1 │   0 │       2 │
│ 2020 │     1 │   0 │       2 │
│ 2020 │    10 │   0 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     0 │   0 │       2 │
│ 2020 │     0 │   0 │       4 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     0 │   0 │       6 │
└──────┴───────┴─────┴─────────┘
```
このクエリは`WITH`キーワードを使用しても書くことができます。
```sql
SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH ROLLUP;
```

**こちらも参照**

- SQL標準互換性のための[group_by_use_nulls](/docs/ja/operations/settings/settings.md#group_by_use_nulls)設定。

## CUBE修飾子

`CUBE`修飾子は、`GROUP BY`リスト内のキー表現のあらゆる組み合わせに対して小計を計算するために使用されます。小計行は結果テーブルの後に追加されます。

小計行では、すべての「グループ化」キー表現の値を`0`または空の行に設定します。

:::note
[HAVING](../../../sql-reference/statements/select/having.md)句が小計の結果に影響を与える可能性があることを理解してください。
:::

**例**

テーブル`t`を考えます:

```text
┌─year─┬─month─┬─day─┐
│ 2019 │     1 │   5 │
│ 2019 │     1 │  15 │
│ 2020 │     1 │   5 │
│ 2020 │     1 │  15 │
│ 2020 │    10 │   5 │
│ 2020 │    10 │  15 │
└──────┴───────┴─────┘
```

クエリ:

```sql
SELECT year, month, day, count(*) FROM t GROUP BY CUBE(year, month, day);
```

`GROUP BY`セクションに3つのキー表現があるため、結果には、すべてのキー表現の組み合わせに対する8つの小計テーブルが含まれます:

- `GROUP BY year, month, day`
- `GROUP BY year, month`
- `GROUP BY year, day`
- `GROUP BY year`
- `GROUP BY month, day`
- `GROUP BY month`
- `GROUP BY day`
- そして合計。

`GROUP BY`から除外されたカラムはゼロで満たされます。

```text
┌─year─┬─month─┬─day─┬─count()─┐
│ 2020 │    10 │  15 │       1 │
│ 2020 │     1 │   5 │       1 │
│ 2019 │     1 │   5 │       1 │
│ 2020 │     1 │  15 │       1 │
│ 2019 │     1 │  15 │       1 │
│ 2020 │    10 │   5 │       1 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     1 │   0 │       2 │
│ 2020 │     1 │   0 │       2 │
│ 2020 │    10 │   0 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2020 │     0 │   5 │       2 │
│ 2019 │     0 │   5 │       1 │
│ 2020 │     0 │  15 │       2 │
│ 2019 │     0 │  15 │       1 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     0 │   0 │       2 │
│ 2020 │     0 │   0 │       4 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     1 │   5 │       2 │
│    0 │    10 │  15 │       1 │
│    0 │    10 │   5 │       1 │
│    0 │     1 │  15 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     1 │   0 │       4 │
│    0 │    10 │   0 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     0 │   5 │       3 │
│    0 │     0 │  15 │       3 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     0 │   0 │       6 │
└──────┴───────┴─────┴─────────┘
```
このクエリは`WITH`キーワードを使用しても書くことができます。
```sql
SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH CUBE;
```

**こちらも参照**

- SQL標準互換性のための[group_by_use_nulls](/docs/ja/operations/settings/settings.md#group_by_use_nulls)設定。

## WITH TOTALS修飾子

`WITH TOTALS`修飾子が指定されている場合、追加の行が計算されます。この行は、キー列にデフォルト値（ゼロや空行）が含まれ、集約関数の列にはすべての行を含む「合計」値が計算されます。

この追加の行は、`JSON*`、`TabSeparated*`、および `Pretty*`フォーマットでのみ、他の行とは別に生成されます：

- `XML`および`JSON*`フォーマットでは、この行は個別の‘totals’フィールドとして出力されます。
- `TabSeparated*`、`CSV*` および `Vertical` フォーマットでは、空行の後（他のデータ後）に続いて主要な結果の後に行が来ます。
- `Pretty*`フォーマットでは、この行は主要な結果の後に別のテーブルとして出力されます。
- `Template`フォーマットでは、指定されたテンプレートに従って行が出力されます。
- その他のフォーマットでは利用できません。

:::note
totalsは`SELECT`クエリの結果に出力され、`INSERT INTO ... SELECT`では出力されません。
:::

[HAVING](../../../sql-reference/statements/select/having.md)が存在する場合、`WITH TOTALS`は異なる方法で実行できます。この動作は`totals_mode`設定に依存します。

### 合計処理の設定

デフォルトでは、`totals_mode = 'before_having'`です。この場合、‘totals’は、HAVINGおよび`max_rows_to_group_by`を通過しなかったすべての行を含めて計算されます。

他の選択肢は、HAVINGを通過した行のみを‘totals’に含め、`max_rows_to_group_by`と`group_by_overflow_mode = 'any'`を持つ場合に異なる動作をします。

`after_having_exclusive` – `max_rows_to_group_by`を通過しなかった行は含まれません。言い換えれば、‘totals’は`max_rows_to_group_by`が省略された場合よりも少ないか、同じ数の行を持ちます。

`after_having_inclusive` – ‘totals’に‘max_rows_to_group_by’を通過しなかったすべての行を含めます。言い換えれば、‘totals’は`max_rows_to_group_by`が省略された場合よりも多くの行を持つことがあります。

`after_having_auto` – HAVINGを通過した行の数を数えます。デフォルトで50％以上の場合、‘totals’に‘max_rows_to_group_by’を通過しなかったすべての行を含めます。そうでなければ、それらを含めません。

`totals_auto_threshold` – デフォルトでは0.5。`after_having_auto`の係数。

`max_rows_to_group_by`と`group_by_overflow_mode = 'any'`が使用されていない場合、`after_having`のすべてのバリエーションは同じであり、どれを使用してもかまいません（例えば、`after_having_auto`）。

[JOIN](../../../sql-reference/statements/select/join.md)句内の副問合せも含めて、副問合せで`WITH TOTALS`を使用できます（この場合、対応する合計値が結合されます）。

## GROUP BY ALL

`GROUP BY ALL`は、集計関数ではないSELECTされたすべての式をリストすることと同等です。

例えば:

``` sql
SELECT
    a * 2,
    b,
    count(c),
FROM t
GROUP BY ALL
```

は

``` sql
SELECT
    a * 2,
    b,
    count(c),
FROM t
GROUP BY a * 2, b
```

と同じです。

概算特殊なケースとして、集計関数と他のフィールドがその引数としてある関数がある場合、`GROUP BY`キーには最大の非集計フィールドが含まれます。

例えば:

``` sql
SELECT
    substring(a, 4, 2),
    substring(substring(a, 1, 2), 1, count(b))
FROM t
GROUP BY ALL
```

は

``` sql
SELECT
    substring(a, 4, 2),
    substring(substring(a, 1, 2), 1, count(b))
FROM t
GROUP BY substring(a, 4, 2), substring(a, 1, 2)
```

と同じです。

## 例

例:

``` sql
SELECT
    count(),
    median(FetchTiming > 60 ? 60 : FetchTiming),
    count() - sum(Refresh)
FROM hits
```

標準SQLに準拠するMySQLとは異なり、キーまたは集約関数にないカラムの値を取得することはできません（定数式を除く）。これを回避するには、「any」集約関数（最初に出現した値を取得）または「min/max」を使用できます。

例:

``` sql
SELECT
    domainWithoutWWW(URL) AS domain,
    count(),
    any(Title) AS title -- 各ドメインの最初に見つかったページヘッダーを取得。
FROM hits
GROUP BY domain
```

異なるキー値が見つかるたびに、`GROUP BY`は集計関数のセットを計算します。

## GROUPING SETS修飾子

これは最も一般的な修飾子です。
この修飾子は、いくつかの集計キーセット（グルーピングセット）を手動で指定することを可能にします。
集計はそれぞれのグルーピングセットに対して個別に行われ、その後、すべての結果が結合されます。
グルーピングセットにカラムが存在しない場合、それはデフォルト値で満たされます。

言い換えれば、上で説明した修飾子は`GROUPING SETS`を使って表現できます。
`ROLLUP`、`CUBE`、および`GROUPING SETS`修飾子を持つクエリは文法的に等しいですが、異なるパフォーマンスを示す可能性があります。
`GROUPING SETS`がすべてを並行して実行しようとするとき、`ROLLUP`および`CUBE`が集計の最終マージを単一スレッドで実行します。

ソースの列がデフォルト値を含む状況では、行がこれらの列をキーとして使用する集計の一部であるかどうかを見分けるのが難しいかもしれません。
この問題を解決するために、`GROUPING`関数を使用してください。

**例**

次の2つのクエリは同等です。

```sql
-- クエリ1
SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH ROLLUP;

-- クエリ2
SELECT year, month, day, count(*) FROM t GROUP BY
GROUPING SETS
(
    (year, month, day),
    (year, month),
    (year),
    ()
);
```

**こちらも参照**

- SQL標準互換性のための[group_by_use_nulls](/docs/ja/operations/settings/settings.md#group_by_use_nulls)設定。

## 実装詳細

集計は列指向DBMSの最も重要な機能のひとつであり、それゆえにClickHouseの最も最適化された部分のひとつです。デフォルトでは、集計はメモリ内でハッシュテーブルを使用して行われます。これは40以上の特殊化を持ち、「グループ化キー」のデータ型に依存して自動的に選択されます。

### テーブルのソートキーに基づくGROUP BYの最適化

テーブルがあるキーでソートされ、`GROUP BY`式がソートキーまたは単射関数の少なくとも接頭辞を含む場合、集計はより効果的に行われます。この場合、テーブルから新しいキーが読み込まれると、集計の中間結果を確定してクライアントに送信することができます。この動作は[optimize_aggregation_in_order](../../../operations/settings/settings.md#optimize_aggregation_in_order)設定で切り替えられます。この最適化は集計中のメモリ使用量を減少させますが、場合によってはクエリの実行を遅くすることがあります。

### 外部メモリでのGROUP BY

`GROUP BY`中のメモリ使用量を制限するために、一時データをディスクに書き出すことができます。[max_bytes_before_external_group_by](../../../operations/settings/query-complexity.md#settings-max_bytes_before_external_group_by)設定は`GROUP BY`の一時データをファイルシステムに書き出すためのRAM消費のしきい値を決定します。0に設定すると（デフォルト）、無効になります。

`max_bytes_before_external_group_by`を使用する場合、`max_memory_usage`を2倍程度高く設定することをお勧めします。これは、データを読み込んで中間データを形成する段階(1)と中間データをマージする段階(2)の2つの段階があるためです。一時データの書き出しは段階1でしか発生しません。もし一時データが書き出されなかった場合、段階2では段階1と同じ量のメモリを必要とする可能性があります。

例えば、[max_memory_usage](../../../operations/settings/query-complexity.md#settings_max_memory_usage)が10000000000に設定されており、外部集計を使用したい場合、`max_bytes_before_external_group_by`を10000000000に、`max_memory_usage`を20000000000に設定することが妥当です。外部集計がトリガされるとき（少なくとも一度の一時データの書き出しがあった場合）、RAMの最大消費量は`max_bytes_before_external_group_by`よりわずかに多い程度です。

分散クエリ処理を使用する場合、外部集計はリモートサーバーで実行されます。リクエスタサーバーが少量のRAMを使用するだけで済むようにするために、`distributed_aggregation_memory_efficient`を1に設定します。

ディスクに書き出されたデータをマージする際、および`distributed_aggregation_memory_efficient`設定が有効な場合にリモートサーバーからの結果をマージする際は、合計RAMの`1/256 * スレッド数`まで消費します。

外部集計が有効な場合、`max_bytes_before_external_group_by`以下のデータが存在した場合（つまりデータが書き出されなかった場合）、クエリは外部集計なしと同様に高速で実行されます。一時データが書き出された場合、実行時間は数倍（約三倍）長くなります。

`GROUP BY`後に`ORDER BY`があり、[LIMIT](../../../sql-reference/statements/select/limit.md)が指定されている場合、使用されるRAMの量はテーブル全体ではなく`LIMIT`内のデータ量に依存します。しかし、`ORDER BY`に`LIMIT`がない場合、外部ソート（`max_bytes_before_external_sort`）を有効にすることを忘れないでください。
