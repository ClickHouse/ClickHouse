---
description: '`GROUP BY` 句のドキュメント'
sidebar_label: 'GROUP BY'
slug: /sql-reference/statements/select/group-by
title: '`GROUP BY` 句'
doc_type: 'reference'
---

`GROUP BY` 句は、`SELECT` クエリを集約モードに切り替えます。動作は次のとおりです。

* `GROUP BY` 句には、式のリスト (または、長さ 1 のリストと見なされる単一の式) が含まれます。このリストは「グループ化キー」として機能し、個々の式はそれぞれ「キー式」と呼ばれます。
* [SELECT](/ja/sql-reference/statements/select/index.md)、[HAVING](/ja/sql-reference/statements/select/having.md)、および [ORDER BY](/ja/sql-reference/statements/select/order-by.md) 句内のすべての式は、**必ず** キー式 に基づいて計算されるか、**または** 非 キー式 (通常のカラムを含む) に対する [集約関数](../../../sql-reference/aggregate-functions/index.md) に基づいて計算されなければなりません。言い換えると、テーブルから選択される各カラムは、キー式 として使うか、集約関数 の中で使うかのいずれかでなければならず、両方で使うことはできません。
* `SELECT` クエリを集約した結果には、元テーブル内の「グループ化キー」の一意な値の数だけ行が含まれます。通常、これにより行数は大幅に減少し、多くの場合は桁違いに少なくなりますが、必ずしもそうとは限りません。すべての「グループ化キー」の値が異なる場合、行数は変わりません。

テーブル内のデータをカラム名ではなくカラム番号でグループ化したい場合は、設定 [enable&#95;positional&#95;arguments](/ja/operations/settings/settings#enable_positional_arguments) を有効にしてください。

:::note
テーブルに対して集約を実行する方法は、これ以外にもあります。クエリ内でテーブルのカラムが 集約関数 の中でしか使われていない場合は、`GROUP BY clause` を省略でき、空のキー集合に対する集約が行われるものと見なされます。このようなクエリは常にちょうど 1 行を返します。
:::

<div id="null-processing">
  ## NULL の処理
</div>

グループ化では、ClickHouse は [NULL](/ja/sql-reference/syntax#null) を値として扱い、`NULL==NULL` とみなします。これは、ほかの多くの文脈での `NULL` の扱いとは異なります。

これが何を意味するのか、例で見てみましょう。

次のテーブルがあるとします。

```text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

クエリ `SELECT sum(x), y FROM t_null_big GROUP BY y` の結果は次のとおりです。

```text
┌─sum(x)─┬────y─┐
│      4 │    2 │
│      3 │    3 │
│      5 │ ᴺᵁᴸᴸ │
└────────┴──────┘
```

`y = NULL` に対する `GROUP BY` では、`NULL` をひとつの値であるかのように扱って `x` が合計されていることがわかります。

`GROUP BY` に複数のキーを渡すと、`NULL` を特定の値であるかのように扱い、選択された値のすべての組み合わせが結果として返されます。

<div id="rollup-modifier">
  ## ROLLUP 修飾子
</div>

`ROLLUP` 修飾子は、`GROUP BY` リスト内の順序に基づいて、キー式の小計を計算するために使用されます。小計行は結果テーブルの後に追加されます。

小計は逆順で計算されます。まずリスト内の最後のキー式に対する小計が計算され、次にその前のキー式に対して計算される、という処理が最初のキー式まで続きます。

小計行では、すでに「grouped」されたキー式の値は `0` または空文字列に設定されます。

:::note
[HAVING](/ja/sql-reference/statements/select/having.md) 句は小計の結果に影響する可能性があることに注意してください。
:::

**例**

テーブル t について考えます。

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

```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY ROLLUP(year, month, day);
```

`GROUP BY` 句には 3 つのキー式があるため、結果には小計が右から左へ「ロールアップ」された 4 つのテーブルが含まれます。

* `GROUP BY year, month, day`;
* `GROUP BY year, month` (`day` カラムは 0 で補完されます) ;
* `GROUP BY year` (この場合、`month` と `day` のカラムはいずれも 0 で補完されます) ;
* および totals (3 つのキー式のカラムはすべて 0 になります) 。

```text title="Response"
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

同じクエリは、`WITH`キーワードを使って書くこともできます。

```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH ROLLUP;
```

**関連項目**

* SQL 標準との互換性に関する [group&#95;by&#95;use&#95;nulls](/ja/operations/settings/settings.md#group_by_use_nulls) 設定。

<div id="cube-modifier">
  ## CUBE 修飾子
</div>

`CUBE` 修飾子は、`GROUP BY` リスト内のキー式のすべての組み合わせについて小計を計算するために使用されます。小計行は結果テーブルの後に追加されます。

小計行では、すべての「grouped」キー式の値が `0` または空文字列に設定されます。

:::note
[HAVING](/ja/sql-reference/statements/select/having.md) 句は小計の結果に影響する可能性がある点に注意してください。
:::

**例**

テーブル t について考えます:

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

```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY CUBE(year, month, day);
```

`GROUP BY` 句には 3 つのキー式があるため、結果には各キー式の組み合わせに対する小計を含む 8 つのテーブルが含まれます。

* `GROUP BY year, month, day`
* `GROUP BY year, month`
* `GROUP BY year, day`
* `GROUP BY year`
* `GROUP BY month, day`
* `GROUP BY month`
* `GROUP BY day`
* および totals。

`GROUP BY` に含まれないカラムは、0 で埋められます。

```text title="Response"
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

同じクエリは、`WITH` キーワードを使って書くこともできます。

```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH CUBE;
```

**関連項目**

* SQL 標準との互換性については、[group&#95;by&#95;use&#95;nulls](/ja/operations/settings/settings.md#group_by_use_nulls) 設定を参照してください。

<div id="with-totals-modifier">
  ## WITH TOTALS 修飾子
</div>

`WITH TOTALS` 修飾子を指定すると、追加でもう 1 行が計算されます。この行には、デフォルト値 (0 または空文字列) を含むキーカラムと、すべての行を対象に計算された値 (「total」値) を持つ集約関数のカラムが含まれます。

この追加行は、他の行とは別に、`JSON*`、`TabSeparated*`、`Pretty*` フォーマットでのみ出力されます。

* `XML` および `JSON*` フォーマットでは、この行は個別の `totals` フィールドとして出力されます。
* `TabSeparated*`、`CSV*`、`Vertical` フォーマットでは、この行はメインの結果の後に、空行を 1 行挟んで出力されます (他のデータの後) 。
* `Pretty*` フォーマットでは、この行はメインの結果の後に別のテーブルとして出力されます。
* `Template` フォーマットでは、この行は指定されたテンプレートに従って出力されます。
* その他のフォーマットでは利用できません。

:::note
totals は `SELECT` クエリの結果では出力されますが、`INSERT INTO ... SELECT` では出力されません。
:::

[HAVING](/ja/sql-reference/statements/select/having.md) がある場合、`WITH TOTALS` は異なる方法で実行されることがあります。動作は `totals_mode` 設定に依存します。

<div id="configuring-totals-processing">
  ### totals 処理の設定
</div>

デフォルトでは、`totals_mode = 'before_having'` です。この場合、&#39;totals&#39; は HAVING と `max_rows_to_group_by` を通過しないものも含め、すべての行に対して計算されます。

その他の選択肢では、HAVING を通過した行だけを &#39;totals&#39; に含めます。また、設定 `max_rows_to_group_by` および `group_by_overflow_mode = 'any'` に対する動作も異なります。

`after_having_exclusive` – `max_rows_to_group_by` を通過しなかった行は含めません。言い換えると、&#39;totals&#39; の行数は、`max_rows_to_group_by` を省略した場合と比べて、同じかそれ以下になります。

`after_having_inclusive` – `max_rows_to_group_by` を通過しなかったすべての行を &#39;totals&#39; に含めます。言い換えると、&#39;totals&#39; の行数は、`max_rows_to_group_by` を省略した場合と比べて、同じかそれ以上になります。

`after_having_auto` – HAVING を通過した行数を数えます。それが一定の割合 (デフォルトでは 50%) を超える場合は、`max_rows_to_group_by` を通過しなかったすべての行を &#39;totals&#39; に含めます。そうでない場合は含めません。

`totals_auto_threshold` – デフォルト値は 0.5 です。`after_having_auto` の係数です。

`max_rows_to_group_by` と `group_by_overflow_mode = 'any'` を使用しない場合、`after_having` の各バリエーションはすべて同じなので、どれを使ってもかまいません (たとえば `after_having_auto`) 。

`WITH TOTALS` はサブクエリでも使用できます。[JOIN](/ja/sql-reference/statements/select/join.md) 句内のサブクエリでも使用でき、その場合は対応する合計値が結合されます。

<div id="group-by-all">
  ## GROUP BY ALL
</div>

`GROUP BY ALL` は、集約関数ではない `SELECT` 内のすべての式を列挙するのと同じです。

たとえば:

```sql
SELECT
    a * 2,
    b,
    count(c),
FROM t
GROUP BY ALL
```

と同じです

```sql
SELECT
    a * 2,
    b,
    count(c),
FROM t
GROUP BY a * 2, b
```

特殊なケースとして、集約関数とその他のフィールドの両方を引数に取る関数がある場合、`GROUP BY` のキーには、そこから抽出可能な非集約フィールドができるだけ多く含まれます。

例えば:

```sql
SELECT
    substring(a, 4, 2),
    substring(substring(a, 1, 2), 1, count(b))
FROM t
GROUP BY ALL
```

と同じです

```sql
SELECT
    substring(a, 4, 2),
    substring(substring(a, 1, 2), 1, count(b))
FROM t
GROUP BY substring(a, 4, 2), substring(a, 1, 2)
```

<div id="examples">
  ## 例
</div>

例:

```sql
SELECT
    count(),
    median(FetchTiming > 60 ? 60 : FetchTiming),
    count() - sum(Refresh)
FROM hits
```

MySQLとは異なり (標準SQLに準拠して) 、キーまたは aggregate function に含まれていないカラムの値は (一部の定数式を除き) 取得できません。これを回避するには、&#39;any&#39; aggregate function (最初に見つかった値を取得) または &#39;min/max&#39; を使用できます。

例:

```sql
SELECT
    domainWithoutWWW(URL) AS domain,
    count(),
    any(Title) AS title -- getting the first occurred page header for each domain.
FROM hits
GROUP BY domain
```

異なるキー値ごとに、`GROUP BY` は各集約関数の値を計算します。

<div id="grouping-sets-modifier">
  ## GROUPING SETS 修飾子
</div>

これは最も汎用的な修飾子です。
この修飾子を使うと、複数の集約キーのセット (grouping sets) を手動で指定できます。
集約は各 grouping set ごとに個別に実行され、その後、すべての結果が結合されます。
あるカラムが grouping set に含まれていない場合、そのカラムはデフォルト値で補完されます。

つまり、前述の修飾子は `GROUPING SETS` で表現できます。
`ROLLUP`、`CUBE`、`GROUPING SETS` 修飾子を持つクエリは構文上は同等ですが、実行方法が異なる場合があります。
`GROUPING SETS` はすべてを並列に実行しようとしますが、`ROLLUP` と `CUBE` では集約結果の最終マージが単一スレッドで実行されます。

元のカラムにデフォルト値が含まれている場合、ある行がそれらのカラムをキーとして使用する集約結果の一部なのかどうかを判別しにくいことがあります。
この問題を解決するには、`GROUPING` 関数を使用する必要があります。

**例**

次の 2 つのクエリは同等です。

```sql
-- Query 1
SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH ROLLUP;

-- Query 2
SELECT year, month, day, count(*) FROM t GROUP BY
GROUPING SETS
(
    (year, month, day),
    (year, month),
    (year),
    ()
);
```

**関連項目**

* SQL標準との互換性については、[group&#95;by&#95;use&#95;nulls](/ja/operations/settings/settings.md#group_by_use_nulls) 設定を参照してください。

<div id="implementation-details">
  ## 実装の詳細
</div>

集約は列指向DBMSでもっとも重要な機能の1つであり、その実装はClickHouseの中でも特に重点的に最適化されている部分の1つです。デフォルトでは、集約はハッシュテーブルを使ってメモリ上で実行されます。これには40種類以上の特殊化実装があり、「グループ化キー」のデータ型に応じて自動的に選択されます。

<div id="group-by-optimization-depending-on-table-sorting-key">
  ### テーブルのソートキーに応じた GROUP BY の最適化
</div>

テーブルが何らかのキーでソートされており、`GROUP BY` 式にソートキーの少なくともプレフィックス、または単射関数が含まれている場合、集約をより効率的に実行できます。この場合、テーブルから新しいキーが読み込まれた時点で、その時点までの集約結果を確定してクライアントに送信できます。この動作は、[optimize&#95;aggregation&#95;in&#95;order](../../../operations/settings/settings.md#optimize_aggregation_in_order) 設定で有効にできます。このような最適化により、集約中のメモリ使用量は削減されますが、場合によってはクエリの実行が遅くなることがあります。

<div id="group-by-in-external-memory">
  ### 外部メモリでの GROUP BY
</div>

`GROUP BY` 中のメモリ使用量を抑えるために、一時データをディスクにダンプするよう設定できます。
[max&#95;bytes&#95;before&#95;external&#95;group&#95;by](/ja/operations/settings/settings#max_bytes_before_external_group_by) 設定は、`GROUP BY` の一時データをファイルシステムにダンプする RAM 使用量のしきい値を決定します。0 (デフォルト) に設定すると無効です。
また、[max&#95;bytes&#95;ratio&#95;before&#95;external&#95;group&#95;by](/ja/operations/settings/settings#max_bytes_ratio_before_external_group_by) を設定することもできます。この設定では、クエリのメモリ使用量が一定のしきい値に達した場合にのみ、`GROUP BY` で外部メモリを使用できます。

`max_bytes_before_external_group_by` を使用する場合は、`max_memory_usage` をその約 2 倍に設定することを推奨します (または `max_bytes_ratio_before_external_group_by=0.5`) 。これは、集約 には 2 つの段階があるためです。すなわち、データを読み取って中間データを作成する段階 (1) と、中間データをマージする段階 (2) です。データをファイルシステムにダンプできるのは段階 1 の間だけです。一時データがダンプされなかった場合、段階 2 では段階 1 と同程度のメモリが必要になる可能性があります。

たとえば、[max&#95;memory&#95;usage](/ja/operations/settings/settings#max_memory_usage) が 10000000000 に設定されていて、外部集約を使用したい場合は、`max_bytes_before_external_group_by` を 10000000000 に、`max_memory_usage` を 20000000000 に設定するのが妥当です。外部集約がトリガーされた場合 (一時データのダンプが少なくとも 1 回行われていれば) 、RAM の最大使用量は `max_bytes_before_external_group_by` をわずかに上回る程度に収まります。

分散クエリ処理では、外部集約はリモートサーバー上で実行されます。リクエスト元サーバーの RAM 使用量を少なく抑えるには、`distributed_aggregation_memory_efficient` を 1 に設定します。

ディスクに書き出されたデータをマージする場合や、`distributed_aggregation_memory_efficient` 設定が有効なときにリモートサーバーからの結果をマージする場合は、RAM 総量のうち最大で `1/256 * the_number_of_threads` が消費されます。

外部集約が有効でも、データ量が `max_bytes_before_external_group_by` 未満であれば (つまりデータが書き出されなければ) 、クエリは外部集約を使わない場合と同じ速度で実行されます。一時データが書き出された場合、実行時間は数倍長くなります (およそ 3 倍) 。

`GROUP BY` の後に [LIMIT](/ja/sql-reference/statements/select/limit.md) を伴う [ORDER BY](/ja/sql-reference/statements/select/order-by.md) がある場合、使用される RAM 量はテーブル全体ではなく、`LIMIT` 内のデータ量に依存します。ただし、`ORDER BY` に `LIMIT` がない場合は、外部ソート (`max_bytes_before_external_sort`) を有効にすることを忘れないでください。