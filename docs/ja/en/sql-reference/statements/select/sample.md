---
description: 'SAMPLE 句のドキュメント'
sidebar_label: 'SAMPLE'
slug: /sql-reference/statements/select/sample
title: 'SAMPLE 句'
doc_type: 'reference'
---

`SAMPLE` 句を使用すると、`SELECT` クエリを近似的に処理できます。

データのサンプリングが有効な場合、クエリはすべてのデータではなく、データの一定割合 (サンプル) に対してのみ実行されます。たとえば、すべての visits に関する統計を計算する必要がある場合は、全 visits の 1/10 に対してクエリを実行し、その結果を 10 倍すれば十分です。

近似的なクエリ処理は、次のような場合に役立ちます。

* 厳しいレイテンシ要件 (100ms 未満など) があるものの、それを満たすために追加のハードウェアリソースへ投資するコストを正当化できない場合。
* 生データ自体が正確ではなく、近似しても品質が目に見えて低下しない場合。
* ビジネス要件として近似結果で十分な場合 (コスト効率のため、または正確な結果をプレミアムユーザー向けに提供するため) 。

:::note
サンプリングを使用できるのは、[MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) ファミリーのテーブルに限られます。また、テーブル作成時にサンプリング式が指定されている必要があります ([MergeTree engine](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) を参照) 。
:::

データサンプリングの特徴は次のとおりです。

* データサンプリングは決定論的な仕組みです。同じ `SELECT .. SAMPLE` クエリの結果は常に同じになります。
* サンプリングは異なるテーブル間でも一貫して機能します。単一のサンプリングキーを持つテーブルでは、同じ係数のサンプルは常に同じデータの部分集合を選択します。たとえば、ユーザー ID のサンプルでは、異なるテーブルであっても、取り得るすべてのユーザー ID のうち同じ部分集合に属する行が取得されます。つまり、[IN](../../../sql-reference/operators/in.md) 句のサブクエリでサンプルを使用できます。また、[JOIN](../../../sql-reference/statements/select/join.md) 句を使ってサンプル同士を結合することもできます。
* サンプリングを使用すると、ディスクから読み取るデータ量を減らせます。なお、サンプリングキーは正しく指定する必要があります。詳細については、[Creating a MergeTree Table](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) を参照してください。

`SAMPLE` 句では、次の構文をサポートしています。

| SAMPLE Clause Syntax | Description                                                                                                                                             |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SAMPLE k`           | ここで `k` は 0 から 1 までの数です。クエリはデータの `k` の割合に対して実行されます。たとえば、`SAMPLE 0.1` はデータの 10% に対してクエリを実行します。[詳細はこちら](#sample-k)                                        |
| `SAMPLE n`           | ここで `n` は十分に大きな整数です。クエリは少なくとも `n` 行を含むサンプルに対して実行されます (ただし、それを大幅に超えることはありません) 。たとえば、`SAMPLE 10000000` は最小で 10,000,000 行に対してクエリを実行します。[詳細はこちら](#sample-n) |
| `SAMPLE k OFFSET m`  | ここで `k` と `m` は 0 から 1 までの数です。クエリはデータの `k` の割合のサンプルに対して実行されます。サンプルに使用されるデータは `m` の割合だけオフセットされます。[詳細はこちら](#sample-k-offset-m)                            |

<div id="sample-k">
  ## SAMPLE K
</div>

ここで `k` は 0 から 1 までの数です (分数表記と小数表記の両方に対応しています) 。たとえば、`SAMPLE 1/2` や `SAMPLE 0.5` です。

`SAMPLE k` 句では、データの `k` の割合からサンプルが取得されます。例を以下に示します。

```sql
SELECT
    Title,
    count() * 10 AS PageViews
FROM hits_distributed
SAMPLE 0.1
WHERE
    CounterID = 34
GROUP BY Title
ORDER BY PageViews DESC LIMIT 1000
```

この例では、データの 0.1 (10%) をサンプルとしてクエリを実行します。集約関数 の値は自動では補正されないため、近似結果を得るには `count()` の値に手動で 10 を掛けます。

<div id="sample-n">
  ## SAMPLE N
</div>

ここで `n` は十分に大きな整数です。たとえば、`SAMPLE 10000000` のように指定します。

この場合、クエリは少なくとも `n` 行のサンプルに対して実行されます (ただし、それを大幅に超えることはありません) 。たとえば、`SAMPLE 10000000` では、最低 10,000,000 行を対象にクエリが実行されます。

データ読み取りの最小単位は 1 つの granule (そのサイズは `index_granularity` 設定で指定されます) であるため、granule のサイズよりも十分に大きいサンプルを設定するのが適切です。

`SAMPLE n` 句を使用する場合、データのうち相対的に何パーセントが処理されたかはわかりません。そのため、集約関数に掛けるべき係数もわかりません。近似結果を取得するには、`_sample_factor` 仮想カラムを使用してください。

`_sample_factor` カラムには、動的に計算される相対係数が含まれます。このカラムは、指定したサンプリングキーを持つテーブルを[作成](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)すると自動的に作成されます。`_sample_factor` カラムの使用例を以下に示します。

サイト訪問の統計を含む `visits` テーブルについて考えてみましょう。最初の例では、ページビュー数の計算方法を示します。

```sql
SELECT sum(PageViews * _sample_factor)
FROM visits
SAMPLE 10000000
```

次の例は、訪問数の合計を計算する方法を示しています。

```sql
SELECT sum(_sample_factor)
FROM visits
SAMPLE 10000000
```

以下の例では、平均セッション時間の計算方法を示します。平均値の計算に相対係数を使用する必要はない点に注意してください。

```sql
SELECT avg(Duration)
FROM visits
SAMPLE 10000000
```

<div id="sample-k-offset-m">
  ## SAMPLE K OFFSET M
</div>

ここで `k` と `m` は 0 以上 1 以下の数です。以下に例を示します。

**例 1**

```sql
SAMPLE 1/10
```

この例では、サンプルは全データの10分の1です。

`[++------------]`

**例 2**

```sql
SAMPLE 1/10 OFFSET 1/2
```

ここでは、データの後半部分から 10% をサンプリングします。

`[------++------]`