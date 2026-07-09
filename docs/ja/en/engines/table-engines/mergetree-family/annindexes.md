---
description: '厳密ベクトル検索と近似ベクトル検索のドキュメント'
keywords: ['ベクトル類似度検索', 'ann', 'knn', 'hnsw', '索引', '索引', '最近傍', 'ベクトル検索']
sidebar_label: '厳密ベクトル検索と近似ベクトル検索'
slug: /engines/table-engines/mergetree-family/annindexes
title: '厳密ベクトル検索と近似ベクトル検索'
doc_type: 'guide'
---

多次元の (ベクトル) 空間において、ある点に対して最も近い N 個の点を見つける問題は、[最近傍探索](https://en.wikipedia.org/wiki/Nearest_neighbor_search)、または略してベクトル検索と呼ばれます。
ベクトル検索を実現する一般的なアプローチは 2 つあります。

* 厳密ベクトル検索では、与えられた点とベクトル空間内のすべての点との距離を計算します。これにより最高レベルの精度が得られ、返される点が真の最近傍であることが保証されます。ベクトル空間全体を網羅的に探索するため、厳密ベクトル検索は実運用では遅すぎる場合があります。
* 近似ベクトル検索は、厳密ベクトル検索よりも大幅に高速に結果を計算する一連の手法 (たとえば、グラフやランダムフォレストのような特殊なデータ構造) を指します。結果の精度は通常、実用上は &quot;十分に高い&quot; とされています。多くの近似手法では、結果精度と検索時間のトレードオフを調整するためのパラメーターを利用できます。

ベクトル検索 (厳密または近似) は、SQL では次のように記述できます。

```sql
WITH [...] AS reference_vector
SELECT [...]
FROM table
WHERE [...] -- a WHERE clause is optional
ORDER BY <DistanceFunction>(vectors, reference_vector)
LIMIT <N>
```

ベクトル空間内の点は、配列型のカラム `vectors` に格納されます。たとえば、[Array(Float64)](../../../sql-reference/data-types/array.md)、[Array(Float32)](../../../sql-reference/data-types/array.md)、または [Array(BFloat16)](../../../sql-reference/data-types/array.md) です。
参照ベクトルは定数の配列で、共通テーブル式として指定します。
`<DistanceFunction>` は、参照点と格納されているすべての点の間の距離を計算します。
これには、利用可能な任意の[距離関数](/ja/sql-reference/functions/distance-functions)を使用できます。
`<N>` は、返す近傍の数を指定します。

<div id="exact-nearest-neighbor-search">
  ## 厳密ベクトル検索
</div>

上記の SELECT クエリをそのまま使用して、厳密ベクトル検索 を実行できます。
このようなクエリの実行時間は、一般に格納されているベクトル数とその次元、つまり配列要素数に比例します。
また、ClickHouse はすべてのベクトルに対して総当たりスキャンを実行するため、実行時間はクエリで使用されるスレッド数にも依存します (設定 [max&#95;threads](../../../operations/settings/settings.md#max_threads) を参照) 。

<div id="exact-nearest-neighbor-search-example">
  ### 例
</div>

```sql
CREATE TABLE tab(id Int32, vec Array(Float32)) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]), (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

WITH [0., 2.] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3;
```

戻り値

```result
   ┌─id─┬─vec─────┐
1. │  6 │ [0,2]   │
2. │  7 │ [0,2.1] │
3. │  8 │ [0,2.2] │
   └────┴─────────┘
```

<div id="approximate-nearest-neighbor-search">
  ## 近似ベクトル検索
</div>

<div id="vector-similarity-index">
  ### ベクトル類似度索引
</div>

ClickHouse では、近似ベクトル検索を行うための特別な「ベクトル類似度」索引を提供しています。

:::note
ベクトル類似度索引は、ClickHouse バージョン 25.8 以降で利用できます。
問題が発生した場合は、[ClickHouse リポジトリ](https://github.com/clickhouse/clickhouse/issues)で issue を登録してください。
:::

<div id="creating-a-vector-similarity-index">
  #### ベクトル類似度索引の作成
</div>

ベクトル類似度索引は、次のように新しいテーブルに作成できます。

```sql
CREATE TABLE table
(
  [...],
  vectors Array(Float*),
  INDEX <index_name> vectors TYPE vector_similarity(<type>, <distance_function>, <dimensions>) [GRANULARITY <N>]
)
ENGINE = MergeTree
ORDER BY [...]
```

または、既存のテーブルにベクトル類似度索引を追加する場合は、次のようにします。

```sql
ALTER TABLE table ADD INDEX <index_name> vectors TYPE vector_similarity(<type>, <distance_function>, <dimensions>) [GRANULARITY <N>];
```

ベクトル類似度索引は、特殊な種類のスキッピング索引です ([こちら](mergetree.md#table_engine-mergetree-data_skipping-indexes)および[こちら](../../../optimize/skipping-indexes)を参照) 。
したがって、上記の `ALTER TABLE` ステートメントでは、テーブルに今後挿入される新しいデータに対してのみ索引が構築されます。
既存のデータに対しても索引を構築するには、マテリアライズが必要です：

```sql
ALTER TABLE table MATERIALIZE INDEX <index_name> SETTINGS mutations_sync = 2;
```

関数 `<distance_function>` は以下のいずれかである必要があります

* `L2Distance`： [ユークリッド距離](https://en.wikipedia.org/wiki/Euclidean_distance) で、ユークリッド空間内の2点を結ぶ線分の長さを表します。
* `cosineDistance`： [コサイン距離](https://en.wikipedia.org/wiki/Cosine_similarity#Cosine_distance) で、2つの非ゼロベクトルのなす角を表します。
* `dotProduct`： [内積](https://en.wikipedia.org/wiki/Dot_product) (inner product) で、2つのベクトルの要素ごとの積の総和を表します。正規化されたデータでは `cosineDistance` と等価です。

正規化されたデータには通常`L2Distance`が最適です。正規化されていない場合は、スケールの影響を補正するために`cosineDistance`の使用を推奨します。

:::note
距離関数 `L2Distance` および `cosineDistance` では、値が小さいほど類似度が高くなりますが、`dotProduct` では、値が大きいほど類似度が高くなります。
そのため、`L2Distance` および `cosineDistance` を使用したベクトル索引は `SELECT [...] ORDER BY [...] ASC` クエリ (`ASC` は `ORDER BY` のデフォルト) でのみ利用可能であり、`dotProduct` を使用して構築されたベクトル索引は `SELECT [...] ORDER BY [...] DESC` クエリでのみ利用可能です。
:::

`<dimensions>` は、基となるカラムのArrayのカーディナリティ (要素数) を指定します。
索引の作成中に異なるカーディナリティのArrayが見つかった場合、ClickHouse はその索引を破棄してエラーを返します。

オプションの GRANULARITY パラメータ `<N>` は、インデックスグラニュールのサイズを指定します ([こちら](../../../optimize/skipping-indexes)を参照) 。
デフォルトのインデックスグラニュラリティとして 1 を使用する通常のスキップ索引とは異なり、ベクトル類似度索引はデフォルトのインデックスグラニュラリティとして 1 億を使用します。
この値により、大きなパーツに対しても内部的に構築される索引の数が少なく抑えられます。
インデックスグラニュラリティの変更は、その影響を十分に理解している上級ユーザーのみに推奨します ([以下](#differences-to-regular-skipping-indexes)を参照) 。

ベクトル類似度索引は、さまざまな近似検索手法に対応できるという意味で汎用的です。
実際に使用する手法は、パラメーター `<type>` で指定します。
現時点で利用可能な手法は HNSW ([学術論文](https://arxiv.org/abs/1603.09320)) のみです。HNSW は、階層的近接グラフに基づく近似ベクトル検索のための、広く普及した最先端の手法です。
HNSW をタイプとして使用する場合、HNSW 固有のパラメーターをオプションで追加指定できます。

```sql
CREATE TABLE table
(
  [...],
  vectors Array(Float*),
  INDEX index_name vectors TYPE vector_similarity('hnsw', <distance_function>, <dimensions>[, <quantization>, <hnsw_max_connections_per_layer>, <hnsw_candidate_list_size_for_construction>]) [GRANULARITY N]
)
ENGINE = MergeTree
ORDER BY [...]
```

利用可能なHNSW固有のパラメータは以下のとおりです：

* `<quantization>` は、近接グラフ内のベクトルの量子化を制御します。設定可能な値は `f64`、`f32`、`f16`、`bf16`、`i8`、`b1` です。デフォルト値は `bf16` です。このパラメータは、基盤となるカラム内でのベクトル表現には影響しないことに注意してください。
* `<hnsw_max_connections_per_layer>` は、グラフの各ノードにおける近傍ノード数 (HNSW のハイパーパラメータ `M` とも呼ばれます) を制御します。デフォルト値は `32` です。値 `0` はデフォルト値を使用することを意味します。
* `<hnsw_candidate_list_size_for_construction>` は、HNSW グラフの構築時における動的候補リストのサイズ (HNSW のハイパーパラメータ `ef_construction` とも呼ばれます) を制御します。デフォルト値は `128` です。値 `0` はデフォルト値を使用することを意味します。

すべての HNSW 固有パラメータのデフォルト値は、ほとんどのユースケースで十分に適切に機能します。
そのため、HNSW 固有パラメータをカスタマイズすることは推奨していません。

さらに、以下の制限があります。

* ベクトル類似度索引は、型が [Array(Float32)](../../../sql-reference/data-types/array.md)、[Array(Float64)](../../../sql-reference/data-types/array.md)、または [Array(BFloat16)](../../../sql-reference/data-types/array.md) のカラムにのみ作成できます。`Array(Nullable(Float32))` や `Array(LowCardinality(Float32))` のような nullable や low-cardinality の浮動小数点数の Array は使用できません。
* ベクトル類似度索引は、単一のカラムに対して作成する必要があります。
* ベクトル類似度索引は計算式に対して作成することもできます (例: `INDEX index_name arraySort(vectors) TYPE vector_similarity([...])`) が、そのような索引は後から近似近傍探索には使用できません。
* ベクトル類似度索引では、基になるカラム内のすべての配列が `<dimension>` 個の要素を持っている必要があります。これは索引の作成時にチェックされます。この要件への違反をできるだけ早く検出するために、ユーザーはベクトルカラムに [制約](/ja/sql-reference/statements/create/table.md#constraints) を追加できます。例: `CONSTRAINT same_length CHECK length(vectors) = 256`。
* 同様に、基になるカラム内の配列値は空 (`[]`) であってはならず、デフォルト値 (これも `[]`) を持つこともできません。

**ストレージ使用量とメモリ消費量の見積もり**

一般的な AI モデル (例: 大規模言語モデル、[LLMs](https://en.wikipedia.org/wiki/Large_language_model)) で使用するために生成されるベクトルは、数百から数千の浮動小数点値で構成されます。
そのため、単一のベクトル値でも複数キロバイトのメモリを消費する場合があります。
テーブル内の基になるベクトルカラムに必要なストレージ量と、ベクトル類似度索引に必要なメインメモリを見積もりたい場合は、以下の 2 つの式を使用できます。

テーブル内のベクトルカラムのストレージ消費量 (非圧縮) :

```text
Storage consumption = Number of vectors * Dimension * Size of column data type
```

[dbpedia dataset](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M)の例：

```text
Storage consumption = 1 million * 1536 * 4 (for Float32) = 6.1 GB
```

検索を実行するには、ベクトル類似度索引全体をディスクから主記憶に読み込む必要があります。
同様に、ベクトル索引も主記憶上で全体を構築してから、ディスクに保存されます。

ベクトル索引の読み込みに必要なメモリ使用量:

```text
Memory for vectors in the index (mv) = Number of vectors * Dimension * Size of quantized data type
Memory for in-memory graph (mg) = Number of vectors * hnsw_max_connections_per_layer * Bytes_per_node_id (= 4) * Layer_node_repetition_factor (= 2)

Memory consumption: mv + mg
```

[dbpedia dataset](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M) の例:

```text
Memory for vectors in the index (mv) = 1 million * 1536 * 2 (for BFloat16) = 3072 MB
Memory for in-memory graph (mg) = 1 million * 64 * 2 * 4 = 512 MB

Memory consumption = 3072 + 512 = 3584 MB
```

上記の式には、ベクトル類似度索引が事前割り当てされたバッファや cache などのランタイムデータ構造を確保するために必要な追加メモリは含まれていません。

<div id="using-a-vector-similarity-index">
  #### ベクトル類似度索引の使用
</div>

:::note
ベクトル類似度索引を使用するには、[compatibility](../../../operations/settings/settings.md) の設定値が `''` (デフォルト値) 、または `'25.1'` 以降である必要があります。
:::

ベクトル類似度索引は、次の形式の SELECT クエリをサポートします。

```sql
WITH [...] AS reference_vector
SELECT [...]
FROM table
WHERE [...] -- a WHERE clause is optional
ORDER BY <DistanceFunction>(vectors, reference_vector)
LIMIT <N>
```

ClickHouseのクエリオプティマイザは、クエリが上記のクエリテンプレートに合致するかどうかを確認し、利用可能なベクトル類似度索引を使用しようとします。
クエリがベクトル類似度索引を使用できるのは、SELECTクエリ内の距離関数が索引定義内の距離関数と同じ場合に限られます。

上級ユーザーは、検索時の候補リストのサイズを調整するために、設定 [hnsw&#95;candidate&#95;list&#95;size&#95;for&#95;search](../../../operations/settings/settings.md#hnsw_candidate_list_size_for_search) (HNSWハイパーパラメータ &quot;ef&#95;search&quot; とも呼ばれます) に独自の値を指定できます (例:  `SELECT [...] SETTINGS hnsw_candidate_list_size_for_search = <value>`) 。
この設定のデフォルト値 256 は、ほとんどのユースケースで十分に機能します。
設定値を大きくすると、パフォーマンスは低下しますが、その分精度は向上します。

クエリがベクトル類似度索引を使用できる場合、ClickHouseはSELECTクエリで指定された LIMIT `<N>` が妥当な範囲内にあることを確認します。
より具体的には、`<N>` が設定 [max&#95;limit&#95;for&#95;vector&#95;search&#95;queries](../../../operations/settings/settings.md#max_limit_for_vector_search_queries) の値 (デフォルト値は 100) を超えると、エラーが返されます。
LIMIT の値が大きすぎると検索が遅くなる可能性があり、通常は使い方の誤りを示しています。

SELECTクエリがベクトル類似度索引を使用しているかどうかを確認するには、クエリの先頭に `EXPLAIN indexes = 1` を付けます。

例として、クエリ

```sql
EXPLAIN indexes = 1
WITH [0.462, 0.084, ..., -0.110] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 10;
```

返される場合があります

```result
    ┌─explain─────────────────────────────────────────────────────────────────────────────────────────┐
 1. │ Expression (Project names)                                                                      │
 2. │   Limit (preliminary LIMIT (without OFFSET))                                                    │
 3. │     Sorting (Sorting for ORDER BY)                                                              │
 4. │       Expression ((Before ORDER BY + (Projection + Change column names to column identifiers))) │
 5. │         ReadFromMergeTree (default.tab)                                                         │
 6. │         Indexes:                                                                                │
 7. │           PrimaryKey                                                                            │
 8. │             Condition: true                                                                     │
 9. │             Parts: 1/1                                                                          │
10. │             Granules: 575/575                                                                   │
11. │           Skip                                                                                  │
12. │             Name: idx                                                                           │
13. │             Description: vector_similarity GRANULARITY 100000000                                │
14. │             Parts: 1/1                                                                          │
15. │             Granules: 10/575                                                                    │
    └─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

この例では、1536 次元のベクトル 100 万件が [dbpedia dataset](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M) の 575 個のグラニュールに格納されており、1 グラニュールあたり約 1.7k 行になります。
このクエリは 10 個の近傍を要求し、ベクトル類似度索引はそれら 10 個の近傍を 10 個の別々のグラニュール内で見つけます。
これら 10 個のグラニュールは、クエリ実行時に読み取られます。

出力に `Skip` とベクトル索引の名前および型 (この例では `idx` と `vector_similarity`) が含まれていれば、ベクトル類似度索引が使用されています。
この場合、ベクトル類似度索引は 4 個のグラニュールのうち 2 個を除外しており、つまりデータの 50% を削減しています。
除外できるグラニュールが多いほど、索引の利用効果は高くなります。

:::tip
索引の使用を強制するには、[force&#95;data&#95;skipping&#95;indexes](../../../operations/settings/settings#force_data_skipping_indices) 設定を指定して SELECT クエリを実行できます (設定値として索引名を指定します) 。
:::

**ポストフィルタリングとプレフィルタリング**

ユーザーは、SELECT クエリに対して追加のフィルタ条件を含む `WHERE` 句を任意で指定できます。
ClickHouse は、ポストフィルタリングまたはプレフィルタリング戦略を使用して、これらのフィルタ条件を評価します。
要するに、どちらの戦略もフィルタの評価順序を決定します。

* ポストフィルタリングでは、最初にベクトル類似度索引が評価され、その後に ClickHouse が `WHERE` 句で指定された追加のフィルタを評価します。
* プレフィルタリングでは、フィルタの評価順序が逆になります。

これらの戦略には、それぞれ異なるトレードオフがあります。

* ポストフィルタリングには、`LIMIT <N>` 句で要求した行数に満たない結果しか返らない可能性があるという一般的な問題があります。これは、ベクトル類似度索引が返した結果行のうち、1 行以上が追加のフィルタを満たさない場合に発生します。
* プリフィルタリングは、一般には未解決の問題です。一部の特化型ベクトルデータベースではプリフィルタリングのアルゴリズムが提供されていますが、ほとんどのリレーショナルデータベース (ClickHouse を含む) では、正確な近傍探索、つまり索引を使わない総当たりスキャンにフォールバックします。

どの戦略が使われるかは、フィルタ条件によって決まります。

*追加のフィルタがパーティションキーの一部である場合*

追加のフィルタ条件がパーティションキーの一部であれば、ClickHouse はパーティションプルーニングを適用します。
例として、テーブルがカラム `year` で範囲パーティション化されていて、次のクエリを実行するとします。

```sql
WITH [0., 2.] AS reference_vec
SELECT id, vec
FROM tab
WHERE year = 2025
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3;
```

ClickHouse は 2025 のパーティション以外をすべて枝刈りします。

*追加フィルタを索引で評価できない場合*

追加のフィルタ条件を索引 (主キー索引、スキッピング索引) で評価できない場合、ClickHouse はポストフィルタリングを適用します。

*追加フィルタを主キー索引で評価できる場合*

追加のフィルタ条件を[主キー](mergetree.md#primary-key)で評価できる場合 (つまり、それらが主キーのプレフィックスを構成する場合) で、かつ

* フィルタ条件によって パーツ 内で少なくとも 1 行が除外される場合、ClickHouse はその パーツ 内の「残った」範囲に対してプレフィルタリングにフォールバックします。
* フィルタ条件によって パーツ 内で 1 行も除外されない場合、ClickHouse はその パーツ に対してポストフィルタリングを実行します。

実際のユースケースでは、後者のケースが発生する可能性はかなり低いです。

*追加フィルタをスキッピング索引で評価できる場合*

追加のフィルタ条件を[スキッピング索引](mergetree.md#table_engine-mergetree-data_skipping-indexes) (minmax index、set index など) で評価できる場合、ClickHouse はポストフィルタリングを実行します。
このような場合は、他のスキッピング索引よりも多くの行を除外すると見込まれるため、ベクトル類似度索引が最初に評価されます。

ポストフィルタリングとプレフィルタリングをより細かく制御するために、2 つの設定を使用できます。

設定 [vector&#95;search&#95;filter&#95;strategy](../../../operations/settings/settings#vector_search_filter_strategy) (デフォルト: `auto`。上記のヒューリスティクスを実装) は `prefilter` に設定できます。
これは、追加のフィルタ条件の選択性が極めて高い場合に、プレフィルタリングを強制するのに役立ちます。
たとえば、次のクエリはプレフィルタリングの恩恵を受ける可能性があります。

```sql
SELECT bookid, author, title
FROM books
WHERE price < 2.00
ORDER BY cosineDistance(book_vector, getEmbedding('Books on ancient Asian empires'))
LIMIT 10
```

2ドル未満の本がごく少数しかない場合、ベクトル索引から返される上位10件の一致結果がすべて2ドルを超えている可能性があるため、ポストフィルタリングでは0行になることがあります。
プレフィルタリングを強制すると (クエリに `SETTINGS vector_search_filter_strategy = 'prefilter'` を追加) 、ClickHouse はまず価格が2ドル未満の本をすべて見つけ、その後、見つかった本に対して総当たりのベクトル検索を実行します。

上記の問題を解決する別の方法として、[vector&#95;search&#95;index&#95;fetch&#95;multiplier](../../../operations/settings/settings#vector_search_index_fetch_multiplier) (デフォルト: `1.0`、最大: `1000.0`) を `1.0` より大きい値 (たとえば `2.0`) に設定することもできます。
ベクトル索引から取得される最近傍の数はこの設定値に応じて増やされ、その後、それらの行に追加のフィルタが適用されて LIMIT 件の行が返されます。
たとえば、乗数 `3.0` を指定して再度クエリを実行できます。

```sql
SELECT bookid, author, title
FROM books
WHERE price < 2.00
ORDER BY cosineDistance(book_vector, getEmbedding('Books on ancient Asian empires'))
LIMIT 10
SETTING vector_search_index_fetch_multiplier = 3.0;
```

ClickHouse は各 パーツ のベクトル索引から 3.0 x 10 = 30 個の最近傍を取得し、その後で追加のフィルタを適用します。
返されるのは、そのうち最も近い 10 個の近傍だけです。
`vector_search_index_fetch_multiplier` を設定することでこの問題は緩和できますが、極端な場合 (WHERE 条件の選択性が非常に高い場合) には、要求した N 行より少ない行数しか返されない可能性があります。

**再スコアリング**

ClickHouse のスキップ索引は、通常グラニュール単位でフィルタリングを行います。つまり、スキップ索引でのルックアップは (内部的には) 一致する可能性のあるグラニュールの一覧を返し、その後のスキャンで読み取るデータ量を削減します。
これは通常のスキップ索引ではうまく機能しますが、ベクトル類似度索引では &quot;粒度のミスマッチ&quot; が生じます。
具体的には、ベクトル類似度索引は、与えられた参照ベクトルに対して最も類似する上位 N 個のベクトルの行番号を特定しますが、その後それらの行番号をグラニュール番号に外挿する必要があります。
その後、ClickHouse はそれらのグラニュールをディスクから読み込み、それらのグラニュール内にあるすべてのベクトルに対して距離計算を再度行います。
このステップは再スコアリングと呼ばれます。理論上は精度を向上させる可能性がありますが、ベクトル類似度索引が返すのはあくまで *近似* 結果であることを踏まえても、性能面で最適とは言えません。

そのため ClickHouse には、再スコアリングを無効にし、最も類似するベクトルとその距離を索引から直接返す最適化が用意されています。
この最適化はデフォルトで有効です。設定 [vector&#95;search&#95;with&#95;rescoring](../../../operations/settings/settings#vector_search_with_rescoring) を参照してください。
大まかには、ClickHouse は最も類似するベクトルとその距離を仮想カラム `_distances` として利用できるようにします。
これを確認するには、`EXPLAIN header = 1` を付けてベクトル検索クエリを実行します。

```sql
EXPLAIN header = 1
WITH [0., 2.] AS reference_vec
SELECT id
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3
SETTINGS vector_search_with_rescoring = 0
```

```result
Query id: a2a9d0c8-a525-45c1-96ca-c5a11fa66f47

    ┌─explain─────────────────────────────────────────────────────────────────────────────────────────────────┐
 1. │ Expression (Project names)                                                                              │
 2. │ Header: id Int32                                                                                        │
 3. │   Limit (preliminary LIMIT (without OFFSET))                                                            │
 4. │   Header: L2Distance(__table1.vec, _CAST([0., 2.]_Array(Float64), 'Array(Float64)'_String)) Float64     │
 5. │           __table1.id Int32                                                                             │
 6. │     Sorting (Sorting for ORDER BY)                                                                      │
 7. │     Header: L2Distance(__table1.vec, _CAST([0., 2.]_Array(Float64), 'Array(Float64)'_String)) Float64   │
 8. │             __table1.id Int32                                                                           │
 9. │       Expression ((Before ORDER BY + (Projection + Change column names to column identifiers)))         │
10. │       Header: L2Distance(__table1.vec, _CAST([0., 2.]_Array(Float64), 'Array(Float64)'_String)) Float64 │
11. │               __table1.id Int32                                                                         │
12. │         ReadFromMergeTree (default.tab)                                                                 │
13. │         Header: id Int32                                                                                │
14. │                 _distance Float32                                                                       │
    └─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

:::note
再スコアリングを行わず (`vector_search_with_rescoring = 0`)、かつ並列レプリカが有効になっている状態で実行したクエリでも、再スコアリングにフォールバックする場合があります。
:::

<div id="performance-tuning">
  #### パフォーマンスチューニング
</div>

**圧縮の調整**

ほぼすべてのユースケースで、元のカラム内のベクトルは高密度で、圧縮してもあまり効果がありません。
その結果、[圧縮](/ja/sql-reference/statements/create/table.md#column_compression_codec) を有効にすると、ベクトルカラムへの書き込みとベクトルカラムからの読み取りが遅くなります。
そのため、圧縮は無効にすることを推奨します。
そのためには、次のようにベクトルカラムに `CODEC(NONE)` を指定します。

```sql
CREATE TABLE tab(id Int32, vec Array(Float32) CODEC(NONE), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id;
```

**索引作成のチューニング**

ベクトル類似度索引のライフサイクルは、パーツのライフサイクルと連動しています。
つまり、ベクトル類似度索引が定義された新しいパーツが作成されるたびに、その索引も作成されます。
これは通常、データが[挿入](https://clickhouse.com/docs/guides/inserting-data)されるとき、または[マージ](https://clickhouse.com/docs/merges)時に発生します。
残念ながら、HNSW は索引の作成に長い時間がかかることで知られており、挿入やマージを大幅に遅くする可能性があります。
ベクトル類似度索引は、データが不変であるか、ほとんど変更されない場合にのみ使用するのが理想的です。

索引作成を高速化するには、次の手法を利用できます。

まず、索引作成は並列化できます。
索引作成スレッドの最大数は、サーバー設定 [max&#95;build&#95;vector&#95;similarity&#95;index&#95;thread&#95;pool&#95;size](/ja/operations/server-configuration-parameters/settings#max_build_vector_similarity_index_thread_pool_size) で設定できます。
最適なパフォーマンスを得るには、この設定値を CPU コア数に合わせる必要があります。

次に、INSERT ステートメントを高速化するために、ユーザーはセッション設定 [materialize&#95;skip&#95;indexes&#95;on&#95;insert](../../../operations/settings/settings.md#materialize_skip_indexes_on_insert) を使用して、新たに挿入されたパーツでのスキッピング索引の作成を無効にできます。
そのようなパーツに対する SELECT クエリは、厳密検索にフォールバックします。
挿入されたパーツは通常、テーブル全体のサイズと比べて小さいため、これによるパフォーマンスへの影響はごくわずかと見込まれます。

3 つ目に、マージを高速化するために、ユーザーはセッション設定 [materialize&#95;skip&#95;indexes&#95;on&#95;merge](../../../operations/settings/merge-tree-settings.md#materialize_skip_indexes_on_merge) を使用して、マージ済みパーツでのスキッピング索引の作成を無効にできます。
これにステートメント [ALTER TABLE [...] MATERIALIZE INDEX [...]](../../../sql-reference/statements/alter/skipping-index.md#materialize-index) を組み合わせることで、ベクトル類似度索引のライフサイクルを明示的に制御できます。
たとえば、すべてのデータが取り込まれるまで、あるいは週末のようなシステム負荷の低い時間帯まで、索引作成を遅らせることができます。

**索引利用のチューニング**

SELECT クエリでベクトル類似度索引を使用するには、それを主記憶に読み込む必要があります。
同じベクトル類似度索引が主記憶に繰り返し読み込まれるのを避けるため、ClickHouse はこの種の索引専用のインメモリ cache を提供しています。
この cache が大きいほど、不要な読み込みは少なくなります。
cache の最大サイズは、サーバー設定 [vector&#95;similarity&#95;index&#95;cache&#95;size](../../../operations/server-configuration-parameters/settings.md#vector_similarity_index_cache_size) で設定できます。
デフォルトでは、cache は最大 5 GB まで拡張できます。

次のログメッセージ (`system.text_log`) は、ベクトル類似度索引が読み込まれていることを示します。
このようなメッセージが異なるベクトル検索クエリで繰り返し現れる場合は、cache サイズが小さすぎることを示しています。

```text
2026-02-03 07:39:10.351635 [1386] f0ac5c85-1b1c-4f35-8848-87a1d1aa00ba : VectorSimilarityIndex Start loading vector similarity index

<...>

2026-02-03 07:40:25.217603 [1386] f0ac5c85-1b1c-4f35-8848-87a1d1aa00ba : VectorSimilarityIndex Loaded vector similarity index: max_level = 2, connectivity = 64, size = 1808111, capacity = 1808111, memory_usage = 8.00 GiB, bytes_per_vector = 4096, scalar_words = 1024, nodes = 1808111, edges = 51356964, max_edges = 233395072
```

:::note
ベクトル類似度索引キャッシュには、ベクトル索引グラニュールが格納されます。
個々のベクトル索引グラニュールがキャッシュサイズより大きい場合、それらはキャッシュされません。
そのため、ベクトル索引のサイズ (「ストレージ容量とメモリ消費量の見積もり」の計算式、または [system.data&#95;skipping&#95;indices](../../../operations/system-tables/data_skipping_indices) に基づく) を必ず算出し、それに応じてキャッシュサイズを設定してください。
:::

*ベクトル検索クエリの遅延を調査する際は、まずベクトル索引キャッシュを確認し、必要に応じて増やすことを最初のステップとしてください。*

現在のベクトル類似度索引キャッシュのサイズは、[system.metrics](../../../operations/system-tables/metrics.md) に表示されます。

```sql
SELECT metric, value
FROM system.metrics
WHERE metric = 'VectorSimilarityIndexCacheBytes'
```

特定の query id を持つクエリの cache のヒット数とミス数は、[system.query&#95;log](../../../operations/system-tables/query_log.md) から取得できます。

```sql
SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['VectorSimilarityIndexCacheHits'], ProfileEvents['VectorSimilarityIndexCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish' AND query_id = '<...>'
ORDER BY event_time_microseconds;
```

本番環境での利用では、すべてのベクトル索引が常にメモリ上に保持されるよう、cache を十分なサイズにすることを推奨します。

**量子化の調整**

[量子化](https://huggingface.co/blog/embedding-quantization)は、ベクトルのメモリ使用量と、ベクトル索引の構築および探索にかかる計算コストを削減するための手法です。
ClickHouse のベクトル索引では、次の量子化オプションをサポートしています。

| 量子化            | 名前                 | 次元あたりのストレージ |
| -------------- | ------------------ | ----------- |
| f32            | 単精度                | 4 バイト       |
| f16            | 半精度                | 2 バイト       |
| bf16 (default) | 半精度 (brain float)  | 2 バイト       |
| i8             | 1/4精度              | 1 バイト       |
| b1             | バイナリ               | 1 ビット       |

量子化を行うと、元のフル精度浮動小数点値 (`f32`) で検索する場合と比べて、ベクトル検索の精度は低下します。
ただし、ほとんどのデータセットでは、半精度 brain float 量子化 (`bf16`) による精度低下はごくわずかです。そのため、ベクトル類似度索引ではこの量子化手法がデフォルトで使用されます。
1/4精度 (`i8`) およびバイナリ (`b1`) 量子化では、ベクトル検索の精度低下が無視できなくなります。
この 2 つの量子化は、ベクトル類似度索引のサイズが利用可能な DRAM 容量を大幅に上回る場合にのみ推奨します。
この場合は、精度を向上させるために rescoring ([vector&#95;search&#95;index&#95;fetch&#95;multiplier](../../../operations/settings/settings#vector_search_index_fetch_multiplier)、[vector&#95;search&#95;with&#95;rescoring](../../../operations/settings/settings#vector_search_with_rescoring)) も有効にすることを推奨します。
バイナリ量子化は、1) 正規化された埋め込み (つまりベクトル長 = 1。OpenAI モデルは通常正規化されています) であり、かつ 2) 距離関数としてコサイン距離を使用する場合にのみ推奨されます。
バイナリ量子化では、近接グラフの構築と検索に内部的にハミング距離を使用します。
rescoring のステップでは、テーブルに格納されている元のフル精度ベクトルを使用して、コサイン距離により最近傍を特定します。

**データ転送の調整**

ベクトル検索クエリの参照ベクトルはユーザーが指定し、通常は大規模言語モデル (LLM) を呼び出して取得します。
ClickHouse でベクトル検索を実行する典型的な Python コードは、次のようになります

```python
search_v = openai_client.embeddings.create(input = "[Good Books]", model='text-embedding-3-large', dimensions=1536).data[0].embedding

params = {'search_v': search_v}
result = chclient.query(
   "SELECT id FROM items
    ORDER BY cosineDistance(vector, %(search_v)s)
    LIMIT 10",
    parameters = params)
```

埋め込みベクトル (上記のスニペットでは `search_v`) は、非常に大きな次元を持つことがあります。
たとえば、OpenAI は 1536 次元、さらには 3072 次元の埋め込みベクトルを生成するモデルを提供しています。
上記のコードでは、ClickHouse Python ドライバーが埋め込みベクトルを人間が読める文字列に置き換え、その後 `SELECT` クエリ全体を文字列として送信します。
埋め込みベクトルが 1536 個の単精度浮動小数点値で構成されているとすると、送信される文字列の長さは 20 kB に達します。
そのため、トークン化、パース、および数千回に及ぶ文字列から浮動小数点数への変換によって、CPU 使用率が高くなります。
また、ClickHouse サーバーのログファイルにもかなりの領域が必要になり、`system.query_log` も肥大化します。

ほとんどの LLM モデルは、埋め込みベクトルをネイティブの浮動小数点数のリストまたは NumPy 配列として返します。
そのため、Python アプリケーションでは、以下のスタイルを使用して参照ベクトルのパラメーターをバイナリ形式でバインドすることを推奨します。

```python
search_v = openai_client.embeddings.create(input = "[Good Books]", model='text-embedding-3-large', dimensions=1536).data[0].embedding

params = {'$search_v_binary$': np.array(search_v, dtype=np.float32).tobytes()}
result = chclient.query(
   "SELECT id FROM items
    ORDER BY cosineDistance(vector, reinterpret($search_v_binary$, 'Array(Float32)'))
    LIMIT 10"
    parameters = params)
```

この例では、参照ベクトルはバイナリ形式のまま送信され、サーバー側で浮動小数点数の配列として再解釈されます。
これにより、サーバー側のCPU時間を節約でき、サーバーログや`system.query_log`の肥大化も防げます。

<div id="administration">
  #### 管理と監視
</div>

ベクトル類似度索引のディスク上のサイズは、[system.data&#95;skipping&#95;indices](../../../operations/system-tables/data_skipping_indices) で確認できます。

```sql
SELECT database, table, name, formatReadableSize(data_compressed_bytes)
FROM system.data_skipping_indices
WHERE type = 'vector_similarity';
```

出力例:

```result
┌─database─┬─table─┬─name─┬─formatReadab⋯ssed_bytes)─┐
│ default  │ tab   │ idx  │ 348.00 MB                │
└──────────┴───────┴──────┴──────────────────────────┘
```

<div id="differences-to-regular-skipping-indexes">
  #### 通常のスキッピング索引との違い
</div>

通常の[スキッピング索引](/ja/optimize/skipping-indexes)と同様に、ベクトル類似度索引はグラニュール単位で構築され、各索引ブロックは `GRANULARITY = [N]` 個のグラニュールで構成されます (通常のスキッピング索引では `[N]` のデフォルト値は 1 です) 。
たとえば、テーブルのプライマリインデックスの粒度が 8192 (設定 `index_granularity = 8192`) で、`GRANULARITY = 2` の場合、各索引ブロックには 16384 行が含まれます。
しかし、近似近傍探索のためのデータ構造とアルゴリズムは、本質的に行指向です。
これらは行の集合をコンパクトに表現して保持し、ベクトル検索クエリに対して行を返します。
そのため、ベクトル類似度索引の動作には、通常のスキッピング索引と比べてやや直感に反する違いがあります。

ユーザーがカラムにベクトル類似度索引を定義すると、ClickHouse は内部的に各索引ブロックごとにベクトル類似度の「サブ索引」を作成します。
このサブ索引は、自身が属する索引ブロック内の行しか把握していないという意味で「ローカル」です。
前の例で、あるカラムに 65536 行あるとすると、4 つの索引ブロック (8 つのグラニュールにまたがる) と、各索引ブロックに対応するベクトル類似度サブ索引が作成されます。
理論上、サブ索引はその索引ブロック内で最も近い N 個の点に対応する行を直接返せます。
しかし、ClickHouse はグラニュール単位でデータをディスクからメモリへ読み込むため、サブ索引は一致した行をグラニュール単位に拡張して扱います。
これは、通常のスキッピング索引が索引ブロック単位でデータをスキップするのとは異なります。

`GRANULARITY` パラメータは、作成されるベクトル類似度サブ索引の数を決定します。
`GRANULARITY` の値が大きいほど、ベクトル類似度サブ索引の数は少なくなりますが、各サブ索引はより大きくなり、最終的にはカラム (またはカラムのデータパーツ) にサブ索引が 1 つだけになることもあります。
その場合、そのサブ索引はカラム内のすべての行を「グローバル」に把握でき、関連する行を含むカラム (パーツ) のグラニュールを直接すべて返せます (そのようなグラニュール数は最大でも `LIMIT [N]` 個です) 。
次の段階で、ClickHouse はこれらのグラニュールを読み込み、グラニュール内のすべての行に対して brute-force で距離計算を行うことで、実際に最適な行を特定します。
`GRANULARITY` の値が小さい場合、各サブ索引は最大 `LIMIT N` 個のグラニュールを返します。
その結果、より多くのグラニュールを読み込み、後段でフィルタリングする必要があります。
どちらの場合でも検索精度は同程度に高く、異なるのは処理性能だけである点に注意してください。
一般に、ベクトル類似度索引では大きな `GRANULARITY` を使用し、ベクトル類似度構造のメモリ消費が過大になるといった問題がある場合にのみ、より小さな `GRANULARITY` の値に切り替えることが推奨されます。
ベクトル類似度索引に `GRANULARITY` が指定されていない場合、デフォルト値は 1 億です。

<div id="approximate-nearest-neighbor-search-example">
  #### 例
</div>

クエリ:

```sql title="Query"
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]), (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

WITH [0., 2.] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3;
```

```result title="Response"
   ┌─id─┬─vec─────┐
1. │  6 │ [0,2]   │
2. │  7 │ [0,2.1] │
3. │  8 │ [0,2.2] │
   └────┴─────────┘
```

近似ベクトル検索を使用する、他のサンプルデータセット：

* [LAION-400M](../../../getting-started/example-datasets/laion-400m-dataset)
* [LAION-5B](../../../getting-started/example-datasets/laion-5b-dataset)
* [dbpedia](../../../getting-started/example-datasets/dbpedia-dataset)
* [hackernews](../../../getting-started/example-datasets/hackernews-vector-search-dataset)

<div id="approximate-nearest-neighbor-search-qbit">
  ### Quantized Bit (QBit)
</div>

厳密なベクトル検索を高速化する一般的な方法の 1 つは、より低精度の[float データ型](../../../sql-reference/data-types/float.md)を使用することです。
たとえば、ベクトルを `Array(Float32)` ではなく `Array(BFloat16)` として格納すると、データサイズは半分になり、クエリの実行時間もそれに応じて短くなることが期待されます。
この手法は量子化として知られています。計算は高速化されますが、すべてのベクトルを完全走査していても、結果の精度が低下する可能性があります。

従来の量子化では、検索時とデータ保存時の両方で精度が失われます。上の例では、`Float32` の代わりに `BFloat16` を保存することになるため、後からより高精度な検索を行いたくなっても、それはできません。代替手段の 1 つとして、量子化したデータとフル精度のデータを 2 つ保持する方法があります。これは機能しますが、余分なストレージが必要になります。たとえば、元のデータが `Float64` で、異なる精度 (16-bit、32-bit、またはフル 64-bit) で検索を実行したいケースを考えてみてください。その場合、データを 3 つの別個のコピーとして保存する必要があります。

ClickHouse は、次の方法でこれらの制約を解消する Quantized Bit (`QBit`) データ型を提供しています。

1. 元のフル精度データを保存する。
2. クエリ時に量子化の精度を指定できる。

これは、データをビット単位でグループ化したフォーマット (つまり、すべてのベクトルの i 番目のビットをまとめて保存する形式) で格納することで実現され、必要な精度レベルだけを読み出せるようになります。これにより、必要に応じて元のデータをすべて利用できる状態を保ちながら、量子化による I/O と計算量の削減による高速化の恩恵を受けられます。最大精度を選択した場合、検索は厳密になります。

`QBit` 型のカラムを宣言するには、次の構文を使用します。

```sql
column_name QBit(element_type, dimension[, stride])
```

ここで:

* `element_type` – 各ベクトル要素の型です。サポートされる型は `Int8`、`BFloat16`、`Float32`、`Float64` です
* `dimension` – 各ベクトルの要素数です
* `stride` – 任意です。`dimension` の約数で、次元を `dimension / stride` 個の連続したグループに分割し、それぞれを別々のストリームに格納します。これにより、先頭の次元のみを対象とする検索では読み取るストリーム数を減らせます (Matryoshka embeddings で有用です) 。既定値は `dimension` です。この場合、この型はストライドなしの `QBit` とバイト単位で同一になります。詳細は [`QBit` データ型のページ](/ja/sql-reference/data-types/qbit) を参照してください。

<div id="qbit-create">
  #### `QBit` テーブルの作成とデータの追加
</div>

```sql
CREATE TABLE fruit_animal (
    word String,
    vec QBit(Float64, 5)
) ENGINE = MergeTree
ORDER BY word;

INSERT INTO fruit_animal VALUES
    ('apple', [-0.99105519, 1.28887844, -0.43526649, -0.98520696, 0.66154391]),
    ('banana', [-0.69372815, 0.25587061, -0.88226235, -2.54593015, 0.05300475]),
    ('orange', [0.93338752, 2.06571317, -0.54612565, -1.51625717, 0.69775337]),
    ('dog', [0.72138876, 1.55757105, 2.10953259, -0.33961248, -0.62217325]),
    ('cat', [-0.56611276, 0.52267331, 1.27839863, -0.59809804, -1.26721048]),
    ('horse', [-0.61435682, 0.48542571, 1.21091247, -0.62530446, -1.33082533]);
```

<div id="qbit-search">
  #### `QBit` を使ったベクトル検索
</div>

L2 距離を使用して、単語「lemon」を表すベクトルの最近傍を見つけてみましょう。距離関数の 3 番目のパラメータでは、ビット単位の精度を指定します。値が大きいほど精度は高くなりますが、その分計算量も増えます。

`QBit` で使用可能な距離関数の一覧は[こちら](../../../sql-reference/data-types/qbit.md#vector-search-functions)を参照してください。

**フル精度検索 (64 ビット) :**

```sql
SELECT
    word,
    L2DistanceTransposed(vec, [-0.88693672, 1.31532824, -0.51182908, -0.99652702, 0.59907770], 64) AS distance
FROM fruit_animal
ORDER BY distance;
```

```text
   ┌─word───┬────────────distance─┐
1. │ apple  │ 0.14639757188169716 │
2. │ banana │   1.998961369007679 │
3. │ orange │   2.039041552613732 │
4. │ cat    │   2.752802631487914 │
5. │ horse  │  2.7555776805484813 │
6. │ dog    │   3.382295083120104 │
   └────────┴─────────────────────┘
```

**精度を落とした検索:**

```sql
SELECT
    word,
    L2DistanceTransposed(vec, [-0.88693672, 1.31532824, -0.51182908, -0.99652702, 0.59907770], 12) AS distance
FROM fruit_animal
ORDER BY distance;
```

```text
   ┌─word───┬───────────distance─┐
1. │ apple  │  0.757668703053566 │
2. │ orange │ 1.5499475034938677 │
3. │ banana │ 1.6168396735102937 │
4. │ cat    │  2.429752230904804 │
5. │ horse  │  2.524650475528617 │
6. │ dog    │   3.17766975527459 │
   └────────┴────────────────────┘
```

12ビット量子化では、距離を十分に近似しつつ、クエリをより高速に実行できることがわかります。相対的な順序もおおむね保たれており、&#39;apple&#39; が引き続き最も近い一致となっています。

<div id="qbit-performance">
  #### パフォーマンスに関する考慮事項
</div>

`QBit` の性能上の利点は、精度を低くするとストレージから読み取るデータ量が減るため、I/O 操作を削減できる点にあります。さらに、`QBit` に `Float32` データが含まれている場合、精度パラメーターが 16 以下であれば、計算量の削減による追加のメリットも得られます。精度パラメーターは、精度と速度のトレードオフを直接左右します。

* **高い精度** (元のデータ幅に近い) : 結果はより正確になる一方、クエリは遅くなります
* **低い精度**: 近似結果になりますが、クエリは高速になり、メモリ使用量も削減されます

<div id="references">
  ### 参考資料
</div>

ブログ記事:

* [ClickHouseのベクトル検索 - 第1部](https://clickhouse.com/blog/vector-search-clickhouse-p1)
* [ClickHouseのベクトル検索 - 第2部](https://clickhouse.com/blog/vector-search-clickhouse-p2)
* [クエリ時に精度を選べるベクトル検索エンジンを構築しました](https://clickhouse.com/blog/qbit-vector-search)