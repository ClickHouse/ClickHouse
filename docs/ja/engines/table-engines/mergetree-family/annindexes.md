# 近似最適近傍探索インデックス [エクスペリメンタル]

最適近傍探索は、N次元ベクトル空間で与えられた点に対して最も近いM個の点を見つける問題です。この問題を解決する最も単純なアプローチは、ベクトル空間内のすべての点と参照点の距離を計算するブルートフォース検索です。この方法は完全な精度を保証しますが、実際のアプリケーションにおいては通常、速度が遅すぎます。したがって、最適近傍探索の問題は、多くの場合[近似アルゴリズム](https://github.com/erikbern/ann-benchmarks)で解決されます。近似最適近傍探索技術は、[埋め込みメソッド](https://cloud.google.com/architecture/overview-extracting-and-serving-feature-embeddings-for-machine-learning)と組み合わせて、大量のメディア（画像、曲、記事など）をミリ秒単位で検索することが可能です。

ブログ:
- [ClickHouseでのベクトル検索 - Part 1](https://clickhouse.com/blog/vector-search-clickhouse-p1)
- [ClickHouseでのベクトル検索 - Part 2](https://clickhouse.com/blog/vector-search-clickhouse-p2)

SQLでは、最適近傍問題を次のように表現できます:

``` sql
SELECT *
FROM table
ORDER BY Distance(vectors, Point)
LIMIT N
```

`vectors`は、[Array(Float32)](../../../sql-reference/data-types/array.md)またはArray(Float64)型のN次元の値（例えば埋め込み）を含みます。関数`Distance`は、二つのベクトル間の距離を計算します。距離関数としては、しばしばユークリッド（L2）距離が選ばれますが、[他の距離関数](/docs/ja/sql-reference/functions/distance-functions.md)も可能です。`Point`は参照点、例えば`(0.17, 0.33, ...)`で、`N`は検索結果の数を制限します。

このクエリは、参照点に最も近いトップ`N`個の点を返します。`N`引数は返される値の数を制限し、事前に`MaxDistance`を決定するのが難しい場合に便利です。

ブルートフォース検索では、`vectors`内のすべての点と`Point`間の距離を計算する必要があるため、クエリは高価です（点の数に対して線形です）。このプロセスを速めるために、近似最適近傍探索インデックス（ANNインデックス）は検索空間のコンパクトな表現（クラスタリング、検索ツリーなどの使用）を保存します。これにより、近似的な答えをより迅速に（サブ線形時間で）計算することが可能です。

# ベクトル類似インデックスの作成と使用

[Array(Float32)](../../../sql-reference/data-types/array.md)カラムに対するベクトル類似インデックスを作成する構文:

```sql
CREATE TABLE table
(
  id Int64,
  vectors Array(Float32),
  INDEX index_name vectors TYPE vector_similarity(method, distance_function[, quantization, hnsw_max_connections_per_layer, hnsw_candidate_list_size_for_construction]) [GRANULARITY N]
)
ENGINE = MergeTree
ORDER BY id;
```

パラメータ:
- `method`: 現在は`hnsw`のみサポートしています。
- `distance_function`: `L2Distance`（[ユークリッド距離](https://en.wikipedia.org/wiki/Euclidean_distance) - ユークリッド空間内の二点間の線の長さ）または`cosineDistance`（[コサイン距離](https://en.wikipedia.org/wiki/Cosine_similarity#Cosine_distance)- ゼロでない二つのベクトル間の角度）。
- `quantization`: ベクトルを精度を落として保存するための`f64`、`f32`、`f16`、`bf16`、または`i8`（オプション、デフォルト: `bf16`）
- `hnsw_max_connections_per_layer`: HNSWグラフノードごとの隣接ノード数、[HNSWペーパー](https://doi.org/10.1109/TPAMI.2018.2889473)で`M`として知られています（オプション、デフォルト: 32）
- `hnsw_candidate_list_size_for_construction`: HNSWグラフを構築する際の動的候補リストのサイズ、元の[HNSWペーパー](https://doi.org/10.1109/TPAMI.2018.2889473)で`ef_construction`として知られています（オプション、デフォルト: 128）

`hnsw_max_connections_per_layer`および`hnsw_candidate_list_size_for_construction`の値が0の場合、これらのパラメータのデフォルト値が使用されます。

例:

```sql
CREATE TABLE table
(
  id Int64,
  vectors Array(Float32),
  INDEX idx vectors TYPE vector_similarity('hnsw', 'L2Distance') -- 代替構文: TYPE vector_similarity(hnsw, L2Distance)
)
ENGINE = MergeTree
ORDER BY id;
```

ベクトル類似インデックスは[USearchライブラリ](https://github.com/unum-cloud/usearch)に基づいており、[HNSWアルゴリズム](https://arxiv.org/abs/1603.09320)を実装しています。すなわち、各点がベクトルを表し、辺が類似性を表す階層的なグラフです。このような階層構造は大規模なコレクションに非常に効率的です。全体のデータセットから0.05％以下のデータを取得しながら、99％の再現率を提供することが頻繁にあります。これは高次元ベクトル作業で特に役立ちます。高次元ベクトルはロードと比較にコストがかかるからです。このライブラリはまた、最新のArm（NEONとSVE）およびx86（AVX2とAVX-512）CPUで距離計算をさらに加速するためのハードウェア固有のSIMD最適化や、RAMにロードせずに不変の永続ファイル周りを効率的にナビゲートするためのOS固有の最適化も備えています。

USearchインデックスは現在エクスペリメンタルであり、使用するにはまず`SET allow_experimental_vector_similarity_index = 1`を設定する必要があります。

ベクトル類似インデックスは現在二つの距離関数をサポートしています:
- `L2Distance`、ユークリッド距離とも呼ばれ、ユークリッド空間内の二点間の線分の長さ（[Wikipedia](https://en.wikipedia.org/wiki/Euclidean_distance)）。
- `cosineDistance`、コサイン類似性とも呼ばれ、二つの（ゼロでない）ベクトル間の角度のコサイン（[Wikipedia](https://en.wikipedia.org/wiki/Cosine_similarity)）。

ベクトル類似インデックスはベクトルを精度を落とした形式で保存することができます。サポートされているスカラー型は`f64`、`f32`、`f16`、`bf16`、および`i8`です。インデックス作成時にスカラー型を指定しなかった場合、デフォルトで`bf16`が使用されます。

正規化されたデータには通常`L2Distance`がより良い選択であり、そうでない場合はスケールを補うため`cosineDistance`が推奨されます。インデックス作成時に距離関数が指定されなかった場合、デフォルトで`L2Distance`が使用されます。

:::note
すべての配列は同じ長さである必要があります。エラーを回避するために、例えば`CONSTRAINT constraint_name_1 CHECK length(vectors) = 256`といった[CONSTRAINT](/docs/ja/sql-reference/statements/create/table.md#constraints)を使用することができます。また、`INSERT`文で空の`Arrays`や指定されていない`Array`値（つまりデフォルト値）はサポートされていません。
:::

:::note
現在、ベクトル類似インデックスはテーブルごとに設定される非デフォルトの`index_granularity`設定では機能しません。[こちらを参照](https://github.com/ClickHouse/ClickHouse/pull/51325#issuecomment-1605920475)。必要があれば、config.xmlで値を変更する必要があります。
:::

ベクトルインデックス作成は遅いことが知られています。このプロセスを速めるために、インデックス作成を並列化することができます。スレッドの最大数は、サーバー設定の[max_build_vector_similarity_index_thread_pool_size](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters_max_build_vector_similarity_index_thread_pool_size)で設定できます。

ANNインデックスはカラムの挿入およびマージ時に構築されます。その結果、`INSERT`や`OPTIMIZE`ステートメントは通常のテーブルに比べて遅くなります。ANNインデックスは基本的に不変またはまれにしか変更されないデータにのみ使用され、読み取り要求が書き込み要求よりも多い場合に理想的です。

:::tip
ベクトル類似インデックスの構築コストを削減するには、新しく挿入されたパーツにスキップインデックスの構築を無効にする`materialize_skip_indexes_on_insert`を設定してください。検索は正確な検索にフォールバックしますが、挿入されたパーツは通常テーブル全体のサイズに比べて小さいので、その影響は無視できる程度です。

ANNインデックスはこの種のクエリをサポートします:

``` sql
WITH [...] AS reference_vector
SELECT *
FROM table
WHERE ...                       -- WHERE句はオプション
ORDER BY Distance(vectors, reference_vector)
LIMIT N
SETTINGS enable_analyzer = 0;   -- 一時的な制限で、解除される予定
```

:::tip
大きなベクトルを出力するのを避けるため、[クエリパラメータ](/docs/ja/interfaces/cli.md#queries-with-parameters-cli-queries-with-parameters)を使用することができます。例えば、

```bash
clickhouse-client --param_vec='hello' --query="SELECT * FROM table WHERE L2Distance(vectors, {vec: Array(Float32)}) < 1.0"
```
:::

HNSWパラメータ`hnsw_candidate_list_size_for_search`（デフォルト: 256）を異なる値で検索するために、`SELECT`クエリを`SETTINGS hnsw_candidate_list_size_for_search = <value>`を指定して実行します。

**制限事項**: 近似アルゴリズムを使用して最適近傍を決定するために、制限が必要です。そのため、`LIMIT`句のないクエリはANNインデックスを利用できません。また、ANNインデックスはクエリに`max_limit_for_ann_queries`（デフォルト: 100万行）より小さい`LIMIT`値がある場合にのみ使用されます。これは、近似近傍検索のために外部ライブラリによる大規模なメモリ割り当てを防ぐための安全策です。

**スキップインデックスとの違い** 通常の[スキップインデックス](https://clickhouse.com/docs/ja/optimize/skipping-indexes)と同様に、ANNインデックスはグラニュールごとに構築され、各インデックスブロックは`GRANULARITY = [N]`グラニュール（通常のスキップインデックスの場合はデフォルトで`[N]` = 1）で構成されます。例えば、テーブルの主キーインデックスグラニュラリティが8192（`index_granularity = 8192`設定）であり、`GRANULARITY = 2`の場合、各インデックスブロックは16384行を含みます。しかし、近似近傍検索のためのデータ構造とアルゴリズム（通常外部ライブラリから提供される）は本質的に行指向です。それらは一連の行をコンパクトに表現し、ANNクエリに対する行も返します。これは通常のスキップインデックスがインデックスブロックの粒度でデータを飛び越える方法とは異なる振る舞いを引き起こします。

ユーザーがカラムにANNインデックスを定義すると、ClickHouseは内部的に各インデックスブロックに対してANN「サブインデックス」を作成します。サブインデックスは「ローカル」であり、自身の含むインデックスブロック内の行のみを認識しています。前述の例で、カラムが65536行あると仮定すると、四つのインデックスブロック（八つのグラニュールにまたがる）が得られ、それぞれにANNサブインデックスが作成されます。サブインデックスは理論的には、インデックスブロック内でN個の最も近い点を持つ行を直接返すことができます。しかし、ClickHouseはデータをディスクからメモリにグラニュールの粒度でロードするため、サブインデックスは一致する行をグラニュール粒度に外挿します。これは通常のスキップインデックスがインデックスブロックの粒度でデータを飛び越える方法と異なります。

`GRANULARITY`パラメータは、いくつのANNサブインデックスが作成されるかを決定します。大きな`GRANULARITY`値は、少ないが大きなANNSサブインデックスを意味し、一つのサブインデックスがあるカラム（またはカラムのデータパート）の場合まで、大きさを増やします。その場合、サブインデックスはカラム行全体（またはその一部）のすべてのグラニュールを直接返せる「グローバル」なビューを持ちます（関連する行があるグラニュールは最大で`LIMIT [N]`個です）。次のステップでは、ClickHouseがこれらのグラニュールをロードし、ブルートフォースで距離計算を行うことにより、実際の最良の行を確認します。小さい`GRANULARITY`値では、各サブインデックスが最大`LIMIT N`までのグラニュールを返します。結果として、より多くのグラニュールをロードして後処理する必要があります。一方、大きな`GRANULARITY`値を使用することが一般的に推奨されます。これは、ANNインデックスの処理性能が異なるだけで、検索精度には両方とも等しく良い影響を与えます。`GRANULARITY`がANNインデックスで指定されていない場合、デフォルト値は1億です。
