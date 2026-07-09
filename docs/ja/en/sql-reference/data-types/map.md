---
description: 'ClickHouse の Map データ型に関するドキュメント'
sidebar_label: 'Map(K, V)'
sidebar_position: 36
slug: /sql-reference/data-types/map
title: 'Map(K, V)'
doc_type: 'reference'
---

データ型 `Map(K, V)` は、キー・バリューのペアを格納します。

他のデータベースとは異なり、ClickHouse の map ではキーは一意ではありません。つまり、1 つの map に同じキーを持つ要素を 2 つ含めることができます。
(これは、map が内部的に `Array(Tuple(K, V))` として実装されているためです。)

構文 `m[k]` を使うと、map `m` 内のキー `k` に対応する値を取得できます。
また、`m[k]` は map 全体を走査するため、この操作の runtime は map のサイズに対して線形です。

**Parameters**

* `K` — Map のキーの型です。[Nullable](../../sql-reference/data-types/nullable.md) と、[Nullable](../../sql-reference/data-types/nullable.md) をネストした [LowCardinality](../../sql-reference/data-types/lowcardinality.md) を除き、任意の型を使用できます。
* `V` — Map の値の型です。任意の型を使用できます。

**Examples**

map 型のカラムを持つテーブルを作成します。

```sql title="Query"
CREATE TABLE tab (m Map(String, UInt64)) ENGINE=Memory;
INSERT INTO tab VALUES ({'key1':1, 'key2':10}), ({'key1':2,'key2':20}), ({'key1':3,'key2':30});
```

`key2` の値を選択するには:

```sql title="Query"
SELECT m['key2'] FROM tab;
```

```text title="Response"
┌─arrayElement(m, 'key2')─┐
│                      10 │
│                      20 │
│                      30 │
└─────────────────────────┘
```

要求されたキー `k` がマップに含まれていない場合、`m[k]` は値の型のデフォルト値を返します。たとえば、整数型では `0`、文字列型では `''` です。
マップにキーが存在するかどうかを確認するには、関数 [mapContains](/ja/sql-reference/functions/tuple-map-functions#mapContainsKey) を使用できます。

```sql title="Query"
CREATE TABLE tab (m Map(String, UInt64)) ENGINE=Memory;
INSERT INTO tab VALUES ({'key1':100}), ({});
SELECT m['key1'] FROM tab;
```

```text title="Response"
┌─arrayElement(m, 'key1')─┐
│                     100 │
│                       0 │
└─────────────────────────┘
```

<div id="converting-tuple-to-map">
  ## TupleからMapへの変換
</div>

`Tuple()` 型の値は、関数 [CAST](/ja/sql-reference/functions/type-conversion-functions#CAST) を使って `Map()` 型の値にキャストできます。

**例**

```sql title="Query"
SELECT CAST(([1, 2, 3], ['Ready', 'Steady', 'Go']), 'Map(UInt8, String)') AS map;
```

```text title="Response"
┌─map───────────────────────────┐
│ {1:'Ready',2:'Steady',3:'Go'} │
└───────────────────────────────┘
```

<div id="reading-subcolumns-of-map">
  ## Map のサブカラムの読み取り
</div>

Map 全体を読み取らなくて済むように、場合によってはサブカラム `keys` と `values` を使用できます。

**例**

```sql title="Query"
CREATE TABLE tab (m Map(String, UInt64)) ENGINE = Memory;
INSERT INTO tab VALUES (map('key1', 1, 'key2', 2, 'key3', 3));

SELECT m.keys FROM tab; --   same as mapKeys(m)
SELECT m.values FROM tab; -- same as mapValues(m)
```

```text title="Response"
┌─m.keys─────────────────┐
│ ['key1','key2','key3'] │
└────────────────────────┘

┌─m.values─┐
│ [1,2,3]  │
└──────────┘
```

<div id="bucketed-map-serialization">
  ## MergeTree における Bucketed Map シリアライゼーション
</div>

デフォルトでは、MergeTree の `Map` カラムは 1 つの `Array(Tuple(K, V))` ストリームとして保存されます。
`m['key']` で 1 つのキーを読み取る場合、必要なのがそのキーだけであっても、カラム全体、つまりすべての行にあるすべてのキー・バリューのペアをスキャンする必要があります。
多数の異なるキーを持つ `Map` では、これがボトルネックになります。

Bucketed シリアライゼーション (`with_buckets`) では、キーをハッシュ化して、キー・バリューのペアを複数の独立したサブストリーム (バケット) に分割します。
クエリが `m['key']` にアクセスすると、そのキーを含むバケットだけがディスクから読み取られ、ほかのすべてのバケットはスキップされます。

<div id="enabling-bucketed-serialization">
  ### Bucketed Serialization を有効にする
</div>

```sql
CREATE TABLE tab (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    max_buckets_in_map = 32,
    map_buckets_strategy = 'sqrt';
```

挿入処理の速度低下を避けるために、ゼロレベルのパーツ (`INSERT` 時に作成される) では `basic` シリアライゼーションのままにし、マージ済みパーツでのみ `with_buckets` を使用できます:

```sql
CREATE TABLE tab (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'basic',
    max_buckets_in_map = 32,
    map_buckets_strategy = 'sqrt';
```

<div id="how-it-works">
  ### 仕組み
</div>

データパーツが `with_buckets` シリアライゼーションで書き込まれる場合、処理は次のように行われます。

1. 1行あたりの平均キー数がブロックの統計情報から計算されます。
2. バケット数は、設定された戦略に基づいて決定されます ([Settings](#bucketed-map-settings) を参照) 。
3. 各キー・バリューのペアは、キーをハッシュ化してバケットに割り当てられます: `bucket = hash(key) % num_buckets`。
4. 各バケットは、それぞれ独自のキー、値、オフセットを持つ独立したサブストリームとして保存されます。
5. `buckets_info` メタデータストリームには、バケット数と統計情報が記録されます。

クエリで特定のキー (`m['key']`) を読み取る際、オプティマイザはその式をキーのサブカラム (`m.key_<serialized_key>`) に書き換えます。
シリアライゼーション層は、要求されたキーが属するバケットを計算し、その1つのバケットだけをディスクから読み取ります。

Map 全体を読み取る場合 (たとえば `SELECT m`) 、すべてのバケットが読み取られ、元の Map に再構成されます。複数のサブストリームの読み取りとマージのオーバーヘッドがあるため、これは `basic` シリアライゼーションよりも低速です。

:::note
`with_buckets` シリアライゼーションを使用すると、Map の値内のキーの順序が元の挿入順と異なる場合があります。キーはハッシュによって各バケットに分散され、挿入順ではなくバケット順で再構成されます。`basic` シリアライゼーションでは、挿入された Map のキー順が保持されます。
:::

バケット数はパーツごとに異なる場合があります。バケット数が異なるパーツがマージされると、新しいパーツのバケット数は、マージ後の統計情報に基づいて再計算されます。`basic` と `with_buckets` シリアライゼーションのパーツは同じテーブル内に共存でき、透過的にマージされます。

<div id="bucketed-map-settings">
  ### 設定
</div>

| 設定                                               | 既定値     | 説明                                                                                                                                                                                                                             |
| ------------------------------------------------ | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `map_serialization_version`                      | `basic` | `Map` カラムのシリアライゼーション形式です。`basic` は単一の配列ストリームとして格納します。`with_buckets` は、単一キーの読み取りを高速化するためにキーをバケットに分割します。                                                                                                                         |
| `map_serialization_version_for_zero_level_parts` | `basic` | ゼロレベルのパーツ (`INSERT` で作成されるもの) のシリアライゼーション形式です。書き込みのオーバーヘッドを避けるため、insert では `basic` のままにしつつ、マージ後のパーツでは `with_buckets` を使用できます。                                                                                                  |
| `max_buckets_in_map`                             | `32`    | バケット数の上限です。実際の数は `map_buckets_strategy` によって決まります。指定可能な最大値は 256 です。                                                                                                                                                            |
| `map_buckets_strategy`                           | `sqrt`  | 平均 map サイズからバケット数を計算する戦略です: `constant` — 常に `max_buckets_in_map` を使用します。`sqrt` — `round(coefficient * sqrt(avg_size))` を使用します。`linear` — `round(coefficient * avg_size)` を使用します。結果は `[1, max_buckets_in_map]` の範囲に収まるよう制限されます。 |
| `map_buckets_coefficient`                        | `1.0`   | `sqrt` および `linear` 戦略で使用する乗数です。戦略が `constant` の場合は無視されます。                                                                                                                                                                     |
| `map_buckets_min_avg_size`                       | `32`    | バケット化を有効にするために必要な、1 行あたりの平均キー数の最小値です。平均がこのしきい値を下回る場合、ほかの設定に関係なく単一のバケットが使用されます。しきい値を無効にするには `0` に設定します。                                                                                                                         |

<div id="performance-trade-offs">
  ### パフォーマンス上のトレードオフ
</div>

次の表は、さまざまな Map サイズ (1行あたり 10 ～ 10,000 個のキー) における、`basic` シリアライゼーションと比較した `with_buckets` のパフォーマンスへの影響をまとめたものです。バケット 数は、32 を上限とする `sqrt` 戦略で決定しています。正確な数値は、キー/値の型、データ分布、ハードウェアによって異なります。

| Operation                                      | 10 keys    | 100 keys   | 1,000 keys | 10,000 keys | Notes                                                                                                                                |
| ---------------------------------------------- | ---------- | ---------- | ---------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| **単一キーのルックアップ** (`m['key']`)                   | 1.6–3.2倍高速 | 4.5–7.7倍高速 | 16–39倍高速   | 21–49倍高速    | カラム全体ではなく、1 つの バケット だけを読み取ります。                                                                                                     |
| **5 個のキーのルックアップ**                              | ~1x        | 1.5–3.1倍高速 | 2.9–8.3倍高速 | 4.5–6.7倍高速  | 各キーごとに対応する バケット を読み取ります。一部の バケット は重複する場合があります。                                                                                   |
| **PREWHERE** (`SELECT m WHERE m['key'] = ...`) | 1.5–3.0倍高速 | 2.9–7.3倍高速 | 5.3–31倍高速  | 20–45倍高速    | PREWHERE フィルターでは 1 つの バケット だけを読み取り、Map 全体の読み取りは一致した行に対してのみ行われます。高速化の度合いは選択性に依存します。つまり、一致する granule が少ないほど、Map 全体に対する I/O は少なくなります。 |
| **Map 全体のスキャン** (`SELECT m`)                   | ~2倍低速      | ~2倍低速      | ~2倍低速      | ~2倍低速       | すべての バケット を読み取って再構成する必要があります。                                                                                                      |
| **INSERT**                                     | 1.5–2.5倍低速 | 1.5–2.5倍低速 | 1.5–2.5倍低速 | 1.5–2.5倍低速  | キーのハッシュ計算と複数のサブストリームへの書き込みによるオーバーヘッドがあります。                                                                                           |

<div id="recommendations">
  ### 推奨事項
</div>

* **小さいマップ (平均キー数が 32 未満) :** `basic` シリアライゼーションのままにしてください。小さいマップでは、バケット化のオーバーヘッドに見合う効果はありません。デフォルトの `map_buckets_min_avg_size = 32` により、これは自動的に適用されます。
* **中程度のマップ (32～100 キー) :** クエリで個々のキーに頻繁にアクセスする場合は、`sqrt` 戦略で `with_buckets` を使用してください。単一キーのルックアップは 4～8 倍高速になります。
* **大きいマップ (100 キー超) :** `with_buckets` を使用してください。単一キーのルックアップは 16～49 倍高速になります。insert 速度をベースラインに近い水準に保つために、`map_serialization_version_for_zero_level_parts = 'basic'` の使用を検討してください。
* **ワークロードの大半がマップ全体のスキャンである場合:** `basic` のままにしてください。bucketed シリアライゼーションでは、フルスキャン時に約 2 倍のオーバーヘッドが追加されます。
* **混在ワークロード (キーのルックアップとフルスキャンが混在する場合) :** ゼロレベルのパーツを `basic` に設定したうえで `with_buckets` を使用してください。`PREWHERE` 最適化により、まず filter に関連するバケットだけを読み取り、その後、一致した行に対してのみマップ全体を読み取るため、全体として大幅な高速化が得られます。

<div id="map-alternatives">
  ### 代替アプローチ
</div>

bucketed `Map` シリアライゼーションがユースケースに適さない場合、キー単位のアクセス性能を改善するための代替アプローチが 2 つあります。

<div id="using-the-json-data-type">
  #### `JSON` データ型の使用
</div>

[JSON](/ja/sql-reference/data-types/newjson) データ型では、頻出する各パスが個別の動的サブカラムとして保存されます。`max_dynamic_paths` の上限を超えたパスは[共有データ構造](/ja/sql-reference/data-types/newjson#shared-data-structure)に格納され、単一パスの読み取りを最適化するために `advanced` シリアライゼーションを使用できます。`advanced` シリアライゼーションの詳細な概要については、[ブログ記事](https://clickhouse.com/blog/json-data-type-gets-even-better)を参照してください。

| 項目           | バケット付き `Map`                                                        | `JSON`                                                                                 |
| ------------ | ------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| 単一キーの読み取り    | 1 つのバケットを読み取ります (他のキーを含む場合があります) 。バケット内のすべてのキー・バリューのペアがデシリアライズされます。 | 頻出パスは動的サブカラムから直接読み取られます。頻度の低いパスは共有データに格納され、`advanced` シリアライゼーションでは対象のパスのデータだけが読み取られます。 |
| 値の型          | すべての値は同じ型 `V` を共有します                                                | 各パスはそれぞれ独自の型を持てます。型ヒントのないパスでは `Dynamic` が使用されます。                                       |
| スキップ索引のサポート  | `mapKeys`/`mapValues` に対して作成された一部の索引タイプで機能します                       | スキップ索引は特定のパスのサブカラムに対してのみ作成でき、すべてのパス/値にまとめて作成することはできません。                                |
| フルカラムの読み取り   | バケットの再構成が必要なため `basic` より約 2 倍遅くなります                                | `Dynamic` 型のエンコードとパス再構成によるオーバーヘッドがあります。                                                |
| ストレージオーバーヘッド | 追加のメタデータは最小限です                                                      | `Dynamic` 型のエンコード、パス名の保存、`advanced` シリアライゼーションで追加されるメタデータにより大きくなります。                   |
| スキーマの柔軟性     | テーブル作成時にキーと値の型が固定されます                                               | 完全に動的で、キーや値の型は行ごとに変えられます。既知のパスについては、直接サブカラムにアクセスできるよう型付きパスヒントを宣言できます。                  |

キーごとに異なる値の型が必要な場合、キーの集合が行ごとに大きく異なる場合、または頻繁にアクセスするキーがあらかじめ分かっていて、型付きパスとして宣言することで直接サブカラムにアクセスしたい場合は、`JSON` を使用してください。

<div id="manual-sharding-into-multiple-map-columns">
  #### 複数の Map カラムへの手動分片
</div>

アプリケーションレベルで、キーのハッシュに基づいて 1 つの `Map` を手動で複数のカラムに分割できます：

```sql
CREATE TABLE tab (
    id UInt64,
    m0 Map(String, UInt64),
    m1 Map(String, UInt64),
    m2 Map(String, UInt64),
    m3 Map(String, UInt64)
) ENGINE = MergeTree ORDER BY id;
```

挿入時には、各キー・バリューのペアをカラム `m{hash(key) % 4}` に振り分けます。クエリ時には、該当するカラム `m{hash('target_key') % 4}['target_key']` を読み取ります。

| Aspect | `Map` with buckets          | Manual sharding                                |
| ------ | --------------------------- | ---------------------------------------------- |
| 使いやすさ  | 透過的 — ストレージエンジン側で処理される      | insert と select のためのアプリケーションレベルのルーティングロジックが必要  |
| 垂直マージ  | 非対応 — すべてのバケットが 1 つのカラムに属する | 対応 — 各 `Map` カラムは独立したカラムであり、垂直マージできます          |
| スキーマ変更 | バケット数はパートごとにデータに応じて自動調整される  | 分片数を変更するには、データの書き換えまたは新しいカラムの追加が必要             |
| クエリ構文  | `m['key']` をそのまま使える         | 正しいカラムを計算する必要があります: `m0['key']`、`m1['key']` など |
| バケット粒度 | パート単位で、データ統計に応じて調整される       | テーブル作成時に固定                                     |

手動分片化は、多数のカラムを持つテーブルのマージ時にメモリ使用量を削減するうえで垂直マージが重要な場合や、分片数を固定して明示的に制御する必要がある場合に有効です。ほとんどのユースケースでは、自動バケット化シリアライゼーションのほうがシンプルで十分です。

**関連項目**

* [map()](/ja/sql-reference/functions/tuple-map-functions#map) 関数
* [CAST()](/ja/sql-reference/functions/type-conversion-functions#CAST) 関数
* [Map データ型の -Map 集約関数コンビネータ](../aggregate-functions/combinators.md#-map)

<div id="related-content">
  ## 関連コンテンツ
</div>

* ブログ: [ClickHouseでオブザーバビリティソリューションを構築する - 第2部 - トレース](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)