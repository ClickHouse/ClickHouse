---
slug: /ja/engines/table-engines/special/time_series
sidebar_position: 60
sidebar_label: TimeSeries
---

# TimeSeries エンジン [エクスペリメンタル]

タイムシリーズ、すなわちタイムスタンプとタグ（またはラベル）に関連付けられた値のセットを格納するテーブルエンジン：

```
metric_name1[tag1=value1, tag2=value2, ...] = {timestamp1: value1, timestamp2: value2, ...}
metric_name2[...] = ...
```

:::info
これは将来のリリースで後方互換性がない形で変更される可能性のあるエクスペリメンタル機能です。
TimeSeries テーブルエンジンの使用を
[allow_experimental_time_series_table](../../../operations/settings/settings.md#allow-experimental-time-series-table) 設定で有効にしてください。
コマンド `set allow_experimental_time_series_table = 1` を入力します。
:::

## 構文 {#syntax}

``` sql
CREATE TABLE name [(columns)] ENGINE=TimeSeries
[SETTINGS var1=value1, ...]
[DATA db.data_table_name | DATA ENGINE data_table_engine(arguments)]
[TAGS db.tags_table_name | TAGS ENGINE tags_table_engine(arguments)]
[METRICS db.metrics_table_name | METRICS ENGINE metrics_table_engine(arguments)]
```

## 使用法 {#usage}

すべてをデフォルトで設定した状態で始めると簡単です（`TimeSeries` テーブルをカラムのリストを指定せずに作成することが許可されています）：

``` sql
CREATE TABLE my_table ENGINE=TimeSeries
```

この後、このテーブルは次のプロトコルで使用できます（サーバー構成でポートを割り当てなければなりません）：
- [prometheus remote-write](../../../interfaces/prometheus.md#remote-write)
- [prometheus remote-read](../../../interfaces/prometheus.md#remote-read)

## ターゲットテーブル {#target-tables}

`TimeSeries` テーブルには独自のデータはなく、すべてがそのターゲットテーブルに格納されます。
これは[Materialized View](../../../sql-reference/statements/create/view#materialized-view) の動作に似ていますが、
Materialized View は1つのターゲットテーブルを持つのに対し、
`TimeSeries` テーブルは [data]{#data-table}、[tags]{#tags-table}、[metrics]{#metrics-table} という名前の3つのターゲットテーブルを持ちます。

ターゲットテーブルは `CREATE TABLE` クエリで明示的に指定することもできますし、
TimeSeries テーブルエンジンが内部ターゲットテーブルを自動的に生成することもできます。

ターゲットテーブルは次の通りです：
1. _data_ テーブル {#data-table} は、特定の識別子に関連付けられたタイムシリーズを含みます。
_data_ テーブルは以下のカラムを持たなければなりません：

| 名前 | 必須? | デフォルトタイプ | 可能なタイプ | 説明 |
|---|---|---|---|---|
| `id` | [x] | `UUID` | 任意 | メトリック名とタグの組み合わせを識別します |
| `timestamp` | [x] | `DateTime64(3)` | `DateTime64(X)` | 時点 |
| `value` | [x] | `Float64` | `Float32` または `Float64` | `timestamp` に関連付けられた値 |

2. _tags_ テーブル {#tags-table} は、メトリック名とタグの各組み合わせに対して計算された識別子を含みます。
_tags_ テーブルは以下のカラムを持たなければなりません：

| 名前 | 必須? | デフォルトタイプ | 可能なタイプ | 説明 |
|---|---|---|---|---|
| `id` | [x] | `UUID` | 任意 （[data]{#data-table} テーブルの `id` のタイプと一致しなければなりません） | `id` はメトリック名とタグの組み合わせを識別します。DEFAULT 式はそのような識別子を計算する方法を示します |
| `metric_name` | [x] | `LowCardinality(String)` | `String` または `LowCardinality(String)` | メトリックの名前 |
| `<tag_value_column>` | [ ] | `String` | `String` または `LowCardinality(String)` または `LowCardinality(Nullable(String))` | 特定のタグの値、タグの名前と対応するカラムの名前は [tags_to_columns](#settings) 設定で指定 |
| `tags` | [x] | `Map(LowCardinality(String), String)` | `Map(String, String)` または `Map(LowCardinality(String), String)` または `Map(LowCardinality(String), LowCardinality(String))` | メトリック名を含むタグ `__name__` と [tags_to_columns](#settings) 設定で列挙される名前のタグを除くタグのマップ |
| `all_tags` | [ ] | `Map(String, String)` | `Map(String, String)` または `Map(LowCardinality(String), String)` または `Map(LowCardinality(String), LowCardinality(String))` | 一時的なカラム、各行はメトリック名を含むタグ `__name__` を除くすべてのタグのマップです。そのカラムの唯一の目的は `id` を計算する際に使用されることです |
| `min_time` | [ ] | `Nullable(DateTime64(3))` | `DateTime64(X)` または `Nullable(DateTime64(X))` | その `id` を持つタイムシリーズの最小タイムスタンプ。このカラムは [store_min_time_and_max_time](#settings) が `true` の場合に作成されます |
| `max_time` | [ ] | `Nullable(DateTime64(3))` | `DateTime64(X)` または `Nullable(DateTime64(X))` | その `id` を持つタイムシリーズの最大タイムスタンプ。このカラムは [store_min_time_and_max_time](#settings) が `true` の場合に作成されます |

3. _metrics_ テーブル {#metrics-table} は、収集されたメトリックについてのいくつかの情報、これらのメトリックのタイプとその説明を含みます。
_metrics_ テーブルは以下のカラムを持たなければなりません：

| 名前 | 必須? | デフォルトタイプ | 可能なタイプ | 説明 |
|---|---|---|---|---|
| `metric_family_name` | [x] | `String` | `String` または `LowCardinality(String)` | メトリックファミリーの名前 |
| `type` | [x] | `String` | `String` または `LowCardinality(String)` | メトリックファミリーのタイプ。これには "counter", "gauge", "summary", "stateset", "histogram", "gaugehistogram" のいずれかがあります |
| `unit` | [x] | `String` | `String` または `LowCardinality(String)` | メトリックで使用される単位 |
| `help` | [x] | `String` | `String` または `LowCardinality(String)` | メトリックの説明 |

`TimeSeries` テーブルに挿入された行は、実際にはこれら3つのターゲットテーブルに格納されます。
`TimeSeries` テーブルには [data]{#data-table}、[tags]{#tags-table}、[metrics]{#metrics-table} のすべてのカラムが含まれます。

## 作成 {#creation}

`TimeSeries` テーブルエンジンを使用してテーブルを作成するにはいくつかの方法があります。最もシンプルな文は次の通りです。

``` sql
CREATE TABLE my_table ENGINE=TimeSeries
```

実際には、次のテーブルを作成します（`SHOW CREATE TABLE my_table` を実行すると見ることができます）：

``` sql
CREATE TABLE my_table
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `timestamp` DateTime64(3),
    `value` Float64,
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String),
    `min_time` Nullable(DateTime64(3)),
    `max_time` Nullable(DateTime64(3)),
    `metric_family_name` String,
    `type` String,
    `unit` String,
    `help` String
)
ENGINE = TimeSeries
DATA ENGINE = MergeTree ORDER BY (id, timestamp)
DATA INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
TAGS ENGINE = AggregatingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id)
TAGS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
METRICS ENGINE = ReplacingMergeTree ORDER BY metric_family_name
METRICS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

したがって、カラムは自動的に生成され、さらに3つの内部UUIDがあります。
それぞれの内部ターゲットテーブルが作成されました。
（内部UUIDは通常は設定
[show_table_uuid_in_table_create_query_if_not_nil](../../../operations/settings/settings#show_table_uuid_in_table_create_query_if_not_nil)
が設定されるまで表示されません。）

内部ターゲットテーブルは `.inner_id.data.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`,
`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, `.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` のような名前を持ち、
各ターゲットテーブルはメインの `TimeSeries` テーブルのカラムのサブセットを持ちます：

``` sql
CREATE TABLE default.`.inner_id.data.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp)
```

``` sql
CREATE TABLE default.`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
ENGINE = AggregatingMergeTree
PRIMARY KEY metric_name
ORDER BY (metric_name, id)
```

``` sql
CREATE TABLE default.`.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `metric_family_name` String,
    `type` String,
    `unit` String,
    `help` String
)
ENGINE = ReplacingMergeTree
ORDER BY metric_family_name
```

## カラムタイプの調整 {#adjusting-column-types}

メインテーブルを定義する際に明示的に指定することで、内部ターゲットテーブルのほぼすべてのカラムのタイプを調整できます。例えば、

``` sql
CREATE TABLE my_table
(
    timestamp DateTime64(6)
) ENGINE=TimeSeries
```

これにより、内部の [data]{#data-table} テーブルがマイクロ秒単位でタイムスタンプを格納します：

``` sql
CREATE TABLE default.`.inner_id.data.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID,
    `timestamp` DateTime64(6),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp)
```

## `id` カラム {#id-column}

`id` カラムには識別子が含まれ、すべての識別子はメトリック名とタグの組み合わせに対して計算されます。
`id` カラムの DEFAULT 式は、そのような識別子を計算するために使用される式です。
`id` カラムのタイプとその式は、明示的に指定することで調整できます：

``` sql
CREATE TABLE my_table
(
    id UInt64 DEFAULT sipHash64(metric_name, all_tags)
) ENGINE=TimeSeries
```

## `tags` および `all_tags` カラム {#tags-and-all-tags}

タグのマップを含む2つのカラム - `tags` と `all_tags` があります。この例では同じ意味を持ちますが、
設定 `tags_to_columns` が使用された場合には異なる場合があります。
特定のタグが `tags` カラム内のマップに格納される代わりに、別のカラムに格納されるべきであることを指定することができます：

``` sql
CREATE TABLE my_table ENGINE=TimeSeries SETTINGS = {'instance': 'instance', 'job': 'job'}
```

この文は次のカラムを追加します：
```
    `instance` String,
    `job` String
```
これにより、`my_table` と内部の [tags]{#tags-table} ターゲットテーブルの両方の定義にカラムが追加されます。
この場合、`tags` カラムには `instance` と `job` のタグが含まれませんが、`all_tags` カラムにはそれらが含まれます。`all_tags` カラムは一時的であり、`id` カラムの DEFAULT 式で使用されることが唯一の目的です。

カラムのタイプは明示的に指定することで調整できます：

``` sql
CREATE TABLE my_table (instance LowCardinality(String), job LowCardinality(Nullable(String)))
ENGINE=TimeSeries SETTINGS = {'instance': 'instance', 'job': 'job'}
```

## 内部ターゲットテーブルのテーブルエンジン {#inner-table-engines}

デフォルトでは、内部ターゲットテーブルは次のテーブルエンジンを使用します：
- [data]{#data-table} テーブルは [MergeTree](../mergetree-family/mergetree) を使用します；
- [tags]{#tags-table} テーブルは [AggregatingMergeTree](../mergetree-family/aggregatingmergetree) を使用します。このテーブルには同じデータが頻繁に複数回挿入されるため、重複を削除する方法が必要であり、`min_time` および `max_time` カラムの集計が必要だからです；
- [metrics]{#metrics-table} テーブルは [ReplacingMergeTree](../mergetree-family/replacingmergetree) を使用します。このテーブルには同じデータが頻繁に複数回挿入されるため、重複を削除する方法が必要です。

内部ターゲットテーブルに他のテーブルエンジンを使用することも指定があれば可能です：

``` sql
CREATE TABLE my_table ENGINE=TimeSeries
DATA ENGINE=ReplicatedMergeTree
TAGS ENGINE=ReplicatedAggregatingMergeTree
METRICS ENGINE=ReplicatedReplacingMergeTree
```

## 外部ターゲットテーブル {#external-target-tables}

`TimeSeries` テーブルが手動で作成されたテーブルを使用することも可能です：

``` sql
CREATE TABLE data_for_my_table
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp);

CREATE TABLE tags_for_my_table ...

CREATE TABLE metrics_for_my_table ...

CREATE TABLE my_table ENGINE=TimeSeries DATA data_for_my_table TAGS tags_for_my_table METRICS metrics_for_my_table;
```

## 設定 {#settings}

`TimeSeries` テーブルを定義する際に指定できる設定のリストは以下の通りです：

| 名前 | タイプ | デフォルト | 説明 |
|---|---|---|---|
| `tags_to_columns` | Map | {} | [tags]{#tags-table} テーブルにおいて、どのタグが別のカラムに配置されるべきかを指定するマップ。構文: `{'tag1': 'column1', 'tag2' : column2, ...}` |
| `use_all_tags_column_to_generate_id` | Bool | true | タイムシリーズの識別子を計算する式を生成する際に、その計算に `all_tags` カラムを使用することを有効にするフラグ |
| `store_min_time_and_max_time` | Bool | true | true に設定されている場合、このテーブルは各タイムシリーズの `min_time` と `max_time` を格納します |
| `aggregate_min_time_and_max_time` | Bool | true | 内部ターゲット `tags` テーブルを作成する際に、このフラグが設定されていると `min_time` カラムのタイプとして `Nullable(DateTime64(3))` の代わりに `SimpleAggregateFunction(min, Nullable(DateTime64(3)))` を使用し、`max_time` カラムの場合も同様です |
| `filter_by_min_time_and_max_time` | Bool | true | true に設定されている場合、このテーブルはタイムシリーズをフィルタリングするために `min_time` と `max_time` カラムを使用します |

# 関数 {#functions}

以下は、`TimeSeries` テーブルを引数としてサポートする関数のリストです：
- [timeSeriesData](../../../sql-reference/table-functions/timeSeriesData.md)
- [timeSeriesTags](../../../sql-reference/table-functions/timeSeriesTags.md)
- [timeSeriesMetrics](../../../sql-reference/table-functions/timeSeriesMetrics.md)
