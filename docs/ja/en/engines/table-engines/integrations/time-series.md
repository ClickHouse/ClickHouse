---
description: '時系列、つまりタイムスタンプとタグ（またはラベル）に関連付けられた値の集合を格納するテーブルエンジン。'
sidebar_label: 'TimeSeries'
sidebar_position: 60
slug: /engines/table-engines/special/time_series
title: 'TimeSeries テーブルエンジン'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="timeseries-table-engine">
  # TimeSeries テーブルエンジン
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

タイムスタンプとタグ (またはラベル) に関連付けられた値の集合、つまり時系列を格納するテーブルエンジンです。

```sql
metric_name1[tag1=value1, tag2=value2, ...] = {timestamp1: value1, timestamp2: value2, ...}
metric_name2[...] = ...
```

:::info
これは実験的な機能であり、将来のリリースで後方互換性のない変更が加えられる可能性があります。
TimeSeries table engine を使用するには、
[allow&#95;experimental&#95;time&#95;series&#95;table](/ja/operations/settings/settings#allow_experimental_time_series_table) 設定を有効にします。
`set allow_experimental_time_series_table = 1` コマンドを入力します。
:::

<div id="syntax">
  ## 構文
</div>

```sql
CREATE TABLE name [(columns)] ENGINE=TimeSeries
[SETTINGS var1=value1, ...]
[SAMPLES db.samples_table_name | [SAMPLES INNER COLUMNS (...)] [SAMPLES INNER ENGINE engine(arguments)]]
[TAGS db.tags_table_name | [TAGS INNER COLUMNS (...)] [TAGS INNER ENGINE engine(arguments)]]
[METRICS db.metrics_table_name | [METRICS INNER COLUMNS (...)] [METRICS INNER ENGINE engine(arguments)]]
```

:::note
キーワード `SAMPLES` には、後方互換性のために `DATA` という別名が残されています。
:::

<div id="usage">
  ## 使用方法
</div>

まずは、すべてデフォルト設定のまま始めるのが簡単です (カラムの一覧を指定せずに `TimeSeries` テーブルを作成できます) ：

```sql
CREATE TABLE my_table ENGINE=TimeSeries
```

このテーブルは、次のプロトコルで使用できます (サーバー設定でポートを割り当てる必要があります) 。

* [prometheus remote-write](/ja/interfaces/prometheus#remote-write)
* [prometheus remote-read](/ja/interfaces/prometheus#remote-read)

<div id="outer-columns">
  ### 外部カラム
</div>

TimeSeries テーブルのカラムは自動的に生成されます。これらは外部カラムであり、データは保持せず、SELECT/INSERT 用のインターフェイスを提供するだけです。実際のデータは[ターゲットテーブル](#target-tables)に格納されます。以下に、外部カラムの一覧を示します。

| 名前              | 型                                              | 説明                                                                                                                                                 |
| --------------- | ---------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `metric_name`   | `String`                                       | メトリクスの名前                                                                                                                                           |
| `tags`          | `Map(String, String)`                          | 時系列のタグ (ラベル) のマップ                                                                                                                                  |
| `time_series`   | デフォルトでは `Array(Tuple(DateTime64(3), Float64))` | 時系列の (timestamp, value) ペアの Array。Tuple の timestamp と scalar の要素型は、samples `INNER COLUMNS` 宣言から導出できます ([外部カラムの指定](#specifying-outer-columns)を参照) 。 |
| `metric_family` | `String`                                       | メトリックファミリーの名前 (メトリクスのメタデータ用)                                                                                                                       |
| `type`          | `String`                                       | メトリクスのタイプ (例: &quot;counter&quot;、&quot;gauge&quot;)                                                                                               |
| `unit`          | `String`                                       | メトリクスの単位                                                                                                                                           |
| `help`          | `String`                                       | メトリクスの説明                                                                                                                                           |

例:

```sql
INSERT INTO my_table (metric_name, tags, time_series) VALUES
    ('cpu_usage', {'job': 'node_exporter', 'instance': 'host1:9100'},
     [(toDateTime64('2024-01-01 00:00:00', 3), 0.5), (toDateTime64('2024-01-01 00:01:00', 3), 0.7)])
```

`metric_name` は INSERT 時に空でも許容されます。つまり、メトリクス名は `tags` 内の `__name__` で指定します。たとえば:

```sql
INSERT INTO my_table (tags, time_series) VALUES
    ({'__name__': 'cpu_usage', 'job': 'test'},
     [(toDateTime64('2024-01-01 00:00:00', 3), 0.5)])
```

メトリクスのメタデータを挿入するには、`metric_family`、`type`、`unit`、`help` の各カラムに値を挿入します。

```sql
INSERT INTO my_table (metric_name, tags, time_series, metric_family, type, unit, help) VALUES
    ('http_requests_total', {'method': 'GET'}, [(now64(), 100.0)],
     'http_requests_total', 'counter', 'requests', 'Total HTTP requests')
```

<div id="specifying-outer-columns">
  ### 外部カラムの指定
</div>

外部 `time_series` カラムは、デフォルトの `Array(Tuple(DateTime64(3), Float64))` 型を上書きするため、`CREATE TABLE` ステートメント内で明示的に指定できます。ClickHouse はタプルからタイムスタンプ型とスカラー型を抽出し、それらを内部の Samples テーブルに引き継ぎます。

```sql
CREATE TABLE my_table (time_series Array(Tuple(UInt32, Float32))) ENGINE=TimeSeries
```

これは、samples の `INNER COLUMNS` 句で、タイムスタンプと値のカラム型を直接宣言することと同等です。

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp UInt32, value Float32)
```

両方の形式を同じ`CREATE TABLE`ステートメント内で使用する場合は、宣言する型を一致させる必要があります。

<div id="target-tables">
  ## ターゲットテーブル
</div>

`TimeSeries` テーブル自体は独自のデータを持たず、すべてのデータはそのターゲットテーブルに格納されます。
これは [materialized view](../../../sql-reference/statements/create/view#materialized-view) の仕組みに似ていますが、
materialized view ではターゲットテーブルが 1 つであるのに対し、
`TimeSeries` テーブルには [Samples](#samples-table)、[タグ](#tags-table)、[メトリクス](#metrics-table) という名前の 3 つのターゲットテーブルがある点が異なります。

ターゲットテーブルは `CREATE TABLE` クエリで明示的に指定することも、
`TimeSeries` テーブルエンジンによって内部ターゲットテーブルが自動生成されることもあります。

`TimeSeries` テーブルに挿入された行は変換され、ブロックに分割されたうえで、これら 3 つのターゲットテーブルに挿入されます。

ターゲットテーブルは次のとおりです:

<div id="samples-table">
  ### Samples テーブル
</div>

*samples* テーブルには、識別子に関連付けられた時系列が格納されます。

*samples* テーブルには、次のカラムが必要です。

| 名前          | 必須? | デフォルト型          | 可能な型                    | 説明                    |
| ----------- | --- | --------------- | ----------------------- | --------------------- |
| `id`        | [x] | `UUID`          | 任意                      | メトリクス名とタグの組み合わせを識別します |
| `timestamp` | [x] | `DateTime64(3)` | `DateTime64(X)`         | 時点                    |
| `value`     | [x] | `Float64`       | `Float32` または `Float64` | `timestamp` に対応する値    |

<div id="tags-table">
  ### タグ テーブル
</div>

*タグ* テーブルには、メトリクス名とタグの各組み合わせごとに算出される識別子が含まれます。

*タグ* テーブルには、次のカラムが必要です。

| 名前                   | 必須? | デフォルト型                                | 使用可能な型                                                                                                                  | 説明                                                                                                               |
| -------------------- | --- | ------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `id`                 | [x] | `UUID`                                | 任意 ([samples](#samples-table) テーブルの `id` の型と一致している必要があります)                                                              | `id` は、メトリクス名とタグの組み合わせを識別します。DEFAULT 式は、この識別子の計算方法を指定します                                                         |
| `metric_name`        | [x] | `LowCardinality(String)`              | `String` or `LowCardinality(String)`                                                                                    | メトリクス名                                                                                                           |
| `<tag_value_column>` | [ ] | `String`                              | `String` or `LowCardinality(String)` or `LowCardinality(Nullable(String))`                                              | 特定のタグの値。タグ名と対応するカラム名は、[tags&#95;to&#95;columns](#settings) 設定で指定します                                              |
| `tags`               | [x] | `Map(LowCardinality(String), String)` | `Map(String, String)` or `Map(LowCardinality(String), String)` or `Map(LowCardinality(String), LowCardinality(String))` | メトリクス名を含む `__name__` タグと、[tags&#95;to&#95;columns](#settings) 設定で列挙された名前のタグを除いたタグのマップ                            |
| `all_tags`           | [ ] | `Map(String, String)`                 | `Map(String, String)` or `Map(LowCardinality(String), String)` or `Map(LowCardinality(String), LowCardinality(String))` | 一時的なカラムです。各行には、メトリクス名を含む `__name__` タグだけを除いたすべてのタグのマップが格納されます。このカラムの唯一の目的は、`id` の計算時に使用することです                    |
| `min_time`           | [ ] | `Nullable(DateTime64(3))`             | `DateTime64(X)` or `Nullable(DateTime64(X))`                                                                            | その `id` を持つ時系列の最小タイムスタンプ。このカラムは、[store&#95;min&#95;time&#95;and&#95;max&#95;time](#settings) が `true` の場合に作成されます |
| `max_time`           | [ ] | `Nullable(DateTime64(3))`             | `DateTime64(X)` or `Nullable(DateTime64(X))`                                                                            | その `id` を持つ時系列の最大タイムスタンプ。このカラムは、[store&#95;min&#95;time&#95;and&#95;max&#95;time](#settings) が `true` の場合に作成されます |

<div id="metrics-table">
  ### メトリクス テーブル
</div>

*メトリクス* テーブルには、収集されるメトリクスに関する情報、各メトリクスの型、およびその説明が格納されます。

*メトリクス* テーブルには、次のカラムが必要です。

| 名前                   | 必須? | デフォルトの型                  | 指定可能な型                                | 説明                                                                                                                                                     |
| -------------------- | --- | ------------------------ | ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `metric_family_name` | [x] | `String`                 | `String` または `LowCardinality(String)` | メトリックファミリー の名前                                                                                                                                      |
| `type`               | [x] | `LowCardinality(String)` | `String` または `LowCardinality(String)` | メトリックファミリー の型。&quot;counter&quot;、&quot;gauge&quot;、&quot;summary&quot;、&quot;stateset&quot;、&quot;histogram&quot;、&quot;gaugehistogram&quot; のいずれか |
| `unit`               | [x] | `LowCardinality(String)` | `String` または `LowCardinality(String)` | メトリクスで使用される単位                                                                                                                                          |
| `help`               | [x] | `String`                 | `String` または `LowCardinality(String)` | メトリクスの説明                                                                                                                                               |

<div id="creation">
  ## 作成
</div>

`TimeSeries` テーブルエンジンを使用してテーブルを作成する方法はいくつかあります。
最も単純なステートメント

```sql
CREATE TABLE my_table ENGINE=TimeSeries
```

実際に、次のテーブルが作成されます (`SHOW CREATE TABLE my_table` を実行すると確認できます) :

```sql
CREATE TABLE my_table
(
    `metric_name` String,
    `tags` Map(String, String),
    `time_series` Array(Tuple(DateTime64(3), Float64)),
    `metric_family` String,
    `type` String,
    `unit` String,
    `help` String
)
ENGINE = TimeSeries
SAMPLES INNER COLUMNS
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
SAMPLES INNER ENGINE = MergeTree ORDER BY (id, timestamp)
TAGS INNER COLUMNS
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
TAGS INNER ENGINE = AggregatingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1
METRICS INNER COLUMNS
(
    `metric_family_name` String,
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
METRICS INNER ENGINE = ReplacingMergeTree ORDER BY metric_family_name
```

そのため、カラムは自動的に生成され、さらにそれぞれ独自のカラム定義を持つ 3 つの内部ターゲットテーブルが
`INNER COLUMNS` 句で定義されています。

内部ターゲットテーブルの名前は `.inner_id.samples.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`、
`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`、`.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
のようになっており、各ターゲットテーブルはそれぞれ独自のカラムセットを持ちます。

```sql
CREATE TABLE default.`.inner_id.samples.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp)
```

```sql
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
SETTINGS allow_dimensions_outside_sorting_key = 1
```

```sql
CREATE TABLE default.`.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `metric_family_name` String,
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
ENGINE = ReplacingMergeTree
ORDER BY metric_family_name
```

<div id="create-as">
  ## 既存のテーブルを AS で指定してテーブルを作成する
</div>

ステートメント `CREATE TABLE new_table AS existing_table` は、`existing_table` から以下をコピーします。

* `SETTINGS`
* kind ごとの `INNER COLUMNS`
* kind ごとの `INNER ENGINE`

`existing_table` に外部ターゲットがある場合、このステートメントは使用できません。
外側のカラム一覧はコピーされず、再生成されます。

<div id="adjusting-column-types">
  ## カラム型の調整
</div>

`INNER COLUMNS` 句を使うと、内部ターゲットテーブル内のカラム型を調整できます。たとえば、タイムスタンプをマイクロ秒単位で保存し、値を `Float32` として格納するには、次のようにします。

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp DateTime64(6), value Float32)
```

同じ句で、コーデックやその他のカラム属性を指定することもできます。

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp DateTime64(3) CODEC(DoubleDelta))
```

<div id="id-column">
  ## `id` カラム
</div>

`id` カラムには識別子が格納されており、各識別子はメトリクス名とタグの組み合わせごとに計算されます。
識別子の生成に使用される型と `DEFAULT` 式は、`TAGS INNER COLUMNS` 句でカスタマイズできます。

```sql
CREATE TABLE my_table ENGINE=TimeSeries
TAGS INNER COLUMNS (id UInt64 DEFAULT sipHash64(metric_name, all_tags))
```

`id` のカラム型は、`UUID`、`UInt64`、`UInt128`、または `FixedString(16)` のいずれかである必要があります。`DEFAULT` 式が指定されていない場合、ClickHouse は `id` の型に基づいて自動的にそれを選択します。samples と タグ の内部テーブルで宣言する `id` の型は一致している必要があります。

`id_generator` 設定を使うと、`INNER COLUMNS` 句を使用せずに同じカスタマイズを行えます。

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SETTINGS id_generator = 'sipHash64(metric_name, all_tags)'
```

この設定が有効な場合、カラムの`DEFAULT`に別の式が含まれていても、`id`の生成にはこの設定が使用されます。

<div id="tags-and-all-tags">
  ## `tags` と `all_tags` のカラム
</div>

タグのマップを格納するカラムは `tags` と `all_tags` の 2 つあります。この例ではどちらも同じ意味ですが、
`tags_to_columns` 設定を使用すると異なる場合があります。この設定を使うと、特定のタグを `tags` カラム内のマップに格納する代わりに、
個別のカラムに格納するよう指定できます。

```sql
CREATE TABLE my_table
ENGINE = TimeSeries 
SETTINGS tags_to_columns = {'instance': 'instance', 'job': 'job'}
```

このステートメントにより、内部の [タグ](#tags-table) ターゲットテーブルにカラム `instance` と `job` が追加されます。
この場合、`tags` カラムには `instance` と `job` のタグは含まれませんが、
`all_tags` カラムにはそれらが含まれます。`all_tags` カラムは一時的なもので、唯一の用途は `id` カラムの
DEFAULT 式で使用することです。

<div id="inner-table-engines">
  ## 内部ターゲットテーブルのテーブルエンジン
</div>

デフォルトでは、内部ターゲットテーブルには次のテーブルエンジンが使用されます。

* [samples](#samples-table) テーブルでは [MergeTree](../mergetree-family/mergetree) を使用します。
* [タグ](#tags-table) テーブルでは [AggregatingMergeTree](../mergetree-family/aggregatingmergetree) を使用します。これは、同じデータがこのテーブルに複数回挿入されることが多いため重複を削除する仕組みが必要であり、
  また、カラム `min_time` と `max_time` の aggregation も必要になるためです。
* [metrics](#metrics-table) テーブルでは [ReplacingMergeTree](../mergetree-family/replacingmergetree) を使用します。これは、同じデータがこのテーブルに複数回挿入されることが多いため重複を削除する仕組みが必要になるためです。

必要に応じて、内部ターゲットテーブルに他のテーブルエンジンを使用することもできます。

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES ENGINE=ReplicatedMergeTree
TAGS ENGINE=ReplicatedAggregatingMergeTree
METRICS ENGINE=ReplicatedReplacingMergeTree
```

[タグ](#tags-table) テーブルでは、タグのカラム (および `tags`/`all_tags` の Map) がソートキーの外にありますが、
`AggregatingMergeTree` はデフォルトでこれを許可しません ([`allow_dimensions_outside_sorting_key`](../mergetree-family/aggregatingmergetree) を参照) 。
ここでこれが問題ないのは、これらのカラムがソートキーの一部である `id` に関数従属しているためで、その結果、
バックグラウンドマージでまとめて集約されるすべての行は同じ値を持つからです。内部の タグ テーブルが生成される場合、またはその
engine が上記のようにインラインで指定される場合、`TimeSeries` はそのテーブルに `allow_dimensions_outside_sorting_key = 1` を自動的に設定します。
一方、手動で作成した[外部](#external-target-tables)の集約 タグ テーブルでは、これを自分で設定する必要があります。

<div id="external-target-tables">
  ## 外部ターゲットテーブル
</div>

手動で作成したテーブルを `TimeSeries` テーブルで使用することもできます。

```sql
CREATE TABLE samples_for_my_table
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp);

CREATE TABLE tags_for_my_table ...

CREATE TABLE metrics_for_my_table ...

CREATE TABLE my_table ENGINE=TimeSeries SAMPLES samples_for_my_table TAGS tags_for_my_table METRICS metrics_for_my_table;
```

外部テーブルのカラム型 (`id`、`timestamp`、`value`、および [`tags_to_columns`](#settings) に記載されている `<tag_value_column>`) は、`TimeSeries` テーブルが内部的に生成するものと一致している必要があります (型の制約については、[Samples table](#samples-table)、[タグ table](#tags-table)、および [メトリクス table](#metrics-table) を参照してください) 。型の不一致は `CREATE` 時に報告されます。

外部 タグ ターゲットの id-generator expression は、INSERT 時に次の順序で解決されます。まず [`id_generator`](#settings) 設定 (設定されている場合) 、次に外部テーブルの `id` カラムに宣言された `DEFAULT` (ある場合) 、最後に `id` 型から導出される canonical generator です。したがって、この設定は外部テーブルに宣言された `DEFAULT` よりも優先されます。詳細は [The `id` column](#id-column) を参照してください。

<div id="altering-settings">
  ## 設定の変更
</div>

`CREATE` の後で変更できる設定は、次の 2 つです。

* `id_generator`
* `filter_by_min_time_and_max_time`

```sql
ALTER TABLE my_table MODIFY SETTING id_generator = 'sipHash64(metric_name, all_tags)';
ALTER TABLE my_table MODIFY SETTING filter_by_min_time_and_max_time = 0;
```

`Tags テーブル` にすでにデータが存在する状態で `id_generator` を変更すると、同じ metric+tag の組み合わせに対して別の ID が生成されることがあります。既存の行は以前の ID のまま保持され、新しい行には新しいジェネレーターが使用されます。

他の設定は、`CREATE` 時に内部テーブルのスキーマに組み込まれるため、`ALTER ... MODIFY SETTING` では変更できません。

<div id="settings">
  ## 設定
</div>

`TimeSeries` テーブルの定義時に指定できる設定は次のとおりです。

| Name                                 | Type       | Default   | Description                                                                                                                                                                          |
| ------------------------------------ | ---------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `id_generator`                       | Expression | `id` 型に依存 | タグから時系列の識別子 (フィンガープリント) を計算する式です。未設定の場合は、`id` カラムのデフォルト式が使用されます。`id` カラムのデフォルト式も未設定の場合は、式が自動的に選択されます                                                                                 |
| `tags_to_columns`                    | Map        | {}        | [タグ](#tags-table) テーブルで、どのタグを個別のカラムに格納するかを指定する Map です。構文: `{'tag1': 'column1', 'tag2' : column2, ...}`                                                                             |
| `use_all_tags_column_to_generate_id` | Bool       | true      | 時系列の識別子を計算する式を生成する際、このフラグを有効にすると、計算に `all_tags` カラムが使用されます                                                                                                                           |
| `store_min_time_and_max_time`        | Bool       | true      | true に設定すると、テーブルは各時系列の `min_time` と `max_time` を保存します                                                                                                                                |
| `aggregate_min_time_and_max_time`    | Bool       | true      | 内部ターゲットの `tags` テーブルを作成する際、このフラグを有効にすると、`min_time` カラムの型として単なる `Nullable(DateTime64(3))` ではなく `SimpleAggregateFunction(min, Nullable(DateTime64(3)))` を使用します。`max_time` カラムについても同様です |
| `filter_by_min_time_and_max_time`    | Bool       | true      | true に設定すると、テーブルは時系列のフィルタリングに `min_time` カラムと `max_time` カラムを使用します                                                                                                                   |

<div id="functions">
  # 関数
</div>

以下は、`TimeSeries` テーブルを引数に取る関数の一覧です。

* [timeSeriesSamples](../../../sql-reference/table-functions/timeSeriesSamples.md)
* [timeSeriesTags](../../../sql-reference/table-functions/timeSeriesTags.md)
* [timeSeriesMetrics](../../../sql-reference/table-functions/timeSeriesMetrics.md)