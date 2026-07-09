---
description: 'Graphiteデータの間引きと集約/平均化（ロールアップ）のために設計されています。'
sidebar_label: 'GraphiteMergeTree'
sidebar_position: 90
slug: /engines/table-engines/mergetree-family/graphitemergetree
title: 'GraphiteMergeTree table engine'
doc_type: 'guide'
---

このエンジンは、[Graphite](http://graphite.readthedocs.io/en/latest/index.html) データの間引きと集約/平均化 (ロールアップ) 向けに設計されています。ClickHouseをGraphiteのデータストアとして使用したい開発者にとって役立つでしょう。

ロールアップが不要であれば、Graphiteデータの保存には任意のClickHouseテーブルエンジンを使用できますが、ロールアップが必要な場合は `GraphiteMergeTree` を使用してください。このエンジンにより、ストレージ使用量を削減し、Graphiteからのクエリ効率を高めることができます。

このエンジンは [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) の特性を継承しています。

<div id="creating-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    Path String,
    Time DateTime,
    Value Float64,
    Version <Numeric_type>
    ...
) ENGINE = GraphiteMergeTree(config_section)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

[CREATE TABLE](/ja/sql-reference/statements/create/table)クエリの詳細な説明を参照してください。

Graphite データ用のテーブルには、次のデータに対応する以下のカラムが必要です。

* メトリクス名 (Graphite sensor) 。データ型: `String`。

* メトリクスを測定した時刻。データ型: `DateTime`。

* メトリクスの値。データ型: `Float64`。

* メトリクスのバージョン。データ型: 任意の数値型 (ClickHouse は、最も高いバージョンの行、またはバージョンが同じ場合は最後に書き込まれた行を保存します。その他の行はデータパーツのマージ中に削除されます) 。

これらのカラム名は、ロールアップ設定で指定する必要があります。

**GraphiteMergeTree パラメーター**

* `config_section` — ロールアップルールが設定されている設定ファイル内のセクション名。

**クエリ句**

`GraphiteMergeTree` テーブルを作成する場合は、`MergeTree` テーブルを作成するときと同じ[句](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)が必要です。

<details markdown="1">
  <summary>非推奨のテーブル作成方法</summary>

  :::note
  新しいプロジェクトではこの方法を使用せず、可能であれば古いプロジェクトも上記の方法に切り替えてください。
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      EventDate Date,
      Path String,
      Time DateTime,
      Value Float64,
      Version <Numeric_type>
      ...
  ) ENGINE [=] GraphiteMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, config_section)
  ```

  `config_section` を除くすべてのパラメーターは、`MergeTree` と同じ意味です。

  * `config_section` — ロールアップルールが設定されている設定ファイル内のセクション名。
</details>

<div id="rollup-configuration">
  ## ロールアップ設定
</div>

ロールアップの設定は、サーバー設定の [graphite&#95;rollup](../../../operations/server-configuration-parameters/settings.md#graphite) パラメータで定義されます。パラメータ名は任意です。複数の設定を作成し、異なるテーブルに適用できます。

ロールアップ設定の構造:

必須カラム
パターン

<div id="required-columns">
  ### 必要なカラム
</div>

<div id="path_column_name">
  #### `path_column_name`
</div>

`path_column_name` — メトリクス名 (Graphite sensor) を格納するカラム名です。デフォルト値: `Path`。

<div id="time_column_name">
  #### `time_column_name`
</div>

`time_column_name` — メトリクスの計測時刻を格納するカラム名です。デフォルト値: `Time`。

<div id="value_column_name">
  #### `value_column_name`
</div>

`value_column_name` — `time_column_name` で指定した時点におけるメトリクスの値を格納するカラム名です。デフォルト値: `Value`。

<div id="version_column_name">
  #### `version_column_name`
</div>

`version_column_name` — メトリクスのバージョンを格納するカラムの名前です。デフォルト値: `Timestamp`。

<div id="patterns">
  ### パターン
</div>

`patterns` セクションの構成:

```text
pattern
    rule_type
    regexp
    function
pattern
    rule_type
    regexp
    age + precision
    ...
pattern
    rule_type
    regexp
    function
    age + precision
    ...
pattern
    ...
default
    function
    age + precision
    ...
```

:::important
パターン は次の厳密な順序で並べる必要があります。

1. `function` も `retention` もない パターン。
2. `function` と `retention` の両方がある パターン。
3. `default` パターン。
   :::

行を処理する際、ClickHouse は `pattern` セクション内のルールを確認します。各 `pattern` セクション (`default` を含む) には、aggregation 用の `function` parameter、`retention` parameters、またはその両方を含めることができます。メトリクス名が `regexp` に一致した場合は `pattern` セクションのルール (1 つまたは複数) が適用され、一致しない場合は `default` セクションのルールが使用されます。

`pattern` および `default` セクションのフィールド:

* `rule_type` - ルールの種類。特定のメトリクスにのみ適用されます。engine はこれを使ってプレーンメトリクスとタグ付きメトリクスを区別します。省略可能な parameter です。デフォルト値: `all`。
  パフォーマンスが重要でない場合や、プレーンメトリクスのように 1 種類のメトリクスしか使わない場合は不要です。デフォルトでは、作成されるルールセットは 1 種類だけです。一方、特別な type が 1 つでも定義されている場合は、2 つの異なるセットが作成されます。1 つはプレーンメトリクス用 (root.branch.leaf) 、もう 1 つはタグ付きメトリクス用 (root.branch.leaf;tag1=value1) です。
  デフォルトルールは両方のセットに含まれます。
  有効な値:
  * `all` (デフォルト) - `rule_type` を省略した場合に使用される universal ルール。
  * `plain` - プレーンメトリクス用のルール。フィールド `regexp` は regular expression として処理されます。
  * `tagged` - タグ付きメトリクス用のルール (メトリクスは DB に `someName?tag1=value1&tag2=value2&tag3=value3` 形式で保存されます) 。regular expression はタグ名でソートされている必要があり、存在する場合は最初のタグが `__name__` でなければなりません。フィールド `regexp` は regular expression として処理されます。
  * `tag_list` - タグ付きメトリクス用のルールで、graphite 形式の `someName;tag1=value1;tag2=value2`、`someName`、または `tag1=value1;tag2=value2` でメトリクスを簡単に記述するためのシンプルな DSL です。フィールド `regexp` は `tagged` ルールに変換されます。タグ名によるソートは不要で、自動的に行われます。タグの値 (名前ではなく) は regular expression として設定できます。例: `env=(dev|staging)`。
* `regexp` – メトリクス名の pattern (regular expression または DSL) 。
* `age` – データの最小経過時間 (秒) 。
* `precision`– データの経過時間を秒単位でどの程度の精度で定義するか。86400 (1 日の秒数) の divisor である必要があります。
* `function` – age が `[age, age + precision]` の範囲に入るデータに適用する集約関数の名前。使用できる関数: min / max / any / avg。平均値は、平均の平均のように不正確に計算されます。

<div id="configuration-example">
  ### ルールタイプを使用しない設定例
</div>

```xml
<graphite_rollup>
    <version_column_name>Version</version_column_name>
    <pattern>
        <regexp>click_cost</regexp>
        <function>any</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup>
```

<div id="configuration-typed-example">
  ### ルールタイプを含む設定例
</div>

```xml
<graphite_rollup>
    <version_column_name>Version</version_column_name>
    <pattern>
        <rule_type>plain</rule_type>
        <regexp>click_cost</regexp>
        <function>any</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <pattern>
        <rule_type>tagged</rule_type>
        <regexp>^((.*)|.)min\?</regexp>
        <function>min</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <pattern>
        <rule_type>tagged</rule_type>
        <regexp><![CDATA[^someName\?(.*&)*tag1=value1(&|$)]]></regexp>
        <function>min</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <pattern>
        <rule_type>tag_list</rule_type>
        <regexp>someName;tag2=value2</regexp>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup>
```

:::note
ロールアップはマージ時に実行されます。通常、古いパーティションではマージが開始されないため、ロールアップを行うには、[optimize](../../../sql-reference/statements/optimize.md) を使用してスケジュール外のマージをトリガーする必要があります。あるいは、[graphite-ch-optimizer](https://github.com/innogames/graphite-ch-optimizer) などの追加ツールを使用してください。
:::