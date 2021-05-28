---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 38
toc_title: GraphiteMergeTree
---

# GraphiteMergeTree {#graphitemergetree}

このエンジ) [黒鉛](http://graphite.readthedocs.io/en/latest/index.html) データ これは、GraphiteのデータストアとしてClickHouseを使用したい開発者にとって役立つかもしれません。

を利用できますClickHouseテーブルエンジンの黒鉛のデータが必要ない場rollupが必要な場合は、rollupを使用 `GraphiteMergeTree`. エンジンは貯蔵量を減らし、グラファイトからの照会の効率を高めます。

エンジンはプロパティを [メルゲツリー](mergetree.md).

## テーブルの作成 {#creating-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    Path String,
    Time DateTime,
    Value <Numeric_type>,
    Version <Numeric_type>
    ...
) ENGINE = GraphiteMergeTree(config_section)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

の詳細な説明を参照してください [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) クエリ。

グラファイトデータのテーブルには、次のデータの次の列が必要です:

-   ミリ規格名（グラファイトセンサ） データ型: `String`.

-   メトリックを測定する時間。 データ型: `DateTime`.

-   メトリックの値。 データ型:任意の数値。

-   指標のバージョン。 データ型:任意の数値。

    ClickHouseは、バージョンが同じ場合は、最も高いバージョンまたは最後に書かれた行を保存します。 他の行は、データパーツのマージ中に削除されます。

これらの列の名前は、ロールアップ構成で設定する必要があります。

**GraphiteMergeTreeパラメータ**

-   `config_section` — Name of the section in the configuration file, where are the rules of rollup set.

**クエリ句**

を作成するとき `GraphiteMergeTree` テーブル、同じ [句](mergetree.md#table_engine-mergetree-creating-a-table) を作成するときのように必要です。 `MergeTree` テーブル。

<details markdown="1">

<summary>推奨されていません法テーブルを作成する</summary>

!!! attention "注意"
    可能であれば、古いプロジェクトを上記の方法に切り替えてください。

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    EventDate Date,
    Path String,
    Time DateTime,
    Value <Numeric_type>,
    Version <Numeric_type>
    ...
) ENGINE [=] GraphiteMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, config_section)
```

以下を除くすべてのパラメータ `config_section` と同じ意味を持つ `MergeTree`.

-   `config_section` — Name of the section in the configuration file, where are the rules of rollup set.

</details>

## ロールアップ構成 {#rollup-configuration}

ロールアップの設定は、 [graphite_rollup](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-graphite) サーバー構成のパラメータ。 パラメーターの名前は任意です。 複数の構成を作成し、異なるテーブルに使用できます。

ロールアップ構成構造:

      required-columns
      patterns

### 必要な列 {#required-columns}

-   `path_column_name` — The name of the column storing the metric name (Graphite sensor). Default value: `Path`.
-   `time_column_name` — The name of the column storing the time of measuring the metric. Default value: `Time`.
-   `value_column_name` — The name of the column storing the value of the metric at the time set in `time_column_name`. デフォルト値: `Value`.
-   `version_column_name` — The name of the column storing the version of the metric. Default value: `Timestamp`.

### パターン {#patterns}

の構造 `patterns` セクション:

``` text
pattern
    regexp
    function
pattern
    regexp
    age + precision
    ...
pattern
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

!!! warning "注意"
    パターンは厳密に注文する必要が:

      1. Patterns without `function` or `retention`.
      1. Patterns with both `function` and `retention`.
      1. Pattern `default`.

行を処理するとき、ClickHouseは `pattern` セクション それぞれの `pattern` （含む `default`)セクションには `function` 集計のパラメータ, `retention` 変数または両方。 メトリック名が `regexp`、からのルール `pattern` セクション(またはセクション)が適用されます。 `default` セクションを使用します。

のフィールド `pattern` と `default` セクション:

-   `regexp`– A pattern for the metric name.
-   `age` – The minimum age of the data in seconds.
-   `precision`– How precisely to define the age of the data in seconds. Should be a divisor for 86400 (seconds in a day).
-   `function` – The name of the aggregating function to apply to data whose age falls within the range `[age, age + precision]`.

### 設定例 {#configuration-example}

``` xml
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

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/graphitemergetree/) <!--hide-->
