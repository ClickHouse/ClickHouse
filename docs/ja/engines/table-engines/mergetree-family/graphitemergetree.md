---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 38
toc_title: "\u30B0\u30E9\u30D5\u30A3\u30C3\u30C8\u30E1\u30FC\u30EB\u30B0\u30C4\u30EA\
  \u30FC"
---

# グラフィットメールグツリー {#graphitemergetree}

このエン) [黒鉛](http://graphite.readthedocs.io/en/latest/index.html) データ。 GraphiteのデータストアとしてClickHouseを使用したい開発者にとっては役に立つかもしれません。

ロールアップが必要ない場合は、任意のclickhouseテーブルエンジンを使用してグラファイトデータを保存できますが、ロールアップが必要な場合は使用します `GraphiteMergeTree`. エンジンはストレージの量を減らし、Graphiteからのクエリの効率を高めます。

エンジンを継承性から [MergeTree](mergetree.md).

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

グラファイトデータのテーブルには、次のデータの列が必要です:

-   メトリック名（黒鉛センサ）。 データ型: `String`.

-   メトリックを測定する時間。 データ型: `DateTime`.

-   メトリックの値。 データ型:任意の数値。

-   メトリックのバージョン。 データ型:任意の数値。

    ClickHouseは、バージョンが同じであれば、最高のバージョンまたは最後に書かれた行を保存します。 その他の行は、データパーツのマージ中に削除されます。

これらの列の名前は、ロールアップ構成で設定する必要があります。

**GraphiteMergeTreeパラメータ**

-   `config_section` — Name of the section in the configuration file, where are the rules of rollup set.

**クエリ句**

作成するとき `GraphiteMergeTree` テーブル、同じ [句](mergetree.md#table_engine-mergetree-creating-a-table) 作成するときと同じように、必須です。 `MergeTree` テーブル。

<details markdown="1">

<summary>テーブルを作成する非推奨の方法</summary>

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

すべてのパラメーターを除く `config_section` と同じ意味を持つ `MergeTree`.

-   `config_section` — Name of the section in the configuration file, where are the rules of rollup set.

</details>

## ロールアップ構成 {#rollup-configuration}

ロールアップの設定は、次のように定義されます。 [graphite\_rollup](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-graphite) サーバー構成のパラメーター。 パラメータの名前は任意です。 複数の構成を作成し、それらを異なるテーブルに使用できます。

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
    パタ:

      1. Patterns without `function` or `retention`.
      1. Patterns with both `function` and `retention`.
      1. Pattern `default`.

行を処理するときに、clickhouseは次のルールをチェックします。 `pattern` セクション。 それぞれの `pattern` （を含む `default`)セクションには `function` 集計のパラメータ, `retention` 変数または両方。 このメトリック名が `regexp`、からのルール `pattern` セクション(またはセクション)が適用されます。 `default` セクションを使用します。

フィールドの `pattern` と `default` セクション:

-   `regexp`– A pattern for the metric name.
-   `age` – The minimum age of the data in seconds.
-   `precision`– How precisely to define the age of the data in seconds. Should be a divisor for 86400 (seconds in a day).
-   `function` – The name of the aggregating function to apply to data whose age falls within the range `[age, age + precision]`.

### 構成例 {#configuration-example}

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
