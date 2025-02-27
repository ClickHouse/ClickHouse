---
slug: /ja/engines/table-engines/mergetree-family/graphitemergetree
sidebar_position: 90
sidebar_label: GraphiteMergeTree
---

# GraphiteMergeTree

このエンジンは、[Graphite](http://graphite.readthedocs.io/en/latest/index.html) データの間引きと集計/平均化（ロールアップ）を目的としています。ClickHouseをGraphiteのデータストアとして使用したい開発者にとって役立つかもしれません。

ロールアップが不要な場合は、任意のClickHouseテーブルエンジンを使用してGraphiteデータを保存できますが、ロールアップが必要な場合は `GraphiteMergeTree` を使用してください。このエンジンはストレージの容量を削減し、Graphiteからのクエリの効率を向上させます。

このエンジンは、[MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md)のプロパティを継承しています。

## テーブルの作成 {#creating-table}

``` sql
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

[CREATE TABLE](../../../sql-reference/statements/create/table.md#create-table-query)クエリの詳細な説明を参照してください。

Graphiteデータのテーブルには、以下のデータ用に以下のカラムが必要です：

- メトリック名（Graphiteセンサー）。データ型：`String`。

- メトリックを計測した時間。データ型：`DateTime`。

- メトリックの値。データ型：`Float64`。

- メトリックのバージョン。データ型：任意の数値型（ClickHouseは最高バージョンの行または同一バージョンの場合は最後に書き込まれた行を保存します。他の行はデータ部分のマージ中に削除されます）。

これらのカラムの名前はロールアップ設定で指定する必要があります。

**GraphiteMergeTreeのパラメータ**

- `config_section` — ロールアップのルールが設定されている設定ファイル内のセクション名。

**クエリ句**

`GraphiteMergeTree` テーブルを作成する際、`MergeTree` テーブルを作成する場合と同様に、同じ[クエリ句](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)が必要です。

<details markdown="1">

<summary>非推奨のテーブル作成方法</summary>

:::note
新しいプロジェクトではこの方法を使用せず、可能であれば古いプロジェクトを上記の方法に切り替えてください。
:::

``` sql
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

`config_section` を除くすべてのパラメータは `MergeTree` と同じ意味を持ちます。

- `config_section` — ロールアップのルールが設定されている設定ファイル内のセクション名。

</details>

## ロールアップ設定 {#rollup-configuration}

ロールアップの設定は、サーバー構成内の[graphite_rollup](../../../operations/server-configuration-parameters/settings.md#graphite)パラメータによって定義されます。パラメータ名は任意に設定できます。複数の構成を作成し、異なるテーブルに使用することが可能です。

ロールアップ構成の構造：

```
required-columns
patterns
```

### 必須カラム {#required-columns}

#### path_column_name

`path_column_name` — メトリック名（Graphiteセンサー）を保存するカラムの名前。デフォルト値は `Path` です。

#### time_column_name
`time_column_name` — メトリックを計測した時間を保存するカラムの名前。デフォルト値は `Time` です。

#### value_column_name
`value_column_name` — `time_column_name` に設定された時間におけるメトリックの値を保存するカラムの名前。デフォルト値は `Value` です。

#### version_column_name
`version_column_name` — メトリックのバージョンを保存するカラムの名前。デフォルト値は `Timestamp` です。

### パターン {#patterns}

`patterns` セクションの構造：

``` text
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
パターンは以下の順序で厳密に記述する必要があります：

1. `function` または `retention` がないパターン。
2. `function` と `retention` の両方を持つパターン。
3. `default` パターン。
:::

行を処理する際、ClickHouseは`pattern`セクションのルールを確認します。各`pattern`（`default`を含む）セクションには集計用の`function`パラメータ、または`retention`パラメータ、または両方を含むことができます。メトリック名が`regexp`に一致すると、`pattern`セクション（またはセクション）からのルールが適用されます。そうでない場合は`default`セクションからのルールが使用されます。

`pattern` と `default` セクション用フィールド：

- `rule_type` - ルールのタイプ。特定のメトリクスにのみ適用されます。エンジンは、プレーンメトリクスとタグ付きメトリクスを分けるために使用します。オプションのパラメータ。デフォルト値は `all` です。
  パフォーマンスが重要でない場合、または1種類のメトリクスしか使用しない場合（例：プレーンメトリクス）には不要です。デフォルトでは1種類のルールセットのみが作成されます。それ以外の場合、特定のタイプが定義されている場合は、2つの異なるセットが作成されます。1つはプレーンメトリクス用（root.branch.leaf）、もう1つはタグ付きメトリクス用（root.branch.leaf;tag1=value1）です。
  デフォルトルールは両方のセットに組み込まれます。
  有効な値：
  
  - `all`（デフォルト）- `rule_type`が省略された場合に使用されるユニバーサルルール。
  - `plain` - プレーンメトリクス用のルール。`regexp`フィールドは正規表現として処理されます。
  - `tagged` - タグ付きメトリクス用のルール（メトリクスは `someName?tag1=value1&tag2=value2&tag3=value3` 形式でDBに保存されます）。正規表現はタグ名によってソートされていなければならず、存在する場合は最初のタグは `__name__` でなければなりません。`regexp`フィールドは正規表現として処理されます。
  - `tag_list` - タグ付きメトリクス用のルール。クリックハウスフォーマットかつ `someName;tag1=value1;tag2=value2` または `someName` または `tag1=value1;tag2=value2` フォーマットで、メトリクスの記述を簡単にするためのDSL。`regexp`フィールドは `tagged` ルールに変換されます。タグ名によるソートは自動的に行われるので不要です。タグの値（名前ではなく）は正規表現として設定できます。例えば `env=(dev|staging)`。

- `regexp` – メトリック名のパターン（正規表現またはDSL）。
- `age` – データの最小年齢（秒単位）。
- `precision` – データの年齢を秒単位でどの程度正確に定義するか。86400（1日の秒数）の約数である必要があります。
- `function` – `[age, age + precision]` の範囲内でデータに適用する集約関数の名前。受け入れられる関数：min / max / any / avg。平均は平均の平均として不正確に計算されます。

### ルールタイプなしの設定例 {#configuration-example}

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

### ルールタイプを含む設定例 {#configuration-typed-example}

``` xml
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
データのロールアップはマージ中に実行されます。通常、古いパーティションに対してはマージが開始されないため、ロールアップのためには[optimize](../../../sql-reference/statements/optimize.md)を使用して予定外のマージをトリガーする必要があります。または、[graphite-ch-optimizer](https://github.com/innogames/graphite-ch-optimizer)のような追加ツールを使用してください。
:::
