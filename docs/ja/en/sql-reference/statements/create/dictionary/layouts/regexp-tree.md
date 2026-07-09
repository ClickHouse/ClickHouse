---
slug: /sql-reference/statements/create/dictionary/layouts/regexp-tree
title: '正規表現ツリー Dictionary のレイアウト'
sidebar_label: 'Regexp Tree'
sidebar_position: 12
description: 'パターンベースのルックアップ向けに正規表現ツリー Dictionary を設定します。'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="overview">
  ## 概要
</div>

`regexp_tree` Dictionary を使うと、階層的な正規表現パターンに基づいてキーを値にマッピングできます。
これは、厳密なキー照合ではなく、パターンマッチによるルックアップ (たとえば、正規表現パターンへの一致に基づいて user agent 文字列のような文字列を分類する処理) 向けに最適化されています。

<iframe width="1024" height="576" src="https://www.youtube.com/embed/ESlAhUJMoz8?si=sY2OVm-zcuxlDRaX" title="ClickHouse の regex tree Dictionary 入門" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />

<div id="use-regular-expression-tree-dictionary-in-clickhouse-open-source">
  ## YAMLRegExpTree ソースで 正規表現ツリー Dictionary を使う
</div>

<CloudNotSupportedBadge />

正規表現ツリー dictionaries は、正規表現ツリーを含む YAML ファイルへのパスを指定した [`YAMLRegExpTree`](../sources/yamlregexptree.md) ソースを使用して、ClickHouse オープンソース版で定義します。

```sql title="Query"
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
...
```

Dictionary ソース [`YAMLRegExpTree`](../sources/yamlregexptree.md) は、正規表現ツリーの構造を表します。例えば:

```yaml
- regexp: 'Linux/(\d+[\.\d]*).+tlinux'
  name: 'TencentOS'
  version: '\1'

- regexp: '\d+/tclwebkit(?:\d+[\.\d]*)'
  name: 'Android'
  versions:
    - regexp: '33/tclwebkit'
      version: '13'
    - regexp: '3[12]/tclwebkit'
      version: '12'
    - regexp: '30/tclwebkit'
      version: '11'
    - regexp: '29/tclwebkit'
      version: '10'
```

このconfigは、正規表現ツリーノードのリストで構成されます。各ノードは次のstructureを持ちます。

* **regexp**: ノードの正規表現。
* **attributes**: ユーザー定義の Dictionary 属性のリスト。この例では、属性は `name` と `version` の2つあります。最初のノードでは両方の属性を定義しています。2番目のノードでは属性 `name` のみを定義しています。属性 `version` は、2番目のノードの子ノードによって提供されます。
  * 属性の値には、一致した正規表現のキャプチャグループを参照する**後方参照**を含めることができます。この例では、最初のノードの属性 `version` の値は、正規表現内のキャプチャグループ `(\d+[\.\d]*)` への後方参照 `\1` で構成されています。後方参照番号は1から9までで、`$1` または `\1` (1の場合) のように記述します。後方参照は、クエリ実行時に一致したキャプチャグループで置き換えられます。
* **child nodes**: 正規表現ツリーノードの子ノードのリストで、それぞれが独自の属性と、必要に応じてさらに子ノードを持ちます。文字列のマッチングは深さ優先で行われます。文字列がregexpノードに一致すると、dictionaryはそのノードの子ノードにも一致するかどうかを確認します。その場合、最も深い一致ノードの属性が割り当てられます。子ノードの属性は、同じ名前の親ノードの属性を上書きします。YAMLファイル内の子ノード名は任意で、たとえば上記の例では `versions` です。

正規表現ツリー dictionariesでは、`dictGet`、`dictGetOrDefault`、`dictGetAll` の各関数を使ったアクセスのみが許可されています。例:

```sql title="Query"
SELECT dictGet('regexp_dict', ('name', 'version'), '31/tclwebkit1024');
```

```text title="Response"
┌─dictGet('regexp_dict', ('name', 'version'), '31/tclwebkit1024')─┐
│ ('Android','12')                                                │
└─────────────────────────────────────────────────────────────────┘
```

この場合、まず最上位レイヤーの2 番目のノードで正規表現 `\d+/tclwebkit(?:\d+[\.\d]*)` に一致します。
その後、Dictionary は子ノードの探索を続け、その文字列が `3[12]/tclwebkit` にも一致することを見つけます。
その結果、属性 `name` の値は `Android` (第 1 レイヤーで定義) 、属性 `version` の値は `12` (子ノードで定義) になります。

高度な YAML 設定ファイルを使えば、正規表現ツリー dictionaries を user agent 文字列のパーサーとして利用できます。
ClickHouse は [uap-core](https://github.com/ua-parser/uap-core) をサポートしており、使い方は機能テスト [02504&#95;regexp&#95;dictionary&#95;ua&#95;parser](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/02504_regexp_dictionary_ua_parser.sh) で確認できます。

<div id="collecting-attribute-values">
  ### 属性値の収集
</div>

一致した複数の正規表現から、リーフノードの値だけでなく、それらの値を返したい場合があります。そのような場合は、専用の [`dictGetAll`](/ja/sql-reference/functions/ext-dict-functions.md#dictGetAll) 関数を使用できます。ノードに型 `T` の属性値がある場合、`dictGetAll` は 0 個以上の値を含む `Array(T)` を返します。

デフォルトでは、キーごとに返される一致数に上限はありません。上限は、`dictGetAll` の省略可能な第 4 引数として指定できます。配列は *トポロジカル順序* で格納されます。つまり、子ノードは親ノードより前に置かれ、兄弟ノードはソース内の順序に従います。

例:

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    tag String,
    topological_index Int64,
    captured Nullable(String),
    parent String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
LIFETIME(0)
```

```yaml
# /var/lib/clickhouse/user_files/regexp_tree.yaml
- regexp: 'clickhouse\.com'
  tag: 'ClickHouse'
  topological_index: 1
  paths:
    - regexp: 'clickhouse\.com/docs(.*)'
      tag: 'ClickHouse Documentation'
      topological_index: 0
      captured: '\1'
      parent: 'ClickHouse'

- regexp: '/docs(/|$)'
  tag: 'Documentation'
  topological_index: 2

- regexp: 'github.com'
  tag: 'GitHub'
  topological_index: 3
  captured: 'NULL'
```

```sql title="Query"
CREATE TABLE urls (url String) ENGINE=MergeTree ORDER BY url;
INSERT INTO urls VALUES ('clickhouse.com'), ('clickhouse.com/docs/en'), ('github.com/clickhouse/tree/master/docs');
SELECT url, dictGetAll('regexp_dict', ('tag', 'topological_index', 'captured', 'parent'), url, 2) FROM urls;
```

```text title="Response"
┌─url────────────────────────────────────┬─dictGetAll('regexp_dict', ('tag', 'topological_index', 'captured', 'parent'), url, 2)─┐
│ clickhouse.com                         │ (['ClickHouse'],[1],[],[])                                                            │
│ clickhouse.com/docs/en                 │ (['ClickHouse Documentation','ClickHouse'],[0,1],['/en'],['ClickHouse'])              │
│ github.com/clickhouse/tree/master/docs │ (['Documentation','GitHub'],[2,3],[NULL],[])                                          │
└────────────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────┘
```

<div id="matching-modes">
  ### マッチングモード
</div>

パターンマッチングの動作は、特定の Dictionary 設定によって変更できます。

* `regexp_dict_flag_case_insensitive`: 大文字と小文字を区別しないマッチングを使用します (デフォルトは `false`) 。個々の式では、`(?i)` および `(?-i)` で上書きできます。
* `regexp_dict_flag_dotall`: `.` が改行文字にもマッチするようにします (デフォルトは `false`) 。

<div id="use-regular-expression-tree-dictionary-in-clickhouse-cloud">
  ## ClickHouse Cloud で 正規表現ツリー dictionary を使用する
</div>

[`YAMLRegExpTree`](../sources/yamlregexptree.md) ソースは ClickHouse Open Source では動作しますが、ClickHouse Cloud では動作しません。
ClickHouse Cloud で 正規表現ツリー dictionaries を使用するには、まずローカル環境の ClickHouse Open Source で YAMLファイルから 正規表現ツリー dictionary を作成し、次に `dictionary` テーブル関数と [INTO OUTFILE](/ja/sql-reference/statements/select/into-outfile.md) 句を使用して、その dictionary を CSVファイルにダンプします。

```sql
SELECT * FROM dictionary(regexp_dict) INTO OUTFILE('regexp_dict.csv')
```

CSVファイルの内容は次のとおりです：

```text
1,0,"Linux/(\d+[\.\d]*).+tlinux","['version','name']","['\\1','TencentOS']"
2,0,"(\d+)/tclwebkit(\d+[\.\d]*)","['comment','version','name']","['test $1 and $2','$1','Android']"
3,2,"33/tclwebkit","['version']","['13']"
4,2,"3[12]/tclwebkit","['version']","['12']"
5,2,"3[12]/tclwebkit","['version']","['11']"
6,2,"3[12]/tclwebkit","['version']","['10']"
```

ダンプファイルのスキーマは次のとおりです。

* `id UInt64`: RegexpTree ノードの id。
* `parent_id UInt64`: ノードの親ノードの id。
* `regexp String`: 正規表現の文字列。
* `keys Array(String)`: ユーザー定義属性の名前。
* `values Array(String)`: ユーザー定義属性の値。

ClickHouse Cloud で Dictionary を作成するには、まず以下のテーブル構造でテーブル `regexp_dictionary_source_table` を作成します。

```sql
CREATE TABLE regexp_dictionary_source_table
(
    id UInt64,
    parent_id UInt64,
    regexp String,
    keys   Array(String),
    values Array(String)
) ENGINE=Memory;
```

次に、ローカルのCSVを以下のように更新します

```bash
clickhouse client \
    --host MY_HOST \
    --secure \
    --password MY_PASSWORD \
    --query "
    INSERT INTO regexp_dictionary_source_table
    SELECT * FROM input ('id UInt64, parent_id UInt64, regexp String, keys Array(String), values Array(String)')
    FORMAT CSV" < regexp_dict.csv
```

詳しくは、[ローカルファイルの挿入方法](/ja/integrations/data-ingestion/insert-local-files) を参照してください。ソーステーブルを初期化した後は、テーブルソースごとに RegexpTree を作成できます。

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
PRIMARY KEY(regexp)
SOURCE(CLICKHOUSE(TABLE 'regexp_dictionary_source_table'))
LIFETIME(0)
LAYOUT(regexp_tree);
```