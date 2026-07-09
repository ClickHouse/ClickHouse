---
slug: /sql-reference/statements/create/dictionary/sources/yamlregexptree
title: 'YAMLRegExpTree Dictionary ソース'
sidebar_position: 15
sidebar_label: 'YAMLRegExpTree'
description: 'YAML ファイルを正規表現ツリー Dictionary の Dictionary ソースとして設定します。'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

`YAMLRegExpTree` ソースは、ローカルファイルシステム上の YAML ファイルから正規表現ツリーを読み込みます。
これは [`regexp_tree`](../layouts/regexp-tree.md) Dictionary レイアウト専用に設計されており、
ユーザーエージェントのパースのようなパターンベースのルックアップ向けに、正規表現から属性への階層的なマッピングを提供します。

:::note
`YAMLRegExpTree` ソースは ClickHouse Open Source でのみ利用できます。
ClickHouse Cloud では、代わりに辞書を CSV にエクスポートし、[ClickHouse table source](./clickhouse.md) を使って読み込んでください。
詳しくは、[ClickHouse Cloud で regexp&#95;tree 辞書を使用する](../layouts/regexp-tree#use-regular-expression-tree-dictionary-in-clickhouse-cloud) を参照してください。
:::

<div id="configuration">
  ## 設定
</div>

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
LIFETIME(0);
```

設定項目:

| 設定     | 説明                                                                              |
| ------ | ------------------------------------------------------------------------------- |
| `PATH` | 正規表現ツリーを含む YAML ファイルの絶対パスです。DDL で作成する場合、ファイルは `user_files` ディレクトリ内に配置する必要があります。 |

<div id="yaml-file-structure">
  ## YAML ファイルの構造
</div>

YAML ファイルには、正規表現ツリーのノードの一覧が含まれています。各ノードは属性や子ノードを持つことができ、階層構造を形成します。

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

各ノードは次の構造を持ちます。

* **`regexp`**: このノードの正規表現。
* **属性**: ユーザー定義の Dictionary 属性 (例: `name`、`version`) 。属性値には、正規表現内のキャプチャグループへの**後方参照**を `\1` または `$1` (1～9 の数字) として含めることができます。これらはクエリ時に、一致したキャプチャグループに置き換えられます。
* **子ノード**: 子ノードのリストです。各子ノードはそれぞれ独自の属性を持ち、必要に応じてさらに子ノードを持つこともできます。子ノードのリスト名は任意です (例: 上記の `versions`) 。文字列の照合は深さ優先で行われます。文字列がノードに一致した場合は、その子ノードもチェックされます。最も深い一致ノードの属性が優先され、同じ名前の親属性を上書きします。

<div id="related-pages">
  ## 関連ページ
</div>

* [regexp&#95;tree Dictionary レイアウト](../layouts/regexp-tree.md) — レイアウトの設定、クエリ例、マッチングモード
* [dictGet](/ja/sql-reference/functions/ext-dict-functions#dictGet), [dictGetAll](/ja/sql-reference/functions/ext-dict-functions#dictGetAll) — regexp tree Dictionary をクエリするための関数