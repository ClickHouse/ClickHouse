---
alias: []
description: 'DWARFフォーマットのドキュメント'
input_format: true
keywords: ['DWARF']
output_format: false
slug: /interfaces/formats/DWARF
title: 'DWARF'
doc_type: 'reference'
---

| 入力 | 出力 | 別名 |
| -- | -- | -- |
| ✔  | ✗  |    |

<div id="description">
  ## 説明
</div>

`DWARF`フォーマットは、ELFファイル (実行可能ファイル、ライブラリ、またはオブジェクトファイル) から DWARF のデバッグシンボルを解析します。
`dwarfdump` に似ていますが、はるかに高速で (数百 MB/秒) 、SQL もサポートしています。
`.debug_info` セクション内の各 Debug Information Entry (DIE) ごとに 1 行を生成し、
DWARF エンコーディングがツリー内の子要素リストの終端に使用する &quot;null&quot; エントリも含まれます。

:::info
`.debug_info` は *unit* で構成されており、それぞれがコンパイル単位に対応します。

* 各 unit は *DIE* のツリーで、`compile_unit` DIE をルートに持ちます。
* 各 DIE は *tag* と *attributes* のリストを持ちます。
* 各 attribute は *name* と *値* を持ちます (さらに *form* もあり、これは値がどのようにエンコードされるかを指定します) 。

DIE はソースコード中の要素を表し、その *tag* によって要素の種類がわかります。たとえば、次のようなものがあります。

* 関数 (tag = `subprogram`) 
* クラス/struct/enum (`class_type`/`structure_type`/`enumeration_type`) 
* 変数 (`variable`) 
* 関数の引数 (`formal_parameter`) 。

このツリー構造は、対応するソースコードの構造を反映しています。たとえば、`class_type` DIE には、そのクラスのメソッドを表す `subprogram` DIE を含めることができます。
:::

`DWARF`フォーマットは、次のカラムを出力します。

* `offset` - `.debug_info` セクション内での DIE の位置
* `size` - エンコードされた DIE のバイト数 (属性を含む) 
* `tag` - DIE の種類。慣例的な &quot;DW&#95;TAG&#95;&quot; プレフィックスは省略されます
* `unit_name` - この DIE を含むコンパイル単位の名前
* `unit_offset` - この DIE を含むコンパイル単位の `.debug_info` セクション内での位置
* `ancestor_tags` - ツリー内の現在の DIE の祖先のタグの配列。最も内側から最も外側の順
* `ancestor_offsets` - `ancestor_tags` に対応する祖先のオフセット
* 利便性のため、attributes 配列から複製された一般的な属性がいくつかあります:
  * `name`
  * `linkage_name` - マングルされた完全修飾名。通常これを持つのは関数だけですが、すべての関数にあるわけではありません
  * `decl_file` - この entity が宣言されているソースコードファイル名
  * `decl_line` - この entity が宣言されているソースコード内の行番号
* attributes を記述する並列配列:
  * `attr_name` - attribute の名前。慣例的な &quot;DW&#95;AT&#95;&quot; プレフィックスは省略されます
  * `attr_form` - attribute がどのようにエンコードされ、解釈されるか。慣例的な DW&#95;FORM&#95; プレフィックスは省略されます
  * `attr_int` - attribute の整数値。attribute が数値を持たない場合は 0
  * `attr_str` - attribute の文字列値。attribute が文字列値を持たない場合は空

<div id="example-usage">
  ## 使用例
</div>

`DWARF` フォーマットを使用すると、最も多くの関数定義 (テンプレートのインスタンス化やインクルードされたヘッダーファイル内の関数を含む) を持つコンパイル単位を見つけることができます。

```sql title="Query"
SELECT
    unit_name,
    count() AS c
FROM file('programs/clickhouse', DWARF)
WHERE tag = 'subprogram' AND NOT has(attr_name, 'declaration')
GROUP BY unit_name
ORDER BY c DESC
LIMIT 3
```

```text title="Response"
┌─unit_name──────────────────────────────────────────────────┬─────c─┐
│ ./src/Core/Settings.cpp                                    │ 28939 │
│ ./src/AggregateFunctions/AggregateFunctionSumMap.cpp       │ 23327 │
│ ./src/AggregateFunctions/AggregateFunctionUniqCombined.cpp │ 22649 │
└────────────────────────────────────────────────────────────┴───────┘

3 rows in set. Elapsed: 1.487 sec. Processed 139.76 million rows, 1.12 GB (93.97 million rows/s., 752.77 MB/s.)
Peak memory usage: 271.92 MiB.
```

<div id="format-settings">
  ## フォーマット設定
</div>
