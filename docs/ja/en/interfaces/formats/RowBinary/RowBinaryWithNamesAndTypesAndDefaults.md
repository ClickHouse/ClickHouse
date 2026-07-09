---
alias: []
description: 'RowBinaryWithNamesAndTypesAndDefaults フォーマットに関するドキュメント'
input_format: true
keywords: ['RowBinaryWithNamesAndTypesAndDefaults']
output_format: false
slug: /interfaces/formats/RowBinaryWithNamesAndTypesAndDefaults
title: 'RowBinaryWithNamesAndTypesAndDefaults'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✔  | ✗  |       |

<div id="description">
  ## 説明
</div>

[`RowBinaryWithNamesAndTypes`](./RowBinaryWithNamesAndTypes.md) フォーマットと同様ですが、各セルの前に、そのカラムの `DEFAULT` 値を使うかどうかを示す 1 バイトが追加されています。これは [`RowBinaryWithDefaults`](./RowBinaryWithDefaults.md) フォーマットとまったく同じです。この組み合わせにより、スキーマの進化に対応した `INSERT` をサポートします。writer はヘッダーからカラムを省略でき (その場合は対象カラムの `DEFAULT` が適用されます) 、さらに送信するカラムについては、`NULL` と混同することなく、個々のセルに「そのカラムの `DEFAULT` を使う」ことを指定できます。

このフォーマットは入力専用です。

<div id="wire-format">
  ## ワイヤ形式
</div>

ヘッダーは [`RowBinaryWithNamesAndTypes`](./RowBinaryWithNamesAndTypes.md) と同一です。

1. カラム数 `N` を表す `VarUInt`。
2. カラム名を格納した、長さプレフィックス付き `String` が `N` 個。
3. `N` 個のカラム型。テキスト名、またはコンパクトなバイナリエンコーディングのいずれかで、`output_format_binary_encode_types_in_binary_format` / `input_format_binary_decode_types_in_binary_format` 設定で制御されます。

ヘッダーの後には、各行が `N` 個のセルで構成されます。各セルについて:

* 1 バイトの `UInt8` マーカー。
  * `0x01` — 対象カラムの `DEFAULT` 式を使用します。後続する値バイトはありません。
  * `0x00` — 値が続き、カラム型の `RowBinary` シリアライザーでシリアライズされます。`Nullable(T)` の場合、値バイトは `Nullable` の null byte (非 NULL の場合は `0`、NULL の場合は `1`) で始まり、非 NULL であればその後に内部値が続きます。

<div id="defaults-vs-null">
  ## デフォルト値と NULL
</div>

各セルのデフォルトマーカーと、`Nullable` に組み込まれている null byte は互いに独立しています。`Nullable(UInt32) DEFAULT 42` のカラムは、各行について次の 3 通りで送信できます。

| バイト列      | 意味                                   |
| --------- | ------------------------------------ |
| `01`      | `DEFAULT 42` を使用。                    |
| `00 01`   | 値のパスを選択し、その後 `Nullable` 型により `NULL`。 |
| `00 00 …` | 値のパスを選択し、その後に非 NULL の内部値。            |

<div id="schema-evolution">
  ## スキーマ進化
</div>

| ケース                             | 動作                                                                                                                            |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| カラムがファイルのヘッダーにまったく存在しない         | `insertDefaultsForNotSeenColumns` によりターゲット側で補完される。`defaults_for_omitted_fields` によって制御される。                                    |
| カラムがヘッダーに存在し、セルマーカーが `0x01`     | 各行で `insertDefault` が適用される。                                                                                                   |
| カラムがヘッダーに存在し、セルマーカーが `0x00`     | 値は通常どおり parse される。                                                                                                            |
| ヘッダーに余分なカラムがあり、ターゲットテーブルには存在しない | `input_format_skip_unknown_fields = 1` の場合は通知なく破棄される (先にマーカーが読み取られる。`0x01` ならそれ以上は何も行われず、`0x00` なら型付きの値が parse されたうえで破棄される) 。 |

<div id="example-usage">
  ## 使用例
</div>

```sql title="Query"
SELECT * FROM format(
    'RowBinaryWithNamesAndTypesAndDefaults',
    'x Nullable(UInt32) DEFAULT 42',
    unhex('01' || '0178' || '10' || hex('Nullable(UInt32)') || '01')
);
```

```response title="Response"
┌──x─┐
│ 42 │
└────┘
```

* ヘッダーには、`x` という名前の `Nullable(UInt32)` 型のカラムが 1 つあります。
* この 1 つのセルではマーカー `0x01` を使用し、これは「`DEFAULT 42` を使用する」ことを意味します。

<div id="format-settings">
  ## フォーマット設定
</div>

<RowBinaryFormatSettings />