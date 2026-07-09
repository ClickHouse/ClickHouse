---
alias: ['TSVRaw', 'Raw']
description: 'TabSeparatedRaw フォーマットのリファレンス'
input_format: true
keywords: ['TabSeparatedRaw']
output_format: true
slug: /interfaces/formats/TabSeparatedRaw
title: 'TabSeparatedRaw'
doc_type: 'reference'
---

| 入力 | 出力 | エイリアス           |
| -- | -- | --------------- |
| ✔  | ✔  | `TSVRaw`, `Raw` |

<div id="description">
  ## 説明
</div>

[`TabSeparated`](/ja/interfaces/formats/TabSeparated) フォーマットとは異なり、行はエスケープせずに書き込まれます。

:::note
このフォーマットでパースする場合、各フィールドにタブや改行を含めることはできません。
:::

`TabSeparatedRaw` フォーマットと `RawBlob` フォーマットの比較については、[Raw Formats Comparison](../RawBLOB.md/#raw-formats-comparison) を参照してください。

<div id="example-usage">
  ## 使用例
</div>

<div id="inserting-data">
  ### データの挿入
</div>

`football.tsv` という名前の次のTSVファイルを使用します。

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

データを挿入します：

```sql
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparatedRaw;
```

<div id="reading-data">
  ### データの読み込み
</div>

`TabSeparatedRaw`フォーマットを使用してデータを読み込みます:

```sql
SELECT *
FROM football
FORMAT TabSeparatedRaw
```

出力はタブ区切り形式になります:

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

<div id="format-settings">
  ## フォーマット設定
</div>
