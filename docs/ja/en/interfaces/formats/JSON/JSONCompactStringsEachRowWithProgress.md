---
alias: []
description: 'JSONCompactStringsEachRowWithProgressフォーマットのドキュメント'
input_format: false
keywords: ['JSONCompactStringsEachRowWithProgress']
output_format: true
slug: /interfaces/formats/JSONCompactStringsEachRowWithProgress
title: 'JSONCompactStringsEachRowWithProgress'
doc_type: 'reference'
---

| 入力 | 出力 | 別名 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 説明
</div>

[`JSONCompactEachRowWithProgress`](/ja/interfaces/formats/JSONCompactEachRowWithProgress) と同様ですが、すべての値が文字列に変換されます。
これは、すべてのデータ型で一貫した文字列表現が必要な場合に便利です。

主な特長:

* `JSONCompactEachRowWithProgress` と同じ構造
* すべての値が文字列として表現されます (数値や配列なども、すべて引用符で囲まれた文字列になります)
* 進行状況の更新、totals、例外処理が含まれます
* 文字列ベースのデータを優先する、または必要とするクライアントに便利です

<div id="example-usage">
  ## 使用例
</div>

```sql title="Query"
SELECT *
FROM generateRandom('a Array(Int8), d Decimal32(4), c Tuple(DateTime64(3), UUID)', 1, 10, 2)
LIMIT 5
FORMAT JSONCompactStringsEachRowWithProgress
```

```response title="Response"
{"meta":[{"name":"a","type":"Array(Int8)"},{"name":"d","type":"Decimal(9, 4)"},{"name":"c","type":"Tuple(DateTime64(3), UUID)"}]}
{"row":["[-8]", "46848.5225", "('2064-06-11 14:00:36.578','b06f4fa1-22ff-f84f-a1b7-a5807d983ae6')"]}
{"row":["[-76]", "-85331.598", "('2038-06-16 04:10:27.271','2bb0de60-3a2c-ffc0-d7a7-a5c88ed8177c')"]}
{"row":["[-32]", "-31470.8994", "('2027-07-18 16:58:34.654','1cdbae4c-ceb2-1337-b954-b175f5efbef8')"]}
{"row":["[-116]", "32104.097", "('1979-04-27 21:51:53.321','66903704-3c83-8f8a-648a-da4ac1ffa9fc')"]}
{"row":["[]", "2427.6614", "('1980-04-24 11:30:35.487','fee19be8-0f46-149b-ed98-43e7455ce2b2')"]}
{"progress":{"read_rows":"5","read_bytes":"184","total_rows_to_read":"5","elapsed_ns":"191151"}}
{"rows_before_limit_at_least":5}
```

<div id="format-settings">
  ## フォーマット設定
</div>
