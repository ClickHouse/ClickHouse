---
alias: []
description: 'JSONColumnsWithMetadata フォーマットのリファレンス'
input_format: true
keywords: ['JSONColumnsWithMetadata']
output_format: true
slug: /interfaces/formats/JSONColumnsWithMetadata
title: 'JSONColumnsWithMetadata'
doc_type: 'reference'
---

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✔  | ✔  |       |

<div id="description">
  ## 説明
</div>

[`JSONColumns`](./JSONColumns.md)フォーマットとの違いは、[`JSON`](./JSON.md)フォーマットと同様に、一部のメタデータと統計情報も含まれる点です。

:::note
`JSONColumnsWithMetadata`フォーマットは、すべてのデータをメモリ内にバッファリングしてから単一のブロックとして出力するため、メモリ消費量が大きくなる可能性があります。
:::

<div id="example-usage">
  ## 使用例
</div>

例:

```json
{
        "meta":
        [
                {
                        "name": "num",
                        "type": "Int32"
                },
                {
                        "name": "str",
                        "type": "String"
                },

                {
                        "name": "arr",
                        "type": "Array(UInt8)"
                }
        ],

        "data":
        {
                "num": [42, 43, 44],
                "str": ["hello", "hello", "hello"],
                "arr": [[0,1], [0,1,2], [0,1,2,3]]
        },

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.000272376,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

`JSONColumnsWithMetadata` 入力フォーマットでは、設定 [`input_format_json_validate_types_from_metadata`](/ja/operations/settings/settings-formats.md/#input_format_json_validate_types_from_metadata) が `1` に設定されている場合、
入力データのメタデータ内の型が、テーブル内の対応するカラムの型と比較されます。

<div id="format-settings">
  ## フォーマット設定
</div>
