---
alias: []
description: 'JSONColumnsWithMetadata 格式文档'
input_format: true
keywords: ['JSONColumnsWithMetadata']
output_format: true
slug: /interfaces/formats/JSONColumnsWithMetadata
title: 'JSONColumnsWithMetadata'
doc_type: 'reference'
---

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 说明
</div>

与 [`JSONColumns`](./JSONColumns.md) 格式不同的是，它还包含一些元数据和统计信息 (类似于 [`JSON`](./JSON.md) 格式) 。

:::note
`JSONColumnsWithMetadata` 格式会先将所有数据缓存在内存中，再以单个块输出，因此可能会导致较高的内存占用。
:::

<div id="example-usage">
  ## 示例用法
</div>

示例：

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

对于 `JSONColumnsWithMetadata` 输入格式，如果将设置 [`input_format_json_validate_types_from_metadata`](/zh/operations/settings/settings-formats.md/#input_format_json_validate_types_from_metadata) 设为 `1`，
则会把输入数据中元数据里的类型与表中对应列的类型进行比较。

<div id="format-settings">
  ## 格式设置
</div>
