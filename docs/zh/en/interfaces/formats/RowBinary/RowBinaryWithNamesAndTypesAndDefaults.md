---
alias: []
description: 'RowBinaryWithNamesAndTypesAndDefaults 格式文档'
input_format: true
keywords: ['RowBinaryWithNamesAndTypesAndDefaults']
output_format: false
slug: /interfaces/formats/RowBinaryWithNamesAndTypesAndDefaults
title: 'RowBinaryWithNamesAndTypesAndDefaults'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✗  |    |

<div id="description">
  ## 说明
</div>

与 [`RowBinaryWithNamesAndTypes`](./RowBinaryWithNamesAndTypes.md) 格式类似，但会在每个单元格前额外增加一个字节，用于指示是否应使用该列的 `DEFAULT` 值——其方式与 [`RowBinaryWithDefaults`](./RowBinaryWithDefaults.md) 格式完全一致。这样的组合支持可随 schema 演进的 `INSERT`：写入端可以在请求头中省略某些列 (这些列会使用目标列的 `DEFAULT`) ，并且对于它实际发送的任意列，都可以将单个单元格标记为“使用该列的 `DEFAULT`”，而不会与 `NULL` 混淆。

此格式仅用于输入。

<div id="wire-format">
  ## 传输格式
</div>

请求头与 [`RowBinaryWithNamesAndTypes`](./RowBinaryWithNamesAndTypes.md) 相同：

1. 一个 `VarUInt`，表示列数 `N`。
2. `N` 个带长度前缀的 `String`，表示列名。
3. `N` 个列类型——可以是文本名称，也可以是紧凑的二进制编码，由 `output_format_binary_encode_types_in_binary_format` / `input_format_binary_decode_types_in_binary_format` 设置控制。

在请求头之后，每一行由 `N` 个单元组成。对于每个单元：

* 一个 `UInt8` 标记字节。
  * `0x01` — 使用目标列的 `DEFAULT` 表达式。后面不跟任何值字节。
  * `0x00` — 后面是一个值，按该列类型的 `RowBinary` 序列化器进行序列化。对于 `Nullable(T)`，值字节以 `Nullable` 的 null 字节开头 (非 NULL 为 `0`，NULL 为 `1`) ；如果不是 NULL，后面再跟内部值。

<div id="defaults-vs-null">
  ## 默认值与 NULL
</div>

每个单元的默认值标记与 `Nullable` 内置的空字节是相互独立的。对于 `Nullable(UInt32) DEFAULT 42` 列，每一行都可以通过以下三种不同方式发送：

| 字节        | 含义                                |
| --------- | --------------------------------- |
| `01`      | 使用 `DEFAULT 42`。                  |
| `00 01`   | 走值路径，然后通过 `Nullable` 类型表示 `NULL`。 |
| `00 00 …` | 走值路径，然后是一个非 NULL 的内部值。            |

<div id="schema-evolution">
  ## schema 演进
</div>

| Case                  | Behavior                                                                                                  |
| --------------------- | --------------------------------------------------------------------------------------------------------- |
| 文件请求头中完全缺少该列            | 通过 `insertDefaultsForNotSeenColumns` 在目标表中填充；受 `defaults_for_omitted_fields` 控制。                          |
| 文件请求头中存在该列，单元标记为 `0x01` | 对每一行执行 `insertDefault`。                                                                                   |
| 文件请求头中存在该列，单元标记为 `0x00` | 按常规解析该值。                                                                                                  |
| 文件请求头中存在额外列，但目标表中没有该列   | 当 `input_format_skip_unknown_fields = 1` 时，会静默丢弃 (先读取该标记；如果是 `0x01`，则不再执行其他操作；如果是 `0x00`，则解析出类型化值后将其丢弃) 。 |

<div id="example-usage">
  ## 示例用法
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

* 请求头包含一个名为 `x`、类型为 `Nullable(UInt32)` 的列。
* 该单元格使用标记 `0x01`，表示 &quot;使用 `DEFAULT 42`&quot;。

<div id="format-settings">
  ## 格式设置
</div>

<RowBinaryFormatSettings />