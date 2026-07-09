---
title: Common Row Binary 格式设置
---

{/* 注：此代码片段会在导入它的任何文件中复用 */ }

以下设置适用于所有 `RowBinary` 类型的格式。

| Setting                                                                                                                                | Description                                                                                                                                                                      | Default |
| -------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| [`format_binary_max_string_size`](/zh/operations/settings/settings-formats.md/#format_binary_max_string_size)                             | `RowBinary` 格式中 String 的最大允许大小。                                                                                                                                                  | `1GiB`  |
| [`output_format_binary_encode_types_in_binary_format`](/zh/operations/settings/formats#input_format_binary_decode_types_in_binary_format) | 允许在 [`RowBinaryWithNamesAndTypes`](../RowBinaryWithNamesAndTypes.md) 输出格式中，使用 [`binary encoding`](/zh/sql-reference/data-types/data-types-binary-encoding.md) 在头部写入类型，而不是使用类型名称字符串。 | `false` |
| [`input_format_binary_decode_types_in_binary_format`](/zh/operations/settings/formats#input_format_binary_decode_types_in_binary_format)  | 允许在 [`RowBinaryWithNamesAndTypes`](../RowBinaryWithNamesAndTypes.md) 输入格式中，使用 [`binary encoding`](/zh/sql-reference/data-types/data-types-binary-encoding.md) 从头部读取类型，而不是读取类型名称字符串。 | `false` |
| [`output_format_binary_write_json_as_string`](/zh/operations/settings/settings-formats.md/#output_format_binary_write_json_as_string)     | 允许在 [`RowBinary`](../RowBinary.md) 输出格式中，将 [`JSON`](/zh/sql-reference/data-types/newjson.md) 数据类型的值写为 `JSON` [String](/zh/sql-reference/data-types/string.md) 值。                       | `false` |
| [`input_format_binary_read_json_as_string`](/zh/operations/settings/settings-formats.md/#input_format_binary_read_json_as_string)         | 允许在 [`RowBinary`](../RowBinary.md) 输入格式中，将 [`JSON`](/zh/sql-reference/data-types/newjson.md) 数据类型的值按 `JSON` [String](/zh/sql-reference/data-types/string.md) 值读取。                      | `false` |