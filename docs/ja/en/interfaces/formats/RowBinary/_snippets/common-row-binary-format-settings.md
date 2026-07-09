---
title: Common Row Binaryフォーマットの設定
---

{/* 注: このスニペットは、インポート先のすべてのファイルで使い回されます */ }

以下の設定は、すべての `RowBinary` 系フォーマットに共通です。

| 設定                                                                                                                                     | 説明                                                                                                                                                                                               | デフォルト   |
| -------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------- |
| [`format_binary_max_string_size`](/ja/operations/settings/settings-formats.md/#format_binary_max_string_size)                             | RowBinary フォーマットにおける String の最大許容サイズです。                                                                                                                                                          | `1GiB`  |
| [`output_format_binary_encode_types_in_binary_format`](/ja/operations/settings/formats#input_format_binary_decode_types_in_binary_format) | [`RowBinaryWithNamesAndTypes`](../RowBinaryWithNamesAndTypes.md) 出力フォーマットで、型名の文字列の代わりに [`binary encoding`](/ja/sql-reference/data-types/data-types-binary-encoding.md) を使用して、ヘッダーに型を書き込めるようにします。    | `false` |
| [`input_format_binary_decode_types_in_binary_format`](/ja/operations/settings/formats#input_format_binary_decode_types_in_binary_format)  | [`RowBinaryWithNamesAndTypes`](../RowBinaryWithNamesAndTypes.md) 入力フォーマットで、型名の文字列の代わりに [`binary encoding`](/ja/sql-reference/data-types/data-types-binary-encoding.md) を使用して、ヘッダー内の型情報を読み取れるようにします。 | `false` |
| [`output_format_binary_write_json_as_string`](/ja/operations/settings/settings-formats.md/#output_format_binary_write_json_as_string)     | [`RowBinary`](../RowBinary.md) 出力フォーマットで、[`JSON`](/ja/sql-reference/data-types/newjson.md) データ型の値を `JSON` [String](/ja/sql-reference/data-types/string.md) 値として書き込めるようにします。                            | `false` |
| [`input_format_binary_read_json_as_string`](/ja/operations/settings/settings-formats.md/#input_format_binary_read_json_as_string)         | [`RowBinary`](../RowBinary.md) 入力フォーマットで、[`JSON`](/ja/sql-reference/data-types/newjson.md) データ型の値を `JSON` [String](/ja/sql-reference/data-types/string.md) 値として読み取れるようにします。                            | `false` |