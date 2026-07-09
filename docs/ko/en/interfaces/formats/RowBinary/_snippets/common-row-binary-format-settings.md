---
title: 공통 Row Binary 포맷 설정
---

{/* 참고: 이 스니펫은 불러오는 모든 파일에서 재사용됩니다. */ }

다음 설정은 모든 `RowBinary` 계열 포맷에 공통으로 적용됩니다.

| 설정                                                                                                                                     | 설명                                                                                                                                                                                          | 기본값     |
| -------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| [`format_binary_max_string_size`](/ko/operations/settings/settings-formats.md/#format_binary_max_string_size)                             | RowBinary 포맷에서 `String`에 허용되는 최대 크기입니다.                                                                                                                                                     | `1GiB`  |
| [`output_format_binary_encode_types_in_binary_format`](/ko/operations/settings/formats#input_format_binary_decode_types_in_binary_format) | [`RowBinaryWithNamesAndTypes`](../RowBinaryWithNamesAndTypes.md) 출력 형식에서 타입 이름 문자열 대신 [`binary encoding`](/ko/sql-reference/data-types/data-types-binary-encoding.md)을 사용해 헤더에 타입을 기록할 수 있습니다. | `false` |
| [`input_format_binary_decode_types_in_binary_format`](/ko/operations/settings/formats#input_format_binary_decode_types_in_binary_format)  | [`RowBinaryWithNamesAndTypes`](../RowBinaryWithNamesAndTypes.md) 입력 형식에서 타입 이름 문자열 대신 [`binary encoding`](/ko/sql-reference/data-types/data-types-binary-encoding.md)을 사용해 헤더의 타입을 읽을 수 있습니다.  | `false` |
| [`output_format_binary_write_json_as_string`](/ko/operations/settings/settings-formats.md/#output_format_binary_write_json_as_string)     | [`RowBinary`](../RowBinary.md) 출력 형식에서 [`JSON`](/ko/sql-reference/data-types/newjson.md) 데이터 타입의 값을 `JSON` [String](/ko/sql-reference/data-types/string.md) 값으로 기록할 수 있습니다.                       | `false` |
| [`input_format_binary_read_json_as_string`](/ko/operations/settings/settings-formats.md/#input_format_binary_read_json_as_string)         | [`RowBinary`](../RowBinary.md) 입력 형식에서 [`JSON`](/ko/sql-reference/data-types/newjson.md) 데이터 타입의 값을 `JSON` [String](/ko/sql-reference/data-types/string.md) 값으로 읽을 수 있습니다.                        | `false` |