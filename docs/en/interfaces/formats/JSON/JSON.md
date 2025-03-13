---
alias: []
description: 'Documentation for the JSON format'
input_format: true
keywords: ['JSON']
output_format: true
slug: /interfaces/formats/JSON
title: 'JSON'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `JSON` format reads and outputs data in the JSON format. 

The `JSON` format returns the following: 

| Parameter                    | Description                                                                                                                                                                                                                                |
|------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `meta`                       | Column names and types.                                                                                                                                                                                                                    |
| `data`                       | Data tables                                                                                                                                                                                                                                |
| `rows`                       | The total number of output rows.                                                                                                                                                                                                           |
| `rows_before_limit_at_least` | The minimal number of rows there would have been without LIMIT. Output only if the query contains LIMIT. If the query contains `GROUP BY`, rows_before_limit_at_least is the exact number of rows there would have been without a `LIMIT`. |
| `statistics`                 | Statistics such as `elapsed`, `rows_read`, `bytes_read`.                                                                                                                                                                                   |
| `totals`                     | Total values (when using WITH TOTALS).                                                                                                                                                                                                     |
| `extremes`                   | Extreme values (when extremes are set to 1).                                                                                                                                                                                               |

The `JSON` type is compatible with JavaScript. To ensure this, some characters are additionally escaped: 
- the slash `/` is escaped as `\/`
- alternative line breaks `U+2028` and `U+2029`, which break some browsers, are escaped as `\uXXXX`. 
- ASCII control characters are escaped: backspace, form feed, line feed, carriage return, and horizontal tab are replaced with `\b`, `\f`, `\n`, `\r`, `\t` , as well as the remaining bytes in the 00-1F range using `\uXXXX` sequences. 
- Invalid UTF-8 sequences are changed to the replacement character � so the output text will consist of valid UTF-8 sequences. 

For compatibility with JavaScript, Int64 and UInt64 integers are enclosed in double quotes by default. 
To remove the quotes, you can set the configuration parameter [`output_format_json_quote_64bit_integers`](/operations/settings/settings-formats.md/#output_format_json_quote_64bit_integers) to `0`.

ClickHouse supports [NULL](/sql-reference/syntax.md), which is displayed as `null` in the JSON output. To enable `+nan`, `-nan`, `+inf`, `-inf` values in output, set the [output_format_json_quote_denormals](/operations/settings/settings-formats.md/#output_format_json_quote_denormals) to `1`.

## Example Usage {#example-usage}

Example:

```sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase WITH TOTALS ORDER BY c DESC LIMIT 5 FORMAT JSON
```

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
        [
                {
                        "num": 42,
                        "str": "hello",
                        "arr": [0,1]
                },
                {
                        "num": 43,
                        "str": "hello",
                        "arr": [0,1,2]
                },
                {
                        "num": 44,
                        "str": "hello",
                        "arr": [0,1,2,3]
                }
        ],

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.001137687,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

## Format Settings {#format-settings}

For JSON input format, if setting [`input_format_json_validate_types_from_metadata`](/operations/settings/settings-formats.md/#input_format_json_validate_types_from_metadata) is set to `1`,
the types from metadata in input data will be compared with the types of the corresponding columns from the table.

## See Also {#see-also}

- [JSONEachRow](/interfaces/formats/JSONEachRow) format
- [output_format_json_array_of_rows](/operations/settings/settings-formats.md/#output_format_json_array_of_rows) setting