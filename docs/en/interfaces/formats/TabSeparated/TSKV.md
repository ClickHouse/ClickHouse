---
alias: []
description: 'Documentation for the TSKV format'
input_format: true
keywords: ['TSKV']
output_format: true
slug: /interfaces/formats/TSKV
title: 'TSKV'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Similar to the [`TabSeparated`](./TabSeparated.md) format, but outputs a value in `name=value` format. 
Names are escaped the same way as in the [`TabSeparated`](./TabSeparated.md) format, and the `=` symbol is also escaped.

```text
SearchPhrase=   count()=8267016
SearchPhrase=bathroom interior design    count()=2166
SearchPhrase=clickhouse     count()=1655
SearchPhrase=2014 spring fashion    count()=1549
SearchPhrase=freeform photos       count()=1480
SearchPhrase=angelina jolie    count()=1245
SearchPhrase=omsk       count()=1112
SearchPhrase=photos of dog breeds    count()=1091
SearchPhrase=curtain designs        count()=1064
SearchPhrase=baku       count()=1000
```


```sql title="Query"
SELECT * FROM t_null FORMAT TSKV
```

```text title="Response"
x=1    y=\N
```

:::note
When there are a large number of small columns, this format is ineffective, and there is generally no reason to use it. 
Nevertheless, it is no worse than the [`JSONEachRow`](../JSON/JSONEachRow.md) format in terms of efficiency.
:::

For parsing, any order is supported for the values of the different columns. 
It is acceptable for some values to be omitted as they are treated as equal to their default values.
In this case, zeros and blank rows are used as default values. 
Complex values that could be specified in the table are not supported as defaults.

Parsing allows an additional field `tskv` to be added without the equal sign or a value. This field is ignored.

During import, columns with unknown names will be skipped, 
if setting [`input_format_skip_unknown_fields`](/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to `1`.

[NULL](/sql-reference/syntax.md) is formatted as `\N`.

## Example Usage {#example-usage}

## Format Settings {#format-settings}