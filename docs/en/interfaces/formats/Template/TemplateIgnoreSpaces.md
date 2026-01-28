---
alias: []
description: 'Documentation for the TemplateIgnoreSpaces format'
input_format: true
keywords: ['TemplateIgnoreSpaces']
output_format: false
slug: /interfaces/formats/TemplateIgnoreSpaces
title: 'TemplateIgnoreSpaces'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✗      |       |

## Description {#description}

Similar to [`Template`], but skips whitespace characters between delimiters and values in the input stream. 
However, if format strings contain whitespace characters, these characters will be expected in the input stream. 
Also allows specifying empty placeholders (`${}` or `${:None}`) to split some delimiter into separate parts to ignore spaces between them. 
Such placeholders are used only for skipping whitespace characters.
It's possible to read `JSON` using this format if the values of columns have the same order in all rows.

:::note
This format is suitable only for input.
:::

## Example Usage {#example-usage}

The following request can be used for inserting data from its output example of format [JSON](/interfaces/formats/JSON):

```sql
INSERT INTO table_name 
SETTINGS
    format_template_resultset = '/some/path/resultset.format',
    format_template_row = '/some/path/row.format',
    format_template_rows_between_delimiter = ','
FORMAT TemplateIgnoreSpaces
```

```text title="/some/path/resultset.format"
{${}"meta"${}:${:JSON},${}"data"${}:${}[${data}]${},${}"totals"${}:${:JSON},${}"extremes"${}:${:JSON},${}"rows"${}:${:JSON},${}"rows_before_limit_at_least"${}:${:JSON}${}}
```

```text title="/some/path/row.format"
{${}"SearchPhrase"${}:${}${phrase:JSON}${},${}"c"${}:${}${cnt:JSON}${}}
```

## Format Settings {#format-settings}