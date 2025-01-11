---
title : TemplateIgnoreSpaces
slug : /en/interfaces/formats/TemplateIgnoreSpaces
keywords : [TemplateIgnoreSpaces]
---

## Description

This format is suitable only for input.
Similar to `Template`, but skips whitespace characters between delimiters and values in the input stream. However, if format strings contain whitespace characters, these characters will be expected in the input stream. Also allows specifying empty placeholders (`${}` or `${:None}`) to split some delimiter into separate parts to ignore spaces between them. Such placeholders are used only for skipping whitespace characters.
It’s possible to read `JSON` using this format if the values of columns have the same order in all rows. For example, the following request can be used for inserting data from its output example of format [JSON](/docs/en/interfaces/formats/JSON):

## Example Usage

``` sql
INSERT INTO table_name SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = ','
FORMAT TemplateIgnoreSpaces
```

`/some/path/resultset.format`:

``` text
{${}"meta"${}:${:JSON},${}"data"${}:${}[${data}]${},${}"totals"${}:${:JSON},${}"extremes"${}:${:JSON},${}"rows"${}:${:JSON},${}"rows_before_limit_at_least"${}:${:JSON}${}}
```

`/some/path/row.format`:

``` text
{${}"SearchPhrase"${}:${}${phrase:JSON}${},${}"c"${}:${}${cnt:JSON}${}}
```

## Format Settings