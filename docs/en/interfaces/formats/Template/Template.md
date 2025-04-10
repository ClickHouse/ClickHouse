---
alias: []
description: 'Documentation for the Template format'
input_format: true
keywords: ['Template']
output_format: true
slug: /interfaces/formats/Template
title: 'Template'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

For cases where you need more customization than other standard formats offer, 
the `Template` format allows the user to specify their own custom format string with placeholders for values,
and specifying escaping rules for the data.

It uses the following settings:

| Setting                                                                                                  | Description                                                                                                                |
|----------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------|
| [`format_template_row`](#format_template_row)                                                            | Specifies the path to the file which contains format strings for rows.                                                     |
| [`format_template_resultset`](#format_template_resultset)                                                | Specifies the path to the file which contains format strings for rows                                                      |
| [`format_template_rows_between_delimiter`](#format_template_rows_between_delimiter)                      | Specifies the delimiter between rows, which is printed (or expected) after every row except the last one (`\n` by default) |
| `format_template_row_format`                                                                             | Specifies the format string for rows [in-line](#inline_specification).                                                     |                                                                           
| `format_template_resultset_format`                                                                       | Specifies the result set format string [in-line](#inline_specification).                                                   |
| Some settings of other formats (e.g.`output_format_json_quote_64bit_integers` when using `JSON` escaping |                                                                                                                            |

## Settings And Escaping Rules {#settings-and-escaping-rules}

### format_template_row {#format_template_row}

The setting `format_template_row` specifies the path to the file which contains format strings for rows with the following syntax:

```text
delimiter_1${column_1:serializeAs_1}delimiter_2${column_2:serializeAs_2} ... delimiter_N
```

Where:

| Part of syntax | Description                                                                                                       |
|----------------|-------------------------------------------------------------------------------------------------------------------|
| `delimiter_i`  | A delimiter between values (`$` symbol can be escaped as `$$`)                                                    |
| `column_i`     | The name or index of a column whose values are to be selected or inserted (if empty, then the column will be skipped) |
|`serializeAs_i` | An escaping rule for the column values.                                                                           |

The following escaping rules are supported:

| Escaping Rule        | Description                              |
|----------------------|------------------------------------------|
| `CSV`, `JSON`, `XML` | Similar to the formats of the same names |
| `Escaped`            | Similar to `TSV`                         |
| `Quoted`             | Similar to `Values`                      |
| `Raw`                | Without escaping, similar to `TSVRaw`    |   
| `None`               | No escaping rule - see note below        |

:::note
If an escaping rule is omitted, then `None` will be used. `XML` is suitable only for output.
:::

Let's look at an example. Given the following format string:

```text
Search phrase: ${s:Quoted}, count: ${c:Escaped}, ad price: $$${p:JSON};
```

The following values will be printed (if using `SELECT`) or expected (if using `INPUT`), 
between columns `Search phrase:`, `, count:`, `, ad price: $` and `;` delimiters respectively:

- `s` (with escape rule `Quoted`)
- `c` (with escape rule `Escaped`)
- `p` (with escape rule `JSON`)

For example:

- If `INSERT`ing, the line below matches the expected template and would read values `bathroom interior design`, `2166`, `$3` into columns `Search phrase`, `count`, `ad price`.
- If `SELECT`ing the line below is the output, assuming that values `bathroom interior design`, `2166`, `$3` are already stored in a table under columns `Search phrase`, `count`, `ad price`.  

```yaml
Search phrase: 'bathroom interior design', count: 2166, ad price: $3;
```

### format_template_rows_between_delimiter {#format_template_rows_between_delimiter}

The setting `format_template_rows_between_delimiter` setting specifies the delimiter between rows, which is printed (or expected) after every row except the last one (`\n` by default)

### format_template_resultset {#format_template_resultset}

The setting `format_template_resultset` specifies the path to the file, which contains a format string for the result set. 

The format string for the result set has the same syntax as a format string for rows. 
It allows for specifying a prefix, a suffix and a way to print some additional information and contains the following placeholders instead of column names:

- `data` is the rows with data in `format_template_row` format, separated by `format_template_rows_between_delimiter`. This placeholder must be the first placeholder in the format string.
- `totals` is the row with total values in `format_template_row` format (when using WITH TOTALS).
- `min` is the row with minimum values in `format_template_row` format (when extremes are set to 1).
- `max` is the row with maximum values in `format_template_row` format (when extremes are set to 1).
- `rows` is the total number of output rows.
- `rows_before_limit` is the minimal number of rows there would have been without LIMIT. Output only if the query contains LIMIT. If the query contains GROUP BY, rows_before_limit_at_least is the exact number of rows there would have been without a LIMIT.
- `time` is the request execution time in seconds.
- `rows_read` is the number of rows has been read.
- `bytes_read` is the number of bytes (uncompressed) has been read.

The placeholders `data`, `totals`, `min` and `max` must not have escaping rule specified (or `None` must be specified explicitly). The remaining placeholders may have any escaping rule specified.

:::note
If the `format_template_resultset` setting is an empty string, `${data}` is used as the default value.
:::

For insert queries format allows skipping some columns or fields if prefix or suffix (see example).

### In-line specification {#inline_specification}

Often times it is challenging or not possible to deploy the format configurations
(set by `format_template_row`, `format_template_resultset`) for the template format to a directory on all nodes in a cluster. 
Furthermore, the format may be so trivial that it does not require being placed in a file.

For these cases, `format_template_row_format` (for `format_template_row`) and `format_template_resultset_format` (for `format_template_resultset`) can be used to set the template string directly in the query, 
rather than as a path to the file which contains it.

:::note
The rules for format strings and escape sequences are the same as those for:
- [`format_template_row`](#format_template_row) when using `format_template_row_format`.
- [`format_template_resultset`](#format_template_resultset) when using `format_template_resultset_format`.
:::

## Example Usage {#example-usage}

Let's look at two examples of how we can use the `Template` format, first for selecting data and then for inserting data.

### Selecting Data {#selecting-data}

```sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase ORDER BY c DESC LIMIT 5 FORMAT Template SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = '\n    '
```

```text title="/some/path/resultset.format"
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    ${data}
  </table>
  <table border="1"> <caption>Max</caption>
    ${max}
  </table>
  <b>Processed ${rows_read:XML} rows in ${time:XML} sec</b>
 </body>
</html>
```

```text title="/some/path/row.format"
<tr> <td>${0:XML}</td> <td>${1:XML}</td> </tr>
```

Result:

```html
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    <tr> <td></td> <td>8267016</td> </tr>
    <tr> <td>bathroom interior design</td> <td>2166</td> </tr>
    <tr> <td>clickhouse</td> <td>1655</td> </tr>
    <tr> <td>spring 2014 fashion</td> <td>1549</td> </tr>
    <tr> <td>freeform photos</td> <td>1480</td> </tr>
  </table>
  <table border="1"> <caption>Max</caption>
    <tr> <td></td> <td>8873898</td> </tr>
  </table>
  <b>Processed 3095973 rows in 0.1569913 sec</b>
 </body>
</html>
```

### Inserting Data {#inserting-data}

```text
Some header
Page views: 5, User id: 4324182021466249494, Useless field: hello, Duration: 146, Sign: -1
Page views: 6, User id: 4324182021466249494, Useless field: world, Duration: 185, Sign: 1
Total rows: 2
```

```sql
INSERT INTO UserActivity SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format'
FORMAT Template
```

```text title="/some/path/resultset.format"
Some header\n${data}\nTotal rows: ${:CSV}\n
```

```text title="/some/path/row.format"
Page views: ${PageViews:CSV}, User id: ${UserID:CSV}, Useless field: ${:CSV}, Duration: ${Duration:CSV}, Sign: ${Sign:CSV}
```

`PageViews`, `UserID`, `Duration` and `Sign` inside placeholders are names of columns in the table. Values after `Useless field` in rows and after `\nTotal rows:` in suffix will be ignored.
All delimiters in the input data must be strictly equal to delimiters in specified format strings.

### In-line Specification {#in-line-specification}

Tired of manually formatting markdown tables? In this example we'll look at how we can use the `Template` format and in-line specification settings to achieve a simple task - `SELECT`ing the names of some ClickHouse formats from the `system.formats` table and formatting them as a markdown table. This can be easily achieved using the `Template` format and settings `format_template_row_format` and `format_template_resultset_format`.

In previous examples we specified the result-set and row format strings in separate files, with the paths to those files specified using the `format_template_resultset` and `format_template_row` settings respectively. Here we'll do it in-line because our template is trivial, consisting only of a few `|` and `-` to make the markdown table. We'll specify our result-set template string using the setting `format_template_resultset_format`. To make the table header we've added `|ClickHouse Formats|\n|---|\n` before `${data}`. We use setting `format_template_row_format` to specify the template string `` |`{0:XML}`| `` for our rows. The `Template` format will insert our rows with the given format into placeholder `${data}`. In this example we have only one column, but if you wanted to add more you could do so by adding `{1:XML}`, `{2:XML}`... etc to your row template string, choosing the escaping rule as appropriate. In this example we've gone with escaping rule `XML`. 

```sql title="Query"
WITH formats AS
(
 SELECT * FROM system.formats
 ORDER BY rand()
 LIMIT 5
)
SELECT * FROM formats
FORMAT Template
SETTINGS
 format_template_row_format='|`${0:XML}`|',
 format_template_resultset_format='|ClickHouse Formats|\n|---|\n${data}\n'
```

Look at that! We've saved ourselves the trouble of having to manually add all those `|`s and `-`s to make that markdown table:

```response title="Response"
|ClickHouse Formats|
|---|
|`BSONEachRow`|
|`CustomSeparatedWithNames`|
|`Prometheus`|
|`DWARF`|
|`Avro`|
```


