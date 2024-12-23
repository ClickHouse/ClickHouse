---
title : Template
slug : /en/interfaces/formats/Template
tags : [Template]
---

# Description

This format allows specifying a custom format string with placeholders for values with a specified escaping rule.

It uses settings `format_template_resultset`, `format_template_row` (`format_template_row_format`), `format_template_rows_between_delimiter` and some settings of other formats (e.g. `output_format_json_quote_64bit_integers` when using `JSON` escaping, see further)

Setting `format_template_row` specifies the path to the file containing format strings for rows with the following syntax:

`delimiter_1${column_1:serializeAs_1}delimiter_2${column_2:serializeAs_2} ... delimiter_N`,

where `delimiter_i` is a delimiter between values (`$` symbol can be escaped as `$$`),
`column_i` is a name or index of a column whose values are to be selected or inserted (if empty, then column will be skipped),
`serializeAs_i` is an escaping rule for the column values. The following escaping rules are supported:

- `CSV`, `JSON`, `XML` (similar to the formats of the same names)
- `Escaped` (similar to `TSV`)
- `Quoted` (similar to `Values`)
- `Raw` (without escaping, similar to `TSVRaw`)
- `None` (no escaping rule, see further)

If an escaping rule is omitted, then `None` will be used. `XML` is suitable only for output.

So, for the following format string:

      `Search phrase: ${SearchPhrase:Quoted}, count: ${c:Escaped}, ad price: $$${price:JSON};`

the values of `SearchPhrase`, `c` and `price` columns, which are escaped as `Quoted`, `Escaped` and `JSON` will be printed (for select) or will be expected (for insert) between `Search phrase:`, `, count:`, `, ad price: $` and `;` delimiters respectively. For example:

`Search phrase: 'bathroom interior design', count: 2166, ad price: $3;`

In cases where it is challenging or not possible to deploy format output configuration for the template format to a directory on all nodes in a cluster, or if the format is trivial then `format_template_row_format` can be used to set the template string directly in the query, rather than a path to the file which contains it.

The `format_template_rows_between_delimiter` setting specifies the delimiter between rows, which is printed (or expected) after every row except the last one (`\n` by default)

Setting `format_template_resultset` specifies the path to the file, which contains a format string for resultset. Setting `format_template_resultset_format` can be used to set the template string for the result set directly in the query itself. Format string for resultset has the same syntax as a format string for row and allows to specify a prefix, a suffix and a way to print some additional information. It contains the following placeholders instead of column names:

- `data` is the rows with data in `format_template_row` format, separated by `format_template_rows_between_delimiter`. This placeholder must be the first placeholder in the format string.
- `totals` is the row with total values in `format_template_row` format (when using WITH TOTALS)
- `min` is the row with minimum values in `format_template_row` format (when extremes are set to 1)
- `max` is the row with maximum values in `format_template_row` format (when extremes are set to 1)
- `rows` is the total number of output rows
- `rows_before_limit` is the minimal number of rows there would have been without LIMIT. Output only if the query contains LIMIT. If the query contains GROUP BY, rows_before_limit_at_least is the exact number of rows there would have been without a LIMIT.
- `time` is the request execution time in seconds
- `rows_read` is the number of rows has been read
- `bytes_read` is the number of bytes (uncompressed) has been read

The placeholders `data`, `totals`, `min` and `max` must not have escaping rule specified (or `None` must be specified explicitly). The remaining placeholders may have any escaping rule specified.
If the `format_template_resultset` setting is an empty string, `${data}` is used as the default value.
For insert queries format allows skipping some columns or fields if prefix or suffix (see example).

# Example Usage

## Selecting Data

Select example:

``` sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase ORDER BY c DESC LIMIT 5 FORMAT Template SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = '\n    '
```

`/some/path/resultset.format`:

``` text
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

`/some/path/row.format`:

``` text
<tr> <td>${0:XML}</td> <td>${1:XML}</td> </tr>
```

Result:

``` html
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

## Inserting Data

Insert example:

``` text
Some header
Page views: 5, User id: 4324182021466249494, Useless field: hello, Duration: 146, Sign: -1
Page views: 6, User id: 4324182021466249494, Useless field: world, Duration: 185, Sign: 1
Total rows: 2
```

``` sql
INSERT INTO UserActivity SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format'
FORMAT Template
```

`/some/path/resultset.format`:

``` text
Some header\n${data}\nTotal rows: ${:CSV}\n
```

`/some/path/row.format`:

``` text
Page views: ${PageViews:CSV}, User id: ${UserID:CSV}, Useless field: ${:CSV}, Duration: ${Duration:CSV}, Sign: ${Sign:CSV}
```

`PageViews`, `UserID`, `Duration` and `Sign` inside placeholders are names of columns in the table. Values after `Useless field` in rows and after `\nTotal rows:` in suffix will be ignored.
All delimiters in the input data must be strictly equal to delimiters in specified format strings.

# Format Settings



