# VerticalRaw

Differs from `Vertical` format in that the rows are not escaped.
This format is only appropriate for outputting a query result, but not for parsing (retrieving data to insert in a table).

Examples:

```
:) SHOW CREATE TABLE geonames FORMAT VerticalRaw;
Row 1:
──────
statement: CREATE TABLE default.geonames ( geonameid UInt32, date Date DEFAULT CAST('2017-12-08' AS Date)) ENGINE = MergeTree(date, geonameid, 8192)

:) SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT VerticalRaw;
Row 1:
──────
test: string with 'quotes' and   with some special
 characters
```

Compare with the Vertical format:

```
:) SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT Vertical;
Row 1:
──────
test: string with \'quotes\' and \t with some special \n characters
```
