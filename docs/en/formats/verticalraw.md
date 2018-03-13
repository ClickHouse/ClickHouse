# VerticalRaw

Differs from `TabSeparated` format in that the rows are written without escaping.
This format is only appropriate for outputting a query result, but not for parsing (retrieving data to insert in a table).

Samples:
```
:) SHOW CREATE TABLE geonames FORMAT VerticalRaw;
Row 1:
──────
statement: CREATE TABLE default.geonames ( geonameid UInt32,  name String,  asciiname String, alternatenames String,  latitude Float32,  longitude Float32,  feature_class String,  feature_code String,  country_code String,  cc2 String,  admin1_code String,  admin2_code String,  admin3_code String,  admin4_code String,  population Int64,  elevation String,  dem String,  timezone String,  modification_date Date, date Date DEFAULT CAST('2017-12-08' AS Date)) ENGINE = MergeTree(date, geonameid, 8192)

:) SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT VerticalRaw;
Row 1:
──────
test: string with 'quotes' and   with some special
 characters

-- the same in Vertical format:
:) SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT Vertical;
Row 1:
──────
test: string with \'quotes\' and \t with some special \n characters
```
