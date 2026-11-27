---
title: 'Exporting JSON'
slug: /integrations/data-formats/json/exporting
description: 'How to export JSON data from ClickHouse'
keywords: ['json', 'clickhouse', 'formats', 'exporting']
doc_type: 'guide'
---

Almost any JSON format used for import can be used for export as well. The most popular is [`JSONEachRow`](/reference/formats/JSON/JSONEachRow):

```sql
SELECT * FROM sometable FORMAT JSONEachRow
```
```response
{"path":"Bob_Dolman","month":"2016-11-01","hits":245}
{"path":"1-krona","month":"2017-01-01","hits":4}
{"path":"Ahmadabad-e_Kalij-e_Sofla","month":"2017-01-01","hits":3}
```

Or we can use [`JSONCompactEachRow`](/reference/formats/JSON/JSONCompactEachRow) to save disk space by skipping column names:

```sql
SELECT * FROM sometable FORMAT JSONCompactEachRow
```
```response
["Bob_Dolman", "2016-11-01", 245]
["1-krona", "2017-01-01", 4]
["Ahmadabad-e_Kalij-e_Sofla", "2017-01-01", 3]
```

## Overriding data types as strings {#overriding-data-types-as-strings}

ClickHouse respects data types and will export JSON accordingly to standards. But in cases where we need to have all values encoded as strings, we can use the [JSONStringsEachRow](/reference/formats/JSON/JSONStringsEachRow) format:

```sql
SELECT * FROM sometable FORMAT JSONStringsEachRow
```
```response
{"path":"Bob_Dolman","month":"2016-11-01","hits":"245"}
{"path":"1-krona","month":"2017-01-01","hits":"4"}
{"path":"Ahmadabad-e_Kalij-e_Sofla","month":"2017-01-01","hits":"3"}
```

Now, the `hits` numeric column is encoded as a string. Exporting as strings is supported for all JSON* formats, just explore `JSONStrings\*` and `JSONCompactStrings\*` formats:

```sql
SELECT * FROM sometable FORMAT JSONCompactStringsEachRow
```
```response
["Bob_Dolman", "2016-11-01", "245"]
["1-krona", "2017-01-01", "4"]
["Ahmadabad-e_Kalij-e_Sofla", "2017-01-01", "3"]
```

## Exporting metadata together with data {#exporting-metadata-together-with-data}

General [JSON](/reference/formats/JSON/JSON) format, which is popular in apps, will export not only resulting data but column types and query stats:

```sql
SELECT * FROM sometable FORMAT JSON
```
```response
{
        "meta":
        [
                {
                        "name": "path",
                        "type": "String"
                },
                ...
        ],

        "data":
        [
                {
                        "path": "Bob_Dolman",
                        "month": "2016-11-01",
                        "hits": 245
                },
                ...
        ],

        "rows": 3,

        "statistics":
        {
                "elapsed": 0.000497457,
                "rows_read": 3,
                "bytes_read": 87
        }
}
```

The [JSONCompact](/reference/formats/JSON/JSONCompact) format will print the same metadata but use a compacted form for the data itself:

```sql
SELECT * FROM sometable FORMAT JSONCompact
```
```response
{
        "meta":
        [
                {
                        "name": "path",
                        "type": "String"
                },
                ...
        ],

        "data":
        [
                ["Bob_Dolman", "2016-11-01", 245],
                ["1-krona", "2017-01-01", 4],
                ["Ahmadabad-e_Kalij-e_Sofla", "2017-01-01", 3]
        ],

        "rows": 3,

        "statistics":
        {
                "elapsed": 0.00074981,
                "rows_read": 3,
                "bytes_read": 87
        }
}
```

Consider [`JSONStrings`](/reference/formats/JSON/JSONStrings) or [`JSONCompactStrings`](/reference/formats/JSON/JSONCompactStrings) variants to encode all values as strings.

## Compact way to export JSON data and structure {#compact-way-to-export-json-data-and-structure}

A more efficient way to have data, as well as it's structure, is to use [`JSONCompactEachRowWithNamesAndTypes`](/reference/formats/JSON/JSONCompactEachRowWithNamesAndTypes) format:

```sql
SELECT * FROM sometable FORMAT JSONCompactEachRowWithNamesAndTypes
```
```response
["path", "month", "hits"]
["String", "Date", "UInt32"]
["Bob_Dolman", "2016-11-01", 245]
["1-krona", "2017-01-01", 4]
["Ahmadabad-e_Kalij-e_Sofla", "2017-01-01", 3]
```

This will use a compact JSON format prepended by two header rows with column names and types. This format can then be used to ingest data into another ClickHouse instance (or other apps).

## Exporting JSON to a file {#exporting-json-to-a-file}

To save exported JSON data to a file, we can use an [INTO OUTFILE](/reference/statements/select/into-outfile) clause:

```sql
SELECT * FROM sometable INTO OUTFILE 'out.json' FORMAT JSONEachRow
```
```response
36838935 rows in set. Elapsed: 2.220 sec. Processed 36.84 million rows, 1.27 GB (16.60 million rows/s., 572.47 MB/s.)
```

It took ClickHouse only 2 seconds to export almost 37 million records to a JSON file. We can also export using a `COMPRESSION` clause to enable compression on the fly:

```sql
SELECT * FROM sometable INTO OUTFILE 'out.json.gz' FORMAT JSONEachRow
```
```response
36838935 rows in set. Elapsed: 22.680 sec. Processed 36.84 million rows, 1.27 GB (1.62 million rows/s., 56.02 MB/s.)
```

It takes more time to accomplish, but generates a much smaller compressed file:

```bash
2.2G    out.json
576M    out.json.gz
```
