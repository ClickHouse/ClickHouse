---
description: 'Documentation for the ParquetMetadata format'
keywords: ['ParquetMetadata']
slug: /interfaces/formats/ParquetMetadata
title: 'ParquetMetadata'
---

## Description {#description}

Special format for reading Parquet file metadata (https://parquet.apache.org/docs/file-format/metadata/). It always outputs one row with the next structure/content:
- `num_columns` - the number of columns
- ``num_rows` - the total number of rows
- `num_row_groups` - the total number of row groups
- `format_version` - parquet format version, always 1.0 or 2.6
- `total_uncompressed_size` - total uncompressed bytes size of the data, calculated as the sum of total_byte_size from all row groups
- `total_compressed_size` - total compressed bytes size of the data, calculated as the sum of total_compressed_size from all row groups
- `columns` - the list of columns metadata with the next structure:
    - `name` - column name
    - `path` - column path (differs from name for nested column)
    - `max_definition_level` - maximum definition level
    - `max_repetition_level` - maximum repetition level
    - `physical_type` - column physical type
    - `logical_type` - column logical type
    - `compression` - compression used for this column
    - `total_uncompressed_size` - total uncompressed bytes size of the column, calculated as the sum of total_uncompressed_size of the column from all row groups
    - `total_compressed_size` - total compressed bytes size of the column,  calculated as the sum of total_compressed_size of the column from all row groups
    - `space_saved` - percent of space saved by compression, calculated as (1 - total_compressed_size/total_uncompressed_size).
    - `encodings` - the list of encodings used for this column
- `row_groups` - the list of row groups metadata with the next structure:
    - `num_columns` - the number of columns in the row group
    - `num_rows` - the number of rows in the row group
    - `total_uncompressed_size` - total uncompressed bytes size of the row group
    - `total_compressed_size` - total compressed bytes size of the row group
    - `columns` - the list of column chunks metadata with the next structure:
        - `name` - column name
        - `path` - column path
        - `total_compressed_size` - total compressed bytes size of the column
        - `total_uncompressed_size` - total uncompressed bytes size of the row group
        - `have_statistics` - boolean flag that indicates if column chunk metadata contains column statistics
        - `statistics` - column chunk statistics (all fields are NULL if have_statistics = false) with the next structure:
            - `num_values` - the number of non-null values in the column chunk
            - `null_count` - the number of NULL values in the column chunk
            - `distinct_count` - the number of distinct values in the column chunk
            - `min` - the minimum value of the column chunk
            - `max` - the maximum column of the column chunk

## Example Usage {#example-usage}

Example:

```sql
SELECT * 
FROM file(data.parquet, ParquetMetadata) 
FORMAT PrettyJSONEachRow
```

```json
{
    "num_columns": "2",
    "num_rows": "100000",
    "num_row_groups": "2",
    "format_version": "2.6",
    "metadata_size": "577",
    "total_uncompressed_size": "282436",
    "total_compressed_size": "26633",
    "columns": [
        {
            "name": "number",
            "path": "number",
            "max_definition_level": "0",
            "max_repetition_level": "0",
            "physical_type": "INT32",
            "logical_type": "Int(bitWidth=16, isSigned=false)",
            "compression": "LZ4",
            "total_uncompressed_size": "133321",
            "total_compressed_size": "13293",
            "space_saved": "90.03%",
            "encodings": [
                "RLE_DICTIONARY",
                "PLAIN",
                "RLE"
            ]
        },
        {
            "name": "concat('Hello', toString(modulo(number, 1000)))",
            "path": "concat('Hello', toString(modulo(number, 1000)))",
            "max_definition_level": "0",
            "max_repetition_level": "0",
            "physical_type": "BYTE_ARRAY",
            "logical_type": "None",
            "compression": "LZ4",
            "total_uncompressed_size": "149115",
            "total_compressed_size": "13340",
            "space_saved": "91.05%",
            "encodings": [
                "RLE_DICTIONARY",
                "PLAIN",
                "RLE"
            ]
        }
    ],
    "row_groups": [
        {
            "num_columns": "2",
            "num_rows": "65409",
            "total_uncompressed_size": "179809",
            "total_compressed_size": "14163",
            "columns": [
                {
                    "name": "number",
                    "path": "number",
                    "total_compressed_size": "7070",
                    "total_uncompressed_size": "85956",
                    "have_statistics": true,
                    "statistics": {
                        "num_values": "65409",
                        "null_count": "0",
                        "distinct_count": null,
                        "min": "0",
                        "max": "999"
                    }
                },
                {
                    "name": "concat('Hello', toString(modulo(number, 1000)))",
                    "path": "concat('Hello', toString(modulo(number, 1000)))",
                    "total_compressed_size": "7093",
                    "total_uncompressed_size": "93853",
                    "have_statistics": true,
                    "statistics": {
                        "num_values": "65409",
                        "null_count": "0",
                        "distinct_count": null,
                        "min": "Hello0",
                        "max": "Hello999"
                    }
                }
            ]
        },
        ...
    ]
}
```


