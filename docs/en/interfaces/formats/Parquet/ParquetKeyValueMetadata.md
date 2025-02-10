---
title : ParquetKeyValueMetadata
slug : /en/interfaces/formats/ParquetKeyValueMetadata
keywords : [ParquetKeyValueMetadata]
---

## Description

Special format for reading Parquet file custom metadata, which is defined by the `key_value_metadata` field in the `FileMetadata` structure in the Parquet [Thrift definition](https://github.com/apache/parquet-format/blob/94b9d631aef332c78b8f1482fb032743a9c3c407/src/main/thrift/parquet.thrift#L1263).

It always outputs one row with the following structure:
- `key_value_metadata` - a Map(String, String) containing all custom key-value metadata pairs from the file

Note: If a Parquet file contains no key-value metadata entries, this format will return one row
with an empty map.

## Example Usage

Example:

```sql
SELECT *
FROM file(data.parquet, ParquetKeyValueMetadata)
FORMAT PrettyJSONEachRow
```

```json
{
    "key_value_metadata": {
        "key": "value",
        "hello": "world",
    }
}
```
