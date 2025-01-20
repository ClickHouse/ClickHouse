---
title : RawBLOB
slug : /en/interfaces/formats/RawBLOB
keywords : [RawBLOB]
---

## Description

In this format, all input data is read to a single value. It is possible to parse only a table with a single field of type [String](/docs/en/sql-reference/data-types/string.md) or similar.
The result is output in binary format without delimiters and escaping. If more than one value is output, the format is ambiguous, and it will be impossible to read the data back.

Below is a comparison of the formats `RawBLOB` and [TabSeparatedRaw](/docs/en/interfaces/formats/TabSeparatedRaw).

`RawBLOB`:
- data is output in binary format, no escaping;
- there are no delimiters between values;
- no newline at the end of each value.

`TabSeparatedRaw`:
- data is output without escaping;
- the rows contain values separated by tabs;
- there is a line feed after the last value in every row.

The following is a comparison of the `RawBLOB` and [RowBinary](/docs/en/interfaces/formats/RowBinary) formats.

`RawBLOB`:
- String fields are output without being prefixed by length.

`RowBinary`:
- String fields are represented as length in varint format (unsigned [LEB128] (https://en.wikipedia.org/wiki/LEB128)), followed by the bytes of the string.

When an empty data is passed to the `RawBLOB` input, ClickHouse throws an exception:

``` text
Code: 108. DB::Exception: No data to insert
```

## Example Usage


``` bash
$ clickhouse-client --query "CREATE TABLE {some_table} (a String) ENGINE = Memory;"
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT RawBLOB"
$ clickhouse-client --query "SELECT * FROM {some_table} FORMAT RawBLOB" | md5sum
```

Result:

``` text
f9725a22f9191e064120d718e26862a9  -
```

## Format Settings
