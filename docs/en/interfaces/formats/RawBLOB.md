---
description: 'Documentation for the RawBLOB format'
keywords: ['RawBLOB']
slug: /interfaces/formats/RawBLOB
title: 'RawBLOB'
---

## Description {#description}

The `RawBLOB` formats reads all input data to a single value. It is possible to parse only a table with a single field of type [`String`](/sql-reference/data-types/string.md) or similar.
The result is output as a binary format without delimiters and escaping. If more than one value is output, the format is ambiguous, and it will be impossible to read the data back.

### Raw Formats Comparison {#raw-formats-comparison}

Below is a comparison of the formats `RawBLOB` and [`TabSeparatedRaw`](./TabSeparated/TabSeparatedRaw.md).

`RawBLOB`:
- data is output in binary format, no escaping;
- there are no delimiters between values;
- no new-line at the end of each value.

`TabSeparatedRaw`:
- data is output without escaping;
- the rows contain values separated by tabs;
- there is a line feed after the last value in every row.

The following is a comparison of the `RawBLOB` and [RowBinary](./RowBinary/RowBinary.md) formats.

`RawBLOB`:
- String fields are output without being prefixed by length.

`RowBinary`:
- String fields are represented as length in varint format (unsigned [LEB128] (https://en.wikipedia.org/wiki/LEB128)), followed by the bytes of the string.

When empty data is passed to the `RawBLOB` input, ClickHouse throws an exception:

```text
Code: 108. DB::Exception: No data to insert
```

## Example Usage {#example-usage}

```bash title="Query"
$ clickhouse-client --query "CREATE TABLE {some_table} (a String) ENGINE = Memory;"
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT RawBLOB"
$ clickhouse-client --query "SELECT * FROM {some_table} FORMAT RawBLOB" | md5sum
```

```text title="Response"
f9725a22f9191e064120d718e26862a9  -
```

## Format Settings {#format-settings}
