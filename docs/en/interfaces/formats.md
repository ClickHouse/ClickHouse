---
description: 'Overview of supported data formats for input and output in ClickHouse'
sidebar_label: 'View all formats...'
sidebar_position: 21
slug: /interfaces/formats
title: 'Formats for input and output data'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# Formats for input and output data {#formats-for-input-and-output-data}

ClickHouse supports most of the known text and binary data formats. This allows easy integration into almost any working
data pipeline to leverage the benefits of ClickHouse.

## Input formats {#input-formats}

Input formats are used for:
- Parsing data provided to `INSERT` statements
- Performing `SELECT` queries from file-backed tables such as `File`, `URL`, or `HDFS`
- Reading dictionaries

Choosing the right input format is crucial for efficient data ingestion in ClickHouse. With over 70 supported formats, 
selecting the most performant option can significantly impact insert speed, CPU and memory usage, and overall system 
efficiency. To help navigate these choices, we benchmarked ingestion performance across formats, revealing key takeaways:

- **The [Native](formats/Native.md) format is the most efficient input format**, offering the best compression, lowest 
  resource usage, and minimal server-side processing overhead.
- **Compression is essential** - LZ4 reduces data size with minimal CPU cost, while ZSTD offers higher compression at the
  expense of additional CPU usage.
- **Pre-sorting has a moderate impact**, as ClickHouse already sorts efficiently.
- **Batching significantly improves efficiency** - larger batches reduce insert overhead and improve throughput.

For a deep dive into the results and best practices, 
read the full [benchmark analysis](https://www.clickhouse.com/blog/clickhouse-input-format-matchup-which-is-fastest-most-efficient).
For the full test results, explore the [FastFormats](https://fastformats.clickhouse.com/) online dashboard.

## Output formats {#output-formats}

Formats supported for output are used for:
- Arranging the results of a `SELECT` query
- Performing `INSERT` operations into file-backed tables

## Formats overview {#formats-overview}

The supported formats are:

| Format                                                                                                     | Input | Output |
|------------------------------------------------------------------------------------------------------------|-----|-------|
| [TabSeparated](./formats/TabSeparated/TabSeparated.md)                                                     | ✔   | ✔     |
| [TabSeparatedRaw](./formats/TabSeparated/TabSeparatedRaw.md)                                               | ✔   | ✔     |
| [TabSeparatedWithNames](./formats/TabSeparated/TabSeparatedWithNames.md)                                   | ✔   | ✔     |
| [TabSeparatedWithNamesAndTypes](./formats/TabSeparated/TabSeparatedWithNamesAndTypes.md)                   | ✔   | ✔     |
| [TabSeparatedRawWithNames](./formats/TabSeparated/TabSeparatedRawWithNames.md)                             | ✔   | ✔     |
| [TabSeparatedRawWithNamesAndTypes](./formats/TabSeparated/TabSeparatedRawWithNamesAndTypes.md)             | ✔   | ✔     |
| [Template](./formats/Template/Template.md)                                                                 | ✔   | ✔     |
| [TemplateIgnoreSpaces](./formats/Template/TemplateIgnoreSpaces.md)                                         | ✔   | ✗     |
| [CSV](./formats/CSV/CSV.md)                                                                                | ✔   | ✔     |
| [CSVWithNames](./formats/CSV/CSVWithNames.md)                                                              | ✔   | ✔     |
| [CSVWithNamesAndTypes](./formats/CSV/CSVWithNamesAndTypes.md)                                              | ✔   | ✔     |
| [CustomSeparated](./formats/CustomSeparated/CustomSeparated.md)                                            | ✔   | ✔     |
| [CustomSeparatedWithNames](./formats/CustomSeparated/CustomSeparatedWithNames.md)                          | ✔   | ✔     |
| [CustomSeparatedWithNamesAndTypes](./formats/CustomSeparated/CustomSeparatedWithNamesAndTypes.md)          | ✔   | ✔     |
| [SQLInsert](./formats/SQLInsert.md)                                                                        | ✗   | ✔     |
| [Values](./formats/Values.md)                                                                              | ✔   | ✔     |
| [Vertical](./formats/Vertical.md)                                                                          | ✗   | ✔     |
| [JSON](./formats/JSON/JSON.md)                                                                             | ✔   | ✔     |
| [JSONAsString](./formats/JSON/JSONAsString.md)                                                             | ✔   | ✗     |
| [JSONAsObject](./formats/JSON/JSONAsObject.md)                                                             | ✔   | ✗     |
| [JSONStrings](./formats/JSON/JSONStrings.md)                                                               | ✔   | ✔     |
| [JSONColumns](./formats/JSON/JSONColumns.md)                                                               | ✔   | ✔     |
| [JSONColumnsWithMetadata](./formats/JSON/JSONColumnsWithMetadata.md)                                       | ✔   | ✔     |
| [JSONCompact](./formats/JSON/JSONCompact.md)                                                               | ✔   | ✔     |
| [JSONCompactStrings](./formats/JSON/JSONCompactStrings.md)                                                 | ✗   | ✔     |
| [JSONCompactColumns](./formats/JSON/JSONCompactColumns.md)                                                 | ✔   | ✔     |
| [JSONEachRow](./formats/JSON/JSONEachRow.md)                                                               | ✔   | ✔     |
| [PrettyJSONEachRow](./formats/JSON/PrettyJSONEachRow.md)                                                   | ✗   | ✔     |
| [JSONEachRowWithProgress](./formats/JSON/JSONEachRowWithProgress.md)                                       | ✗   | ✔     |
| [JSONStringsEachRow](./formats/JSON/JSONStringsEachRow.md)                                                 | ✔   | ✔     |
| [JSONStringsEachRowWithProgress](./formats/JSON/JSONStringsEachRowWithProgress.md)                         | ✗   | ✔     |
| [JSONCompactEachRow](./formats/JSON/JSONCompactEachRow.md)                                                 | ✔   | ✔     |
| [JSONCompactEachRowWithNames](./formats/JSON/JSONCompactEachRowWithNames.md)                               | ✔   | ✔     |
| [JSONCompactEachRowWithNamesAndTypes](./formats/JSON/JSONCompactEachRowWithNamesAndTypes.md)               | ✔   | ✔     |
| [JSONCompactEachRowWithProgress](./formats/JSON/JSONCompactEachRowWithProgress.md)                         | ✗   | ✔     |
| [JSONCompactStringsEachRow](./formats/JSON/JSONCompactStringsEachRow.md)                                   | ✔   | ✔     |
| [JSONCompactStringsEachRowWithNames](./formats/JSON/JSONCompactStringsEachRowWithNames.md)                 | ✔   | ✔     |
| [JSONCompactStringsEachRowWithNamesAndTypes](./formats/JSON/JSONCompactStringsEachRowWithNamesAndTypes.md) | ✔   | ✔     |
| [JSONCompactStringsEachRowWithProgress](./formats/JSON/JSONCompactStringsEachRowWithProgress.md)           | ✗   | ✔     |
| [JSONObjectEachRow](./formats/JSON/JSONObjectEachRow.md)                                                   | ✔   | ✔     |
| [BSONEachRow](./formats/BSONEachRow.md)                                                                    | ✔   | ✔     |
| [TSKV](./formats/TabSeparated/TSKV.md)                                                                     | ✔   | ✔     |
| [Pretty](./formats/Pretty/Pretty.md)                                                                       | ✗   | ✔     |
| [PrettyNoEscapes](./formats/Pretty/PrettyNoEscapes.md)                                                     | ✗   | ✔     |
| [PrettyMonoBlock](./formats/Pretty/PrettyMonoBlock.md)                                                     | ✗   | ✔     |
| [PrettyNoEscapesMonoBlock](./formats/Pretty/PrettyNoEscapesMonoBlock.md)                                   | ✗   | ✔     |
| [PrettyCompact](./formats/Pretty/PrettyCompact.md)                                                         | ✗   | ✔     |
| [PrettyCompactNoEscapes](./formats/Pretty/PrettyCompactNoEscapes.md)                                       | ✗   | ✔     |
| [PrettyCompactMonoBlock](./formats/Pretty/PrettyCompactMonoBlock.md)                                       | ✗   | ✔     |
| [PrettyCompactNoEscapesMonoBlock](./formats/Pretty/PrettyCompactNoEscapesMonoBlock.md)                     | ✗   | ✔     |
| [PrettySpace](./formats/Pretty/PrettySpace.md)                                                             | ✗   | ✔     |
| [PrettySpaceNoEscapes](./formats/Pretty/PrettySpaceNoEscapes.md)                                           | ✗   | ✔     |
| [PrettySpaceMonoBlock](./formats/Pretty/PrettySpaceMonoBlock.md)                                           | ✗   | ✔     |
| [PrettySpaceNoEscapesMonoBlock](./formats/Pretty/PrettySpaceNoEscapesMonoBlock.md)                         | ✗   | ✔     |
| [Prometheus](./formats/Prometheus.md)                                                                      | ✗   | ✔     |
| [Protobuf](./formats/Protobuf/Protobuf.md)                                                                 | ✔   | ✔     |
| [ProtobufSingle](./formats/Protobuf/ProtobufSingle.md)                                                     | ✔   | ✔     |
| [ProtobufList](./formats/Protobuf/ProtobufList.md)                                                         | ✔   | ✔     |
| [Avro](./formats/Avro/Avro.md)                                                                             | ✔   | ✔     |
| [AvroConfluent](./formats/Avro/AvroConfluent.md)                                                           | ✔   | ✗     |
| [Parquet](./formats/Parquet/Parquet.md)                                                                    | ✔   | ✔     |
| [ParquetMetadata](./formats/Parquet/ParquetMetadata.md)                                                    | ✔   | ✗     |
| [Arrow](./formats/Arrow/Arrow.md)                                                                          | ✔   | ✔     |
| [ArrowStream](./formats/Arrow/ArrowStream.md)                                                              | ✔   | ✔     |
| [ORC](./formats/ORC.md)                                                                                    | ✔   | ✔     |
| [One](./formats/One.md)                                                                                    | ✔   | ✗     |
| [Npy](./formats/Npy.md)                                                                                    | ✔   | ✔     |
| [RowBinary](./formats/RowBinary/RowBinary.md)                                                              | ✔   | ✔     |
| [RowBinaryWithNames](./formats/RowBinary/RowBinaryWithNames.md)                                            | ✔   | ✔     |
| [RowBinaryWithNamesAndTypes](./formats/RowBinary/RowBinaryWithNamesAndTypes.md)                            | ✔   | ✔     |
| [RowBinaryWithDefaults](./formats/RowBinary/RowBinaryWithDefaults.md)                                      | ✔   | ✗     |
| [Native](./formats/Native.md)                                                                              | ✔   | ✔     |
| [Null](./formats/Null.md)                                                                                  | ✗   | ✔     |
| [Hash](./formats/Hash.md)                                                                                  | ✗   | ✔     |
| [XML](./formats/XML.md)                                                                                    | ✗   | ✔     |
| [CapnProto](./formats/CapnProto.md)                                                                        | ✔   | ✔     |
| [LineAsString](./formats/LineAsString/LineAsString.md)                                                     | ✔   | ✔     |
| [LineAsStringWithNames](./formats/LineAsString/LineAsStringWithNames.md)                                   | ✔   | ✔     |
| [LineAsStringWithNamesAndTypes](./formats/LineAsString/LineAsStringWithNamesAndTypes.md)                   | ✔   | ✔     |
| [Regexp](./formats/Regexp.md)                                                                              | ✔   | ✗     |
| [RawBLOB](./formats/RawBLOB.md)                                                                            | ✔   | ✔     |
| [MsgPack](./formats/MsgPack.md)                                                                            | ✔   | ✔     |
| [MySQLDump](./formats/MySQLDump.md)                                                                        | ✔   | ✗     |
| [DWARF](./formats/DWARF.md)                                                                                | ✔   | ✗     |
| [Markdown](./formats/Markdown.md)                                                                          | ✗   | ✔     |
| [Form](./formats/Form.md)                                                                                  | ✔   | ✗     |

You can control some format processing parameters with the ClickHouse settings. For more information read the [Settings](/operations/settings/settings-formats.md) section.

## Format schema {#formatschema}

The file name containing the format schema is set by the setting `format_schema`.
It's required to set this setting when it is used one of the formats `Cap'n Proto` and `Protobuf`.
The format schema is a combination of a file name and the name of a message type in this file, delimited by a colon,
e.g. `schemafile.proto:MessageType`.
If the file has the standard extension for the format (for example, `.proto` for `Protobuf`),
it can be omitted and in this case, the format schema looks like `schemafile:MessageType`.

If you input or output data via the [client](/interfaces/cli.md) in interactive mode, the file name specified in the format schema
can contain an absolute path or a path relative to the current directory on the client.
If you use the client in the [batch mode](/interfaces/cli.md/#batch-mode), the path to the schema must be relative due to security reasons.

If you input or output data via the [HTTP interface](/interfaces/http.md) the file name specified in the format schema
should be located in the directory specified in [format_schema_path](/operations/server-configuration-parameters/settings.md/#format_schema_path)
in the server configuration.

## Skipping errors {#skippingerrors}

Some formats such as `CSV`, `TabSeparated`, `TSKV`, `JSONEachRow`, `Template`, `CustomSeparated` and `Protobuf` can skip broken row if parsing error occurred and continue parsing from the beginning of next row. See [input_format_allow_errors_num](/operations/settings/settings-formats.md/#input_format_allow_errors_num) and
[input_format_allow_errors_ratio](/operations/settings/settings-formats.md/#input_format_allow_errors_ratio) settings.
Limitations:
- In case of parsing error `JSONEachRow` skips all data until the new line (or EOF), so rows must be delimited by `\n` to count errors correctly.
- `Template` and `CustomSeparated` use delimiter after the last column and delimiter between rows to find the beginning of next row, so skipping errors works only if at least one of them is not empty.
