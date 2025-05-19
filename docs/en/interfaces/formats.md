---
description: 'Overview of supported data formats for input and output in ClickHouse'
sidebar_label: 'View all formats...'
sidebar_position: 21
title: 'Formats for input and output data'
---


# Formats for input and output data 

ClickHouse supports most of the known text and binary data formats. This allows easy integration into almost any working
data pipeline to leverage the benefits of ClickHouse.

## Input formats 

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

## Output formats 

Formats supported for output are used for:
- Arranging the results of a `SELECT` query
- Performing `INSERT` operations into file-backed tables

## Formats overview 

The supported formats are:

| Format                                                                                    | Input | Output |
|-------------------------------------------------------------------------------------------|-----|-------|
| [TabSeparated](#tabseparated)                                                             | ✔   | ✔     |
| [TabSeparatedRaw](#tabseparatedraw)                                                       | ✔   | ✔     |
| [TabSeparatedWithNames](#tabseparatedwithnames)                                           | ✔   | ✔     |
| [TabSeparatedWithNamesAndTypes](#tabseparatedwithnamesandtypes)                           | ✔   | ✔     |
| [TabSeparatedRawWithNames](#tabseparatedrawwithnames)                                     | ✔   | ✔     |
| [TabSeparatedRawWithNamesAndTypes](#tabseparatedrawwithnamesandtypes)                     | ✔   | ✔     |
| [Template](#format-template)                                                              | ✔   | ✔     |
| [TemplateIgnoreSpaces](#templateignorespaces)                                             | ✔   | ✗     |
| [CSV](#csv)                                                                               | ✔   | ✔     |
| [CSVWithNames](#csvwithnames)                                                             | ✔   | ✔     |
| [CSVWithNamesAndTypes](#csvwithnamesandtypes)                                             | ✔   | ✔     |
| [CustomSeparated](#format-customseparated)                                                | ✔   | ✔     |
| [CustomSeparatedWithNames](#customseparatedwithnames)                                     | ✔   | ✔     |
| [CustomSeparatedWithNamesAndTypes](#customseparatedwithnamesandtypes)                     | ✔   | ✔     |
| [SQLInsert](#sqlinsert)                                                                   | ✗   | ✔     |
| [Values](#data-format-values)                                                             | ✔   | ✔     |
| [Vertical](#vertical)                                                                     | ✗   | ✔     |
| [JSON](#json)                                                                             | ✔   | ✔     |
| [JSONAsString](#jsonasstring)                                                             | ✔   | ✗     |
| [JSONAsObject](#jsonasobject)                                                             | ✔   | ✗     |
| [JSONStrings](#jsonstrings)                                                               | ✔   | ✔     |
| [JSONColumns](#jsoncolumns)                                                               | ✔   | ✔     |
| [JSONColumnsWithMetadata](#jsoncolumnsmonoblock)                                          | ✔   | ✔     |
| [JSONCompact](#jsoncompact)                                                               | ✔   | ✔     |
| [JSONCompactStrings](#jsoncompactstrings)                                                 | ✗   | ✔     |
| [JSONCompactColumns](#jsoncompactcolumns)                                                 | ✔   | ✔     |
| [JSONEachRow](#jsoneachrow)                                                               | ✔   | ✔     |
| [PrettyJSONEachRow](#prettyjsoneachrow)                                                   | ✗   | ✔     |
| [JSONEachRowWithProgress](#jsoneachrowwithprogress)                                       | ✗   | ✔     |
| [JSONStringsEachRow](#jsonstringseachrow)                                                 | ✔   | ✔     |
| [JSONStringsEachRowWithProgress](#jsonstringseachrowwithprogress)                         | ✗   | ✔     |
| [JSONCompactEachRow](#jsoncompacteachrow)                                                 | ✔   | ✔     |
| [JSONCompactEachRowWithNames](#jsoncompacteachrowwithnames)                               | ✔   | ✔     |
| [JSONCompactEachRowWithNamesAndTypes](#jsoncompacteachrowwithnamesandtypes)               | ✔   | ✔     |
| [JSONCompactEachRowWithProgress](#jsoncompacteachrow)                                     | ✗    | ✔     |
| [JSONCompactStringsEachRow](#jsoncompactstringseachrow)                                   | ✔   | ✔     |
| [JSONCompactStringsEachRowWithNames](#jsoncompactstringseachrowwithnames)                 | ✔   | ✔     |
| [JSONCompactStringsEachRowWithNamesAndTypes](#jsoncompactstringseachrowwithnamesandtypes) | ✔   | ✔     |
| [JSONCompactStringsEachRowWithProgress](#jsoncompactstringseachrowwithnamesandtypes)      | ✗   | ✔     |
| [JSONObjectEachRow](#jsonobjecteachrow)                                                   | ✔   | ✔     |
| [BSONEachRow](#bsoneachrow)                                                               | ✔   | ✔     |
| [TSKV](#tskv)                                                                             | ✔   | ✔     |
| [Pretty](#pretty)                                                                         | ✗   | ✔     |
| [PrettyNoEscapes](#prettynoescapes)                                                       | ✗   | ✔     |
| [PrettyMonoBlock](#prettymonoblock)                                                       | ✗   | ✔     |
| [PrettyNoEscapesMonoBlock](#prettynoescapesmonoblock)                                     | ✗   | ✔     |
| [PrettyCompact](#prettycompact)                                                           | ✗   | ✔     |
| [PrettyCompactNoEscapes](#prettycompactnoescapes)                                         | ✗   | ✔     |
| [PrettyCompactMonoBlock](#prettycompactmonoblock)                                         | ✗   | ✔     |
| [PrettyCompactNoEscapesMonoBlock](#prettycompactnoescapesmonoblock)                       | ✗   | ✔     |
| [PrettySpace](#prettyspace)                                                               | ✗   | ✔     |
| [PrettySpaceNoEscapes](#prettyspacenoescapes)                                             | ✗   | ✔     |
| [PrettySpaceMonoBlock](#prettyspacemonoblock)                                             | ✗   | ✔     |
| [PrettySpaceNoEscapesMonoBlock](#prettyspacenoescapesmonoblock)                           | ✗   | ✔     |
| [Prometheus](#prometheus)                                                                 | ✗   | ✔     |
| [Protobuf](#protobuf)                                                                     | ✔   | ✔     |
| [ProtobufSingle](#protobufsingle)                                                         | ✔   | ✔     |
| [ProtobufList](#protobuflist)                                                                                     | ✔   | ✔     |
| [Avro](#data-format-avro)                                                                 | ✔   | ✔     |
| [AvroConfluent](#data-format-avro-confluent)                                              | ✔   | ✗     |
| [Parquet](#data-format-parquet)                                                           | ✔   | ✔     |
| [ParquetMetadata](#data-format-parquet-metadata)                                          | ✔   | ✗     |
| [Arrow](#data-format-arrow)                                                               | ✔   | ✔     |
| [ArrowStream](#data-format-arrow-stream)                                                  | ✔   | ✔     |
| [ORC](#data-format-orc)                                                                   | ✔   | ✔     |
| [One](#data-format-one)                                                                   | ✔   | ✗     |
| [Npy](#data-format-npy)                                                                   | ✔   | ✔     |
| [RowBinary](#rowbinary)                                                                   | ✔   | ✔     |
| [RowBinaryWithNames](#rowbinarywithnamesandtypes)                                         | ✔   | ✔     |
| [RowBinaryWithNamesAndTypes](#rowbinarywithnamesandtypes)                                 | ✔   | ✔     |
| [RowBinaryWithDefaults](#rowbinarywithdefaults)                                           | ✔   | ✗     |
| [Native](#native)                                                                         | ✔   | ✔     |
| [Null](#null)                                                                             | ✗   | ✔     |
| [XML](#xml)                                                                               | ✗   | ✔     |
| [CapnProto](#capnproto)                                                                   | ✔   | ✔     |
| [LineAsString](#lineasstring)                                                             | ✔   | ✔     |
| [Regexp](#data-format-regexp)                                                             | ✔   | ✗     |
| [RawBLOB](#rawblob)                                                                       | ✔   | ✔     |
| [MsgPack](#msgpack)                                                                       | ✔   | ✔     |
| [MySQLDump](#mysqldump)                                                                   | ✔   | ✗     |
| [DWARF](#dwarf)                                                                           | ✔   | ✗     |
| [Markdown](#markdown)                                                                     | ✗   | ✔     |
| [Form](#form)                                                                             | ✔   | ✗     |


You can control some format processing parameters with the ClickHouse settings. For more information read the [Settings](/operations/settings/settings-formats.md) section.

### TabSeparated 

See [TabSeparated](/interfaces/formats/TabSeparated)

### TabSeparatedRaw 

See [TabSeparatedRaw](/interfaces/formats/TabSeparatedRaw)

### TabSeparatedWithNames 

See [TabSeparatedWithNames](/interfaces/formats/TabSeparatedWithNames)

### TabSeparatedWithNamesAndTypes 

See [TabSeparatedWithNamesAndTypes](/interfaces/formats/TabSeparatedWithNamesAndTypes)

### TabSeparatedRawWithNames 

See [TabSeparatedRawWithNames](/interfaces/formats/TabSeparatedRawWithNames)

### TabSeparatedRawWithNamesAndTypes 

See [TabSeparatedRawWithNamesAndTypes](/interfaces/formats/TabSeparatedRawWithNamesAndTypes)

### Template 

See [Template](/interfaces/formats/Template)

### TemplateIgnoreSpaces 

See [TemplateIgnoreSpaces](/interfaces/formats/TemplateIgnoreSpaces)

### TSKV 

See [TSKV](/interfaces/formats/TSKV)

### CSV 

See [CSV](../interfaces/formats/CSV)

### CSVWithNames 

See [CSVWithNames](/interfaces/formats/CSVWithNames)

### CSVWithNamesAndTypes 

See [CSVWithNamesAndTypes](/interfaces/formats/CSVWithNamesAndTypes)

### CustomSeparated 

See [CustomSeparated](/interfaces/formats/CustomSeparated)

### CustomSeparatedWithNames 

See [CustomSeparatedWithNames](/interfaces/formats/CustomSeparatedWithNames)

### CustomSeparatedWithNamesAndTypes 

See [CustomSeparatedWithNamesAndTypes](/interfaces/formats/CustomSeparatedWithNamesAndTypes)

### SQLInsert 

See [SQLInsert](/interfaces/formats/SQLInsert)

### JSON 

See [JSON](/interfaces/formats/JSON)

### JSONStrings 

See [JSONStrings](/interfaces/formats/JSONStrings)

### JSONColumns 

See [JSONColumns](/interfaces/formats/JSONColumns)

### JSONColumnsWithMetadata 

See [JSONColumnsWithMetadata](/interfaces/formats/JSONColumnsWithMetadata)

### JSONAsString 

See [JSONAsString](/interfaces/formats/JSONAsString)

### JSONAsObject 

See [JSONAsObject](/interfaces/formats/JSONAsObject)

### JSONCompact 

See [JSONCompact](/interfaces/formats/JSONCompact)

### JSONCompactStrings 

See [JSONCompactStrings](/interfaces/formats/JSONCompactStrings)

### JSONCompactColumns 

See [JSONCompactColumns](/interfaces/formats/JSONCompactColumns)

### JSONEachRow 

See [JSONEachRow](/interfaces/formats/JSONEachRow)

### PrettyJSONEachRow 

See [PrettyJSONEachRow](/interfaces/formats/PrettyJSONEachRow)

### JSONStringsEachRow 

See [JSONStringsEachRow](/interfaces/formats/JSONStringsEachRow)

### JSONCompactEachRow 

See [JSONCompactEachRow](/interfaces/formats/JSONCompactEachRow)

### JSONCompactStringsEachRow 

See [JSONCompactStringsEachRow](/interfaces/formats/JSONCompactStringsEachRow)

### JSONEachRowWithProgress 

See [JSONEachRowWithProgress](/interfaces/formats/JSONEachRowWithProgress)

### JSONStringsEachRowWithProgress 

See [JSONStringsEachRowWithProgress](/interfaces/formats/JSONStringsEachRowWithProgress)

### JSONCompactEachRowWithNames 

See [JSONCompactEachRowWithNames](/interfaces/formats/JSONCompactEachRowWithNames)

### JSONCompactEachRowWithNamesAndTypes 

See [JSONCompactEachRowWithNamesAndTypes](/interfaces/formats/JSONCompactEachRowWithNamesAndTypes)

### JSONCompactEachRowWithProgress 

Similar to `JSONEachRowWithProgress` but outputs `row` events in a compact form, like in the `JSONCompactEachRow` format.

### JSONCompactStringsEachRowWithNames 

See [JSONCompactStringsEachRowWithNames](/interfaces/formats/JSONCompactStringsEachRowWithNames)

### JSONCompactStringsEachRowWithNamesAndTypes 

See [JSONCompactStringsEachRowWithNamesAndTypes](/interfaces/formats/JSONCompactStringsEachRowWithNamesAndTypes)

### JSONObjectEachRow 

See [JSONObjectEachRow](/interfaces/formats/JSONObjectEachRow)

### JSON Formats Settings 

See [JSON Format Settings](/operations/settings/formats)

### BSONEachRow 

See [BSONEachRow](/interfaces/formats/BSONEachRow)

### Native 

See [Native](/interfaces/formats/Native)

### Null 

See [Null](/interfaces/formats/Null)

### Pretty 

See [Pretty](/interfaces/formats/Pretty)

### PrettyNoEscapes 

See [PrettyNoEscapes](/interfaces/formats/PrettyNoEscapes)

### PrettyMonoBlock 

See [PrettyMonoBlock](/interfaces/formats/PrettyMonoBlock)

### PrettyNoEscapesMonoBlock 

See [PrettyNoEscapesMonoBlock](/interfaces/formats/PrettyNoEscapesMonoBlock)

### PrettyCompact 

See [PrettyCompact](/interfaces/formats/PrettyCompact)

### PrettyCompactNoEscapes 

See [PrettyCompactNoEscapes](/interfaces/formats/PrettyCompactNoEscapes)

### PrettyCompactMonoBlock 

See [PrettyCompactMonoBlock](/interfaces/formats/PrettyCompactMonoBlock)

### PrettyCompactNoEscapesMonoBlock 

See [PrettyCompactNoEscapesMonoBlock](/interfaces/formats/PrettyCompactNoEscapesMonoBlock)

### PrettySpace 

See [PrettySpace](/interfaces/formats/PrettySpace)

### PrettySpaceNoEscapes 

See [PrettySpaceNoEscapes](/interfaces/formats/PrettySpaceNoEscapes)

### PrettySpaceMonoBlock 

See [PrettySpaceMonoBlock](/interfaces/formats/PrettySpaceMonoBlock)

### PrettySpaceNoEscapesMonoBlock 

See [PrettySpaceNoEscapesMonoBlock](/interfaces/formats/PrettySpaceNoEscapesMonoBlock)

### RowBinary 

See [RowBinary](/interfaces/formats/RowBinary)

### RowBinaryWithNames 

See [RowBinaryWithNames](/interfaces/formats/RowBinaryWithNames)

### RowBinaryWithNamesAndTypes 

See [RowBinaryWithNamesAndTypes](/interfaces/formats/RowBinaryWithNamesAndTypes)

### RowBinaryWithDefaults 

See [RowBinaryWithDefaults](/interfaces/formats/RowBinaryWithDefaults)

### Values 

See [Values](/interfaces/formats/Values)

### Vertical 

See [Vertical](/interfaces/formats/Vertical)

### XML 

See [XML](/interfaces/formats/XML)

### CapnProto 

See [CapnProto](/interfaces/formats/CapnProto)

### Prometheus 

See [Prometheus](/interfaces/formats/Prometheus)

### Protobuf 

See [Protobuf](/interfaces/formats/Protobuf)

### ProtobufSingle 

See [ProtobufSingle](/interfaces/formats/ProtobufSingle)

### ProtobufList 

See [ProtobufList](/interfaces/formats/ProtobufList)

### Avro 

See [Avro](/interfaces/formats/Avro)

### AvroConfluent 

See [AvroConfluent](/interfaces/formats/AvroConfluent)

### Parquet 

See [Parquet](/interfaces/formats/Parquet)

### ParquetMetadata 

See [ParquetMetadata](/interfaces/formats/ParquetMetadata)

### Arrow 

See [Arrow](/interfaces/formats/ArrowStream)

### ArrowStream 

See [ArrowStream](/interfaces/formats/ArrowStream)

### ORC 

See [ORC](/interfaces/formats/ORC)

### One 

See [One](/interfaces/formats/One)

### Npy 

See [Npy](/interfaces/formats/Npy)

### LineAsString 

See:
- [LineAsString](/interfaces/formats/LineAsString)
- [LineAsStringWithNames](/interfaces/formats/LineAsStringWithNames)
- [LineAsStringWithNamesAndTypes](/interfaces/formats/LineAsStringWithNamesAndTypes)

### Regexp 

See [Regexp](/interfaces/formats/Regexp)

### RawBLOB 

See [RawBLOB](/interfaces/formats/RawBLOB)

### Markdown 

See [Markdown](/interfaces/formats/Markdown)

### MsgPack 

See [MsgPack](/interfaces/formats/MsgPack)

### MySQLDump 

See [MySQLDump](/interfaces/formats/MySQLDump)

### DWARF 

See [Dwarf](/interfaces/formats/DWARF)

### Form 

See [Form](/interfaces/formats/Form)

## Format Schema 

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

## Skipping Errors 

Some formats such as `CSV`, `TabSeparated`, `TSKV`, `JSONEachRow`, `Template`, `CustomSeparated` and `Protobuf` can skip broken row if parsing error occurred and continue parsing from the beginning of next row. See [input_format_allow_errors_num](/operations/settings/settings-formats.md/#input_format_allow_errors_num) and
[input_format_allow_errors_ratio](/operations/settings/settings-formats.md/#input_format_allow_errors_ratio) settings.
Limitations:
- In case of parsing error `JSONEachRow` skips all data until the new line (or EOF), so rows must be delimited by `\n` to count errors correctly.
- `Template` and `CustomSeparated` use delimiter after the last column and delimiter between rows to find the beginning of next row, so skipping errors works only if at least one of them is not empty.
