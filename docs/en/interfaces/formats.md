---
slug: /interfaces/formats
sidebar_position: 21
sidebar_label: View all formats...
title: Formats for Input and Output Data
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
| [ProtobufList](#protobuflist)								                                                     | ✔   | ✔     |
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

### TabSeparated {#tabseparated}

See [TabSeparated](/interfaces/formats/TabSeparated/TabSeparated)

### TabSeparatedRaw {#tabseparatedraw}

See [TabSeparatedRaw](/interfaces/formats/TabSeparatedRaw)

### TabSeparatedWithNames {#tabseparatedwithnames}

See [TabSeparatedWithNames](/interfaces/formats/TabSeparated/TabSeparatedWithNames)

### TabSeparatedWithNamesAndTypes {#tabseparatedwithnamesandtypes}

See [TabSeparatedWithNamesAndTypes](/interfaces/formats/TabSeparated/TabSeparatedWithNamesAndTypes)

### TabSeparatedRawWithNames {#tabseparatedrawwithnames}

See [TabSeparatedRawWithNames](/interfaces/formats/TabSeparated/TabSeparatedRawWithNames)

### TabSeparatedRawWithNamesAndTypes {#tabseparatedrawwithnamesandtypes}

See [TabSeparatedRawWithNamesAndTypes](/interfaces/formats/TabSeparated/TabSeparatedRawWithNamesAndTypes)

### Template {#format-template}

See [Template](/interfaces/formats/Template)

### TemplateIgnoreSpaces {#templateignorespaces}

See [TemplateIgnoreSpaces](/interfaces/formats/Template/TemplateIgnoreSpaces)

### TSKV {#tskv}

See [TSKV](/interfaces/formats/TabSeparated/TSKV)

### CSV {#csv}

See [CSV](../interfaces/formats/CSV/CSV)

### CSVWithNames {#csvwithnames}

See [CSVWithNames](/interfaces/formats/CSV/CSVWithNames)

### CSVWithNamesAndTypes {#csvwithnamesandtypes}

See [CSVWithNamesAndTypes](/interfaces/formats/CSV/CSVWithNamesAndTypes)

### CustomSeparated {#format-customseparated}

See [CustomSeparated](/interfaces/formats/CustomSeparated/CustomSeparated)

### CustomSeparatedWithNames {#customseparatedwithnames}

See [CustomSeparatedWithNames](/interfaces/formats/CustomSeparated/CustomSeparatedWithNames)

### CustomSeparatedWithNamesAndTypes {#customseparatedwithnamesandtypes}

See [CustomSeparatedWithNamesAndTypes](/interfaces/formats/CustomSeparated/CustomSeparatedWithNamesAndTypes)

### SQLInsert {#sqlinsert}

See [SQLInsert](/interfaces/formats/SQLInsert)

### JSON {#json}

See [JSON](/interfaces/formats/JSON/JSON)

### JSONStrings {#jsonstrings}

See [JSONStrings](/interfaces/formats/JSON/JSONStrings)

### JSONColumns {#jsoncolumns}

See [JSONColumns](/interfaces/formats/JSON/JSONColumns)

### JSONColumnsWithMetadata {#jsoncolumnsmonoblock}

See [JSONColumnsWithMetadata](/interfaces/formats/JSON/JSONColumnsWithMetadata)

### JSONAsString {#jsonasstring}

See [JSONAsString](/interfaces/formats/JSON/JSONAsString)

### JSONAsObject {#jsonasobject}

See [JSONAsObject](/interfaces/formats/JSON/JSONAsObject)

### JSONCompact {#jsoncompact}

See [JSONCompact](/interfaces/formats/JSON/JSONCompact)

### JSONCompactStrings {#jsoncompactstrings}

See [JSONCompactStrings](/interfaces/formats/JSON/JSONCompactStrings)

### JSONCompactColumns {#jsoncompactcolumns}

See [JSONCompactColumns](/interfaces/formats/JSON/JSONCompactColumns)

### JSONEachRow {#jsoneachrow}

See [JSONEachRow](/interfaces/formats/JSON/JSONEachRow)

### PrettyJSONEachRow {#prettyjsoneachrow}

See [PrettyJSONEachRow](/interfaces/formats/JSON/PrettyJSONEachRow)

### JSONStringsEachRow {#jsonstringseachrow}

See [JSONStringsEachRow](/interfaces/formats/JSON/JSONStringsEachRow)

### JSONCompactEachRow {#jsoncompacteachrow}

See [JSONCompactEachRow](/interfaces/formats/JSON/JSONCompactEachRow)

### JSONCompactStringsEachRow {#jsoncompactstringseachrow}

See [JSONCompactStringsEachRow](/interfaces/formats/JSON/JSONCompactStringsEachRow)

### JSONEachRowWithProgress {#jsoneachrowwithprogress}

See [JSONEachRowWithProgress](/interfaces/formats/JSON/JSONEachRowWithProgress)

### JSONStringsEachRowWithProgress {#jsonstringseachrowwithprogress}

See [JSONStringsEachRowWithProgress](/interfaces/formats/JSON/JSONStringsEachRowWithProgress)

### JSONCompactEachRowWithNames {#jsoncompacteachrowwithnames}

See [JSONCompactEachRowWithNames](/interfaces/formats/JSON/JSONCompactEachRowWithNames)

### JSONCompactEachRowWithNamesAndTypes {#jsoncompacteachrowwithnamesandtypes}

See [JSONCompactEachRowWithNamesAndTypes](/interfaces/formats/JSON/JSONCompactEachRowWithNamesAndTypes)

### JSONCompactEachRowWithProgress {#jsoncompacteachrowwithprogress}

Similar to `JSONEachRowWithProgress` but outputs `row` events in a compact form, like in the `JSONCompactEachRow` format.

### JSONCompactStringsEachRowWithNames {#jsoncompactstringseachrowwithnames}

See [JSONCompactStringsEachRowWithNames](/interfaces/formats/JSON/JSONCompactStringsEachRowWithNames)

### JSONCompactStringsEachRowWithNamesAndTypes {#jsoncompactstringseachrowwithnamesandtypes}

See [JSONCompactStringsEachRowWithNamesAndTypes](/interfaces/formats/JSON/JSONCompactStringsEachRowWithNamesAndTypes)

### JSONObjectEachRow {#jsonobjecteachrow}

See [JSONObjectEachRow](/interfaces/formats/JSON/JSONObjectEachRow)

### JSON Formats Settings {#json-formats-settings}

See [JSON Format Settings](/interfaces/formats/JSON/format-settings)

### BSONEachRow {#bsoneachrow}

See [BSONEachRow](/interfaces/formats/BSONEachRow)

### Native {#native}

See [Native](/interfaces/formats/Native)

### Null {#null}

See [Null](/interfaces/formats/Null)

### Pretty {#pretty}

See [Pretty](/interfaces/formats/Pretty/Pretty)

### PrettyNoEscapes {#prettynoescapes}

See [PrettyNoEscapes](/interfaces/formats/Pretty/PrettyNoEscapes)

### PrettyMonoBlock {#prettymonoblock}

See [PrettyMonoBlock](/interfaces/formats/Pretty/PrettyMonoBlock)

### PrettyNoEscapesMonoBlock {#prettynoescapesmonoblock}

See [PrettyNoEscapesMonoBlock](/interfaces/formats/Pretty/PrettyNoEscapesMonoBlock)

### PrettyCompact {#prettycompact}

See [PrettyCompact](/interfaces/formats/Pretty/PrettyCompact)

### PrettyCompactNoEscapes {#prettycompactnoescapes}

See [PrettyCompactNoEscapes](/interfaces/formats/Pretty/PrettyCompactNoEscapes)

### PrettyCompactMonoBlock {#prettycompactmonoblock}

See [PrettyCompactMonoBlock](/interfaces/formats/Pretty/PrettyCompactMonoBlock)

### PrettyCompactNoEscapesMonoBlock {#prettycompactnoescapesmonoblock}

See [PrettyCompactNoEscapesMonoBlock](/interfaces/formats/Pretty/PrettyCompactNoEscapesMonoBlock)

### PrettySpace {#prettyspace}

See [PrettySpace](/interfaces/formats/Pretty/PrettySpace)

### PrettySpaceNoEscapes {#prettyspacenoescapes}

See [PrettySpaceNoEscapes](/interfaces/formats/Pretty/PrettySpaceNoEscapes)

### PrettySpaceMonoBlock {#prettyspacemonoblock}

See [PrettySpaceMonoBlock](/interfaces/formats/Pretty/PrettySpaceMonoBlock)

### PrettySpaceNoEscapesMonoBlock {#prettyspacenoescapesmonoblock}

See [PrettySpaceNoEscapesMonoBlock](/interfaces/formats/Pretty/PrettySpaceNoEscapesMonoBlock)

### RowBinary {#rowbinary}

See [RowBinary](/interfaces/formats/RowBinary/RowBinary)

### RowBinaryWithNames {#rowbinarywithnames}

See [RowBinaryWithNames](/interfaces/formats/RowBinary/RowBinaryWithNames)

### RowBinaryWithNamesAndTypes {#rowbinarywithnamesandtypes}

See [RowBinaryWithNamesAndTypes](/interfaces/formats/RowBinary/RowBinaryWithNamesAndTypes)

### RowBinaryWithDefaults {#rowbinarywithdefaults}

See [RowBinaryWithDefaults](/interfaces/formats/RowBinary/RowBinaryWithDefaults)

### Values {#data-format-values}

See [Values](/interfaces/formats/Values)

### Vertical {#vertical}

See [Vertical](/interfaces/formats/Vertical)

### XML {#xml}

See [XML](/interfaces/formats/XML)

### CapnProto {#capnproto}

See [CapnProto](/interfaces/formats/CapnProto)

### Prometheus {#prometheus}

See [Prometheus](/interfaces/formats/Prometheus)

### Protobuf {#protobuf}

See [Protobuf](/interfaces/formats/Protobuf/Protobuf)

### ProtobufSingle {#protobufsingle}

See [ProtobufSingle](/interfaces/formats/Protobuf/ProtobufSingle)

### ProtobufList {#protobuflist}

See [ProtobufList](/interfaces/formats/Protobuf/ProtobufList)

### Avro {#data-format-avro}

See [Avro](/interfaces/formats/Avro/Avro)

### AvroConfluent {#data-format-avro-confluent}

See [AvroConfluent](/interfaces/formats/Avro/AvroConfluent)

### Parquet {#data-format-parquet}

See [Parquet](/interfaces/formats/Parquet/Parquet)

### ParquetMetadata {#data-format-parquet-metadata}

See [ParquetMetadata](/interfaces/formats/Parquet/ParquetMetadata)

### Arrow {#data-format-arrow}

See [Arrow](/interfaces/formats/Arrow/ArrowStream)

### ArrowStream {#data-format-arrow-stream}

See [ArrowStream](/interfaces/formats/Arrow/ArrowStream)

### ORC {#data-format-orc}

See [ORC](/interfaces/formats/ORC)

### One {#data-format-one}

See [One](/interfaces/formats/One)

### Npy {#data-format-npy}

See [Npy](/interfaces/formats/Npy)

### LineAsString {#lineasstring}

See:
- [LineAsString](/interfaces/formats/LineAsString/LineAsString.md)
- [LineAsStringWithNames](/interfaces/formats/LineAsString/LineAsStringWithNames.md)
- [LineAsStringWithNamesAndTypes](/interfaces/formats/LineAsString/LineAsStringWithNamesAndTypes.md)

### Regexp {#data-format-regexp}

See [Regexp](/interfaces/formats/Regexp)

### RawBLOB {#rawblob}

See [RawBLOB](/interfaces/formats/RawBLOB.md)

### Markdown {#markdown}

See [Markdown](/interfaces/formats/Markdown.md)

### MsgPack {#msgpack}

See [MsgPack](/interfaces/formats/MsgPack.md)

### MySQLDump {#mysqldump}

See [MySQLDump](/interfaces/formats/MySQLDump.md)

### DWARF {#dwarf}

See [Dwarf](/interfaces/formats/DWARF.md)

### Form {#form}

See [Form](/interfaces/formats/Form.md)

## Format Schema {#formatschema}

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

## Skipping Errors {#skippingerrors}

Some formats such as `CSV`, `TabSeparated`, `TSKV`, `JSONEachRow`, `Template`, `CustomSeparated` and `Protobuf` can skip broken row if parsing error occurred and continue parsing from the beginning of next row. See [input_format_allow_errors_num](/operations/settings/settings-formats.md/#input_format_allow_errors_num) and
[input_format_allow_errors_ratio](/operations/settings/settings-formats.md/#input_format_allow_errors_ratio) settings.
Limitations:
- In case of parsing error `JSONEachRow` skips all data until the new line (or EOF), so rows must be delimited by `\n` to count errors correctly.
- `Template` and `CustomSeparated` use delimiter after the last column and delimiter between rows to find the beginning of next row, so skipping errors works only if at least one of them is not empty.
