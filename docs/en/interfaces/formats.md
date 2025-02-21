---
slug: /interfaces/formats
sidebar_position: 21
sidebar_label: View all formats...
title: Formats for Input and Output Data
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

ClickHouse can accept and return data in various formats. A format supported for input can be used to parse the data provided to `INSERT`s, to perform `SELECT`s from a file-backed table such as File, URL or HDFS, or to read a dictionary. A format supported for output can be used to arrange the
results of a `SELECT`, and to perform `INSERT`s into a file-backed table.
All format names are case-insensitive.

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


You can control some format processing parameters with the ClickHouse settings. For more information read the [Settings](/docs/operations/settings/settings-formats.md) section.

## TabSeparated {#tabseparated}

See [TabSeparated](../interfaces/formats/TabSeparated/TabSeparated.md)

## TabSeparatedRaw {#tabseparatedraw}

See [TabSeparatedRaw](/interfaces/formats/TabSeparatedRaw)

## TabSeparatedWithNames {#tabseparatedwithnames}

See [TabSeparatedWithNames](../interfaces/formats/TabSeparated/TabSeparatedWithNames.md)

## TabSeparatedWithNamesAndTypes {#tabseparatedwithnamesandtypes}

See [TabSeparatedWithNamesAndTypes](../interfaces/formats/TabSeparated/TabSeparatedWithNamesAndTypes.md)

## TabSeparatedRawWithNames {#tabseparatedrawwithnames}

See [TabSeparatedRawWithNames](../interfaces/formats/TabSeparated/TabSeparatedRawWithNames.md)

## TabSeparatedRawWithNamesAndTypes {#tabseparatedrawwithnamesandtypes}

See [TabSeparatedRawWithNamesAndTypes](../interfaces/formats/TabSeparated/TabSeparatedRawWithNamesAndTypes.md)

## Template {#format-template}

See [Template](../interfaces/formats/Template)

## TemplateIgnoreSpaces {#templateignorespaces}

See [TemplateIgnoreSpaces](../interfaces/formats/Template/TemplateIgnoreSpaces.md)

## TSKV {#tskv}

See [TSKV](formats/TabSeparated/TSKV.md)

## CSV {#csv}

See [CSV](../interfaces/formats/CSV/CSV.md)

## CSVWithNames {#csvwithnames}

See [CSVWithNames](formats/CSV/CSVWithNames.md)

## CSVWithNamesAndTypes {#csvwithnamesandtypes}

See [CSVWithNamesAndTypes](formats/CSV/CSVWithNamesAndTypes.md)

## CustomSeparated {#format-customseparated}

See [CustomSeparated](formats/CustomSeparated/CustomSeparated.md)

## CustomSeparatedWithNames {#customseparatedwithnames}

See [CustomSeparatedWithNames](formats/CustomSeparated/CustomSeparatedWithNames.md)

## CustomSeparatedWithNamesAndTypes {#customseparatedwithnamesandtypes}

See [CustomSeparatedWithNamesAndTypes](formats/CustomSeparated/CustomSeparatedWithNamesAndTypes.md)

## SQLInsert {#sqlinsert}

See [SQLInsert](formats/SQLInsert.md)

## JSON {#json}

See [JSON](formats/JSON/JSON.md)

## JSONStrings {#jsonstrings}

See [JSONStrings](formats/JSON/JSONStrings.md)

## JSONColumns {#jsoncolumns}

See [JSONColumns](formats/JSON/JSONColumns.md)

## JSONColumnsWithMetadata {#jsoncolumnsmonoblock}

See [JSONColumnsWithMetadata](formats/JSON/JSONColumnsWithMetadata.md)

## JSONAsString {#jsonasstring}

See [JSONAsString](formats/JSON/JSONAsString.md)

## JSONAsObject {#jsonasobject}

See [JSONAsObject](formats/JSON/JSONAsObject.md)

## JSONCompact {#jsoncompact}

See [JSONCompact](formats/JSON/JSONCompact.md)

## JSONCompactStrings {#jsoncompactstrings}

See [JSONCompactStrings](formats/JSON/JSONCompactStrings.md)

## JSONCompactColumns {#jsoncompactcolumns}

See [JSONCompactColumns](formats/JSON/JSONCompactColumns.md)

## JSONEachRow {#jsoneachrow}

See [JSONEachRow](formats/JSON/JSONEachRow.md)

## PrettyJSONEachRow {#prettyjsoneachrow}

See [PrettyJSONEachRow](formats/JSON/PrettyJSONEachRow.md)

## JSONStringsEachRow {#jsonstringseachrow}

See [JSONStringsEachRow](formats/JSON/JSONStringsEachRow.md)

## JSONCompactEachRow {#jsoncompacteachrow}

See [JSONCompactEachRow](formats/JSON/JSONCompactEachRow.md)

## JSONCompactStringsEachRow {#jsoncompactstringseachrow}

See [JSONCompactStringsEachRow](formats/JSON/JSONCompactStringsEachRow.md)

## JSONEachRowWithProgress {#jsoneachrowwithprogress}

See [JSONEachRowWithProgress](formats/JSON/JSONEachRowWithProgress.md)

## JSONStringsEachRowWithProgress {#jsonstringseachrowwithprogress}

See [JSONStringsEachRowWithProgress](formats/JSON/JSONStringsEachRowWithProgress.md)

## JSONCompactEachRowWithNames {#jsoncompacteachrowwithnames}

See [JSONCompactEachRowWithNames](formats/JSON/JSONCompactEachRowWithNames.md)

## JSONCompactEachRowWithNamesAndTypes {#jsoncompacteachrowwithnamesandtypes}

See [JSONCompactEachRowWithNamesAndTypes](formats/JSON/JSONCompactEachRowWithNamesAndTypes.md)

## JSONCompactEachRowWithProgress

Similar to `JSONEachRowWithProgress` but outputs `row` events in a compact form, like in the `JSONCompactEachRow` format. 

## JSONCompactStringsEachRowWithNames {#jsoncompactstringseachrowwithnames}

See [JSONCompactStringsEachRowWithNames](formats/JSON/JSONCompactStringsEachRowWithNames.md)

## JSONCompactStringsEachRowWithNamesAndTypes {#jsoncompactstringseachrowwithnamesandtypes}

See [JSONCompactStringsEachRowWithNamesAndTypes](formats/JSON/JSONCompactStringsEachRowWithNamesAndTypes.md)

## JSONObjectEachRow {#jsonobjecteachrow}

See [JSONObjectEachRow](formats/JSON/JSONObjectEachRow.md)

### JSON Formats Settings {#json-formats-settings}

See [JSON Format Settings](formats/JSON/format-settings.md)

## BSONEachRow {#bsoneachrow}

See [BSONEachRow](formats/BSONEachRow.md)

## Native {#native}

See [Native](formats/Native.md)

## Null {#null}

See [Null](formats/Null.md)

## Pretty {#pretty}

See [Pretty](formats/Pretty/Pretty.md)

## PrettyNoEscapes {#prettynoescapes}

See [PrettyNoEscapes](formats/Pretty/PrettyNoEscapes.md)

## PrettyMonoBlock {#prettymonoblock}

See [PrettyMonoBlock](formats/Pretty/PrettyMonoBlock.md)

## PrettyNoEscapesMonoBlock {#prettynoescapesmonoblock}

See [PrettyNoEscapesMonoBlock](formats/Pretty/PrettyNoEscapesMonoBlock.md)

## PrettyCompact {#prettycompact}

See [PrettyCompact](formats/Pretty/PrettyCompact.md)

## PrettyCompactNoEscapes {#prettycompactnoescapes}

See [PrettyCompactNoEscapes](formats/Pretty/PrettyCompactNoEscapes.md)

## PrettyCompactMonoBlock {#prettycompactmonoblock}

See [PrettyCompactMonoBlock](formats/Pretty/PrettyCompactMonoBlock.md)

## PrettyCompactNoEscapesMonoBlock {#prettycompactnoescapesmonoblock}

See [PrettyCompactNoEscapesMonoBlock](formats/Pretty/PrettyCompactNoEscapesMonoBlock.md)

## PrettySpace {#prettyspace}

See [PrettySpace](formats/Pretty/PrettySpace.md)

## PrettySpaceNoEscapes {#prettyspacenoescapes}

See [PrettySpaceNoEscapes](formats/Pretty/PrettySpaceNoEscapes)

## PrettySpaceMonoBlock {#prettyspacemonoblock}

See [PrettySpaceMonoBlock](formats/Pretty/PrettySpaceMonoBlock.md)

## PrettySpaceNoEscapesMonoBlock {#prettyspacenoescapesmonoblock}

See [PrettySpaceNoEscapesMonoBlock](formats/Pretty/PrettySpaceNoEscapesMonoBlock.md)

## RowBinary {#rowbinary}

See [RowBinary](formats/RowBinary/RowBinary.md)

## RowBinaryWithNames {#rowbinarywithnames}

See [RowBinaryWithNames](formats/RowBinary/RowBinaryWithNames.md)

## RowBinaryWithNamesAndTypes {#rowbinarywithnamesandtypes}

See [RowBinaryWithNamesAndTypes](formats/RowBinary/RowBinaryWithNamesAndTypes.md)

## RowBinaryWithDefaults {#rowbinarywithdefaults}

See [RowBinaryWithDefaults](formats/RowBinary/RowBinaryWithDefaults.md)

## Values {#data-format-values}

See [Values](formats/Values.md)

## Vertical {#vertical}

See [Vertical](formats/Vertical.md)

## XML {#xml}

See [XML](formats/XML.md)

## CapnProto {#capnproto}

See [CapnProto](formats/CapnProto.md)

## Prometheus {#prometheus}

See [Prometheus](formats/Prometheus.md)

## Protobuf {#protobuf}

See [Protobuf](formats/Protobuf/Protobuf.md)

## ProtobufSingle {#protobufsingle}

See [ProtobufSingle](formats/Protobuf/ProtobufSingle.md)

## ProtobufList {#protobuflist}

See [ProtobufList](formats/Protobuf/ProtobufList.md)

## Avro {#data-format-avro}

See [Avro](formats/Avro/Avro.md)

## AvroConfluent {#data-format-avro-confluent}

See [AvroConfluent](formats/Avro/AvroConfluent.md)

## Parquet {#data-format-parquet}

See [Parquet](formats/Parquet/Parquet.md)

## ParquetMetadata {#data-format-parquet-metadata}

See [ParquetMetadata](formats/Parquet/ParquetMetadata.md)

## Arrow {#data-format-arrow}

See [Arrow](formats/Arrow/ArrowStream.md)

## ArrowStream {#data-format-arrow-stream}

See [ArrowStream](formats/Arrow/ArrowStream.md)

## ORC {#data-format-orc}

See [ORC](formats/ORC.md)

## One {#data-format-one}

See [One](formats/One.md)

## Npy {#data-format-npy}

See [Npy](formats/Npy.md)

## LineAsString {#lineasstring}

See [LineAsString](formats/LineAsString/LineAsString.md)

See also: [LineAsStringWithNames](formats/LineAsString/LineAsStringWithNames.md), [LineAsStringWithNamesAndTypes](formats/LineAsString/LineAsStringWithNamesAndTypes.md)

## Regexp {#data-format-regexp}

See [Regexp](formats/Regexp.md)

## Format Schema {#formatschema}

The file name containing the format schema is set by the setting `format_schema`.
It's required to set this setting when it is used one of the formats `Cap'n Proto` and `Protobuf`.
The format schema is a combination of a file name and the name of a message type in this file, delimited by a colon,
e.g. `schemafile.proto:MessageType`.
If the file has the standard extension for the format (for example, `.proto` for `Protobuf`),
it can be omitted and in this case, the format schema looks like `schemafile:MessageType`.

If you input or output data via the [client](/docs/interfaces/cli.md) in interactive mode, the file name specified in the format schema
can contain an absolute path or a path relative to the current directory on the client.
If you use the client in the [batch mode](/docs/interfaces/cli.md/#batch-mode), the path to the schema must be relative due to security reasons.

If you input or output data via the [HTTP interface](/docs/interfaces/http.md) the file name specified in the format schema
should be located in the directory specified in [format_schema_path](/docs/operations/server-configuration-parameters/settings.md/#format_schema_path)
in the server configuration.

## Skipping Errors {#skippingerrors}

Some formats such as `CSV`, `TabSeparated`, `TSKV`, `JSONEachRow`, `Template`, `CustomSeparated` and `Protobuf` can skip broken row if parsing error occurred and continue parsing from the beginning of next row. See [input_format_allow_errors_num](/docs/operations/settings/settings-formats.md/#input_format_allow_errors_num) and
[input_format_allow_errors_ratio](/docs/operations/settings/settings-formats.md/#input_format_allow_errors_ratio) settings.
Limitations:
- In case of parsing error `JSONEachRow` skips all data until the new line (or EOF), so rows must be delimited by `\n` to count errors correctly.
- `Template` and `CustomSeparated` use delimiter after the last column and delimiter between rows to find the beginning of next row, so skipping errors works only if at least one of them is not empty.

## RawBLOB {#rawblob}

See [RawBLOB](formats/RawBLOB.md)

## Markdown

See [Markdown](formats/Markdown.md)

## MsgPack {#msgpack}

See [MsgPack](formats/MsgPack.md)

## MySQLDump {#mysqldump}

See [MySQLDump](formats/MySQLDump.md)

## DWARF {#dwarf}

See [Dwarf](formats/DWARF.md)

## Form {#form}

See [Form](formats/Form.md)
