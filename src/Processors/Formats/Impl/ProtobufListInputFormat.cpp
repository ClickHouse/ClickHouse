#include <Processors/Formats/Impl/ProtobufListInputFormat.h>

#if USE_PROTOBUF
#   include <Columns/IColumn.h>
#   include <Core/Block.h>
#   include <Formats/FormatFactory.h>
#   include <Formats/ProtobufReader.h>
#   include <Formats/ProtobufSchemas.h>
#   include <Formats/ProtobufSerializer.h>

namespace DB
{

ProtobufListInputFormat::ProtobufListInputFormat(
    ReadBuffer & in_,
    SharedHeader header_,
    const Params & params_,
    const ProtobufSchemaInfo & schema_info_,
    bool flatten_google_wrappers_,
    const String & google_protos_path)
    : IRowInputFormat(header_, in_, params_)
    , reader(std::make_unique<ProtobufReader>(in_))
    , descriptor_holder(ProtobufSchemas::instance().getMessageTypeForFormatSchema(
          schema_info_.getSchemaInfo(), ProtobufSchemas::WithEnvelope::Yes, google_protos_path))
    , serializer(ProtobufSerializer::create(
          header_->getNames(),
          header_->getDataTypes(),
          missing_column_indices,
          descriptor_holder,
          /* with_length_delimiter = */ true,
          /* with_envelope = */ true,
          flatten_google_wrappers_,
          /* oneof_presence = */ false,
          *reader))
{
}

void ProtobufListInputFormat::setReadBuffer(ReadBuffer & in_)
{
    reader->setReadBuffer(in_);
    IRowInputFormat::setReadBuffer(in_);
}

void ProtobufListInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
    /// ProtobufSerializerEnvelope carries across-rows state (the
    /// "have we opened the envelope yet" flag), and the reader keeps the
    /// envelope's message-bounds stack — rewind both so the next readRow
    /// opens a fresh envelope. This also recovers from a partially-read
    /// envelope after an exception.
    reader->resetState();
    serializer->resetState();
}

bool ProtobufListInputFormat::readRow(MutableColumns & columns, RowReadExtension & row_read_extension)
{
    size_t row_num = columns.empty() ? 0 : columns[0]->size();
    if (!row_num)
    {
        serializer->setColumns(columns.data(), columns.size());

        /// Check for EOF before starting to read the envelope message.
        /// An empty input (0 bytes) is valid and means 0 rows.
        if (reader->eof())
            return false;

        /// Start the outer message before checking eof below.
        /// This is needed because the eof check relies on knowing the message bounds.
        serializer->startReading();
    }

    if (reader->eof())
    {
        reader->endMessage(/*ignore_errors =*/ false);
        return false;
    }

    serializer->readRow(row_num);

    row_read_extension.read_columns.clear();
    row_read_extension.read_columns.resize(columns.size(), true);
    for (size_t column_idx : missing_column_indices)
        row_read_extension.read_columns[column_idx] = false;
    return true;
}

size_t ProtobufListInputFormat::countRows(size_t max_block_size)
{
    if (getRowNum() == 0)
    {
        /// Check for EOF before starting to read the envelope message.
        /// An empty input (0 bytes) is valid and means 0 rows.
        if (reader->eof())
            return 0;
        reader->startMessage(true);
    }

    if (reader->eof())
    {
        reader->endMessage(false);
        return 0;
    }

    size_t num_rows = 0;
    while (!reader->eof() && num_rows < max_block_size)
    {
        int tag = 0;
        reader->readFieldNumber(tag);
        reader->startNestedMessage();
        reader->endNestedMessage();
        ++num_rows;
    }

    return num_rows;
}

ProtobufListSchemaReader::ProtobufListSchemaReader(const FormatSettings & format_settings)
    : schema_info(
          /*format_schema_source=*/format_settings.schema.format_schema_source,
          /*format_schema=*/format_settings.schema.format_schema,
          /*format_schema_message_name=*/format_settings.schema.format_schema_message_name,
          /*format=*/"Protobuf",
          /*require_message=*/true,
          /*is_server=*/format_settings.schema.is_server,
          /*format_schema_path=*/format_settings.schema.format_schema_path)
    , skip_unsupported_fields(format_settings.protobuf.skip_fields_with_unsupported_types_in_schema_inference)
    , oneof_presence(format_settings.protobuf.oneof_presence)
    , google_protos_path(format_settings.protobuf.google_protos_path)
{
}

NamesAndTypesList ProtobufListSchemaReader::readSchema()
{
    auto descriptor = ProtobufSchemas::instance().getMessageTypeForFormatSchema(
        schema_info, ProtobufSchemas::WithEnvelope::Yes, google_protos_path);
    return protobufSchemaToCHSchema(descriptor.message_descriptor, skip_unsupported_fields, oneof_presence);
}

void registerInputFormatProtobufList(FormatFactory & factory);
void registerInputFormatProtobufList(FormatFactory & factory)
{
    factory.registerInputFormat(
        "ProtobufList",
        [](ReadBuffer & buf, const Block & sample, RowInputFormatParams params, const FormatSettings & settings)
        {
            return std::make_shared<ProtobufListInputFormat>(
                buf,
                std::make_shared<const Block>(sample),
                std::move(params),
                ProtobufSchemaInfo(settings, "Protobuf", sample, settings.protobuf.use_autogenerated_schema, true),
                settings.protobuf.input_flatten_google_wrappers,
                settings.protobuf.google_protos_path);
        });
    factory.markFormatSupportsSubsetOfColumns("ProtobufList");
    factory.registerAdditionalInfoForSchemaCacheGetter(
        "ProtobufList",
        [](const FormatSettings & settings)
        {
            return fmt::format(
                "format_schema={}, skip_fields_with_unsupported_types_in_schema_inference={}",
                settings.schema.format_schema,
                settings.protobuf.skip_fields_with_unsupported_types_in_schema_inference);
        });

    factory.setDocumentation("ProtobufList", Documentation{
        .description = R"DOCS_MD(
:::note
This format is not supported in ClickHouse Cloud.
:::

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `ProtobufList` format is similar to the [`Protobuf`](./Protobuf.md) format but rows are represented as a sequence of sub-messages contained in a message with a fixed name of "Envelope".

## Example usage {#example-usage}

For example:

```sql
SELECT * FROM test.table FORMAT ProtobufList SETTINGS format_schema = 'schemafile:MessageType'
```

```bash
cat protobuflist_messages.bin | clickhouse-client --query "INSERT INTO test.table FORMAT ProtobufList SETTINGS format_schema='schemafile:MessageType'"
```

Where the file `schemafile.proto` looks like this:

```capnp title="schemafile.proto"
syntax = "proto3";
message Envelope {
  message MessageType {
    string name = 1;
    string surname = 2;
    uint32 birthDate = 3;
    repeated string phoneNumbers = 4;
  };
  MessageType row = 1;
};
```

The message type specified in `format_schema` is resolved by first looking for it as a nested type inside a top-level `Envelope` message. If no match is found there — either because the schema has no `Envelope` message, or the `Envelope` does not contain a message with the requested name — the top-level message with that name is used directly.

## Format settings {#format-settings}
)DOCS_MD"});
}

void registerProtobufListSchemaReader(FormatFactory & factory);
void registerProtobufListSchemaReader(FormatFactory & factory)
{
    factory.registerExternalSchemaReader("ProtobufList", [](const FormatSettings & settings)
    {
        return std::make_shared<ProtobufListSchemaReader>(settings);
    });
}

}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatProtobufList(FormatFactory &);
void registerProtobufListSchemaReader(FormatFactory &);
void registerInputFormatProtobufList(FormatFactory &) {}
void registerProtobufListSchemaReader(FormatFactory &) {}
}

#endif
