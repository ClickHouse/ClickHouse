#include <Processors/Formats/Impl/ProtobufRowInputFormat.h>

#if USE_PROTOBUF
#   include <Columns/IColumn.h>
#   include <Core/Block.h>
#   include <Formats/FormatFactory.h>
#   include <Formats/ProtobufReader.h>
#   include <Formats/ProtobufSchemas.h>
#   include <Formats/ProtobufSerializer.h>

namespace DB
{

ProtobufRowInputFormat::ProtobufRowInputFormat(
    ReadBuffer & in_,
    SharedHeader header_,
    const Params & params_,
    const ProtobufSchemaInfo & schema_info_,
    bool with_length_delimiter_,
    bool flatten_google_wrappers_,
    bool oneof_presence_,
    const String & google_protos_path)
    : IRowInputFormat(header_, in_, params_)
    , descriptor(ProtobufSchemas::instance().getMessageTypeForFormatSchema(
          schema_info_.getSchemaInfo(), ProtobufSchemas::WithEnvelope::No, google_protos_path))
    , with_length_delimiter(with_length_delimiter_)
    , flatten_google_wrappers(flatten_google_wrappers_)
    , oneof_presence(oneof_presence_)
{
}

void ProtobufRowInputFormat::createReaderAndSerializer()
{
    reader = std::make_unique<ProtobufReader>(*in);
    serializer = ProtobufSerializer::create(
        getPort().getHeader().getNames(),
        getPort().getHeader().getDataTypes(),
        missing_column_indices,
        descriptor,
        with_length_delimiter,
        /* with_envelope = */ false,
        flatten_google_wrappers,
        oneof_presence,
        *reader);
}

void ProtobufRowInputFormat::destroyReaderAndSerializer()
{
    serializer = nullptr;
    reader = nullptr;
}

bool ProtobufRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & row_read_extension)
try
{
    if (!reader)
        createReaderAndSerializer();

    if (reader->eof())
        return false;

    size_t row_num = columns.empty() ? 0 : columns[0]->size();
    if (!row_num)
        serializer->setColumns(columns.data(), columns.size());

    serializer->readRow(row_num);

    row_read_extension.read_columns.clear();
    row_read_extension.read_columns.resize(columns.size(), true);
    for (size_t column_idx : missing_column_indices)
        row_read_extension.read_columns[column_idx] = false;
    return true;
}
catch (...)
{
    /// Unlike text formats that rely on syncAfterError to scan for a row delimiter,
    /// Protobuf knows the message boundary from the length prefix. We can resync
    /// here directly via endMessage(true), which skips any unread bytes in the
    /// current message. This keeps the logic self-contained in readRow and leaves
    /// syncAfterError as a no-op.
    if (reader)
    {
        try
        {
            /// Skip remaining bytes in the current message to position ReadBuffer
            /// at the next message boundary. Without this, if a parse error occurs
            /// mid-message (e.g. invalid date string in field 1 while field 2 follows),
            /// the ReadBuffer stays inside the bad message and the next readRow would
            /// misinterpret the remaining bytes as a new message's length prefix.
            reader->endMessage(true);
        }
        catch (...)
        {
            /// endMessage fails when the stream is truncated (message length prefix
            /// claims more bytes than exist). The stream is unrecoverable — rethrow
            /// so IRowInputFormat fails the query instead of silently skipping.
            destroyReaderAndSerializer();
            throw;
        }
    }
    /// Resync succeeded — destroy and rethrow the original parse error.
    /// IRowInputFormat will skip this row and call readRow again, which
    /// recreates reader and serializer at the correct stream position.
    destroyReaderAndSerializer();
    throw;
}

void ProtobufRowInputFormat::setReadBuffer(ReadBuffer & in_)
{
    if (reader)
        reader->setReadBuffer(in_);
    IRowInputFormat::setReadBuffer(in_);
}

bool ProtobufRowInputFormat::allowSyncAfterError() const
{
    /// ProtobufSingle (with_length_delimiter = false) has one message per input —
    /// there's no next row boundary to skip to after a field-level parse error.
    return with_length_delimiter;
}

void ProtobufRowInputFormat::syncAfterError()
{
    /// endMessage was already called in readRow's catch block before destroying
    /// the reader. Nothing left to do here.
}

size_t ProtobufRowInputFormat::countRows(size_t max_block_size)
try
{
    if (!reader)
        createReaderAndSerializer();

    size_t num_rows = 0;
    while (!reader->eof() && num_rows < max_block_size)
    {
        reader->startMessage(with_length_delimiter);
        reader->endMessage(false);
        ++num_rows;
    }

    return num_rows;
}
catch (...)
{
    destroyReaderAndSerializer();
    throw;
}

void registerInputFormatProtobuf(FormatFactory & factory);
void registerInputFormatProtobuf(FormatFactory & factory)
{
    for (bool with_length_delimiter : {false, true})
    {
        factory.registerInputFormat(
            with_length_delimiter ? "Protobuf" : "ProtobufSingle",
            [with_length_delimiter](ReadBuffer & buf, const Block & sample, IRowInputFormat::Params params, const FormatSettings & settings)
            {
                return std::make_shared<ProtobufRowInputFormat>(
                    buf,
                    std::make_shared<const Block>(sample),
                    std::move(params),
                    ProtobufSchemaInfo(settings, "Protobuf", sample, settings.protobuf.use_autogenerated_schema),
                    with_length_delimiter,
                    settings.protobuf.input_flatten_google_wrappers,
                    settings.protobuf.oneof_presence,
                    settings.protobuf.google_protos_path);
            });
        factory.markFormatSupportsSubsetOfColumns(with_length_delimiter ? "Protobuf" : "ProtobufSingle");
    }
}

ProtobufSchemaReader::ProtobufSchemaReader(const FormatSettings & format_settings)
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

NamesAndTypesList ProtobufSchemaReader::readSchema()
{
    auto descriptor = ProtobufSchemas::instance().getMessageTypeForFormatSchema(
        schema_info, ProtobufSchemas::WithEnvelope::No, google_protos_path);
    return protobufSchemaToCHSchema(descriptor.message_descriptor, skip_unsupported_fields, oneof_presence);
}

void registerProtobufSchemaReader(FormatFactory & factory);
void registerProtobufSchemaReader(FormatFactory & factory)
{
    factory.registerExternalSchemaReader("Protobuf", [](const FormatSettings & settings)
    {
        return std::make_shared<ProtobufSchemaReader>(settings);
    });
    factory.registerFileExtension("pb", "Protobuf");

    factory.registerExternalSchemaReader("ProtobufSingle", [](const FormatSettings & settings)
    {
        return std::make_shared<ProtobufSchemaReader>(settings);
    });

    for (const auto & name : {"Protobuf", "ProtobufSingle"})
    {
        factory.registerAdditionalInfoForSchemaCacheGetter(
            name,
            [](const FormatSettings & settings)
            {
                return fmt::format(
                    "format_schema={}, skip_fields_with_unsupported_types_in_schema_inference={}",
                    settings.schema.format_schema,
                    settings.protobuf.skip_fields_with_unsupported_types_in_schema_inference);
            });
    }
}

}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatProtobuf(FormatFactory &);
void registerProtobufSchemaReader(FormatFactory &);
void registerInputFormatProtobuf(FormatFactory &) {}
void registerProtobufSchemaReader(FormatFactory &) {}
}

#endif
