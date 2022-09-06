#include "ProtobufRowInputFormat.h"

#if USE_PROTOBUF
#   include <Core/Block.h>
#   include <Formats/FormatFactory.h>
#   include <Formats/ProtobufReader.h>
#   include <Formats/ProtobufSchemas.h>
#   include <Formats/ProtobufSerializer.h>

namespace DB
{

ProtobufRowInputFormat::ProtobufRowInputFormat(ReadBuffer & in_, const Block & header_, const Params & params_,
    const FormatSchemaInfo & schema_info_, bool with_length_delimiter_, bool flatten_google_wrappers_)
    : IRowInputFormat(header_, in_, params_)
    , reader(std::make_unique<ProtobufReader>(in_))
    , serializer(ProtobufSerializer::create(
          header_.getNames(),
          header_.getDataTypes(),
          missing_column_indices,
          *ProtobufSchemas::instance().getMessageTypeForFormatSchema(schema_info_, ProtobufSchemas::WithEnvelope::No),
          with_length_delimiter_,
          /* with_envelope = */ false,
          flatten_google_wrappers_,
         *reader))
{
}

bool ProtobufRowInputFormat::readRow(MutableColumns & columns, RowReadExtension & row_read_extension)
{
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

bool ProtobufRowInputFormat::allowSyncAfterError() const
{
    return true;
}

void ProtobufRowInputFormat::syncAfterError()
{
    reader->endMessage(true);
}

void registerInputFormatProtobuf(FormatFactory & factory)
{
    for (bool with_length_delimiter : {false, true})
    {
        factory.registerInputFormat(with_length_delimiter ? "Protobuf" : "ProtobufSingle", [with_length_delimiter](
            ReadBuffer & buf,
            const Block & sample,
            IRowInputFormat::Params params,
            const FormatSettings & settings)
        {
            return std::make_shared<ProtobufRowInputFormat>(buf, sample, std::move(params),
                FormatSchemaInfo(settings, "Protobuf", true),
                with_length_delimiter,
                settings.protobuf.input_flatten_google_wrappers);
        });
        factory.markFormatSupportsSubsetOfColumns(with_length_delimiter ? "Protobuf" : "ProtobufSingle");
    }
}

ProtobufSchemaReader::ProtobufSchemaReader(const FormatSettings & format_settings)
    : schema_info(
          format_settings.schema.format_schema,
          "Protobuf",
          true,
          format_settings.schema.is_server, format_settings.schema.format_schema_path)
    , skip_unsupported_fields(format_settings.protobuf.skip_fields_with_unsupported_types_in_schema_inference)
{
}

NamesAndTypesList ProtobufSchemaReader::readSchema()
{
    const auto * message_descriptor = ProtobufSchemas::instance().getMessageTypeForFormatSchema(schema_info, ProtobufSchemas::WithEnvelope::No);
    return protobufSchemaToCHSchema(message_descriptor, skip_unsupported_fields);
}

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
        factory.registerAdditionalInfoForSchemaCacheGetter(
            name, [](const FormatSettings & settings) { return fmt::format("format_schema={}", settings.schema.format_schema); });
}

}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatProtobuf(FormatFactory &) {}
void registerProtobufSchemaReader(FormatFactory &) {}
}

#endif
