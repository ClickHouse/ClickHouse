#include "ProtobufListInputFormat.h"

#if USE_PROTOBUF
#   include <Core/Block.h>
#   include <Formats/FormatFactory.h>
#   include <Formats/ProtobufReader.h>
#   include <Formats/ProtobufSchemas.h>
#   include <Formats/ProtobufSerializer.h>

namespace DB
{

ProtobufListInputFormat::ProtobufListInputFormat(
    ReadBuffer & in_,
    const Block & header_,
    const Params & params_,
    const FormatSchemaInfo & schema_info_,
    bool flatten_google_wrappers_)
    : IRowInputFormat(header_, in_, params_)
    , reader(std::make_unique<ProtobufReader>(in_))
    , serializer(ProtobufSerializer::create(
        header_.getNames(),
        header_.getDataTypes(),
        missing_column_indices,
        *ProtobufSchemas::instance().getMessageTypeForFormatSchema(schema_info_, ProtobufSchemas::WithEnvelope::Yes),
        /* with_length_delimiter = */ true,
        /* with_envelope = */ true,
        flatten_google_wrappers_,
         *reader))
{
}

bool ProtobufListInputFormat::readRow(MutableColumns & columns, RowReadExtension & row_read_extension)
{
    if (reader->eof())
    {
        reader->endMessage(/*ignore_errors =*/ false);
        return false;
    }

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

ProtobufListSchemaReader::ProtobufListSchemaReader(const FormatSettings & format_settings)
    : schema_info(
          format_settings.schema.format_schema,
          "Protobuf",
          true,
          format_settings.schema.is_server,
          format_settings.schema.format_schema_path)
{
}

NamesAndTypesList ProtobufListSchemaReader::readSchema()
{
    const auto * message_descriptor = ProtobufSchemas::instance().getMessageTypeForFormatSchema(schema_info, ProtobufSchemas::WithEnvelope::Yes);
    return protobufSchemaToCHSchema(message_descriptor);
}

void registerInputFormatProtobufList(FormatFactory & factory)
{
    factory.registerInputFormat(
            "ProtobufList",
            [](ReadBuffer &buf,
                const Block & sample,
                RowInputFormatParams params,
                const FormatSettings & settings)
            {
                return std::make_shared<ProtobufListInputFormat>(buf, sample, std::move(params),
                    FormatSchemaInfo(settings, "Protobuf", true), settings.protobuf.input_flatten_google_wrappers);
            });
    factory.markFormatSupportsSubsetOfColumns("ProtobufList");
}

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
void registerInputFormatProtobufList(FormatFactory &) {}
void registerProtobufListSchemaReader(FormatFactory &) {}
}

#endif
