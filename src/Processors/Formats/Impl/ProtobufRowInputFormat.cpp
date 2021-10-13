#include "ProtobufRowInputFormat.h"

#if USE_PROTOBUF
#   include <Core/Block.h>
#   include <Formats/FormatFactory.h>
#   include <Formats/FormatSchemaInfo.h>
#   include <Formats/ProtobufReader.h>
#   include <Formats/ProtobufSchemas.h>
#   include <Formats/ProtobufSerializer.h>
#   include <Interpreters/Context.h>
#   include <common/range.h>


namespace DB
{
ProtobufRowInputFormat::ProtobufRowInputFormat(ReadBuffer & in_, const Block & header_, const Params & params_, const FormatSchemaInfo & schema_info_, bool with_length_delimiter_)
    : IRowInputFormat(header_, in_, params_)
    , reader(std::make_unique<ProtobufReader>(in_))
    , serializer(ProtobufSerializer::create(
          header_.getNames(),
          header_.getDataTypes(),
          missing_column_indices,
          *ProtobufSchemas::instance().getMessageTypeForFormatSchema(schema_info_),
          with_length_delimiter_,
         *reader))
{
}

ProtobufRowInputFormat::~ProtobufRowInputFormat() = default;

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

void registerInputFormatProcessorProtobuf(FormatFactory & factory)
{
    for (bool with_length_delimiter : {false, true})
    {
        factory.registerInputFormatProcessor(with_length_delimiter ? "Protobuf" : "ProtobufSingle", [with_length_delimiter](
            ReadBuffer & buf,
            const Block & sample,
            IRowInputFormat::Params params,
            const FormatSettings & settings)
        {
            return std::make_shared<ProtobufRowInputFormat>(buf, sample, std::move(params),
                FormatSchemaInfo(settings.schema.format_schema, "Protobuf", true,
                                settings.schema.is_server, settings.schema.format_schema_path),
                with_length_delimiter);
        });
    }
}

}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatProcessorProtobuf(FormatFactory &) {}
}

#endif
