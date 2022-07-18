#include "ProtobufListOutputFormat.h"

#if USE_PROTOBUF
#   include <Formats/FormatFactory.h>
#   include <Formats/FormatSchemaInfo.h>
#   include <Formats/ProtobufWriter.h>
#   include <Formats/ProtobufSerializer.h>
#   include <Formats/ProtobufSchemas.h>

namespace DB
{

ProtobufListOutputFormat::ProtobufListOutputFormat(
    WriteBuffer & out_,
    const Block & header_,
    const RowOutputFormatParams & params_,
    const FormatSchemaInfo & schema_info_)
    : IRowOutputFormat(header_, out_, params_)
    , writer(std::make_unique<ProtobufWriter>(out))
    , serializer(ProtobufSerializer::create(
          header_.getNames(),
          header_.getDataTypes(),
          *ProtobufSchemas::instance().getMessageTypeForFormatSchema(schema_info_, ProtobufSchemas::WithEnvelope::Yes),
          /* with_length_delimiter = */ true,
          /* with_envelope = */ true,
          *writer))
{
}

void ProtobufListOutputFormat::write(const Columns & columns, size_t row_num)
{
    if (row_num == 0)
        serializer->setColumns(columns.data(), columns.size());

    serializer->writeRow(row_num);
}

void ProtobufListOutputFormat::finalizeImpl()
{
    serializer->finalizeWrite();
}

void registerOutputFormatProtobufList(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "ProtobufList",
        [](WriteBuffer & buf,
           const Block & header,
           const RowOutputFormatParams & params,
           const FormatSettings & settings)
        {
            return std::make_shared<ProtobufListOutputFormat>(
                buf, header, params,
                FormatSchemaInfo(settings, "Protobuf", true));
        });
}

}

#else

namespace DB
{
class FormatFactory;
void registerOutputFormatProtobufList(FormatFactory &) {}
}

#endif
