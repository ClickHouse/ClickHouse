#include <Formats/FormatFactory.h>
#include "ProtobufRowOutputFormat.h"

#if USE_PROTOBUF

#include <Core/Block.h>
#include <Formats/FormatSchemaInfo.h>
#include <Formats/ProtobufSchemas.h>
#include <Interpreters/Context.h>
#include <google/protobuf/descriptor.h>


namespace DB
{
namespace ErrorCodes
{
}


ProtobufRowOutputFormat::ProtobufRowOutputFormat(
    WriteBuffer & out_,
    const Block & header,
    const RowOutputFormatParams & params,
    const FormatSchemaInfo & format_schema,
    const bool single_message_mode_)
    : IRowOutputFormat(header, out_, params)
    , data_types(header.getDataTypes())
    , writer(out, ProtobufSchemas::instance().getMessageTypeForFormatSchema(format_schema), header.getNames(), single_message_mode_)
{
    value_indices.resize(header.columns());
}

void ProtobufRowOutputFormat::write(const Columns & columns, size_t row_num)
{
    writer.startMessage();
    std::fill(value_indices.begin(), value_indices.end(), 0);
    size_t column_index;
    while (writer.writeField(column_index))
        data_types[column_index]->serializeProtobuf(
                *columns[column_index], row_num, writer, value_indices[column_index]);
    writer.endMessage();
}


void registerOutputFormatProcessorProtobuf(FormatFactory & factory)
{
    for (bool single_message_mode : {false, true})
    {
        factory.registerOutputFormatProcessor(
            single_message_mode ? "ProtobufSingle" : "Protobuf",
            [single_message_mode](WriteBuffer & buf,
               const Block & header,
               const RowOutputFormatParams & params,
               const FormatSettings & settings)
            {
                return std::make_shared<ProtobufRowOutputFormat>(buf, header, params,
                    FormatSchemaInfo(settings.schema.format_schema, "Protobuf", true,
                                     settings.schema.is_server, settings.schema.format_schema_path),
                                     single_message_mode);
            });
    }
}

}

#else

namespace DB
{
    class FormatFactory;
    void registerOutputFormatProcessorProtobuf(FormatFactory &) {}
}

#endif
