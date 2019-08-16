#include <Formats/FormatFactory.h>

#include "config_formats.h"
#if USE_PROTOBUF

#include <Processors/Formats/Impl/ProtobufRowOutputFormat.h>

#include <Core/Block.h>
#include <Formats/FormatSchemaInfo.h>
#include <Formats/ProtobufSchemas.h>
#include <Interpreters/Context.h>
#include <google/protobuf/descriptor.h>



namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int NO_DATA_FOR_REQUIRED_PROTOBUF_FIELD;
}


ProtobufRowOutputFormat::ProtobufRowOutputFormat(
    WriteBuffer & out_,
    const Block & header,
    const FormatSchemaInfo & format_schema)
    : IRowOutputFormat(header, out_)
    , data_types(header.getDataTypes())
    , writer(out, ProtobufSchemas::instance().getMessageTypeForFormatSchema(format_schema), header.getNames())
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
    factory.registerOutputFormatProcessor(
        "Protobuf", [](WriteBuffer & buf, const Block & header, const Context & context, const FormatSettings &)
        {
            return std::make_shared<ProtobufRowOutputFormat>(buf, header, FormatSchemaInfo(context, "Protobuf"));
        });
}

}

#else

namespace DB
{
    class FormatFactory;
    void registerOutputFormatProcessorProtobuf(FormatFactory &) {}
}

#endif
