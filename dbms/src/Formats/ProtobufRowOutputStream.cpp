#include <Formats/FormatFactory.h>

#include <Common/config.h>
#if USE_PROTOBUF

#include "ProtobufRowOutputStream.h"

#include <Core/Block.h>
#include <Formats/BlockOutputStreamFromRowOutputStream.h>
#include <Formats/FormatSchemaInfo.h>
#include <Formats/ProtobufSchemas.h>
#include <google/protobuf/descriptor.h>


namespace DB
{
ProtobufRowOutputStream::ProtobufRowOutputStream(WriteBuffer & out, const Block & header, const FormatSchemaInfo & format_schema)
    : data_types(header.getDataTypes()), writer(out, ProtobufSchemas::instance().getMessageTypeForFormatSchema(format_schema), header.getNames())
{
    value_indices.resize(header.columns());
}

void ProtobufRowOutputStream::write(const Block & block, size_t row_num)
{
    writer.startMessage();
    std::fill(value_indices.begin(), value_indices.end(), 0);
    size_t column_index;
    while (writer.writeField(column_index))
        data_types[column_index]->serializeProtobuf(
            *block.getByPosition(column_index).column, row_num, writer, value_indices[column_index]);
    writer.endMessage();
}


void registerOutputFormatProtobuf(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "Protobuf", [](WriteBuffer & buf, const Block & header, const Context & context, const FormatSettings &)
        {
            return std::make_shared<BlockOutputStreamFromRowOutputStream>(
                std::make_shared<ProtobufRowOutputStream>(buf, header, FormatSchemaInfo(context, "Protobuf")), header);
        });
}

}

#else

namespace DB
{
    class FormatFactory;
    void registerOutputFormatProtobuf(FormatFactory &) {}
}

#endif
