#include <Formats/FormatFactory.h>

#include <Common/config.h>
#if USE_PROTOBUF

#include "ProtobufRowOutputStream.h"

#include <Core/Block.h>
#include <Formats/BlockOutputStreamFromRowOutputStream.h>
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


ProtobufRowOutputStream::ProtobufRowOutputStream(
    WriteBuffer & buffer_,
    const Block & header,
    const google::protobuf::Descriptor * message_type)
    : writer(buffer_, message_type)
{
    std::vector<const ColumnWithTypeAndName *> columns_in_write_order;
    const auto & fields_in_write_order = writer.fieldsInWriteOrder();
    column_indices.reserve(fields_in_write_order.size());
    data_types.reserve(fields_in_write_order.size());
    for (size_t i = 0; i != fields_in_write_order.size(); ++i)
    {
        const auto * field = fields_in_write_order[i];
        size_t column_index = static_cast<size_t>(-1);
        DataTypePtr data_type = nullptr;
        if (header.has(field->name()))
        {
            column_index = header.getPositionByName(field->name());
            data_type = header.getByPosition(column_index).type;
        }
        else if (field->is_required())
        {
            throw Exception(
                "Output doesn't have a column named '" + field->name() + "' which is required to write the output in the protobuf format.",
                ErrorCodes::NO_DATA_FOR_REQUIRED_PROTOBUF_FIELD);
        }
        column_indices.emplace_back(column_index);
        data_types.emplace_back(data_type);
    }
}

void ProtobufRowOutputStream::write(const Block & block, size_t row_num)
{
    writer.newMessage();
    for (size_t i = 0; i != data_types.size(); ++i)
    {
        if (data_types[i])
            data_types[i]->serializeProtobuf(*block.getByPosition(column_indices[i]).column, row_num, writer);
        writer.nextField();
    }
}


void registerOutputFormatProtobuf(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "Protobuf", [](WriteBuffer & buf, const Block & header, const Context & context, const FormatSettings &)
        {
            const auto * message_type = ProtobufSchemas::instance().getMessageTypeForFormatSchema(FormatSchemaInfo(context, "proto"));
            return std::make_shared<BlockOutputStreamFromRowOutputStream>(
                std::make_shared<ProtobufRowOutputStream>(buf, header, message_type), header);
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
