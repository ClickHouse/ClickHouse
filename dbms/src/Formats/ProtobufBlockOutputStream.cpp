#include <Formats/FormatFactory.h>

#include <Common/config.h>
#if USE_PROTOBUF

#include "ProtobufBlockOutputStream.h"

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


ProtobufBlockOutputStream::ProtobufBlockOutputStream(
    WriteBuffer & buffer_,
    const Block & header_,
    const google::protobuf::Descriptor * message_type,
    const FormatSettings & format_settings_)
    : writer(buffer_, message_type), header(header_), format_settings(format_settings_)
{
}

void ProtobufBlockOutputStream::write(const Block & block)
{
    std::vector<const ColumnWithTypeAndName *> columns_in_write_order;
    const auto & fields_in_write_order = writer.fieldsInWriteOrder();
    columns_in_write_order.reserve(fields_in_write_order.size());
    for (size_t i = 0; i != fields_in_write_order.size(); ++i)
    {
        const auto * field = fields_in_write_order[i];
        const ColumnWithTypeAndName * column = nullptr;
        if (block.has(field->name()))
        {
            column = &block.getByName(field->name());
        }
        else if (field->is_required())
        {
            throw Exception(
                "Output doesn't have a column named '" + field->name() + "' which is required to write the output in the protobuf format.",
                ErrorCodes::NO_DATA_FOR_REQUIRED_PROTOBUF_FIELD);
        }
        columns_in_write_order.emplace_back(column);
    }

    for (size_t row_num = 0; row_num != block.rows(); ++row_num)
    {
        writer.newMessage();
        for (const auto * column : columns_in_write_order)
        {
            if (column)
            {
                assert(column->name == writer.currentField()->name());
                column->type->serializeProtobuf(*(column->column), row_num, writer);
            }
            writer.nextField();
        }
    }
}


void registerOutputFormatProtobuf(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "Protobuf", [](WriteBuffer & buf, const Block & header, const Context & context, const FormatSettings & format_settings)
        {
            const auto * message_type = ProtobufSchemas::instance().getMessageTypeForFormatSchema(FormatSchemaInfo(context, "proto"));
            return std::make_shared<ProtobufBlockOutputStream>(buf, header, message_type, format_settings);
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
