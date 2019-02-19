#include <Formats/FormatFactory.h>

#include <Common/config.h>
#if USE_PROTOBUF

#include "ProtobufBlockOutputFormat.h"

#include <Core/Block.h>
#include <Processors/Formats/Impl/FormatSchemaInfo.h>
#include <Processors/Formats/Impl/ProtobufSchemas.h>
#include <Interpreters/Context.h>
#include <google/protobuf/descriptor.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int NO_DATA_FOR_REQUIRED_PROTOBUF_FIELD;
}


ProtobufBlockOutputFormat::ProtobufBlockOutputFormat(
    WriteBuffer & out,
    Block header,
    const google::protobuf::Descriptor * message_type,
    const FormatSettings & format_settings)
    : IOutputFormat(std::move(header), out), writer(out, message_type), format_settings(format_settings)
{
}

void ProtobufBlockOutputFormat::consume(Chunk chunk)
{
    auto header = getPort(PortKind::Main).getHeader();
    auto & columns = chunk.getColumns();

    std::vector<const ColumnWithTypeAndName *> header_in_write_order;
    ColumnRawPtrs columns_in_write_order;
    const auto & fields_in_write_order = writer.fieldsInWriteOrder();
    header_in_write_order.reserve(fields_in_write_order.size());
    columns_in_write_order.reserve(fields_in_write_order.size());

    for (size_t i = 0; i != fields_in_write_order.size(); ++i)
    {
        const auto * field = fields_in_write_order[i];
        const ColumnWithTypeAndName * header_ptr = nullptr;
        const IColumn * column_ptr = nullptr;

        if (header.has(field->name()))
        {
            auto pos = header.getPositionByName(field->name());

            header_ptr = &header.getByPosition(pos);
            column_ptr = columns[pos].get();
        }
        else if (field->is_required())
        {
            throw Exception(
                "Output doesn't have a column named '" + field->name() + "' which is required to write the output in the protobuf format.",
                ErrorCodes::NO_DATA_FOR_REQUIRED_PROTOBUF_FIELD);
        }
        columns_in_write_order.emplace_back(column_ptr);
        header_in_write_order.emplace_back(header_ptr);
    }

    auto num_rows = chunk.getNumRows();
    for (size_t row_num = 0; row_num < num_rows; ++row_num)
    {
        writer.newMessage();
        auto num_columns_to_write = header_in_write_order.size();
        for (size_t ps = 0; ps < num_columns_to_write; ++ps)
        {
            auto * header_ptr = header_in_write_order[ps];
            auto * column_ptr = columns_in_write_order[ps];
            if (header_ptr)
            {
                assert(header_ptr->name == writer.currentField()->name());
                header_ptr->type->serializeProtobuf(*column_ptr, row_num, writer);
            }
            writer.nextField();
        }
    }
}


void registerOutputFormatProcessorProtobuf(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor(
        "Protobuf", [](WriteBuffer & buf, const Block & header, const Context & context, const FormatSettings & format_settings)
        {
            const auto * message_type = ProtobufSchemas::instance().getMessageTypeForFormatSchema(FormatSchemaInfo(context, "proto"));
            return std::make_shared<ProtobufBlockOutputFormat>(buf, header, message_type, format_settings);
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
