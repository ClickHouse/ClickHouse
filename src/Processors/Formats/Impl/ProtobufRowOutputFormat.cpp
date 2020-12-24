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
    extern const int NO_ROW_DELIMITER;
}


ProtobufRowOutputFormat::ProtobufRowOutputFormat(
    WriteBuffer & out_,
    const Block & header,
    const RowOutputFormatParams & params_,
    const FormatSchemaInfo & format_schema,
    const FormatSettings & settings)
    : IRowOutputFormat(header, out_, params_)
    , data_types(header.getDataTypes())
    , writer(out,
        ProtobufSchemas::instance().getMessageTypeForFormatSchema(format_schema),
        header.getNames(), settings.protobuf.write_row_delimiters)
    , allow_only_one_row(
        !settings.protobuf.write_row_delimiters
            && !settings.protobuf.allow_many_rows_no_delimiters)
{
    value_indices.resize(header.columns());
}

void ProtobufRowOutputFormat::write(const Columns & columns, size_t row_num)
{
    if (allow_only_one_row && !first_row)
    {
        throw Exception("The ProtobufSingle format can't be used to write multiple rows because this format doesn't have any row delimiter.", ErrorCodes::NO_ROW_DELIMITER);
    }

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
    for (bool write_row_delimiters : {false, true})
    {
        factory.registerOutputFormatProcessor(
            write_row_delimiters ? "Protobuf" : "ProtobufSingle",
            [write_row_delimiters](WriteBuffer & buf,
               const Block & header,
               const RowOutputFormatParams & params,
               const FormatSettings & _settings)
            {
                FormatSettings settings = _settings;
                settings.protobuf.write_row_delimiters = write_row_delimiters;
                return std::make_shared<ProtobufRowOutputFormat>(
                    buf, header, params,
                    FormatSchemaInfo(settings.schema.format_schema, "Protobuf",
                        true, settings.schema.is_server,
                        settings.schema.format_schema_path),
                    settings);
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
