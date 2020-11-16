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
    const bool use_length_delimiters_)
    : IRowOutputFormat(header, out_, params_)
    , data_types(header.getDataTypes())
    , writer(out, ProtobufSchemas::instance().getMessageTypeForFormatSchema(format_schema), header.getNames(), use_length_delimiters_)
    , throw_on_multiple_rows_undelimited(!use_length_delimiters_ && !params_.ignore_no_row_delimiter)
{
    value_indices.resize(header.columns());
}

void ProtobufRowOutputFormat::write(const Columns & columns, size_t row_num)
{
    if (throw_on_multiple_rows_undelimited && !first_row)
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
    for (bool use_length_delimiters : {false, true})
    {
        factory.registerOutputFormatProcessor(
            use_length_delimiters ? "Protobuf" : "ProtobufSingle",
            [use_length_delimiters](WriteBuffer & buf,
               const Block & header,
               const RowOutputFormatParams & params,
               const FormatSettings & settings)
            {
                return std::make_shared<ProtobufRowOutputFormat>(buf, header, params,
                    FormatSchemaInfo(settings.schema.format_schema, "Protobuf", true,
                                     settings.schema.is_server, settings.schema.format_schema_path),
                                     use_length_delimiters);
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
