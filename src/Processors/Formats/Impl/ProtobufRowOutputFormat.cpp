#include "ProtobufRowOutputFormat.h"

#if USE_PROTOBUF
#   include <Formats/FormatFactory.h>
#   include <Core/Block.h>
#   include <Formats/FormatSchemaInfo.h>
#   include <Formats/ProtobufSchemas.h>
#   include <Formats/ProtobufSerializer.h>
#   include <Formats/ProtobufWriter.h>
#   include <google/protobuf/descriptor.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NO_ROW_DELIMITER;
}


ProtobufRowOutputFormat::ProtobufRowOutputFormat(
    WriteBuffer & out_,
    const Block & header_,
    const RowOutputFormatParams & params_,
    const FormatSchemaInfo & schema_info_,
    const FormatSettings & settings_,
    bool with_length_delimiter_)
    : IRowOutputFormat(header_, out_, params_)
    , writer(std::make_unique<ProtobufWriter>(out))
    , serializer(ProtobufSerializer::create(
          header_.getNames(),
          header_.getDataTypes(),
          *ProtobufSchemas::instance().getMessageTypeForFormatSchema(schema_info_),
          with_length_delimiter_,
          *writer))
    , allow_multiple_rows(with_length_delimiter_ || settings_.protobuf.allow_multiple_rows_without_delimiter)
{
}

void ProtobufRowOutputFormat::write(const Columns & columns, size_t row_num)
{
    if (!allow_multiple_rows && !first_row)
        throw Exception(
            "The ProtobufSingle format can't be used to write multiple rows because this format doesn't have any row delimiter.",
            ErrorCodes::NO_ROW_DELIMITER);

    if (!row_num)
        serializer->setColumns(columns.data(), columns.size());

    serializer->writeRow(row_num);
}


void registerOutputFormatProcessorProtobuf(FormatFactory & factory)
{
    for (bool with_length_delimiter : {false, true})
    {
        factory.registerOutputFormatProcessor(
            with_length_delimiter ? "Protobuf" : "ProtobufSingle",
            [with_length_delimiter](WriteBuffer & buf,
               const Block & header,
               const RowOutputFormatParams & params,
               const FormatSettings & settings)
            {
                return std::make_shared<ProtobufRowOutputFormat>(
                    buf, header, params,
                    FormatSchemaInfo(settings.schema.format_schema, "Protobuf",
                        true, settings.schema.is_server,
                        settings.schema.format_schema_path),
                    settings,
                    with_length_delimiter);
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
