#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCSchemaReader.h>

#if USE_ARROW

#include <Processors/Formats/Impl/ArrowIPC/MessageReader.h>
#include <Processors/Formats/Impl/ArrowIPC/SchemaConverter.h>
#include <IO/ReadBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int NOT_IMPLEMENTED;
}

ArrowIPCSchemaReader::ArrowIPCSchemaReader(ReadBuffer & in_, bool stream_, const FormatSettings & format_settings_)
    : ISchemaReader(in_), stream(stream_), format_settings(format_settings_)
{
}

NamesAndTypesList ArrowIPCSchemaReader::readSchema()
{
    if (!stream)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Native Arrow IPC schema reader supports only the streaming format so far");

    ArrowIPC::MessageReader reader(in);
    ArrowIPC::MessageReader::Message msg;
    if (!reader.readNextMessage(msg))
        throw Exception(ErrorCodes::INCORRECT_DATA, "The Arrow stream is empty");
    if (msg.header->header_type() != ArrowIPC::flatbuf::MessageHeader_Schema)
        throw Exception(ErrorCodes::INCORRECT_DATA, "The first Arrow IPC message must be the schema");

    ArrowIPC::ArrowSchema schema = ArrowIPC::parseSchema(*msg.header->header_as_Schema());

    /// `schema_inference_make_columns_nullable`: 0 = never nullable, 1 = always nullable,
    /// otherwise (auto) follow the Arrow field's own nullability. This mirrors the library reader.
    const UInt64 make_columns_nullable = format_settings.schema_inference_make_columns_nullable;

    NamesAndTypesList result;
    for (const ArrowIPC::ArrowField & field : schema.fields)
    {
        bool make_nullable;
        if (make_columns_nullable == 0)
            make_nullable = false;
        else if (make_columns_nullable == 1)
            make_nullable = true;
        else
            make_nullable = field.nullable;
        result.emplace_back(field.name, ArrowIPC::fieldToCHType(field, format_settings, make_nullable));
    }
    return result;
}

std::optional<size_t> ArrowIPCSchemaReader::readNumberOrRows()
{
    return std::nullopt;
}

}

#endif
