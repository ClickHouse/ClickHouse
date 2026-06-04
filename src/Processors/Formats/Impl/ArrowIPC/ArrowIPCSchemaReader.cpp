#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCSchemaReader.h>

#if USE_ARROW

#include <IO/ReadBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

ArrowIPCSchemaReader::ArrowIPCSchemaReader(ReadBuffer & in_, bool stream_, const FormatSettings & format_settings_)
    : ISchemaReader(in_), stream(stream_), format_settings(format_settings_)
{
}

NamesAndTypesList ArrowIPCSchemaReader::readSchema()
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Native Arrow IPC schema reader is not implemented yet (format: {})",
        stream ? "ArrowStream" : "Arrow");
}

std::optional<size_t> ArrowIPCSchemaReader::readNumberOrRows()
{
    return std::nullopt;
}

}

#endif
