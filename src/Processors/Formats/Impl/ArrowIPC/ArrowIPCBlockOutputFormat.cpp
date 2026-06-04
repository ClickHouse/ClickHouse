#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCBlockOutputFormat.h>

#if USE_ARROW

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

ArrowIPCBlockOutputFormat::ArrowIPCBlockOutputFormat(
    WriteBuffer & out_, SharedHeader header_, bool stream_, const FormatSettings & format_settings_)
    : IOutputFormat(header_, out_), stream(stream_), format_settings(format_settings_)
{
}

void ArrowIPCBlockOutputFormat::consume(Chunk)
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Native Arrow IPC writer is not implemented yet (format: {})",
        stream ? "ArrowStream" : "Arrow");
}

void ArrowIPCBlockOutputFormat::finalizeImpl()
{
}

void ArrowIPCBlockOutputFormat::resetFormatterImpl()
{
}

}

#endif
