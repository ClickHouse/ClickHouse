#include <Processors/Formats/Impl/ArrowIPC/ArrowIPCBlockInputFormat.h>

#if USE_ARROW

#include <Processors/Formats/Impl/ArrowIPC/FlatBuffersCommon.h>
#include <IO/ReadBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

ArrowIPCBlockInputFormat::ArrowIPCBlockInputFormat(
    ReadBuffer & in_, SharedHeader header_, bool stream_, const FormatSettings & format_settings_)
    : IInputFormat(header_, &in_)
    , stream(stream_)
    , block_missing_values(getPort().getHeader().columns())
    , format_settings(format_settings_)
{
}

Chunk ArrowIPCBlockInputFormat::read()
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Native Arrow IPC reader is not implemented yet (format: {})",
        stream ? "ArrowStream" : "Arrow");
}

void ArrowIPCBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();
    block_missing_values.clear();
    approx_bytes_read_for_chunk = 0;
}

}

#endif
