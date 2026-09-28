#include <Processors/Formats/Framing/FramingFormatJSONEachPacket.h>

#include <Core/Block.h>
#include <IO/Progress.h>
#include <IO/WriteHelpers.h>
#include <Common/Base64.h>

namespace DB
{

void FramingFormatJSONEachPacket::writePayloadPacket(FramedPacketKind kind, std::string_view data)
{
    writeCString("{\"packet\":\"", out);
    writeString(getPacketKindName(kind), out);
    writeCString("\",\"data\":", out);
    if (base64)
    {
        writeChar('"', out);
        writeString(base64Encode(std::string(data)), out);
        writeChar('"', out);
    }
    else
    {
        writeJSONString(data, out, format_settings);
    }
    writeCString("}\n", out);
}

void FramingFormatJSONEachPacket::writeProgressPacket(const Progress & progress)
{
    writeCString("{\"packet\":\"progress\",\"progress\":", out);
    progress.writeJSON(out, Progress::DisplayMode::Minimal);
    writeCString("}\n", out);
}

void FramingFormatJSONEachPacket::writeLogsPacket(const Block & block)
{
    size_t rows = block.rows();
    for (size_t i = 0; i < rows; ++i)
    {
        writeCString("{\"packet\":\"log\",\"log\":", out);
        writeLogRowJSON(block, i, out);
        writeCString("}\n", out);
    }
}

void FramingFormatJSONEachPacket::writeProfileEventsPacket(const Block & block)
{
    writeCString("{\"packet\":\"profile_events\",\"profile_events\":[", out);
    size_t rows = block.rows();
    for (size_t i = 0; i < rows; ++i)
    {
        if (i != 0)
            writeChar(',', out);
        writeProfileEventRowJSON(block, i, out);
    }
    writeCString("]}\n", out);
}

void FramingFormatJSONEachPacket::writeExceptionPacket(const String & message)
{
    writeCString("{\"packet\":\"exception\",\"exception\":", out);
    writeJSONStringValidUTF8(message, out, format_settings);
    writeCString("}\n", out);
}

}
