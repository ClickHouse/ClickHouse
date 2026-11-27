#include <Processors/Formats/Framing/FramingFormatEventStream.h>

#include <Core/Block.h>
#include <IO/Progress.h>
#include <IO/WriteHelpers.h>
#include <Common/Base64.h>

namespace DB
{

void FramingFormatEventStream::writePayloadPacket(FramedPacketKind kind, std::string_view data)
{
    writeCString("event: ", out);
    writeString(getPacketKindName(kind), out);
    writeChar('\n', out);
    /// Base64 has no line breaks, so the whole block of formatted data is a single `data:` field, and
    /// it decodes to the fully formatted payload with all of its newlines. The client base64-decodes
    /// it; the concatenation of the decoded payloads of the `data`, `totals` and `extremes` packets is
    /// exactly what the output format would have produced.
    writeCString("data: ", out);
    writeString(base64Encode(String(data)), out);
    writeCString("\n\n", out);
}

void FramingFormatEventStream::writeProgressPacket(const Progress & progress)
{
    writeCString("event: progress\ndata: ", out);
    progress.writeJSON(out, Progress::DisplayMode::Minimal);
    writeCString("\n\n", out);
}

void FramingFormatEventStream::writeLogsPacket(const Block & block)
{
    size_t rows = block.rows();
    for (size_t i = 0; i < rows; ++i)
    {
        writeCString("event: log\ndata: ", out);
        writeLogRowJSON(block, i, out);
        writeCString("\n\n", out);
    }
}

void FramingFormatEventStream::writeProfileEventsPacket(const Block & block)
{
    /// Serialize the whole block as a single JSON array in one `data:` field. An SSE client
    /// reconstructs `event.data` by joining consecutive `data:` fields with '\n', so emitting one
    /// `data:` field per row would produce `{...}\n{...}`, which is not valid JSON and does not match
    /// the documented `profile_events` contract (an array of profile events as JSON). This mirrors the
    /// `profile_events` array of the `JSONEachPacket` framings.
    writeCString("event: profile_events\ndata: [", out);
    size_t rows = block.rows();
    for (size_t i = 0; i < rows; ++i)
    {
        if (i != 0)
            writeChar(',', out);
        writeProfileEventRowJSON(block, i, out);
    }
    writeCString("]\n\n", out);
}

void FramingFormatEventStream::writeExceptionPacket(const String & message)
{
    writeCString("event: exception\ndata: {\"exception\":", out);
    writeJSONStringValidUTF8(message, out, format_settings);
    writeCString("}\n\n", out);
}

}
