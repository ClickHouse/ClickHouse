#include <Processors/Formats/Framing/FramingFormatEventStream.h>

#include <Core/Block.h>
#include <IO/Progress.h>
#include <IO/WriteHelpers.h>
#include <Common/Base64.h>
#include <base/find_symbols.h>

namespace DB
{

void FramingFormatEventStream::writeDataFields(std::string_view data)
{
    const char * pos = data.data();
    const char * end = pos + data.size();

    /// Emit one SSE `data:` field per line of the payload. The client reconstructs the event data
    /// by joining the values of consecutive `data:` fields with '\n' and then stripping a single
    /// trailing '\n' (per the server-sent events specification). To make this reconstruction
    /// byte-exact, a payload that ends with '\n' has to produce an extra empty `data:` field:
    /// without it the stripped trailing '\n' would be lost. This is the common case for line-based
    /// output formats such as `JSONEachRow`, `TSV`, or `CSV`, whose payload ends with '\n'.
    while (true)
    {
        const char * line_end = find_first_symbols<'\n'>(pos, end);
        writeCString("data: ", out);
        out.write(pos, line_end - pos);
        writeChar('\n', out);
        /// The last payload line may have no trailing '\n' (for example `FORMAT JSON`). Stop when the
        /// scan reaches `end`; otherwise advancing past `line_end` would be undefined pointer arithmetic.
        /// When the payload ends with '\n', `pos` becomes `end` and the loop runs once more to emit the
        /// trailing empty `data:` field described above.
        if (line_end == end)
            break;
        pos = line_end + 1;
    }
}

void FramingFormatEventStream::writePayloadPacket(FramedPacketKind kind, std::string_view data)
{
    writeCString("event: ", out);
    writeString(getPacketKindName(kind), out);
    writeChar('\n', out);
    if (base64)
    {
        /// Base64 has no line breaks, so the whole payload is a single `data:` field. The client
        /// base64-decodes it; the concatenation of the decoded payloads of the `data`, `totals` and
        /// `extremes` packets is exactly what the output format would have produced.
        writeCString("data: ", out);
        writeString(base64Encode(String(data)), out);
        writeChar('\n', out);
    }
    else
    {
        writeDataFields(data);
    }
    writeChar('\n', out);
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
    writeJSONString(message, out, format_settings);
    writeCString("}\n\n", out);
}

}
