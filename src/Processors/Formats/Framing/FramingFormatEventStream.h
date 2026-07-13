#pragma once

#include <Processors/Formats/Framing/IFramingFormat.h>

namespace DB
{

/** Frames packets as HTTP server-sent events (`text/event-stream`).
  *
  * Every packet is represented as an event with the name corresponding to the packet kind
  * (`data`, `totals`, `extremes`, `progress`, `log`, `profile_events`, `exception`).
  * The bytes produced by the output format become the `data` fields of the event, one field
  * per line (per the SSE specification, the client joins consecutive `data` fields with a newline).
  * Auxiliary packets are represented as JSON, e.g.:
  *
  * event: data
  * data: {"number":"0"}
  * data: {"number":"1"}
  *
  * event: progress
  * data: {"read_rows":"2","read_bytes":"16","total_rows_to_read":"2","elapsed_ns":"105341"}
  *
  * Server-sent events is a text protocol. Text output formats are embedded as text, one `data:`
  * field per line. Output formats that may produce non-UTF-8 bytes (binary formats such as `Native`
  * or `RowBinary`, and raw passthrough formats such as `RawBLOB` or `TSVRaw`), or that may emit raw
  * carriage returns (`TSV` / `CSV` with a CRLF row terminator - the transport treats `\r` as a line
  * terminator, so it cannot be carried losslessly as text), are base64-encoded instead, so arbitrary
  * bytes survive the text transport. In that case the `Content-Type` carries a `payload=base64`
  * parameter, so the client knows to base64-decode the `data`, `totals` and `extremes` payloads (the
  * auxiliary JSON packets - progress, logs, profile events, exceptions - are never encoded).
  */
class FramingFormatEventStream final : public IFramingFormat
{
public:
    FramingFormatEventStream(WriteBuffer & out_, const FormatSettings & format_settings_, bool base64_ = false)
        : IFramingFormat(out_, format_settings_), base64(base64_)
    {
    }

    String getName() const override { return "EventStream"; }
    String getContentType() const override
    {
        return base64 ? "text/event-stream; charset=UTF-8; payload=base64" : "text/event-stream; charset=UTF-8";
    }
    /// `EventStream` embeds the output as text, but falls back to base64 for non-UTF-8 output, so it
    /// does not require a text output format (unlike `JSONEachPacketString`).
    bool requiresTextPayload() const override { return false; }

protected:
    void writePayloadPacket(FramedPacketKind kind, std::string_view data) override;
    void writeProgressPacket(const Progress & progress) override;
    void writeLogsPacket(const Block & block) override;
    void writeProfileEventsPacket(const Block & block) override;
    void writeExceptionPacket(const String & message) override;

private:
    void writeDataFields(std::string_view data);

    /// Base64-encode the payloads (for binary and raw output formats).
    const bool base64;
};

}
