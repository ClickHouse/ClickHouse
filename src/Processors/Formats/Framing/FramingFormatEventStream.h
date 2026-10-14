#pragma once

#include <Processors/Formats/Framing/IFramingFormat.h>

namespace DB
{

/** Frames packets as HTTP server-sent events (`text/event-stream`).
  *
  * Every packet is represented as an event with the name corresponding to the packet kind
  * (`data`, `totals`, `extremes`, `progress`, `log`, `profile_events`, `exception`).
  * A block of data produced by the output format is base64-encoded into a single `data` field of the
  * event, and decodes to the fully formatted payload with all of its newlines.
  * Auxiliary packets are represented as JSON, e.g.:
  *
  * event: data
  * data: eyJudW1iZXIiOiIwIn0KeyJudW1iZXIiOiIxIn0K
  *
  * event: progress
  * data: {"read_rows":"2","read_bytes":"16","total_rows_to_read":"2","elapsed_ns":"105341"}
  *
  * Server-sent events is a text protocol that treats line breaks (including a carriage return) as
  * field delimiters, so the formatted data cannot be embedded verbatim: it would either be split
  * across many `data` fields (one per line) or be mangled by a raw carriage return of a `String`
  * value. Base64 has no line breaks, so one packet is always exactly one `data` field and arbitrary
  * bytes - including the output of binary formats such as `Native` or `RowBinary` - survive the text
  * transport byte-exactly. The `Content-Type` carries a `payload=base64` parameter to say so; the
  * auxiliary JSON packets (progress, logs, profile events, exceptions) are never encoded.
  */
class FramingFormatEventStream final : public IFramingFormat
{
public:
    FramingFormatEventStream(WriteBuffer & out_, const FormatSettings & format_settings_)
        : IFramingFormat(out_, format_settings_)
    {
    }

    String getName() const override { return "EventStream"; }
    String getContentType() const override { return "text/event-stream; charset=UTF-8; payload=base64"; }
    /// The payloads are base64-encoded, so any output format can be carried.
    bool requiresTextPayload() const override { return false; }

protected:
    void writePayloadPacket(FramedPacketKind kind, std::string_view data) override;
    void writeProgressPacket(const Progress & progress) override;
    void writeLogsPacket(const Block & block) override;
    void writeProfileEventsPacket(const Block & block) override;
    void writeExceptionPacket(const String & message) override;
};

}
