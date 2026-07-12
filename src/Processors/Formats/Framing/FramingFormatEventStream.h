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
  * Server-sent events is a text protocol, so it is suitable only for text output formats.
  */
class FramingFormatEventStream final : public IFramingFormat
{
public:
    FramingFormatEventStream(WriteBuffer & out_, const FormatSettings & format_settings_)
        : IFramingFormat(out_, format_settings_)
    {
    }

    String getName() const override { return "EventStream"; }
    String getContentType() const override { return "text/event-stream; charset=UTF-8"; }
    bool requiresTextPayload() const override { return true; }

protected:
    void writePayloadPacket(FramedPacketKind kind, std::string_view data) override;
    void writeProgressPacket(const Progress & progress) override;
    void writeLogsPacket(const Block & block) override;
    void writeProfileEventsPacket(const Block & block) override;
    void writeExceptionPacket(const String & message) override;

private:
    void writeDataFields(std::string_view data);
};

}
