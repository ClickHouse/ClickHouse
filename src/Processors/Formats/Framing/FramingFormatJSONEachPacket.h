#pragma once

#include <Processors/Formats/Framing/IFramingFormat.h>

namespace DB
{

/** Frames every packet as a JSON object on a separate line, containing the info about the packet.
  *
  * The bytes produced by the output format are put into the `data` field, either base64-encoded
  * (the `JSONEachPacketBase64` framing format, suitable for binary output formats)
  * or as a JSON string (the `JSONEachPacketString` framing format). Auxiliary packets
  * are represented as JSON, e.g.:
  *
  * {"packet":"data","data":"{\"number\":\"0\"}\n{\"number\":\"1\"}\n"}
  * {"packet":"progress","progress":{"read_rows":"2","read_bytes":"16","elapsed_ns":"105341"}}
  * {"packet":"log","log":{"event_time":"2026-07-11 00:00:00.000000","host_name":"...","query_id":"...","thread_id":"1","priority":"Debug","source":"...","text":"..."}}
  * {"packet":"profile_events","profile_events":[{"host_name":"...","current_time":"2026-07-11 00:00:00","thread_id":"0","type":"increment","name":"SelectedRows","value":"2"}]}
  * {"packet":"exception","exception":"Code: 395. DB::Exception: ..."}
  */
class FramingFormatJSONEachPacket final : public IFramingFormat
{
public:
    FramingFormatJSONEachPacket(WriteBuffer & out_, const FormatSettings & format_settings_, bool base64_)
        : IFramingFormat(out_, format_settings_), base64(base64_)
    {
    }

    String getName() const override { return base64 ? "JSONEachPacketBase64" : "JSONEachPacketString"; }

    /// The two variants do not encode the `data` field the same way, and only the base64 one
    /// guarantees valid UTF-8 for the whole stream, so the response metadata is variant-specific:
    /// the `payload` parameter tells the client how to decode `data`, and `charset=UTF-8`
    /// is promised only when the framing enforces it.
    String getContentType() const override
    {
        return base64
            ? "application/x-ndjson; charset=UTF-8; payload=base64"
            : "application/x-ndjson; payload=string";
    }

    bool requiresTextPayload() const override { return !base64; }

protected:
    void writePayloadPacket(FramedPacketKind kind, std::string_view data) override;
    void writeProgressPacket(const Progress & progress) override;
    void writeLogsPacket(const Block & block) override;
    void writeProfileEventsPacket(const Block & block) override;
    void writeExceptionPacket(const String & message) override;

private:
    const bool base64;
};

}
