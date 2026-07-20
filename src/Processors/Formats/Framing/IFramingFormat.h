#pragma once

#include <Core/Types.h>
#include <Formats/FormatSettings.h>
#include <IO/Progress.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ProfileEventsExt.h>
#include <Common/Stopwatch.h>

#include <memory>

namespace DB
{

class Block;
class InternalTextLogsQueue;

/// Which part of the query result a formatted payload belongs to.
enum class FramedPacketKind : uint8_t
{
    Data,
    Totals,
    Extremes,
};

/** A framing format multiplexes different parts of the query response in a single stream:
  * chunks of data, totals and extremes, progress packets, profile events (metrics), server logs,
  * and exceptions - everything that the native protocol supports. This allows rich data exchange
  * in the HTTP protocol.
  *
  * Framing formats are independent of output formats: they encapsulate bytes produced by any
  * output format, by separating and potentially encoding these chunks of bytes. The framing format
  * works as a multiplexor: the output format writes into the buffer returned by `getPayloadBuffer`,
  * and `IOutputFormat` notifies the framing format on packet boundaries (`onPayload`), which wraps
  * everything accumulated since the previous boundary into a packet of the corresponding kind.
  * The concatenation of the payloads of all `data`, `totals` and `extremes` packets is exactly
  * what the output format would have written without framing.
  *
  * One deliberate exception: an output format that cannot represent totals and extremes in its plain
  * output and drops them (the `JSONCompactEachRow` family, where they would be indistinguishable from
  * ordinary rows) does emit them under framing, into the `totals` and `extremes` packets, because the
  * packet kind tells them apart. For such formats the concatenation of the `data` packets alone is
  * exactly the unframed output, and the `totals` and `extremes` packets carry additional rows that
  * the unframed output does not contain.
  *
  * Auxiliary packets (progress, logs, profile events, exceptions) are represented as JSON.
  *
  * The framing format is selected by the query setting `framing_output_format`. It applies to the
  * HTTP protocol, but may apply in other protocols as well in the future.
  *
  * Processing of multiple queries at once is out of scope of the first implementation, but the
  * design allows it: every packet can be extended with the information about the query index
  * along multiple queries.
  *
  * The methods are not thread-safe: `IOutputFormat` serializes the calls under its writing mutex.
  */
class IFramingFormat
{
public:
    IFramingFormat(WriteBuffer & out_, const FormatSettings & format_settings_);
    virtual ~IFramingFormat();

    virtual String getName() const = 0;

    /// The content type for the HTTP response.
    virtual String getContentType() const = 0;

    /// Whether this framing embeds the output payload as UTF-8 text (`EventStream`,
    /// `JSONEachPacketString`) rather than in a binary-safe way (`JSONEachPacketBase64`).
    /// Text framings can only be used with text output formats; binary output formats
    /// (such as `Native` or `RowBinary`) require a binary-safe framing.
    virtual bool requiresTextPayload() const = 0;

    /// The buffer where the output format writes formatted data.
    WriteBuffer & getPayloadBuffer() { return payload; }

    /// Called after the output format has written a portion of the given kind into the payload
    /// buffer. Wraps everything accumulated since the previous call into a packet
    /// (does nothing if the payload buffer is empty). Also pumps pending logs and profile events.
    void onPayload(FramedPacketKind kind);

    /// Called on query progress, possibly from another thread than `onPayload`
    /// (but the calls are serialized by IOutputFormat). Also pumps pending logs and profile events.
    void onProgress(const Progress & progress);

    /// Remember an exception to be written as the last packet on `finalize`.
    void setException(const String & message) { exception_message = message; }

    /// Remember the final progress (with the final counters: `result_rows`, `result_bytes`,
    /// `memory_usage`, known only after the query finished) to be written as the last `progress`
    /// packet on `finalize` - after the trailing logs and profile events emitted by the
    /// query-finish logging are drained, so that a successful stream really ends with it, as
    /// `docs/en/interfaces/framing-formats.md` documents. Writing it eagerly would order it
    /// before that trailing drain. The passed value is accumulated, so passing deltas is fine.
    void setFinalProgress(const Progress & progress);

    /// Write the remaining payload, pending logs and profile events, the final progress if any,
    /// and the exception if any, then flush the output. No more packets can be written after this call.
    void finalize();

    /// Server logs (as selected by the `send_logs_level` setting) will be written as packets.
    void setLogsQueue(const std::shared_ptr<InternalTextLogsQueue> & logs_queue_) { logs_queue = logs_queue_; }

    /// Profile events of the query will be written as packets, at most once in `period_us` microseconds.
    void setProfileEventsQueue(const InternalProfileEventsQueuePtr & queue, const String & host_name_, UInt64 period_us);

    /// Accessors for the log and profile-events queue wiring, so it can be carried over when a
    /// framing format is recreated for the buffered exception path (see `HTTPHandler`), keeping the
    /// `log` and `profile_events` packets collected during parsing and planning.
    const std::shared_ptr<InternalTextLogsQueue> & getLogsQueue() const { return logs_queue; }
    const InternalProfileEventsQueuePtr & getProfileEventsQueue() const { return profile_events_queue; }
    const String & getProfileEventsHostName() const { return host_name; }
    UInt64 getProfileEventsPeriodMicroseconds() const { return profile_events_period_us; }

protected:
    virtual void writePayloadPacket(FramedPacketKind kind, std::string_view data) = 0;
    virtual void writeProgressPacket(const Progress & progress) = 0;
    /// The block has the structure of `InternalTextLogsQueue::getSampleBlock`.
    virtual void writeLogsPacket(const Block & block) = 0;
    /// The block has the structure of `ProfileEvents::getSampleBlock` (see ProfileEventsExt.h).
    virtual void writeProfileEventsPacket(const Block & block) = 0;
    virtual void writeExceptionPacket(const String & message) = 0;
    virtual void finalizeImpl() {}

    static std::string_view getPacketKindName(FramedPacketKind kind);

    /// Writes `s` as a JSON string, replacing invalid UTF-8 sequences with the replacement character.
    /// Auxiliary packets (`log`, `profile_events`, `exception`) are always JSON, unlike the query result
    /// payload, which - depending on the framing format - may embed non-UTF-8 bytes verbatim (`EventStream`
    /// and `JSONEachPacketString` with a text output format) or byte-exactly (base64). Auxiliary packets have
    /// no such escape hatch, and some of their string fields (for example `query_id` in the `log` packet)
    /// can come from user input, so they must always be sanitized to keep the packet valid JSON.
    static void writeJSONStringValidUTF8(std::string_view s, WriteBuffer & buf, const FormatSettings & settings);

    /// Helpers to represent single entries of auxiliary packets as JSON objects.
    void writeLogRowJSON(const Block & block, size_t row_num, WriteBuffer & buf) const;
    void writeProfileEventRowJSON(const Block & block, size_t row_num, WriteBuffer & buf) const;

    WriteBuffer & out;
    const FormatSettings format_settings;

private:
    void extractAndWritePayload(FramedPacketKind kind);
    void pumpLogs();
    void pumpProfileEvents(bool force);
    /// Flush `out` down to the underlying buffer (including the nested compressed buffer, if any).
    void flushOut();

    /// If a previous packet write threw partway through (`writing` is still set), stop: mark the
    /// framing finalized so no further packets are produced, and return true so the caller writes
    /// nothing more. Returns false when it is safe to proceed.
    bool failClosedAfterPartialWrite();

    WriteBufferFromOwnString payload;

    std::shared_ptr<InternalTextLogsQueue> logs_queue;
    InternalProfileEventsQueuePtr profile_events_queue;
    String host_name;
    UInt64 profile_events_period_us = 0;
    Stopwatch profile_events_watch;
    ProfileEvents::ThreadIdToCountersSnapshot profile_events_snapshots;

    String exception_message;

    /// The final progress, deferred to `finalize` (see `setFinalProgress`).
    Progress final_progress;
    bool has_final_progress = false;

    bool finalized = false;

    /// Set while a packet is being written to `out`, and cleared once the write completes. The public
    /// methods (`onPayload`, `onProgress`, `finalize`) are serialized by `IOutputFormat` and never
    /// nested (see the class comment), so entering one with this flag already set means a previous write
    /// threw partway through - after some bytes may already have reached the socket. We then fail closed
    /// (see `failClosedAfterPartialWrite`): a half-written packet is on the wire and the exception
    /// recovery path retries `finalize`; re-emitting the buffered payload or the tail packets would
    /// append a well-formed-looking duplicate after the truncated packet and corrupt the stream. Failing
    /// closed lets the error terminate the already-broken stream instead.
    bool writing = false;
};

using FramingFormatPtr = std::shared_ptr<IFramingFormat>;

}
