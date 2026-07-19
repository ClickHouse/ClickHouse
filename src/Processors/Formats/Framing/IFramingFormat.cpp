#include <Processors/Formats/Framing/IFramingFormat.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Core/Block.h>
#include <Core/Defines.h>
#include <IO/Progress.h>
#include <IO/WriteBufferDecorator.h>
#include <IO/WriteBufferValidUTF8.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int FAULT_INJECTED;
}

namespace FailPoints
{
    extern const char framing_throw_after_writing_packet[];
}

IFramingFormat::IFramingFormat(WriteBuffer & out_, const FormatSettings & format_settings_)
    : out(out_), format_settings(format_settings_)
{
}

IFramingFormat::~IFramingFormat()
{
    if (!payload.isFinalized())
        payload.cancel();
}

std::string_view IFramingFormat::getPacketKindName(FramedPacketKind kind)
{
    switch (kind)
    {
        case FramedPacketKind::Data:
            return "data";
        case FramedPacketKind::Totals:
            return "totals";
        case FramedPacketKind::Extremes:
            return "extremes";
    }
}

void IFramingFormat::setProfileEventsQueue(const InternalProfileEventsQueuePtr & queue, const String & host_name_, UInt64 period_us)
{
    profile_events_queue = queue;
    host_name = host_name_;
    profile_events_period_us = period_us;
}

bool IFramingFormat::failClosedAfterPartialWrite()
{
    if (!writing)
        return false;

    /// A packet write is still marked in progress although we are being entered again: it must have
    /// thrown partway through, leaving a half-written packet on the wire. Do not write anything more -
    /// see the `writing` member for the full rationale.
    finalized = true;
    return true;
}

void IFramingFormat::onPayload(FramedPacketKind kind)
{
    if (finalized || failClosedAfterPartialWrite())
        return;

    writing = true;
    extractAndWritePayload(kind);
    pumpLogs();
    pumpProfileEvents(/*force=*/ false);
    flushOut();
    writing = false;
}

void IFramingFormat::onProgress(const Progress & progress)
{
    if (finalized || failClosedAfterPartialWrite())
        return;

    writing = true;
    writeProgressPacket(progress);
    pumpLogs();
    pumpProfileEvents(/*force=*/ false);
    flushOut();
    writing = false;
}

void IFramingFormat::finalize()
{
    if (finalized || failClosedAfterPartialWrite())
        return;

    writing = true;
    extractAndWritePayload(FramedPacketKind::Data);
    pumpLogs();
    pumpProfileEvents(/*force=*/ true);

    if (!exception_message.empty())
        writeExceptionPacket(exception_message);

    finalizeImpl();
    flushOut();

    payload.finalize();
    finalized = true;
    writing = false;
}

namespace
{

/// Flushes `buf` and, if it wraps another buffer (an HTTP compression layer such as gzip, and/or the
/// internal `compress=1` layer, which can be stacked), flushes the whole chain down to the underlying
/// HTTP buffer, so that packets are delivered interactively rather than sitting in a compression
/// buffer until enough data accumulates or the query finishes.
void flushBufferChain(WriteBuffer & buf)
{
    buf.next();
    if (auto * out_with_nested = dynamic_cast<WriteBufferWithOwnMemoryDecorator *>(&buf))
        flushBufferChain(*out_with_nested->getNestedBuffer());
    else if (auto * out_compressed = dynamic_cast<CompressedWriteBuffer *>(&buf))
        flushBufferChain(*out_compressed->getNestedBuffer());
}

}

void IFramingFormat::flushOut()
{
    flushBufferChain(out);
}

void IFramingFormat::extractAndWritePayload(FramedPacketKind kind)
{
    std::string & data = payload.str();
    if (!data.empty())
    {
        writePayloadPacket(kind, data);

        /// Test-only: emulate a packet write throwing after some bytes have reached `out` but before the
        /// payload buffer is cleared, to check that the exception recovery fails closed (the `writing`
        /// flag stays set, so the retried `finalize` becomes a no-op via `failClosedAfterPartialWrite`)
        /// instead of re-emitting this packet.
        fiu_do_on(FailPoints::framing_throw_after_writing_packet,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault after writing a framing packet");
        });
    }
    payload.restart(DBMS_DEFAULT_BUFFER_SIZE);
}

void IFramingFormat::pumpLogs()
{
    if (!logs_queue)
        return;

    MutableColumns logs_columns;
    MutableColumns curr_logs_columns;
    bool has_logs = false;

    while (logs_queue->tryPop(curr_logs_columns))
    {
        if (!has_logs)
        {
            logs_columns = std::move(curr_logs_columns);
            has_logs = true;
        }
        else
        {
            for (size_t j = 0; j < logs_columns.size(); ++j)
                logs_columns[j]->insertRangeFrom(*curr_logs_columns[j], 0, curr_logs_columns[j]->size());
        }
    }

    if (!has_logs || logs_columns.at(0)->empty())
        return;

    Block block = InternalTextLogsQueue::getSampleBlock();
    block.setColumns(std::move(logs_columns));
    writeLogsPacket(block);
}

void IFramingFormat::pumpProfileEvents(bool force)
{
    if (!profile_events_queue)
        return;

    if (!force && profile_events_watch.elapsedMicroseconds() < profile_events_period_us)
        return;

    /// Collecting profile events requires the current thread to be attached to the thread group of the query.
    if (!CurrentThread::isInitialized() || !CurrentThread::getGroup())
        return;

    Block block = ProfileEvents::getProfileEvents(host_name, profile_events_queue, profile_events_snapshots);
    if (block.rows() != 0)
        writeProfileEventsPacket(block);

    profile_events_watch.restart();
}

static void writeDateTimeWithMicrosecondsJSON(UInt32 datetime, UInt32 microseconds, WriteBuffer & buf)
{
    writeChar('"', buf);
    writeDateTimeText(datetime, buf);
    writeChar('.', buf);
    writeChar('0' + ((microseconds / 100000) % 10), buf);
    writeChar('0' + ((microseconds / 10000) % 10), buf);
    writeChar('0' + ((microseconds / 1000) % 10), buf);
    writeChar('0' + ((microseconds / 100) % 10), buf);
    writeChar('0' + ((microseconds / 10) % 10), buf);
    writeChar('0' + (microseconds % 10), buf);
    writeChar('"', buf);
}

void IFramingFormat::writeJSONStringValidUTF8(std::string_view s, WriteBuffer & buf, const FormatSettings & settings)
{
    WriteBufferValidUTF8 validating_buf(buf);
    writeJSONString(s, validating_buf, settings);
}

void IFramingFormat::writeLogRowJSON(const Block & block, size_t row_num, WriteBuffer & buf) const
{
    const auto & event_time = assert_cast<const ColumnUInt32 &>(*block.getByName("event_time").column).getData();
    const auto & event_time_microseconds = assert_cast<const ColumnUInt32 &>(*block.getByName("event_time_microseconds").column).getData();
    const auto & log_host_name = assert_cast<const ColumnString &>(*block.getByName("host_name").column);
    const auto & query_id = assert_cast<const ColumnString &>(*block.getByName("query_id").column);
    const auto & thread_id = assert_cast<const ColumnUInt64 &>(*block.getByName("thread_id").column).getData();
    const auto & priority = assert_cast<const ColumnInt8 &>(*block.getByName("priority").column).getData();
    const auto & source = assert_cast<const ColumnString &>(*block.getByName("source").column);
    const auto & text = assert_cast<const ColumnString &>(*block.getByName("text").column);

    writeCString("{\"event_time\":", buf);
    writeDateTimeWithMicrosecondsJSON(event_time[row_num], event_time_microseconds[row_num], buf);
    writeCString(",\"host_name\":", buf);
    writeJSONStringValidUTF8(log_host_name.getDataAt(row_num), buf, format_settings);
    writeCString(",\"query_id\":", buf);
    writeJSONStringValidUTF8(query_id.getDataAt(row_num), buf, format_settings);
    writeCString(",\"thread_id\":\"", buf);
    writeIntText(thread_id[row_num], buf);
    writeCString("\",\"priority\":\"", buf);
    writeString(InternalTextLogsQueue::getPriorityName(priority[row_num]), buf);
    writeCString("\",\"source\":", buf);
    writeJSONStringValidUTF8(source.getDataAt(row_num), buf, format_settings);
    writeCString(",\"text\":", buf);
    writeJSONStringValidUTF8(text.getDataAt(row_num), buf, format_settings);
    writeChar('}', buf);
}

void IFramingFormat::writeProfileEventRowJSON(const Block & block, size_t row_num, WriteBuffer & buf) const
{
    const auto & event_host_name = assert_cast<const ColumnString &>(*block.getByName("host_name").column);
    const auto & current_time = assert_cast<const ColumnUInt32 &>(*block.getByName("current_time").column).getData();
    const auto & thread_id = assert_cast<const ColumnUInt64 &>(*block.getByName("thread_id").column).getData();
    const auto & type = assert_cast<const ColumnInt8 &>(*block.getByName("type").column).getData();
    const auto & name = assert_cast<const ColumnString &>(*block.getByName("name").column);
    const auto & value = assert_cast<const ColumnInt64 &>(*block.getByName("value").column).getData();

    writeCString("{\"host_name\":", buf);
    writeJSONStringValidUTF8(event_host_name.getDataAt(row_num), buf, format_settings);
    writeCString(",\"current_time\":\"", buf);
    writeDateTimeText(current_time[row_num], buf);
    writeCString("\",\"thread_id\":\"", buf);
    writeIntText(thread_id[row_num], buf);
    writeCString("\",\"type\":\"", buf);
    writeCString(type[row_num] == ProfileEvents::Type::GAUGE ? "gauge" : "increment", buf);
    writeCString("\",\"name\":", buf);
    writeJSONStringValidUTF8(name.getDataAt(row_num), buf, format_settings);
    writeCString(",\"value\":\"", buf);
    writeIntText(value[row_num], buf);
    writeCString("\"}", buf);
}

}
