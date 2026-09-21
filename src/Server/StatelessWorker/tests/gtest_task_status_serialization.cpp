#include <gtest/gtest.h>

#include <Server/StatelessWorker/StatelessWorkerProtocol.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Formats/NativeWriter.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Core/ProtocolDefines.h>
#include <Core/Field.h>
#include <Core/Block.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnsNumber.h>

using namespace DB;

namespace
{

void fillProgress(Progress & p)
{
    p.read_rows = 123;
    p.read_bytes = 456;
    p.total_rows_to_read = 789;
    p.written_rows = 7;
    p.written_bytes = 88;
    p.elapsed_ns = 999999;
}

void expectProgressEq(const Progress & out, const Progress & in)
{
    EXPECT_EQ(out.read_rows.load(), in.read_rows.load());
    EXPECT_EQ(out.read_bytes.load(), in.read_bytes.load());
    EXPECT_EQ(out.total_rows_to_read.load(), in.total_rows_to_read.load());
    EXPECT_EQ(out.written_rows.load(), in.written_rows.load());
    EXPECT_EQ(out.written_bytes.load(), in.written_bytes.load());
    EXPECT_EQ(out.elapsed_ns.load(), in.elapsed_ns.load());
}

Block makeLogRows(size_t num_rows)
{
    MutableColumns cols = InternalTextLogsQueue::getSampleColumns();
    for (size_t row = 0; row < num_rows; ++row)
    {
        size_t c = 0;
        cols[c++]->insert(Field(UInt64(1000 + row)));      // event_time
        cols[c++]->insert(Field(UInt64(2000 + row)));      // event_time_microseconds
        cols[c++]->insert(Field(String("worker-host")));   // host_name
        cols[c++]->insert(Field(String("q::stage_0_0")));  // query_id
        cols[c++]->insert(Field(UInt64(42)));              // thread_id
        cols[c++]->insert(Field(Int64(3)));                // priority
        cols[c++]->insert(Field(String("Executor")));      // source
        cols[c++]->insert(Field(String("some log line"))); // text
    }
    Block b = InternalTextLogsQueue::getSampleBlock();
    b.setColumns(std::move(cols));
    return b;
}

DistributedQueryTaskStatus makeStatus()
{
    DistributedQueryTaskStatus s;
    s.status = "Failed";
    s.error_message = "Code: 395. DB::Exception: boom on worker";
    s.error_code = 395;
    fillProgress(s.progress);
    return s;
}

/// The reply body exactly as a worker from before log forwarding writes it, and exactly as a
/// coordinator from before log forwarding reads it. Pinned here so a change to the fixed body
/// shows up as a test failure.
void writeFixedBodyAsOldWorker(const DistributedQueryTaskStatus & s, WriteBuffer & out)
{
    writeStringBinary(s.status, out);
    writeStringBinary(s.error_message, out);
    s.progress.write(out, DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS);
    writeIntBinary(s.error_code, out);
}

void readFixedBodyAsOldCoordinator(DistributedQueryTaskStatus & s, ReadBuffer & in)
{
    readStringBinary(s.status, in);
    readStringBinary(s.error_message, in);
    s.progress.read(in, DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS);
    readIntBinary(s.error_code, in);
}

String serialize(const DistributedQueryTaskStatus & s, const TaskCollectors & collectors)
{
    WriteBufferFromOwnString wb;
    s.write(wb, collectors);
    wb.finalize();
    return wb.str();
}

DistributedQueryTaskStatus deserialize(const String & bytes)
{
    ReadBufferFromString rb(bytes);
    DistributedQueryTaskStatus out;
    out.read(rb);
    EXPECT_TRUE(rb.eof()) << "reader left bytes unread";
    return out;
}

/// One-row block of UInt64 columns, used to hand-craft meta blocks of other schemas.
Block makeMetaBlock(const std::vector<std::pair<String, UInt64>> & fields)
{
    Block b;
    for (const auto & [name, value] : fields)
    {
        auto col = ColumnUInt64::create();
        col->insertValue(value);
        b.insert({std::move(col), std::make_shared<DataTypeUInt64>(), name});
    }
    return b;
}

void writeNative(WriteBuffer & out, const Block & block)
{
    NativeWriter writer(out, STATELESS_WORKER_PAYLOAD_NATIVE_REVISION, std::make_shared<const Block>(block.cloneEmpty()));
    writer.write(block);
}

}

TEST(TaskCollectors, ParseAndFormat)
{
    EXPECT_FALSE(TaskCollectors::parse("").any());
    EXPECT_TRUE(TaskCollectors::parse("logs").logs);
    /// Unknown collector names are what a newer coordinator sends; an older worker ignores them.
    auto mixed = TaskCollectors::parse("logs,profile_events");
    EXPECT_TRUE(mixed.logs);
    EXPECT_TRUE(mixed.any());
    EXPECT_FALSE(TaskCollectors::parse("profile_events").any());

    TaskCollectors c;
    EXPECT_EQ(c.toString(), "");
    c.logs = true;
    EXPECT_EQ(c.toString(), "logs");
}

/// A worker that was not asked to collect anything writes the fixed body and nothing else, byte for
/// byte what a pre-logs worker writes. A pre-logs coordinator can read it.
TEST(TaskStatusSerialization, NoCollectorsWritesOnlyTheFixedBody)
{
    auto in = makeStatus();
    String bytes = serialize(in, TaskCollectors{});

    WriteBufferFromOwnString expected;
    writeFixedBodyAsOldWorker(in, expected);
    expected.finalize();
    EXPECT_EQ(bytes, expected.str());

    ReadBufferFromString rb(bytes);
    DistributedQueryTaskStatus out;
    readFixedBodyAsOldCoordinator(out, rb);
    EXPECT_TRUE(rb.eof());
    EXPECT_EQ(out.status, in.status);
    EXPECT_EQ(out.error_code, in.error_code);
}

/// A pre-logs worker never appends payloads. The new reader sees the body end and reports no logs.
TEST(TaskStatusSerialization, OldWorkerReplyReadsAsNoLogs)
{
    auto in = makeStatus();
    WriteBufferFromOwnString wb;
    writeFixedBodyAsOldWorker(in, wb);
    wb.finalize();

    auto out = deserialize(wb.str());
    EXPECT_EQ(out.status, in.status);
    EXPECT_EQ(out.error_message, in.error_message);
    EXPECT_EQ(out.error_code, in.error_code);
    expectProgressEq(out.progress, in.progress);
    EXPECT_FALSE(out.logs.has_value());
}

TEST(TaskStatusSerialization, LogsPayloadRoundTrip)
{
    auto in = makeStatus();
    in.logs = TaskLogsPayload{.begin_offset = 250, .dropped_total = 17, .rows = makeLogRows(5)};
    TaskCollectors collectors;
    collectors.logs = true;

    auto out = deserialize(serialize(in, collectors));
    EXPECT_EQ(out.status, in.status);
    EXPECT_EQ(out.error_code, in.error_code);
    expectProgressEq(out.progress, in.progress);
    ASSERT_TRUE(out.logs.has_value());
    EXPECT_EQ(out.logs->begin_offset, 250u);
    EXPECT_EQ(out.logs->dropped_total, 17u);
    ASSERT_EQ(out.logs->rows.rows(), 5u);
    EXPECT_EQ(out.logs->rows.getByName("text").column->getDataAt(4), std::string_view("some log line"));
    EXPECT_EQ(out.logs->rows.getByName("query_id").column->getDataAt(0), std::string_view("q::stage_0_0"));
}

/// While logs are collected the payload is on every reply, also when the worker has nothing to send.
TEST(TaskStatusSerialization, EmptyLogsPayloadIsStillPresent)
{
    auto in = makeStatus();
    TaskCollectors collectors;
    collectors.logs = true;

    auto out = deserialize(serialize(in, collectors));
    ASSERT_TRUE(out.logs.has_value());
    EXPECT_EQ(out.logs->begin_offset, 0u);
    EXPECT_EQ(out.logs->dropped_total, 0u);
    EXPECT_EQ(out.logs->rows.rows(), 0u);
}

/// A payload tag this reader does not know is skipped by its length; what follows is still read.
TEST(TaskStatusSerialization, UnknownTagIsSkipped)
{
    auto in = makeStatus();
    in.logs = TaskLogsPayload{.begin_offset = 3, .dropped_total = 0, .rows = makeLogRows(2)};

    WriteBufferFromOwnString wb;
    writeFixedBodyAsOldWorker(in, wb);
    writeTaskStatusPayloadFrame(wb, /*tag=*/ 77, [](WriteBuffer & payload) { writeString("junk!", payload); });
    writeTaskStatusPayloadFrame(wb, TASK_STATUS_PAYLOAD_LOGS, [&](WriteBuffer & payload) { writeTaskLogsPayload(*in.logs, payload); });
    writeVarUInt(TASK_STATUS_PAYLOAD_END, wb);
    wb.finalize();

    auto out = deserialize(wb.str());
    ASSERT_TRUE(out.logs.has_value());
    EXPECT_EQ(out.logs->begin_offset, 3u);
    EXPECT_EQ(out.logs->rows.rows(), 2u);
}

/// A newer worker may add columns to the meta block. This reader takes the ones it knows by name.
TEST(TaskStatusSerialization, MetaBlockWithExtraColumnIsRead)
{
    auto in = makeStatus();
    WriteBufferFromOwnString wb;
    writeFixedBodyAsOldWorker(in, wb);
    writeTaskStatusPayloadFrame(wb, TASK_STATUS_PAYLOAD_LOGS, [](WriteBuffer & payload)
    {
        writeNative(payload, makeMetaBlock({{"begin_offset", 11}, {"dropped_total", 5}, {"bytes_dropped", 9999}}));
        writeNative(payload, makeLogRows(1));
    });
    writeVarUInt(TASK_STATUS_PAYLOAD_END, wb);
    wb.finalize();

    auto out = deserialize(wb.str());
    ASSERT_TRUE(out.logs.has_value());
    EXPECT_EQ(out.logs->begin_offset, 11u);
    EXPECT_EQ(out.logs->dropped_total, 5u);
    EXPECT_EQ(out.logs->rows.rows(), 1u);
}

/// An older worker may lack columns this reader knows. They read as their defaults.
TEST(TaskStatusSerialization, MetaBlockWithMissingColumnDefaults)
{
    auto in = makeStatus();
    WriteBufferFromOwnString wb;
    writeFixedBodyAsOldWorker(in, wb);
    writeTaskStatusPayloadFrame(wb, TASK_STATUS_PAYLOAD_LOGS, [](WriteBuffer & payload)
    {
        writeNative(payload, makeMetaBlock({{"begin_offset", 11}}));
        writeNative(payload, makeLogRows(0));
    });
    writeVarUInt(TASK_STATUS_PAYLOAD_END, wb);
    wb.finalize();

    auto out = deserialize(wb.str());
    ASSERT_TRUE(out.logs.has_value());
    EXPECT_EQ(out.logs->begin_offset, 11u);
    EXPECT_EQ(out.logs->dropped_total, 0u);
    EXPECT_EQ(out.logs->rows.rows(), 0u);
}

/// Bytes after the fields this reader knows inside a payload are skipped, so a payload can grow.
TEST(TaskStatusSerialization, TrailingBytesInsidePayloadAreSkipped)
{
    auto in = makeStatus();
    WriteBufferFromOwnString wb;
    writeFixedBodyAsOldWorker(in, wb);
    writeTaskStatusPayloadFrame(wb, TASK_STATUS_PAYLOAD_LOGS, [](WriteBuffer & payload)
    {
        writeNative(payload, makeMetaBlock({{"begin_offset", 1}, {"dropped_total", 2}}));
        writeNative(payload, makeLogRows(1));
        writeString("future third block", payload);
    });
    writeVarUInt(TASK_STATUS_PAYLOAD_END, wb);
    wb.finalize();

    auto out = deserialize(wb.str());
    ASSERT_TRUE(out.logs.has_value());
    EXPECT_EQ(out.logs->begin_offset, 1u);
    EXPECT_EQ(out.logs->rows.rows(), 1u);
}
