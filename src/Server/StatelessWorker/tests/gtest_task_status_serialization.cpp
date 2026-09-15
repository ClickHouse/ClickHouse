#include <gtest/gtest.h>

#include <Server/StatelessWorker/StatelessWorkerProtocol.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <Core/ProtocolDefines.h>
#include <Core/Field.h>
#include <Core/Block.h>
#include <Columns/IColumn.h>

using namespace DB;

namespace
{

/// Every progress field that can survive the status round trip. total_bytes_to_read is gated at
/// DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS (54463), so whether it survives depends on
/// the negotiated progress version.
void fillProgress(Progress & p)
{
    p.read_rows = 123;
    p.read_bytes = 456;
    p.total_rows_to_read = 789;
    p.total_bytes_to_read = 5000;   /// carried only at progress version >= DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS
    p.written_rows = 7;
    p.written_bytes = 88;
    p.elapsed_ns = 999999;
}

/// Assert every progress field carried by the status round trip survived (total_bytes_to_read is
/// checked separately by each test since it is intentionally not carried).
void expectProgressCarriedEq(const Progress & out, const Progress & in)
{
    EXPECT_EQ(out.read_rows.load(), in.read_rows.load());
    EXPECT_EQ(out.read_bytes.load(), in.read_bytes.load());
    EXPECT_EQ(out.total_rows_to_read.load(), in.total_rows_to_read.load());
    EXPECT_EQ(out.written_rows.load(), in.written_rows.load());
    EXPECT_EQ(out.written_bytes.load(), in.written_bytes.load());
    EXPECT_EQ(out.elapsed_ns.load(), in.elapsed_ns.load());
}

/// Build a status carrying `num_log_rows` log lines in the standard InternalTextLogsQueue schema.
DistributedQueryTaskStatus makeStatus(size_t num_log_rows)
{
    DistributedQueryTaskStatus s;
    s.status = "Failed";
    s.error_message = "Code: 395. DB::Exception: boom on worker";
    s.error_code = 395;
    s.num_dropped_logs = 17;
    s.forwarded_log_count = 250;
    fillProgress(s.progress);

    if (num_log_rows > 0)
    {
        MutableColumns cols = InternalTextLogsQueue::getSampleColumns();
        for (size_t row = 0; row < num_log_rows; ++row)
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
        s.logs = std::move(b);
    }
    return s;
}

/// task status version that introduced forwarded worker logs.
constexpr UInt64 TASK_STATUS_VERSION_WITH_LOGS = DBMS_MIN_DISTRIBUTED_TASK_SERIALIZATION_VERSION_WITH_LOGS;
/// task status version spoken by a coordinator that predates log forwarding.
constexpr UInt64 TASK_STATUS_VERSION_LEGACY = DBMS_DISTRIBUTED_TASK_SERIALIZATION_VERSION_WITHOUT_LOGS;

DistributedQueryTaskStatus roundTrip(const DistributedQueryTaskStatus & in, UInt64 task_version, UInt64 progress_version)
{
    WriteBufferFromOwnString wb;
    in.write(wb, task_version, progress_version);
    wb.finalize();

    DistributedQueryTaskStatus out;
    ReadBufferFromString rb(wb.str());
    out.read(rb, task_version, progress_version);
    /// A write/read asymmetry (a byte written under a version gate but not read, or vice versa)
    /// leaves the buffer partly consumed - the exact desync a mixed-version pair would hit.
    EXPECT_TRUE(rb.eof()) << "reader did not consume the whole buffer at task version " << task_version
                          << ", progress version " << progress_version;
    return out;
}

}

/// Version 3 (legacy, pre-logs): status/error/error_code/progress survive, no logs field on the wire.
TEST(TaskStatusSerialization, Version3NoLogs)
{
    auto in = makeStatus(/*num_log_rows=*/5);
    auto out = roundTrip(in, TASK_STATUS_VERSION_LEGACY, DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS);

    EXPECT_EQ(out.status, in.status);
    EXPECT_EQ(out.error_message, in.error_message);
    EXPECT_EQ(out.error_code, in.error_code);
    EXPECT_EQ(out.logs.rows(), 0u); /// logs field does not exist below task status version 4
    EXPECT_EQ(out.num_dropped_logs, 0u); /// loss counters are not on the wire below version 4
    EXPECT_EQ(out.forwarded_log_count, 0u);
    expectProgressCarriedEq(out.progress, in.progress);
}

/// Version 4 round-trips the logs block plus every carried status field.
TEST(TaskStatusSerialization, Version4RoundTripsLogs)
{
    auto in = makeStatus(/*num_log_rows=*/5);
    auto out = roundTrip(in, TASK_STATUS_VERSION_WITH_LOGS, DBMS_TCP_PROTOCOL_VERSION);

    EXPECT_EQ(out.status, in.status);
    EXPECT_EQ(out.error_message, in.error_message);
    EXPECT_EQ(out.error_code, in.error_code);
    expectProgressCarriedEq(out.progress, in.progress);
    ASSERT_EQ(out.logs.rows(), 5u);
    EXPECT_EQ(out.logs.getByName("text").column->getDataAt(4), std::string_view("some log line"));
    EXPECT_EQ(out.logs.getByName("query_id").column->getDataAt(0), std::string_view("q::stage_0_0"));
    EXPECT_EQ(out.num_dropped_logs, in.num_dropped_logs);
    EXPECT_EQ(out.forwarded_log_count, in.forwarded_log_count);
}

/// The "no logs" case at version 4: has_logs=false, nothing else emitted, still symmetric.
TEST(TaskStatusSerialization, Version4EmptyLogs)
{
    auto in = makeStatus(/*num_log_rows=*/0);
    auto out = roundTrip(in, TASK_STATUS_VERSION_WITH_LOGS, DBMS_TCP_PROTOCOL_VERSION);

    EXPECT_EQ(out.status, in.status);
    EXPECT_EQ(out.logs.rows(), 0u);
    /// The loss counters ride outside the has_logs branch, so they survive even with no logs.
    EXPECT_EQ(out.num_dropped_logs, in.num_dropped_logs);
    EXPECT_EQ(out.forwarded_log_count, in.forwarded_log_count);
}

/// The progress version gates total_bytes_to_read independently of the task status version: it is
/// carried at DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS and above, dropped below it.
TEST(TaskStatusSerialization, ProgressVersionGatesTotalBytes)
{
    auto in = makeStatus(/*num_log_rows=*/0);

    auto old_progress = roundTrip(in, TASK_STATUS_VERSION_WITH_LOGS, DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS);
    expectProgressCarriedEq(old_progress.progress, in.progress);
    EXPECT_EQ(old_progress.progress.total_bytes_to_read.load(), 0u); /// below the 54463 gate

    auto new_progress = roundTrip(in, TASK_STATUS_VERSION_WITH_LOGS, DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS);
    expectProgressCarriedEq(new_progress.progress, in.progress);
    EXPECT_EQ(new_progress.progress.total_bytes_to_read.load(), in.progress.total_bytes_to_read.load());
}

/// The task status version the worker serializes the status with, negotiated from the
/// coordinator's `task_status_version` request parameter.
TEST(TaskStatusNegotiation, ClampAndLegacyDefault)
{
    /// Old coordinator sends nothing -> legacy task status version 3 (no logs).
    EXPECT_EQ(negotiateTaskStatusVersion(std::nullopt), TASK_STATUS_VERSION_LEGACY);

    /// Same version on both sides -> used as-is.
    EXPECT_EQ(negotiateTaskStatusVersion(DBMS_DISTRIBUTED_TASK_SERIALIZATION_VERSION),
              DBMS_DISTRIBUTED_TASK_SERIALIZATION_VERSION);

    /// The only case that exercises the clamp: a newer coordinator must be pinned to what this
    /// worker can serialize.
    EXPECT_EQ(negotiateTaskStatusVersion(DBMS_DISTRIBUTED_TASK_SERIALIZATION_VERSION + 100),
              DBMS_DISTRIBUTED_TASK_SERIALIZATION_VERSION);
}

/// The native progress revision the worker serializes progress/logs with, negotiated from the
/// coordinator's `progress_version` request parameter.
TEST(TaskStatusNegotiation, ProgressVersionClampAndLegacyDefault)
{
    /// Old coordinator sends nothing -> the revision master serializes status progress at.
    EXPECT_EQ(negotiateProgressVersion(std::nullopt), DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS);

    /// Same version on both sides -> used as-is.
    EXPECT_EQ(negotiateProgressVersion(DBMS_TCP_PROTOCOL_VERSION), DBMS_TCP_PROTOCOL_VERSION);

    /// A newer coordinator must be pinned to what this worker can serialize.
    EXPECT_EQ(negotiateProgressVersion(DBMS_TCP_PROTOCOL_VERSION + 100), DBMS_TCP_PROTOCOL_VERSION);
}
