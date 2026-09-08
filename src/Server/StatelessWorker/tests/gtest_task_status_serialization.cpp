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

/// Every progress field serialized by ProgressValues::write, set to a distinct value so a
/// missed or shifted field is caught. total_rows_to_read is non-zero so the writer's
/// low-version "approximate total_rows from total_bytes" branch is not taken.
void fillProgress(Progress & p)
{
    p.read_rows = 123;
    p.read_bytes = 456;
    p.total_rows_to_read = 789;
    p.total_bytes_to_read = 5000;   /// only serialized at >= DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS
    p.written_rows = 7;             /// only serialized at >= DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO
    p.written_bytes = 88;
    p.elapsed_ns = 999999;          /// only serialized at >= DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS
}

/// Assert every serialized progress field survived the round trip.
void expectProgressEq(const Progress & out, const Progress & in)
{
    EXPECT_EQ(out.read_rows.load(), in.read_rows.load());
    EXPECT_EQ(out.read_bytes.load(), in.read_bytes.load());
    EXPECT_EQ(out.total_rows_to_read.load(), in.total_rows_to_read.load());
    EXPECT_EQ(out.total_bytes_to_read.load(), in.total_bytes_to_read.load());
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

DistributedQueryTaskStatus roundTrip(const DistributedQueryTaskStatus & in, UInt64 version)
{
    WriteBufferFromOwnString wb;
    in.write(wb, version);
    wb.finalize();

    DistributedQueryTaskStatus out;
    ReadBufferFromString rb(wb.str());
    out.read(rb, version);
    /// The load-bearing check: a write/read asymmetry (a byte written under a version gate but not
    /// read, or vice versa) leaves the buffer partly consumed. This is the exact desync class that a
    /// mixed-version coordinator/worker pair would hit on the wire.
    EXPECT_TRUE(rb.eof()) << "reader did not consume the whole buffer at version " << version;
    return out;
}

}

/// Legacy version predates the logs field (dropped) and the total_bytes_to_read progress field
/// (dropped); every other progress field is serialized at 54460 and must survive.
TEST(TaskStatusSerialization, LegacyVersionIgnoresLogs)
{
    auto in = makeStatus(/*num_log_rows=*/5);
    auto out = roundTrip(in, DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS);

    EXPECT_EQ(out.status, in.status);
    EXPECT_EQ(out.error_message, in.error_message);
    EXPECT_EQ(out.logs.rows(), 0u); /// logs field does not exist below DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_TASK_LOGS

    /// Fields serialized at the legacy version round-trip exactly...
    EXPECT_EQ(out.progress.read_rows.load(), in.progress.read_rows.load());
    EXPECT_EQ(out.progress.read_bytes.load(), in.progress.read_bytes.load());
    EXPECT_EQ(out.progress.total_rows_to_read.load(), in.progress.total_rows_to_read.load());
    EXPECT_EQ(out.progress.written_rows.load(), in.progress.written_rows.load());
    EXPECT_EQ(out.progress.written_bytes.load(), in.progress.written_bytes.load());
    EXPECT_EQ(out.progress.elapsed_ns.load(), in.progress.elapsed_ns.load());

    /// total_bytes_to_read is gated at DBMS_MIN_PROTOCOL_VERSION_WITH_TOTAL_BYTES_IN_PROGRESS (54463),
    /// hence it is not carried and stays default.
    EXPECT_EQ(out.progress.total_bytes_to_read.load(), 0u);
}

/// New version round-trips the logs block and every progress field intact.
TEST(TaskStatusSerialization, NewVersionRoundTripsLogs)
{
    auto in = makeStatus(/*num_log_rows=*/5);
    auto out = roundTrip(in, DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_TASK_LOGS);

    EXPECT_EQ(out.status, in.status);
    EXPECT_EQ(out.error_message, in.error_message);
    expectProgressEq(out.progress, in.progress); /// all fields survive at the new version
    ASSERT_EQ(out.logs.rows(), 5u);
    EXPECT_EQ(out.logs.getByName("text").column->getDataAt(4), std::string_view("some log line"));
    EXPECT_EQ(out.logs.getByName("query_id").column->getDataAt(0), std::string_view("q::stage_0_0"));
}

/// The "no logs" case at the new version: has_logs=false, nothing else emitted, still symmetric.
TEST(TaskStatusSerialization, NewVersionEmptyLogs)
{
    auto in = makeStatus(/*num_log_rows=*/0);
    auto out = roundTrip(in, DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_TASK_LOGS);

    EXPECT_EQ(out.status, in.status);
    EXPECT_EQ(out.logs.rows(), 0u);
}

/// The version the worker serializes a status with, negotiated from the coordinator's
/// `task_status_version` request parameter. Extracted from the get_status endpoint so it can be
/// tested without an HTTPServerResponse (which needs a live socket session).
TEST(TaskStatusNegotiation, ClampAndLegacyDefault)
{
    /// Old coordinator sends nothing -> legacy format it can read.
    EXPECT_EQ(negotiateTaskStatusVersion(std::nullopt),
              DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS);

    /// Same version on both sides -> used as-is.
    EXPECT_EQ(negotiateTaskStatusVersion(DBMS_TCP_PROTOCOL_VERSION),
              DBMS_TCP_PROTOCOL_VERSION);

    /// The logs feature version round-trips (regression guard on the specific constant).
    EXPECT_EQ(negotiateTaskStatusVersion(DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_TASK_LOGS),
              DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_TASK_LOGS);

    /// Newer coordinator than this worker -> clamp down to what the worker can serialize.
    EXPECT_EQ(negotiateTaskStatusVersion(DBMS_TCP_PROTOCOL_VERSION + 100),
              DBMS_TCP_PROTOCOL_VERSION);
}
