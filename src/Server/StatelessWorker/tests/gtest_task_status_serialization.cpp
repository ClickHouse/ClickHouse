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

/// Build a status carrying `num_log_rows` log lines in the standard InternalTextLogsQueue schema.
DistributedQueryTaskStatus makeStatus(size_t num_log_rows)
{
    DistributedQueryTaskStatus s;
    s.status = "Failed";
    s.error_message = "Code: 395. DB::Exception: boom on worker";
    s.progress.read_rows = 123;
    s.progress.read_bytes = 456;

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

/// Legacy version predates the logs field: it must never be written or read, even if logs are present.
TEST(TaskStatusSerialization, LegacyVersionIgnoresLogs)
{
    auto in = makeStatus(/*num_log_rows=*/5);
    auto out = roundTrip(in, DBMS_MIN_PROTOCOL_VERSION_WITH_SERVER_QUERY_TIME_IN_PROGRESS);

    EXPECT_EQ(out.status, in.status);
    EXPECT_EQ(out.error_message, in.error_message);
    EXPECT_EQ(out.progress.read_rows.load(), 123u);
    EXPECT_EQ(out.logs.rows(), 0u); /// logs dropped at legacy version
}

/// New version round-trips the logs block intact.
TEST(TaskStatusSerialization, NewVersionRoundTripsLogs)
{
    auto in = makeStatus(/*num_log_rows=*/5);
    auto out = roundTrip(in, DBMS_MIN_PROTOCOL_VERSION_WITH_DISTRIBUTED_TASK_LOGS);

    EXPECT_EQ(out.status, in.status);
    EXPECT_EQ(out.error_message, in.error_message);
    EXPECT_EQ(out.progress.read_rows.load(), 123u);
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
