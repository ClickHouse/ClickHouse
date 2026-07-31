#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/IAST.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/ISource.h>
#include <Processors/Sinks/NullSink.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <QueryPipeline/SizeLimits.h>
#include <QueryPipeline/StreamLocalLimits.h>
#include <Common/Scheduler/MemoryReservation.h>
#include <Common/tests/gtest_global_context.h>

using namespace DB;

namespace
{

/// A source shaped like `RemoteSource` around finalization: the progress of the last packets
/// is not visible while the executor's worker threads run, and becomes visible only to the
/// extra poll that `PipelineExecutor::finalizeExecution` makes after the threads are joined -
/// like trailing `Progress` packets drained from a remote connection after an early `LIMIT`
/// cancellation.
class LateProgressSource : public ISource
{
public:
    LateProgressSource(SharedHeader header, UInt64 late_rows_, StorageLimitsList limits_)
        : ISource(std::move(header), /*enable_auto_progress=*/ false)
        , late_rows(late_rows_)
        , limits(std::move(limits_))
    {
    }

    String getName() const override { return "LateProgressSource"; }

    void work() override
    {
        ++work_calls;
        ISource::work();
    }

    std::optional<ReadProgress> getReadProgress() override
    {
        /// During execution `ExecutionThreadContext::executeJob` polls the progress of a source
        /// node exactly once right after each `work` call, so while the worker threads run the
        /// polls stay paired with the `work` calls. The first unpaired poll is the finalize-time
        /// replay in `PipelineExecutor::finalizeExecution`; report the late progress there.
        ++progress_polls;
        if (progress_polls <= work_calls)
            return std::nullopt;

        if (late_progress_reported)
            return std::nullopt;
        late_progress_reported = true;

        ReadProgressCounters counters;
        counters.read_rows = late_rows;
        counters.read_bytes = late_rows * 8;
        return ReadProgress{counters, limits};
    }

protected:
    Chunk generate() override
    {
        if (produced)
            return {};
        produced = true;
        auto column = ColumnUInt64::create();
        column->insertValue(42);
        return Chunk(Columns{std::move(column)}, 1);
    }

private:
    const UInt64 late_rows;
    const StorageLimitsList limits;
    size_t work_calls = 0;
    size_t progress_polls = 0;
    bool produced = false;
    bool late_progress_reported = false;
};

QueryStatusPtr makeQueryStatus(const String & query_id)
{
    ClientInfo client_info;
    client_info.current_query_id = query_id;
    Settings settings;
    return std::make_shared<QueryStatus>(
        getContext().context,
        "SELECT 1",
        /*normalized_query_hash_*/ 0,
        client_info,
        /*priority_handle_*/ QueryPriorities::Handle{},
        /*query_slot_*/ nullptr,
        /*memory_reservation_*/ nullptr,
        /*thread_group_*/ nullptr,
        IAST::QueryKind::Select,
        settings,
        /*watch_start_nanoseconds*/ 0,
        /*is_internal*/ false);
}

struct ReplayRunResult
{
    bool source_cancelled = false;
    UInt64 delivered_rows = 0;
};

/// Runs a pipeline whose source reports `late_rows` of progress only at finalization time,
/// with `max_rows_to_read = max_rows` and `read_overflow_mode = 'break'`.
ReplayRunResult runPipelineWithLateProgress(UInt64 late_rows, UInt64 max_rows, const String & query_id)
{
    StorageLimits storage_limits;
    storage_limits.local_limits.mode = LimitsMode::LIMITS_TOTAL;
    storage_limits.local_limits.size_limits = SizeLimits(max_rows, /*max_bytes_*/ 0, OverflowMode::BREAK);

    SharedHeader header = std::make_shared<Block>(
        Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "x")});

    auto source = std::make_shared<LateProgressSource>(std::move(header), late_rows, StorageLimitsList{storage_limits});
    auto sink = std::make_shared<NullSink>(source->getPort().getSharedHeader());
    connect(source->getPort(), sink->getPort());

    auto processors = std::make_shared<Processors>();
    processors->emplace_back(source);
    processors->emplace_back(std::move(sink));

    ReplayRunResult result;

    auto callback = std::make_unique<ReadProgressCallback>();
    callback->setProgressCallback([&](const Progress & progress) { result.delivered_rows += progress.read_rows; });
    /// The `max_rows_to_read` check in `ReadProgressCallback::onProgress` reads the accumulated
    /// progress from the process list element, so the limit is only enforced when one is set.
    callback->setProcessListElement(makeQueryStatus(query_id));

    QueryStatusPtr element;
    PipelineExecutor executor(processors, element);
    executor.setReadProgressCallback(std::move(callback));
    executor.execute(/*num_threads*/ 1, /*concurrency_control*/ false);

    result.source_cancelled = source->isCancelled();
    return result;
}

}

/// The finalize-time replay of drained progress must honour the stop contract of
/// `ReadProgressCallback::onProgress`: when the replayed rows cross a limit with
/// `overflow_mode = 'break'`, the callback returns false and the processor has to be
/// cancelled, exactly as `ExecutionThreadContext::executeJob` does on the regular path.
TEST(PipelineExecutor, FinalizeProgressReplayHonoursBreakOverflowMode)
{
    auto result = runPipelineWithLateProgress(/*late_rows*/ 1000, /*max_rows*/ 100, "gtest_finalize_replay_break");

    /// The drained statistics must not be lost: the progress callback runs before the limit check.
    EXPECT_EQ(result.delivered_rows, 1000);
    /// This is the assertion that depends on honouring the `false` result of `onProgress`:
    /// without the `cancel` call in `PipelineExecutor::finalizeExecution` the source stays
    /// uncancelled after the replay crosses the limit.
    EXPECT_TRUE(result.source_cancelled);
}

TEST(PipelineExecutor, FinalizeProgressReplayBelowLimitDoesNotCancel)
{
    auto result = runPipelineWithLateProgress(/*late_rows*/ 1000, /*max_rows*/ 1000000, "gtest_finalize_replay_no_break");

    EXPECT_EQ(result.delivered_rows, 1000);
    EXPECT_FALSE(result.source_cancelled);
}
