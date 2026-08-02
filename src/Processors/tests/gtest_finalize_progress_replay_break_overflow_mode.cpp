#include <gtest/gtest.h>

#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Sinks/NullSink.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <QueryPipeline/StreamLocalLimits.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/Stopwatch.h>

using namespace DB;

namespace
{

/// Reports read progress once, and only while the pipeline is being finalized.
///
/// `ExecutionThreadContext::executeJob` polls `getReadProgress` for source nodes only, while the
/// second pass of `PipelineExecutor::finalizeExecution` polls every node of the graph. A processor
/// with a connected input port is therefore asked for its progress exclusively by the finalization
/// pass, which is exactly the situation of a `RemoteSource` whose trailing `Progress` packet is
/// delivered only by the drain that happens after the pipeline has already completed.
class TrailingProgressProbe : public ISimpleTransform
{
public:
    TrailingProgressProbe(SharedHeader header, StorageLimitsList limits_)
        : ISimpleTransform(header, header, false)
        , limits(std::move(limits_))
    {
    }

    String getName() const override { return "TrailingProgressProbe"; }

    void transform(Chunk &) override { }

    std::optional<ReadProgress> getReadProgress() override
    {
        if (std::exchange(progress_reported, true))
            return std::nullopt;

        ReadProgressCounters counters;
        counters.read_rows = 1000;
        counters.read_bytes = 8000;
        return ReadProgress{counters, limits};
    }

private:
    StorageLimitsList limits;
    bool progress_reported = false;
};

/// Runs `source -> probe -> sink` to completion with `limits` attached to the trailing progress of
/// the probe, and reports whether the finalization pass cancelled the probe.
bool probeIsCancelledAfterExecution(StorageLimitsList limits)
{
    SharedHeader header = std::make_shared<Block>(
        Block{ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "x")});

    Columns columns;
    columns.emplace_back(ColumnUInt64::create(1, 1));
    Chunk chunk(std::move(columns), 1);

    auto source = std::make_shared<SourceFromSingleChunk>(header, std::move(chunk));
    auto probe = std::make_shared<TrailingProgressProbe>(header, std::move(limits));
    auto sink = std::make_shared<NullSink>(header);

    connect(source->getPort(), probe->getInputPort());
    connect(probe->getOutputPort(), sink->getPort());

    auto processors = std::make_shared<Processors>();
    processors->emplace_back(source);
    processors->emplace_back(probe);
    processors->emplace_back(sink);

    auto read_progress_callback = std::make_unique<ReadProgressCallback>();

    /// `ReadProgressCallback` measures the elapsed time from its own construction with
    /// `CLOCK_MONOTONIC_COARSE`, so wait for that clock to advance past the `max_execution_time`
    /// used by the test below. Spinning (rather than sleeping) keeps the wait bounded by the clock
    /// granularity itself instead of by a guessed duration.
    Stopwatch coarse_watch(CLOCK_MONOTONIC_COARSE);
    while (coarse_watch.elapsedMilliseconds() < 10)
        continue;

    QueryStatusPtr element;
    PipelineExecutor executor(processors, element);
    executor.setReadProgressCallback(std::move(read_progress_callback));
    executor.execute(1, false);

    return probe->isCancelled();
}

StorageLimitsList makeTimeLimits(Poco::Timespan max_execution_time, OverflowMode timeout_overflow_mode)
{
    StorageLimits limits;
    limits.local_limits.speed_limits.max_execution_time = max_execution_time;
    limits.local_limits.timeout_overflow_mode = timeout_overflow_mode;

    StorageLimitsList list;
    list.emplace_back(limits);
    return list;
}

}

/// `ReadProgressCallback::onProgress` returns false when a limit with `overflow_mode = 'break'` is
/// reached. The regular execution path answers that by cancelling the processor, and the
/// finalization pass has to honour the same contract - the trailing progress drained from a remote
/// replica can be what crosses the threshold.
TEST(PipelineExecutorFinalizeProgressReplay, CancelsProcessorWhenBreakLimitIsReached)
{
    /// 1 ms, and the coarse clock is advanced by at least 10 ms before the pipeline is executed.
    ASSERT_TRUE(probeIsCancelledAfterExecution(makeTimeLimits(Poco::Timespan(0, 1000), OverflowMode::BREAK)));
}

/// The inverse: without a reached limit the finalization pass must not cancel anything, otherwise
/// the assertion above would hold for the wrong reason.
TEST(PipelineExecutorFinalizeProgressReplay, DoesNotCancelProcessorWithoutLimits)
{
    ASSERT_FALSE(probeIsCancelledAfterExecution(makeTimeLimits(Poco::Timespan(0), OverflowMode::BREAK)));
}
