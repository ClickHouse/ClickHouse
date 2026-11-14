#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/Scheduler/MemoryReservation.h>
#include <Common/tests/gtest_global_context.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/IAST.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PushingAsyncPipelineExecutor.h>
#include <Processors/Executors/Runtime/V1/PipelineExecutor.h>
#include <Processors/IProcessor.h>
#include <Processors/ISource.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>

#include <atomic>

using namespace DB;
using namespace DB::Runtime::V1;

namespace DB::ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace
{

/// Works once, then becomes asynchronous, and the executor fails to schedule it.
class ThrowingScheduleSource final : public IProcessor
{
public:
    ThrowingScheduleSource(SharedHeader header, const bool & exception_callback_called_)
        : IProcessor({}, OutputPorts(1, header))
        , exception_callback_called(exception_callback_called_)
    {
    }

    String getName() const override { return "ThrowingScheduleSource"; }

    Status prepare() override
    {
        if (outputs.front().isFinished())
            return Status::Finished;
        return worked ? Status::Async : Status::Ready;
    }

    void work() override { worked = true; }

    std::tuple<int, uint32_t, Int64> scheduleForEvent() override
    {
        throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "Cannot schedule the processor");
    }

    bool cancelled = false;
    bool exception_callback_called_on_cancel = false;

protected:
    void onCancel() noexcept override
    {
        cancelled = true;
        exception_callback_called_on_cancel = exception_callback_called;
    }

private:
    const bool & exception_callback_called;
    bool worked = false;
};

/// Generates a row on every call and never ends.
class EndlessSource final : public ISource
{
public:
    EndlessSource(SharedHeader header, const std::atomic_bool & exception_callback_called_)
        : ISource(std::move(header))
        , exception_callback_called(exception_callback_called_)
    {
    }

    String getName() const override { return "EndlessSource"; }

    std::atomic_bool cancelled = false;
    std::atomic_bool exception_callback_called_on_cancel = false;

protected:
    Chunk generate() override
    {
        auto column = ColumnUInt8::create();
        column->insertValue(1);
        return Chunk(Columns{std::move(column)}, 1);
    }

    void onCancel() noexcept override
    {
        exception_callback_called_on_cancel = exception_callback_called.load();
        cancelled = true;
    }

private:
    const std::atomic_bool & exception_callback_called;
};

SharedHeader makeHeader()
{
    return std::make_shared<Block>(Block{ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "x")});
}

std::exception_ptr makeCause()
{
    return std::make_exception_ptr(Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "Memory limit exceeded in the caller"));
}

QueryStatusPtr makeQueryStatus()
{
    ClientInfo client_info;
    client_info.current_query_id = "pipeline_executor_cancel_cause";
    Settings settings;
    return std::make_shared<QueryStatus>(
        getContext().context,
        "SELECT 1",
        /*normalized_query_hash_*/ 0,
        client_info,
        /*user_id_*/ std::nullopt,
        /*priority_handle_*/ QueryPriorities::Handle{},
        /*query_slot_*/ nullptr,
        /*memory_reservation_*/ nullptr,
        /*thread_group_*/ nullptr,
        IAST::QueryKind::Select,
        settings,
        /*watch_start_nanoseconds*/ 0,
        /*is_internal*/ false);
}

}

/// A processor can react to its cancellation by making other processors throw (the in-memory exchanges of a
/// distributed plan), so the exception that stops the execution must be recorded before the processors are cancelled.
TEST(PipelineExecutor, PushTasksFailureIsRecordedBeforeCancellingProcessors)
{
    bool exception_callback_called = false;

    SharedHeader header = makeHeader();
    auto source = std::make_shared<ThrowingScheduleSource>(header, exception_callback_called);
    auto sink = std::make_shared<EmptySink>(header);
    connect(source->getOutputs().front(), sink->getPort());
    auto processors = std::make_shared<Processors>(Processors{source, sink});

    PipelineExecutor executor(processors, /*elem=*/ nullptr);
    executor.setExceptionCallback([&] { exception_callback_called = true; });

    try
    {
        executor.execute(1, /*concurrency_control=*/ false);
        FAIL() << "The execution did not fail";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    }

    EXPECT_TRUE(exception_callback_called);
    EXPECT_TRUE(source->cancelled);
    EXPECT_TRUE(source->exception_callback_called_on_cancel);
}

/// The consumer fails while the pipeline runs: its exception must be recorded before the processors are cancelled,
/// and it is the one the cancellation rethrows.
TEST(PullingAsyncPipelineExecutor, CancelWithCauseRecordsItFirst)
{
    std::atomic_bool exception_callback_called = false;

    auto source = std::make_shared<EndlessSource>(makeHeader(), exception_callback_called);
    Pipe pipe(source);
    QueryPipeline pipeline(std::move(pipe));

    PullingAsyncPipelineExecutor executor(pipeline);
    executor.setExceptionCallback([&] { exception_callback_called = true; });

    Chunk chunk;
    ASSERT_TRUE(executor.pull(chunk));
    ASSERT_EQ(chunk.getNumRows(), 1);

    try
    {
        executor.cancel(makeCause());
        FAIL() << "The cancellation did not rethrow the cause";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    }

    EXPECT_TRUE(source->cancelled);
    EXPECT_TRUE(source->exception_callback_called_on_cancel);
}

TEST(PushingAsyncPipelineExecutor, CancelWithCauseRethrowsIt)
{
    SinkToStoragePtr sink = std::make_shared<NullSinkToStorage>(makeHeader());
    QueryPipeline pipeline(sink);

    PushingAsyncPipelineExecutor executor(pipeline);
    executor.start();

    auto column = ColumnUInt8::create();
    column->insertValue(1);
    executor.push(Block{ColumnWithTypeAndName(std::move(column), std::make_shared<DataTypeUInt8>(), "x")});

    try
    {
        executor.cancel(makeCause());
        FAIL() << "The cancellation did not rethrow the cause";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    }
}

/// A query cancelled with an exception (a client `Cancel` or disconnect) must record it in its executors
/// before their processors are cancelled.
TEST(QueryStatus, CancelQueryWithExceptionRecordsItBeforeCancellingProcessors)
{
    std::atomic_bool exception_callback_called = false;

    /// Outlives the executor, which unregisters itself from it.
    auto query_status = makeQueryStatus();

    auto source = std::make_shared<EndlessSource>(makeHeader(), exception_callback_called);
    Pipe pipe(source);
    QueryPipeline pipeline(std::move(pipe));
    pipeline.setProcessListElement(query_status);

    PullingAsyncPipelineExecutor executor(pipeline);
    executor.setExceptionCallback([&] { exception_callback_called = true; });

    Chunk chunk;
    ASSERT_TRUE(executor.pull(chunk));
    ASSERT_EQ(chunk.getNumRows(), 1);

    query_status->cancelQuery(CancelReason::CANCELLED_BY_USER, makeCause());

    try
    {
        executor.cancel();
        FAIL() << "The cancellation did not rethrow the cause";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    }

    EXPECT_TRUE(source->cancelled);
    EXPECT_TRUE(source->exception_callback_called_on_cancel);
}

#endif
