#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>

#include <atomic>
#include <thread>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int ABORTED;
}

namespace
{

/// Cancels the thread group after a few chunks and stops on its own much later.
class CancellingSource : public ISource
{
public:
    CancellingSource(SharedHeader header, std::atomic_bool & cancelled_)
        : ISource(std::move(header))
        , cancelled(cancelled_)
    {
    }

    String getName() const override { return "CancellingSource"; }

protected:
    Chunk generate() override
    {
        ++generated;
        if (generated == 10)
            cancelled = true;
        if (generated > 10000)
            return {};
        return Chunk(Columns{ColumnUInt8::create(1, static_cast<UInt8>(1))}, 1);
    }

private:
    std::atomic_bool & cancelled;
    size_t generated = 0;
};

/// Drops every chunk, like a filter that matches no row.
class DropAllTransform : public ISimpleTransform
{
public:
    explicit DropAllTransform(SharedHeader header)
        : ISimpleTransform(header, header, /* skip_empty_chunks */ true)
    {
    }

    String getName() const override { return "DropAllTransform"; }

protected:
    void transform(Chunk & chunk) override { chunk.clear(); }
};

template <typename Executor>
void checkCancellationIsThrown()
{
    std::thread thread([]
    {
        ThreadStatus thread_status;
        std::atomic_bool cancelled = false;
        auto thread_group = std::make_shared<ThreadGroup>(getContext().context, 0);
        thread_group->setQueryCancellationPredicates(
            [&] { return cancelled.load(); },
            [&]
            {
                if (cancelled)
                    throw Exception(ErrorCodes::ABORTED, "Cancelled");
            });
        ThreadGroupSwitcher switcher(thread_group, ThreadName::MERGE_MUTATE);

        auto header = std::make_shared<const Block>(Block{ColumnWithTypeAndName(std::make_shared<DataTypeUInt8>(), "x")});
        Pipe pipe(std::make_shared<CancellingSource>(header, cancelled));
        pipe.addSimpleTransform([](const SharedHeader & input_header) { return std::make_shared<DropAllTransform>(input_header); });
        QueryPipeline pipeline(std::move(pipe));

        Executor executor(pipeline);
        Block block;
        try
        {
            while (executor.pull(block))
            {
            }
            ADD_FAILURE() << "The cancellation was reported as the end of the data";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::ABORTED) << e.message();
        }
    });
    thread.join();
}

}

/// A pipeline outside of the process list (of a merge or a mutation) reports the cancellation of its thread group to the caller.
TEST(PipelineExecutor, ThreadGroupCancellationIsThrown)
{
    checkCancellationIsThrown<PullingPipelineExecutor>();
    checkCancellationIsThrown<PullingAsyncPipelineExecutor>();
}
