#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Executors/Runtime/Pipeline/ExecutingPipeline.h>
#include <Processors/Port.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

SharedHeader makeHeader()
{
    return std::make_shared<Block>(Block{ColumnWithTypeAndName(ColumnUInt8::create(), std::make_shared<DataTypeUInt8>(), "x")});
}

class Source final : public IProcessor
{
public:
    Source() : IProcessor({}, {OutputPort(makeHeader())}) {}
    String getName() const override { return "Source"; }
    Status prepare() override { return Status::Finished; }
    OutputPort & output() { return outputs.front(); }
};

class Sink final : public IProcessor
{
public:
    Sink() : IProcessor({InputPort(makeHeader())}, {}) {}
    String getName() const override { return "Sink"; }
    Status prepare() override { return Status::Finished; }
    InputPort & input() { return inputs.front(); }
};

struct Chain
{
    std::shared_ptr<Source> source = std::make_shared<Source>();
    std::shared_ptr<Sink> sink = std::make_shared<Sink>();
    std::shared_ptr<Processors> processors;

    Chain()
    {
        connect(source->output(), sink->input());
        processors = std::make_shared<Processors>(Processors{source, sink});
    }
};

}

TEST(ExecutingPipeline, CancelIsStickyAndReachesAddedProcessors)
{
    Chain chain;
    ExecutingPipeline pipeline(chain.processors, nullptr, nullptr);
    EXPECT_FALSE(pipeline.cancelled());

    pipeline.cancel(IProcessor::CancelReason::PartialResult);
    EXPECT_EQ(IProcessor::CancelReason::PartialResult, pipeline.cancel_reason.load());
    EXPECT_FALSE(pipeline.cancelled());
    EXPECT_FALSE(chain.source->isCancelled());

    pipeline.cancel(IProcessor::CancelReason::CancelledByUser);
    EXPECT_EQ(IProcessor::CancelReason::CancelledByUser, pipeline.cancel_reason.load());
    EXPECT_TRUE(pipeline.cancelled());
    EXPECT_TRUE(chain.source->isCancelled());
    EXPECT_TRUE(chain.sink->isCancelled());

    pipeline.cancel(IProcessor::CancelReason::CancelledByTimeout);
    EXPECT_EQ(IProcessor::CancelReason::CancelledByUser, pipeline.cancel_reason.load());

    auto another_source = std::make_shared<Source>();
    auto another_sink = std::make_shared<Sink>();
    connect(another_source->output(), another_sink->input());
    ProcessorState & requester = chain.sink->input().getUpdateChannel().getOwner();
    EXPECT_EQ(2u, pipeline.updateProcessors(requester, {another_source, another_sink}, {}).size());
    EXPECT_TRUE(another_source->isCancelled());
    EXPECT_TRUE(another_sink->isCancelled());
    EXPECT_EQ(4u, chain.processors->size());
}

TEST(ExecutingPipeline, FailKeepsTheFirstExceptionAndCancels)
{
    Chain chain;
    ExecutingPipeline pipeline(chain.processors, nullptr, nullptr);
    EXPECT_FALSE(pipeline.exception);

    pipeline.fail(std::make_exception_ptr(Exception(ErrorCodes::BAD_ARGUMENTS, "first")));
    pipeline.fail(std::make_exception_ptr(Exception(ErrorCodes::BAD_ARGUMENTS, "second")));

    EXPECT_EQ(IProcessor::CancelReason::Exception, pipeline.cancel_reason.load());
    EXPECT_TRUE(pipeline.cancelled());
    EXPECT_TRUE(chain.sink->isCancelled());
    EXPECT_EQ(ErrorCodes::BAD_ARGUMENTS, getExceptionErrorCode(pipeline.exception));
}

TEST(ExecutingPipeline, RemoveReadyRemovesAFinishedGroup)
{
    Chain chain;
    ExecutingPipeline pipeline(chain.processors, nullptr, nullptr);

    pipeline.submitForRemoval({chain.source, chain.sink});
    pipeline.recordAsFinished(*chain.source);
    pipeline.recordAsFinished(*chain.sink);
    chain.source->output().getUpdateChannel().getOwner().last_status = IProcessor::Status::Finished;
    chain.sink->input().getUpdateChannel().getOwner().last_status = IProcessor::Status::Finished;
    pipeline.removeReady();

    EXPECT_TRUE(chain.processors->empty());
    EXPECT_FALSE(chain.source->output().getUpdateChannel().isConnected());
}

TEST(ExecutingPipeline, AllFinishedNeedsEveryLockClosed)
{
    Chain chain;
    ExecutingPipeline pipeline(chain.processors, nullptr, nullptr);
    ProcessorState & source_state = chain.source->output().getUpdateChannel().getOwner();
    ProcessorState & sink_state = chain.sink->input().getUpdateChannel().getOwner();

    EXPECT_FALSE(pipeline.allFinished());
    source_state.lock.finish();
    EXPECT_FALSE(pipeline.allFinished());
    sink_state.lock.finish();
    EXPECT_TRUE(pipeline.allFinished());
}

TEST(ExecutingPipeline, SinksAreTheProcessorsWithoutAConnectedOutputInReverseOrder)
{
    Chain chain;
    auto lone_source = std::make_shared<Source>();
    chain.processors->push_back(lone_source);
    ExecutingPipeline pipeline(chain.processors, nullptr, nullptr);

    auto sinks = pipeline.sinks();
    ASSERT_EQ(2u, sinks.size());
    EXPECT_EQ(lone_source.get(), sinks[0]->processor);
    EXPECT_EQ(chain.sink.get(), sinks[1]->processor);
}
