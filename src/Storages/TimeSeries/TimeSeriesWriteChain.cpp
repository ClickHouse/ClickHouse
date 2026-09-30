#include <Storages/TimeSeries/TimeSeriesSink.h>
#include <Storages/TimeSeries/TimeSeriesVersion.h>

#include <Core/Block.h>
#include <Common/EventFD.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Executors/PushingAsyncPipelineExecutor.h>
#include <Processors/IProcessor.h>
#include <Processors/ISink.h>
#include <Processors/Port.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/MergeTree/MergeTreeSink.h>
#include <Storages/MergeTree/ReplicatedMergeTreeSink.h>

#include <atomic>
#include <functional>
#include <memory>
#include <optional>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// Output 0 passes the original chunk through, outputs 1..N carry the blocks for the target tables.
class TimeSeriesSplitTransform final : public IProcessor
{
public:
    TimeSeriesSplitTransform(
        const SharedHeader & input_header,
        const std::vector<SharedHeader> & branch_headers,
        std::shared_ptr<TimeSeriesSink> sink_)
        : IProcessor(InputPorts{input_header}, makeOutputs(input_header, branch_headers))
        , sink(std::move(sink_))
    {
    }

    String getName() const override { return "TimeSeriesSplitTransform"; }

    Status prepare() override
    {
        auto & input = inputs.front();

        size_t num_open_outputs = 0;
        bool all_can_push = true;
        for (const auto & output : outputs)
        {
            if (output.isFinished())
                continue;
            ++num_open_outputs;
            if (!output.canPush())
                all_can_push = false;
        }

        if (!num_open_outputs)
        {
            input.close();
            return Status::Finished;
        }

        if (!all_can_push)
        {
            input.setNotNeeded();
            return Status::PortFull;
        }

        if (prepared)
        {
            size_t index = 0;
            for (auto & output : outputs)
            {
                if (!output.isFinished())
                    output.push(std::move(index ? prepared->branches[index - 1] : prepared->passthrough));
                ++index;
            }
            prepared.reset();
            return Status::PortFull;
        }

        if (input.isFinished())
        {
            for (auto & output : outputs)
                output.finish();
            return Status::Finished;
        }

        input.setNeeded();
        if (!input.hasData())
            return Status::NeedData;

        incoming = input.pull();
        return Status::Ready;
    }

    void work() override
    {
        prepared = sink->prepareChunk(std::move(incoming));
    }

private:
    static OutputPorts makeOutputs(const SharedHeader & input_header, const std::vector<SharedHeader> & branch_headers)
    {
        OutputPorts ports;
        ports.emplace_back(input_header);
        for (const auto & header : branch_headers)
            ports.emplace_back(header);
        return ports;
    }

    std::shared_ptr<TimeSeriesSink> sink;
    Chunk incoming;
    std::optional<TimeSeriesSink::PreparedChunk> prepared;
};


/// One arrival per target per chunk. Later chunks do not reuse an earlier arrival.
struct TimeSeriesCommitBarrier
{
    explicit TimeSeriesCommitBarrier(size_t expected_) : expected(expected_) {}

    void addWaiter(EventFD * event_fd) { waiters.push_back(event_fd); }

    void arrive()
    {
        const size_t count = arrived.fetch_add(1) + 1;
        if (expected && count % expected == 0)
            wake();
    }

    void fail()
    {
        failed.store(true);
        wake();
    }

    void leaderFinished()
    {
        leader_done.store(true);
        wake();
    }

    bool isFailed() const { return failed.load(); }

    bool isCommitFailed() const { return commit_failed.load(); }

    bool isLeaderFinished() const { return leader_done.load(); }

    bool commitAllowed(size_t rank, size_t epoch) const
    {
        return commit_steps.load() >= expected * epoch + rank;
    }

    bool commitEpochDone(size_t epoch) const
    {
        return commit_steps.load() >= expected * epoch;
    }

    void commitStepDone()
    {
        commit_steps.fetch_add(1);
        wake();
    }

    void failCommit()
    {
        commit_failed.store(true);
        wake();
    }

    bool readyFor(size_t epoch) const { return arrived.load() >= expected * (epoch + 1); }

private:
    void wake()
    {
#if defined(OS_LINUX) || defined(OS_DARWIN)
        for (auto * event_fd : waiters)
            event_fd->write();
#else
        (void)waiters;
#endif
    }

    size_t expected = 0;
    std::atomic<size_t> arrived{0};
    std::atomic<size_t> commit_steps{0};
    std::atomic<bool> failed{false};
    std::atomic<bool> commit_failed{false};
    std::atomic<bool> leader_done{false};
    std::vector<EventFD *> waiters;
};


/// Sits in front of the storage sink. Every target must have the chunk before any sink consumes it.
class TimeSeriesCommitGate final : public IProcessor
{
public:
    TimeSeriesCommitGate(SharedHeader header, std::shared_ptr<TimeSeriesCommitBarrier> barrier_, bool is_leader_)
        : IProcessor({header}, {header})
        , input(inputs.front())
        , output(outputs.front())
        , barrier(std::move(barrier_))
        , is_leader(is_leader_)
    {
        barrier->addWaiter(&event);
    }

    String getName() const override { return "TimeSeriesCommitGate"; }

    Status prepare() override
    {
        if (isCancelled())
            return Status::Finished;

        if (!output.canPush())
            return Status::PortFull;

        if (barrier->isFailed())
            return Status::Finished;

        if (!holding)
        {
            if (!input.hasData() && !input.isFinished())
            {
                input.setNeeded();
                return Status::NeedData;
            }

            if (input.hasData() && input.getOutputPort().getProcessor().isCancelled())
            {
                auto data = input.pullData(true);
                barrier->fail();
                if (data.exception)
                    output.pushException(std::move(data.exception));
                return Status::PortFull;
            }

            if (input.hasData())
            {
                auto data = input.pullData(true);
                if (data.exception)
                {
                    barrier->fail();
                    output.pushException(std::move(data.exception));
                    return Status::PortFull;
                }
                held = std::move(data.chunk);
                barrier->arrive();
                holding = true;
            }
            else
            {
                if (is_leader && !leader_signaled)
                {
                    leader_signaled = true;
                    barrier->leaderFinished();
                }
                else if (!is_leader && !barrier->isLeaderFinished())
                {
                    waiting = true;
                    return Status::Async;
                }
                output.finish();
                return Status::Finished;
            }
        }

        if (!barrier->readyFor(epoch))
        {
            waiting = true;
            return Status::Async;
        }

        output.push(std::move(held));
        holding = false;
        ++epoch;
        return Status::PortFull;
    }

    void work() override
    {
        if (!waiting)
            return;
        waiting = false;
#if defined(OS_LINUX) || defined(OS_DARWIN)
        event.read();
#endif
    }

    int schedule() override
    {
#if defined(OS_LINUX) || defined(OS_DARWIN)
        return event.fd;
#else
        return -1;
#endif
    }

private:
    InputPort & input;
    OutputPort & output;
    std::shared_ptr<TimeSeriesCommitBarrier> barrier;
    bool is_leader = false;
    bool holding = false;
    bool waiting = false;
    bool leader_signaled = false;
    size_t epoch = 0;
    Chunk held;
    EventFD event;
};


/// Ends one target chain. `onFinish` runs only after the target sink finished without an exception.
class TimeSeriesBranchSink final : public ISink
{
public:
    TimeSeriesBranchSink(SharedHeader header, std::function<void()> on_finish_)
        : ISink(std::move(header))
        , on_finish(std::move(on_finish_))
    {
    }

    String getName() const override { return "TimeSeriesBranchSink"; }

protected:
    void consume(Chunk) override {}

    void onFinish() override
    {
        if (on_finish)
            on_finish();
    }

private:
    std::function<void()> on_finish;
};


void attachCommitOrder(IProcessor & processor, const std::shared_ptr<TimeSeriesCommitBarrier> & barrier, size_t rank)
{
    auto * sink = dynamic_cast<SinkToStorage *>(&processor);
    if (!sink)
        return;
    if (!dynamic_cast<MergeTreeSink *>(&processor) && !dynamic_cast<ReplicatedMergeTreeSink *>(&processor))
        return;

#if defined(OS_LINUX) || defined(OS_DARWIN)
    auto wake = std::make_shared<EventFD>();
    barrier->addWaiter(wake.get());
    sink->setCommitOrder(
        [barrier, rank](size_t epoch) { return barrier->commitAllowed(rank, epoch); },
        [barrier] { return barrier->isCommitFailed(); },
        [barrier] { barrier->commitStepDone(); },
        [barrier] { barrier->failCommit(); },
        [barrier](size_t epoch) { return barrier->commitEpochDone(epoch); },
        [wake] { return wake->fd; },
        [wake] { wake->read(); },
        [wake] { wake->write(); });
#else
    (void)rank;
    sink->setCommitOrder(
        [](size_t) { return true; },
        [] { return false; },
        [] {},
        [] {},
        [](size_t) { return true; },
        [] { return -1; },
        [] {},
        [] {});
#endif
}

void insertCommitGateBeforeStorageSink(
    Chain & chain, const std::shared_ptr<TimeSeriesCommitBarrier> & barrier, bool is_leader, size_t commit_rank)
{
    IProcessor * processor = &chain.getInputPort().getProcessor();
    while (!dynamic_cast<SinkToStorage *>(processor))
    {
        if (processor->getOutputs().size() != 1 || !processor->getOutputs().front().isConnected())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "TimeSeries target chain has no storage sink");
        processor = &processor->getOutputs().front().getInputPort().getProcessor();
    }

    /// Commit tags, then samples, then later targets.
    attachCommitOrder(*processor, barrier, commit_rank);

    InputPort & sink_input = processor->getInputs().front();
    auto gate = std::make_shared<TimeSeriesCommitGate>(sink_input.getSharedHeader(), barrier, is_leader);

    /// The chain input and output stay on the first and last processors.
    if (&sink_input == &chain.getInputPort())
    {
        chain.addSource(std::move(gate));
        return;
    }

    OutputPort * upstream = nullptr;
    for (auto & current : chain.getProcessors())
    {
        for (auto & output : current->getOutputs())
        {
            if (output.isConnected() && &output.getInputPort() == &sink_input)
                upstream = &output;
        }
    }

    if (!upstream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "TimeSeries target chain has no storage sink");

    disconnect(*upstream, sink_input);
    connect(*upstream, gate->getInputs().front());
    connect(gate->getOutputs().front(), sink_input);
    chain.getProcessors().insert(std::prev(chain.getProcessors().end()), std::move(gate));
}

}


Chain buildTimeSeriesWriteChain(
    StorageTimeSeries & storage,
    const SharedHeader & input_header,
    const ASTPtr & query,
    ContextPtr context,
    bool async_insert)
{
    checkTimeSeriesVersionIsWritable(storage);

    Names insert_columns;
    if (const auto * insert_query = query->as<ASTInsertQuery>())
    {
        if (insert_query->columns)
        {
            for (const auto & column : insert_query->columns->children)
                insert_columns.push_back(column->getColumnName());
        }
    }

    auto sink = std::make_shared<TimeSeriesSink>(storage, *input_header, insert_columns, context, async_insert);
    sink->buildChains();

    std::vector<SharedHeader> branch_input_headers;
    for (const auto & target : sink->getTargets())
        branch_input_headers.push_back(target.output_header);

    auto split = std::make_shared<TimeSeriesSplitTransform>(input_header, branch_input_headers, sink);
    auto tail = std::make_shared<ExpressionTransform>(
        input_header, std::make_shared<ExpressionActions>(ActionsDAG(input_header->getColumnsWithTypeAndName())));
    connect(split->getOutputs().front(), tail->getInputs().front());

    Processors processors;
    QueryPlanResourceHolder resources;
    processors.push_back(split);

    auto & targets = sink->getTargets();
    auto barrier = std::make_shared<TimeSeriesCommitBarrier>(targets.size());
    size_t leader_index = 0;
    for (size_t i = 0; i < targets.size(); ++i)
    {
        if (targets[i].is_tags)
        {
            leader_index = i;
            break;
        }
    }

    auto split_output = std::next(split->getOutputs().begin());
    for (size_t i = 0; i < targets.size(); ++i)
    {
        auto & target = targets[i];
        std::function<void()> on_finish;
        if (target.is_tags)
            on_finish = [sink] { sink->markTagsWritten(); };
        else if (target.is_metric_families)
            on_finish = [sink] { sink->markMetricFamiliesWritten(); };

        insertCommitGateBeforeStorageSink(target.chain, barrier, i == leader_index, i);
        auto branch_sink = std::make_shared<TimeSeriesBranchSink>(target.chain.getOutputSharedHeader(), std::move(on_finish));
        connect(*split_output, target.chain.getInputPort());
        connect(target.chain.getOutputPort(), branch_sink->getPort());
        ++split_output;

        resources.append(target.chain.detachResources());
        for (const auto & processor : target.chain.getProcessors())
            processors.push_back(processor);
        processors.push_back(std::move(branch_sink));
    }
    processors.push_back(tail);

    Chain result(std::move(processors));
    result.attachResources(std::move(resources));
    result.setNumThreads(0);
    return result;
}


namespace
{

class TimeSeriesWriteSink final : public SinkToStorage
{
public:
    explicit TimeSeriesWriteSink(Chain chain)
        : SinkToStorage(chain.getInputSharedHeader())
        , pipeline(std::move(chain))
    {
        executor = std::make_unique<PushingAsyncPipelineExecutor>(pipeline);
    }

    String getName() const override { return "TimeSeriesWriteSink"; }

    ~TimeSeriesWriteSink() override
    {
        if (finished)
            return;
        try
        {
            executor->cancel();
        }
        catch (...)
        {
            tryLogCurrentException("TimeSeriesWriteSink");
        }
    }

protected:
    void onStart() override { executor->start(); }

    void consume(Chunk & chunk) override { executor->push(std::move(chunk)); }

    void onFinish() override
    {
        finished = true;
        executor->finish();
    }

private:
    QueryPipeline pipeline;
    std::unique_ptr<PushingAsyncPipelineExecutor> executor;
    bool finished = false;
};

}


SinkToStoragePtr wrapTimeSeriesWriteChain(Chain chain)
{
    return std::make_shared<TimeSeriesWriteSink>(std::move(chain));
}

}
