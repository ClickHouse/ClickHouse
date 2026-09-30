#include <Storages/TimeSeries/TimeSeriesSink.h>
#include <Storages/TimeSeries/TimeSeriesVersion.h>

#include <Core/Block.h>
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

#include <deque>
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

/// Cumulative number of tags rows that must be committed before the chunk may reach its target table.
struct TimeSeriesRequiredTagsRows : public ChunkInfoCloneable<TimeSeriesRequiredTagsRows>
{
    explicit TimeSeriesRequiredTagsRows(size_t rows_) : rows(rows_) {}

    size_t rows = 0;
};


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

        const auto & targets = sink->getTargets();
        for (size_t i = 0; i < targets.size(); ++i)
        {
            if (targets[i].is_tags)
                tags_rows += prepared->branches[i].getNumRows();
        }
        for (size_t i = 0; i < targets.size(); ++i)
        {
            if (targets[i].is_samples || targets[i].is_recent_samples)
                prepared->branches[i].getChunkInfos().add(std::make_shared<TimeSeriesRequiredTagsRows>(tags_rows));
        }
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
    size_t tags_rows = 0;
};


/// Holds the samples and recent samples chunks until the tags rows they depend on are committed.
/// The signal input is the tags chain output: the tags sink emits block N+1 only after block N is committed.
class TimeSeriesCommitGate final : public IProcessor
{
public:
    TimeSeriesCommitGate(
        const SharedHeader & signal_header,
        const std::vector<SharedHeader> & lane_headers,
        std::function<void()> on_tags_written_)
        : IProcessor(makeInputs(signal_header, lane_headers), makeOutputs(lane_headers))
        , signal(inputs.front())
        , has_row_signal(signal_header->columns() != 0)
        , on_tags_written(std::move(on_tags_written_))
    {
        auto input = std::next(inputs.begin());
        auto output = outputs.begin();
        for (size_t i = 0; i < lane_headers.size(); ++i, ++input, ++output)
            lanes.push_back(Lane{.input = &*input, .output = &*output, .queue = {}});
    }

    String getName() const override { return "TimeSeriesCommitGate"; }

    Status prepare() override
    {
        pullSignal();

        if (signal_finished && !tags_marked)
            return Status::Ready;

        size_t num_finished_lanes = 0;
        for (auto & lane : lanes)
        {
            auto & input = *lane.input;
            auto & output = *lane.output;

            if (output.isFinished())
            {
                input.close();
                ++num_finished_lanes;
                continue;
            }

            if (!lane.queue.empty() && output.canPush() && (all_committed || lane.queue.front().required_rows <= committed_rows))
            {
                output.push(std::move(lane.queue.front().chunk));
                lane.queue.pop_front();
            }

            while (true)
            {
                if (has_row_signal && lane.queue.size() >= lane_capacity)
                {
                    input.setNotNeeded();
                    break;
                }

                input.setNeeded();
                if (!input.hasData())
                    break;

                Chunk chunk = input.pull();
                auto required = chunk.getChunkInfos().extract<TimeSeriesRequiredTagsRows>();
                if (!required)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "TimeSeries gated chunk has no required tags rows");
                lane.queue.push_back(Pending{.chunk = std::move(chunk), .required_rows = required->rows});
            }

            if (input.isFinished() && lane.queue.empty())
            {
                output.finish();
                ++num_finished_lanes;
            }
        }

        /// The tags chain treats a closed output as an error, so the gate waits for the signal to finish.
        if (num_finished_lanes == lanes.size() && signal_finished)
            return Status::Finished;

        return Status::PortFull;
    }

    void work() override
    {
        tags_marked = true;
        if (on_tags_written)
            on_tags_written();
    }

private:
    struct Pending
    {
        Chunk chunk;
        size_t required_rows = 0;
    };

    struct Lane
    {
        InputPort * input = nullptr;
        OutputPort * output = nullptr;
        std::deque<Pending> queue;
    };

    static InputPorts makeInputs(const SharedHeader & signal_header, const std::vector<SharedHeader> & lane_headers)
    {
        InputPorts ports;
        ports.emplace_back(signal_header);
        for (const auto & header : lane_headers)
            ports.emplace_back(header);
        return ports;
    }

    static OutputPorts makeOutputs(const std::vector<SharedHeader> & lane_headers)
    {
        OutputPorts ports;
        for (const auto & header : lane_headers)
            ports.emplace_back(header);
        return ports;
    }

    void pullSignal()
    {
        if (signal_finished)
            return;

        while (true)
        {
            signal.setNeeded();
            if (!signal.hasData())
                break;

            Chunk chunk = signal.pull();
            seen_rows += chunk.getNumRows();
            last_signal_rows = chunk.getNumRows();
        }

        if (signal.isFinished())
        {
            signal_finished = true;
            all_committed = true;
            committed_rows = seen_rows;
        }
        else
        {
            committed_rows = seen_rows - last_signal_rows;
        }
    }

    /// The split pushes a block to every output at once, so a lane must accept block N+1 while it holds block N.
    static constexpr size_t lane_capacity = 2;

    InputPort & signal;
    /// A tags chain that ends in dependent views has an empty output header and emits no rows.
    bool has_row_signal = false;
    std::function<void()> on_tags_written;
    std::vector<Lane> lanes;

    size_t seen_rows = 0;
    size_t last_signal_rows = 0;
    size_t committed_rows = 0;
    bool all_committed = false;
    bool signal_finished = false;
    bool tags_marked = false;
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
    auto & targets = sink->getTargets();

    std::vector<SharedHeader> branch_input_headers;
    for (const auto & target : targets)
        branch_input_headers.push_back(target.output_header);

    auto split = std::make_shared<TimeSeriesSplitTransform>(input_header, branch_input_headers, sink);
    auto tail = std::make_shared<ExpressionTransform>(
        input_header, std::make_shared<ExpressionActions>(ActionsDAG(input_header->getColumnsWithTypeAndName())));
    connect(split->getOutputs().front(), tail->getInputs().front());

    Processors processors;
    QueryPlanResourceHolder resources;
    processors.push_back(split);

    const TimeSeriesSink::Target * tags_target = nullptr;
    std::vector<SharedHeader> lane_headers;
    for (const auto & target : targets)
    {
        if (target.is_tags)
            tags_target = &target;
        else if (target.is_samples || target.is_recent_samples)
            lane_headers.push_back(target.output_header);
    }

    std::shared_ptr<TimeSeriesCommitGate> gate;
    if (tags_target)
    {
        gate = std::make_shared<TimeSeriesCommitGate>(
            tags_target->chain.getOutputSharedHeader(), lane_headers, [sink] { sink->markTagsWritten(); });
        connect(tags_target->chain.getOutputPort(), gate->getInputs().front());
    }

    auto split_output = std::next(split->getOutputs().begin());
    auto lane_input = gate ? std::next(gate->getInputs().begin()) : InputPorts::iterator{};
    auto lane_output = gate ? gate->getOutputs().begin() : OutputPorts::iterator{};
    for (auto & target : targets)
    {
        if (target.is_tags)
        {
            connect(*split_output, target.chain.getInputPort());
        }
        else if (target.is_samples || target.is_recent_samples)
        {
            if (!gate)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "TimeSeries samples target without a tags target");
            connect(*split_output, *lane_input);
            connect(*lane_output, target.chain.getInputPort());
            ++lane_input;
            ++lane_output;

            auto branch_sink = std::make_shared<TimeSeriesBranchSink>(target.chain.getOutputSharedHeader(), nullptr);
            connect(target.chain.getOutputPort(), branch_sink->getPort());
            processors.push_back(std::move(branch_sink));
        }
        else
        {
            connect(*split_output, target.chain.getInputPort());

            auto branch_sink = std::make_shared<TimeSeriesBranchSink>(
                target.chain.getOutputSharedHeader(), [sink] { sink->markMetricFamiliesWritten(); });
            connect(target.chain.getOutputPort(), branch_sink->getPort());
            processors.push_back(std::move(branch_sink));
        }
        ++split_output;

        resources.append(target.chain.detachResources());
        for (const auto & processor : target.chain.getProcessors())
            processors.push_back(processor);
    }
    if (gate)
        processors.push_back(gate);
    processors.push_back(tail);

    Chain result(std::move(processors));
    result.attachResources(std::move(resources));
    return result;
}


namespace
{

class TimeSeriesWriteSink final : public SinkToStorage
{
public:
    TimeSeriesWriteSink(Chain chain, size_t max_threads, bool concurrency_control)
        : SinkToStorage(chain.getInputSharedHeader())
        , pipeline(std::move(chain))
    {
        pipeline.setNumThreads(max_threads);
        pipeline.setConcurrencyControl(concurrency_control);
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

    void consume(Chunk & chunk) override { executor->push(chunk.clone()); }

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


SinkToStoragePtr wrapTimeSeriesWriteChain(Chain chain, size_t max_threads, bool concurrency_control)
{
    return std::make_shared<TimeSeriesWriteSink>(std::move(chain), max_threads, concurrency_control);
}

}
