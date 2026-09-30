#include <Storages/TimeSeries/TimeSeriesSink.h>
#include <Storages/TimeSeries/TimeSeriesVersion.h>

#include <Core/Block.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/IProcessor.h>
#include <Processors/ISink.h>
#include <Processors/Port.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Storages/StorageTimeSeries.h>

#include <functional>
#include <optional>
#include <vector>


namespace DB
{

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

    auto split_output = std::next(split->getOutputs().begin());
    for (auto & target : sink->getTargets())
    {
        std::function<void()> on_finish;
        if (target.is_tags)
            on_finish = [sink] { sink->markTagsWritten(); };
        else if (target.is_metric_families)
            on_finish = [sink] { sink->markMetricFamiliesWritten(); };

        auto branch_sink = std::make_shared<TimeSeriesBranchSink>(target.chain.getOutputSharedHeader(), std::move(on_finish));
        connect(*split_output, target.chain.getInputPort());
        connect(target.chain.getOutputPort(), branch_sink->getPort());
        ++split_output;

        resources = target.chain.detachResources();
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

}
