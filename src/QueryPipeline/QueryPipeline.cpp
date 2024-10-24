#include <QueryPipeline/QueryPipeline.h>

#include <queue>
#include <Core/Settings.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/IProcessor.h>
#include <Processors/ISource.h>
#include <Processors/LimitTransform.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Sinks/NullSink.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Sources/DelayedSource.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/Sources/SourceFromChunks.h>
#include <Processors/Transforms/AggregatingInOrderTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/Transforms/CountingTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/LimitsCheckingTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Processors/Transforms/StreamInQueryCacheTransform.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <QueryPipeline/Chain.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/ReadProgressCallback.h>
#include <QueryPipeline/printPipeline.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool rows_before_aggregation;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

QueryPipeline::QueryPipeline()
    : processors(std::make_shared<Processors>())
{
}

QueryPipeline::QueryPipeline(QueryPipeline &&) noexcept = default;
QueryPipeline & QueryPipeline::operator=(QueryPipeline &&) noexcept = default;
QueryPipeline::~QueryPipeline() = default;

static void checkInput(const InputPort & input, const ProcessorPtr & processor)
{
    if (!input.isConnected())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create QueryPipeline because {} has disconnected input",
            processor->getName());
}

static void checkOutput(const OutputPort & output, const ProcessorPtr & processor, const Processors & processors = {})
{
    if (!output.isConnected())
    {
        WriteBufferFromOwnString out;
        if (!processors.empty())
            printPipeline(processors, out);

        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create QueryPipeline because {} {} has disconnected output: {}",
            processor->getName(), processor->getDescription(), out.str());
    }
}

static void checkPulling(
    Processors & processors,
    OutputPort * output,
    OutputPort * totals,
    OutputPort * extremes)
{
    if (!output || output->isConnected())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create pulling QueryPipeline because its output port is connected or null");

    if (totals && totals->isConnected())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create pulling QueryPipeline because its totals port is connected");

    if (extremes && extremes->isConnected())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create pulling QueryPipeline because its extremes port is connected");

    bool found_output = false;
    bool found_totals = false;
    bool found_extremes = false;
    for (const auto & processor : processors)
    {
        for (const auto & in : processor->getInputs())
            checkInput(in, processor);

        for (const auto & out : processor->getOutputs())
        {
            if (&out == output)
                found_output = true;
            else if (totals && &out == totals)
                found_totals = true;
            else if (extremes && &out == extremes)
                found_extremes = true;
            else
                checkOutput(out, processor, processors);
        }
    }

    if (!found_output)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create pulling QueryPipeline because its output port does not belong to any processor");
    if (totals && !found_totals)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create pulling QueryPipeline because its totals port does not belong to any processor");
    if (extremes && !found_extremes)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create pulling QueryPipeline because its extremes port does not belong to any processor");
}

static void checkCompleted(Processors & processors)
{
    for (const auto & processor : processors)
    {
        for (const auto & in : processor->getInputs())
            checkInput(in, processor);

        for (const auto & out : processor->getOutputs())
            checkOutput(out, processor);
    }
}

static void initRowsBeforeLimit(IOutputFormat * output_format)
{
    RowsBeforeStepCounterPtr rows_before_limit_at_least;
    std::vector<IProcessor *> processors;
    std::map<LimitTransform *, std::vector<size_t>> limit_candidates;
    std::unordered_set<IProcessor *> visited;
    bool has_limit = false;

    struct QueuedEntry
    {
        IProcessor * processor;
        LimitTransform * limit_processor;
        ssize_t limit_input_port;
    };

    std::queue<QueuedEntry> queue;

    queue.push({ output_format, nullptr, -1 });
    visited.emplace(output_format);

    while (!queue.empty())
    {
        auto * processor = queue.front().processor;
        auto * limit_processor = queue.front().limit_processor;
        auto limit_input_port = queue.front().limit_input_port;
        queue.pop();

        /// Set counter based on the following cases:
        ///   1. Remote: Set counter on Remote
        ///   2. Limit ... PartialSorting: Set counter on PartialSorting
        ///   3. Limit ... TotalsHaving(with filter) ... Remote: Set counter on the input port of Limit
        ///   4. Limit ... Remote: Set counter on Remote
        ///   5. Limit ... : Set counter on the input port of Limit

        /// Case 1.
        if ((typeid_cast<RemoteSource *>(processor) || typeid_cast<DelayedSource *>(processor)) && !limit_processor)
        {
            processors.emplace_back(processor);
            continue;
        }

        if (auto * limit = typeid_cast<LimitTransform *>(processor))
        {
            has_limit = true;

            /// Ignore child limits
            if (limit_processor)
                continue;

            limit_processor = limit;
            limit_candidates[limit_processor] = {};
        }
        else if (limit_processor)
        {
            /// Case 2.
            if (typeid_cast<PartialSortingTransform *>(processor))
            {
                processors.emplace_back(processor);
                limit_candidates[limit_processor].push_back(limit_input_port);
                continue;
            }

            /// Case 3.
            if (auto * having = typeid_cast<TotalsHavingTransform *>(processor))
            {
                if (having->hasFilter())
                    continue;
            }

            /// Case 4.
            if (typeid_cast<RemoteSource *>(processor) || typeid_cast<DelayedSource *>(processor))
            {
                processors.emplace_back(processor);
                limit_candidates[limit_processor].push_back(limit_input_port);
                continue;
            }
        }

        /// Skip totals and extremes port for output format.
        if (auto * format = dynamic_cast<IOutputFormat *>(processor))
        {
            auto * child_processor = &format->getPort(IOutputFormat::PortKind::Main).getOutputPort().getProcessor();
            if (visited.emplace(child_processor).second)
                queue.push({ child_processor, limit_processor, limit_input_port });

            continue;
        }

        if (limit_processor == processor)
        {
            ssize_t i = 0;
            for (auto & child_port : processor->getInputs())
            {
                auto * child_processor = &child_port.getOutputPort().getProcessor();
                if (visited.emplace(child_processor).second)
                    queue.push({ child_processor, limit_processor, i });
                ++i;
            }
        }
        else
        {
            for (auto & child_port : processor->getInputs())
            {
                auto * child_processor = &child_port.getOutputPort().getProcessor();
                if (visited.emplace(child_processor).second)
                    queue.push({ child_processor, limit_processor, limit_input_port });
            }
        }
    }

    /// Case 5.
    for (auto && [limit, ports] : limit_candidates)
    {
        /// If there are some input ports which don't have the counter, add it to LimitTransform.
        if (ports.size() < limit->getInputs().size())
        {
            processors.push_back(limit);
            for (auto port : ports)
                limit->setInputPortHasCounter(port);
        }
    }

    if (!processors.empty())
    {
        rows_before_limit_at_least = std::make_shared<RowsBeforeStepCounter>();
        for (auto & processor : processors)
            processor->setRowsBeforeLimitCounter(rows_before_limit_at_least);

        /// If there is a limit, then enable rows_before_limit_at_least
        /// It is needed when zero rows is read, but we still want rows_before_limit_at_least in result.
        if (has_limit)
            rows_before_limit_at_least->add(0);

        output_format->setRowsBeforeLimitCounter(rows_before_limit_at_least);
    }
}
static void initRowsBeforeAggregation(std::shared_ptr<Processors> processors, IOutputFormat * output_format)
{
    bool has_aggregation = false;

    if (!processors->empty())
    {
        RowsBeforeStepCounterPtr rows_before_aggregation = std::make_shared<RowsBeforeStepCounter>();
        for (const auto & processor : *processors)
        {
            if (typeid_cast<AggregatingTransform *>(processor.get()) || typeid_cast<AggregatingInOrderTransform *>(processor.get()))
            {
                processor->setRowsBeforeAggregationCounter(rows_before_aggregation);
                has_aggregation = true;
            }
            if (typeid_cast<RemoteSource *>(processor.get()) || typeid_cast<DelayedSource *>(processor.get()))
                processor->setRowsBeforeAggregationCounter(rows_before_aggregation);
        }
        if (has_aggregation)
            rows_before_aggregation->add(0);
        output_format->setRowsBeforeAggregationCounter(rows_before_aggregation);
    }
}

QueryPipeline::QueryPipeline(
    QueryPlanResourceHolder resources_,
    std::shared_ptr<Processors> processors_)
    : resources(std::move(resources_))
    , processors(std::move(processors_))
{
    checkCompleted(*processors);
}

QueryPipeline::QueryPipeline(
    QueryPlanResourceHolder resources_,
    std::shared_ptr<Processors> processors_,
    InputPort * input_)
    : resources(std::move(resources_))
    , processors(std::move(processors_))
    , input(input_)
{
    if (!input || input->isConnected())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create pushing QueryPipeline because its input port is connected or null");

    bool found_input = false;
    for (const auto & processor : *processors)
    {
        for (const auto & in : processor->getInputs())
        {
            if (&in == input)
                found_input = true;
            else
                checkInput(in, processor);
        }

        for (const auto & out : processor->getOutputs())
            checkOutput(out, processor);
    }

    if (!found_input)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot create pushing QueryPipeline because its input port does not belong to any processor");
}

QueryPipeline::QueryPipeline(std::shared_ptr<ISource> source) : QueryPipeline(Pipe(std::move(source))) {}

QueryPipeline::QueryPipeline(
    QueryPlanResourceHolder resources_,
    std::shared_ptr<Processors> processors_,
    OutputPort * output_,
    OutputPort * totals_,
    OutputPort * extremes_)
    : resources(std::move(resources_))
    , processors(std::move(processors_))
    , output(output_)
    , totals(totals_)
    , extremes(extremes_)
{
    checkPulling(*processors, output, totals, extremes);
}

QueryPipeline::QueryPipeline(Pipe pipe)
{
    if (pipe.numOutputPorts() > 0)
    {
        pipe.resize(1);
        output = pipe.getOutputPort(0);
        totals = pipe.getTotalsPort();
        extremes = pipe.getExtremesPort();
        processors = std::move(pipe.processors);
        checkPulling(*processors, output, totals, extremes);
    }
    else
    {
        processors = std::move(pipe.processors);
        checkCompleted(*processors);
    }
}

QueryPipeline::QueryPipeline(Chain chain)
    : resources(chain.detachResources())
    , processors(std::make_shared<Processors>())
    , input(&chain.getInputPort())
    , num_threads(chain.getNumThreads())
{
    processors->reserve(chain.getProcessors().size() + 1);
    for (auto processor : chain.getProcessors())
        processors->emplace_back(std::move(processor));

    auto sink = std::make_shared<EmptySink>(chain.getOutputPort().getHeader());
    connect(chain.getOutputPort(), sink->getPort());
    processors->emplace_back(std::move(sink));

    input = &chain.getInputPort();
}

QueryPipeline::QueryPipeline(std::shared_ptr<IOutputFormat> format)
    : processors(std::make_shared<Processors>())
{
    auto & format_main = format->getPort(IOutputFormat::PortKind::Main);
    auto & format_totals = format->getPort(IOutputFormat::PortKind::Totals);
    auto & format_extremes = format->getPort(IOutputFormat::PortKind::Extremes);

    if (!totals)
    {
        auto source = std::make_shared<NullSource>(format_totals.getHeader());
        totals = &source->getPort();
        processors->emplace_back(std::move(source));
    }

    if (!extremes)
    {
        auto source = std::make_shared<NullSource>(format_extremes.getHeader());
        extremes = &source->getPort();
        processors->emplace_back(std::move(source));
    }

    connect(*totals, format_totals);
    connect(*extremes, format_extremes);

    input = &format_main;
    totals = nullptr;
    extremes = nullptr;

    output_format = format.get();

    processors->emplace_back(std::move(format));
}

static void drop(OutputPort *& port, Processors & processors)
{
    if (!port)
        return;

    auto null_sink = std::make_shared<NullSink>(port->getHeader());
    connect(*port, null_sink->getPort());

    processors.emplace_back(std::move(null_sink));
    port = nullptr;
}

QueryPipeline::QueryPipeline(std::shared_ptr<SinkToStorage> sink) : QueryPipeline(Chain(std::move(sink))) {}

void QueryPipeline::complete(std::shared_ptr<ISink> sink)
{
    if (!pulling())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline must be pulling to be completed with sink");

    drop(totals, *processors);
    drop(extremes, *processors);

    connect(*output, sink->getPort());
    processors->emplace_back(std::move(sink));
    output = nullptr;
}

void QueryPipeline::complete(Chain chain)
{
    if (!pulling())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline must be pulling to be completed with chain");

    resources = chain.detachResources();

    drop(totals, *processors);
    drop(extremes, *processors);

    processors->reserve(processors->size() + chain.getProcessors().size() + 1);
    for (auto processor : chain.getProcessors())
        processors->emplace_back(std::move(processor));

    auto sink = std::make_shared<EmptySink>(chain.getOutputPort().getHeader());
    connect(*output, chain.getInputPort());
    connect(chain.getOutputPort(), sink->getPort());
    processors->emplace_back(std::move(sink));
    output = nullptr;
}

void QueryPipeline::complete(std::shared_ptr<SinkToStorage> sink)
{
    complete(Chain(std::move(sink)));
}

void QueryPipeline::complete(Pipe pipe)
{
    if (!pushing())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline must be pushing to be completed with pipe");

    pipe.resize(1);
    pipe.dropExtremes();
    pipe.dropTotals();
    connect(*pipe.getOutputPort(0), *input);
    input = nullptr;

    auto pipe_processors = Pipe::detachProcessors(std::move(pipe));
    processors->insert(processors->end(), pipe_processors.begin(), pipe_processors.end());
}

static void addMaterializing(OutputPort *& output, Processors & processors)
{
    if (!output)
        return;

    auto materializing = std::make_shared<MaterializingTransform>(output->getHeader());
    connect(*output, materializing->getInputPort());
    output = &materializing->getOutputPort();
    processors.emplace_back(std::move(materializing));
}

void QueryPipeline::complete(std::shared_ptr<IOutputFormat> format)
{
    if (!pulling())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline must be pulling to be completed with output format");

    if (format->expectMaterializedColumns())
    {
        addMaterializing(output, *processors);
        addMaterializing(totals, *processors);
        addMaterializing(extremes, *processors);
    }

    auto & format_main = format->getPort(IOutputFormat::PortKind::Main);
    auto & format_totals = format->getPort(IOutputFormat::PortKind::Totals);
    auto & format_extremes = format->getPort(IOutputFormat::PortKind::Extremes);

    if (!totals)
    {
        auto source = std::make_shared<NullSource>(format_totals.getHeader());
        totals = &source->getPort();
        processors->emplace_back(std::move(source));
    }

    if (!extremes)
    {
        auto source = std::make_shared<NullSource>(format_extremes.getHeader());
        extremes = &source->getPort();
        processors->emplace_back(std::move(source));
    }

    connect(*output, format_main);
    connect(*totals, format_totals);
    connect(*extremes, format_extremes);

    output = nullptr;
    totals = nullptr;
    extremes = nullptr;

    initRowsBeforeLimit(format.get());
    for (const auto & context : resources.interpreter_context)
    {
        if (context->getSettingsRef()[Setting::rows_before_aggregation])
        {
            initRowsBeforeAggregation(processors, format.get());
            break;
        }
    }
    output_format = format.get();

    processors->emplace_back(std::move(format));
}

Block QueryPipeline::getHeader() const
{
    if (input)
        return input->getHeader();
    if (output)
        return output->getHeader();
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Header is available only for pushing or pulling QueryPipeline");
}

void QueryPipeline::setProgressCallback(const ProgressCallback & callback)
{
    progress_callback = callback;
}

void QueryPipeline::setProcessListElement(QueryStatusPtr elem)
{
    process_list_element = elem;

    if (pushing())
    {
        if (auto * counting = dynamic_cast<CountingTransform *>(&input->getProcessor()))
        {
            counting->setProcessListElement(elem);
        }
    }
}

void QueryPipeline::setQuota(std::shared_ptr<const EnabledQuota> quota_)
{
    quota = std::move(quota_);
}

void QueryPipeline::setLimitsAndQuota(const StreamLocalLimits & limits, std::shared_ptr<const EnabledQuota> quota_)
{
    if (!pulling())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "It is possible to set limits and quota only to pulling QueryPipeline");

    auto transform = std::make_shared<LimitsCheckingTransform>(output->getHeader(), limits);
    transform->setQuota(quota_);
    connect(*output, transform->getInputPort());
    output = &transform->getOutputPort();
    processors->emplace_back(std::move(transform));
}

bool QueryPipeline::tryGetResultRowsAndBytes(UInt64 & result_rows, UInt64 & result_bytes) const
{
    if (!output_format)
        return false;

    result_rows = output_format->getResultRows();
    result_bytes = output_format->getResultBytes();
    return true;
}

void QueryPipeline::writeResultIntoQueryCache(std::shared_ptr<QueryCache::Writer> query_cache_writer)
{
    assert(pulling());

    /// Attach a special transform to all output ports (result + possibly totals/extremes). The only purpose of the transform is
    /// to write each chunk into the query cache. All transforms hold a refcounted reference to the same query cache writer object.
    /// This ensures that all transforms write to the single same cache entry. The writer object synchronizes internally, the
    /// expensive stuff like cloning chunks happens outside lock scopes).

    auto add_stream_in_query_cache_transform = [&](OutputPort *& out_port, QueryCache::Writer::ChunkType chunk_type)
    {
        if (!out_port)
            return;

        auto transform = std::make_shared<StreamInQueryCacheTransform>(out_port->getHeader(), query_cache_writer, chunk_type);
        connect(*out_port, transform->getInputPort());
        out_port = &transform->getOutputPort();
        processors->emplace_back(std::move(transform));
    };

    using enum QueryCache::Writer::ChunkType;

    add_stream_in_query_cache_transform(output, Result);
    add_stream_in_query_cache_transform(totals, Totals);
    add_stream_in_query_cache_transform(extremes, Extremes);
}

void QueryPipeline::finalizeWriteInQueryCache()
{
    auto it = std::find_if(
        processors->begin(), processors->end(),
        [](ProcessorPtr processor){ return dynamic_cast<StreamInQueryCacheTransform *>(&*processor); });

    /// The pipeline can contain up to three StreamInQueryCacheTransforms which all point to the same query cache writer object.
    /// We can call finalize() on any of them.
    if (it != processors->end())
        dynamic_cast<StreamInQueryCacheTransform &>(**it).finalizeWriteInQueryCache();
}

void QueryPipeline::readFromQueryCache(
        std::unique_ptr<SourceFromChunks> source,
        std::unique_ptr<SourceFromChunks> source_totals,
        std::unique_ptr<SourceFromChunks> source_extremes)
{
    /// Construct the pipeline from the input source processors. The processors are provided by the query cache to produce chunks of a
    /// previous query result.

    auto add_stream_from_query_cache_source = [&](OutputPort *& out_port, std::unique_ptr<SourceFromChunks> source_)
    {
        if (!source_)
            return;
        out_port = &source_->getPort();
        processors->emplace_back(std::shared_ptr<SourceFromChunks>(std::move(source_)));
    };

    add_stream_from_query_cache_source(output, std::move(source));
    add_stream_from_query_cache_source(totals, std::move(source_totals));
    add_stream_from_query_cache_source(extremes, std::move(source_extremes));
}

void QueryPipeline::addStorageHolder(StoragePtr storage)
{
    resources.storage_holders.emplace_back(std::move(storage));
}

void QueryPipeline::addCompletedPipeline(QueryPipeline other)
{
    if (!other.completed())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add not completed pipeline");

    resources = std::move(other.resources);
    processors->insert(processors->end(), other.processors->begin(), other.processors->end());
}

void QueryPipeline::reset()
{
    QueryPipeline to_remove = std::move(*this);
    *this = QueryPipeline();
}

static void addExpression(OutputPort *& port, ExpressionActionsPtr actions, Processors & processors)
{
    if (port)
    {
        auto transform = std::make_shared<ExpressionTransform>(port->getHeader(), actions);
        connect(*port, transform->getInputPort());
        port = &transform->getOutputPort();
        processors.emplace_back(std::move(transform));
    }
}

void QueryPipeline::convertStructureTo(const ColumnsWithTypeAndName & columns)
{
    if (!pulling())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline must be pulling to convert header");

    auto converting = ActionsDAG::makeConvertingActions(
        output->getHeader().getColumnsWithTypeAndName(),
        columns,
        ActionsDAG::MatchColumnsMode::Position);

    auto actions = std::make_shared<ExpressionActions>(std::move(converting));
    addExpression(output, actions, *processors);
    addExpression(totals, actions, *processors);
    addExpression(extremes, actions, *processors);
}

std::unique_ptr<ReadProgressCallback> QueryPipeline::getReadProgressCallback() const
{
    auto callback = std::make_unique<ReadProgressCallback>();

    callback->setProgressCallback(progress_callback);
    callback->setQuota(quota);
    callback->setProcessListElement(process_list_element);

    if (!update_profile_events)
        callback->disableProfileEventUpdate();

    return callback;
}

}
