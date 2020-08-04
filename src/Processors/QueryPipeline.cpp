#include <Processors/QueryPipeline.h>

#include <Processors/ResizeProcessor.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/NullSink.h>
#include <Processors/LimitTransform.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <Processors/Transforms/ExtremesTransform.h>
#include <Processors/Transforms/CreatingSetsTransform.h>
#include <Processors/Transforms/ConvertingTransform.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Sources/SourceFromInputStream.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Common/typeid_cast.h>
#include <Common/CurrentThread.h>
#include <Processors/DelayedPortsProcessor.h>
#include <Processors/RowsBeforeLimitCounter.h>
#include <Processors/Sources/RemoteSource.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void QueryPipeline::checkInitialized()
{
    if (!initialized())
        throw Exception("QueryPipeline wasn't initialized.", ErrorCodes::LOGICAL_ERROR);
}

void QueryPipeline::checkInitializedAndNotCompleted()
{
    checkInitialized();

    if (pipe.isCompleted())
        throw Exception("QueryPipeline was already completed.", ErrorCodes::LOGICAL_ERROR);
}

static void checkSource(const ProcessorPtr & source, bool can_have_totals)
{
    if (!source->getInputs().empty())
        throw Exception("Source for query pipeline shouldn't have any input, but " + source->getName() + " has " +
                        toString(source->getInputs().size()) + " inputs.", ErrorCodes::LOGICAL_ERROR);

    if (source->getOutputs().empty())
        throw Exception("Source for query pipeline should have single output, but it doesn't have any",
                ErrorCodes::LOGICAL_ERROR);

    if (!can_have_totals && source->getOutputs().size() != 1)
        throw Exception("Source for query pipeline should have single output, but " + source->getName() + " has " +
                        toString(source->getOutputs().size()) + " outputs.", ErrorCodes::LOGICAL_ERROR);

    if (source->getOutputs().size() > 2)
        throw Exception("Source for query pipeline should have 1 or 2 outputs, but " + source->getName() + " has " +
                        toString(source->getOutputs().size()) + " outputs.", ErrorCodes::LOGICAL_ERROR);
}

void QueryPipeline::init(Pipe pipe_)
{
    if (initialized())
        throw Exception("Pipeline has already been initialized.", ErrorCodes::LOGICAL_ERROR);

    if (pipe.empty())
        throw Exception("Can't initialize pipeline with empty pipe.", ErrorCodes::LOGICAL_ERROR);

    pipe = std::move(pipe_);
}

void QueryPipeline::addSimpleTransform(const Pipe::ProcessorGetter & getter)
{
    checkInitializedAndNotCompleted();
    pipe.addSimpleTransform(getter);
}

void QueryPipeline::addSimpleTransform(const Pipe::ProcessorGetterWithStreamKind & getter)
{
    checkInitializedAndNotCompleted();
    pipe.addSimpleTransform(getter);
}

void QueryPipeline::addTransform(ProcessorPtr transform)
{
    checkInitializedAndNotCompleted();
    pipe.addTransform(std::move(transform));
}

void QueryPipeline::setSinks(const Pipe::ProcessorGetterWithStreamKind & getter)
{
    checkInitializedAndNotCompleted();
    pipe.setSinks(getter);
}

void QueryPipeline::addDelayedStream(ProcessorPtr source)
{
    checkInitializedAndNotCompleted();

    checkSource(source, false);
    assertBlocksHaveEqualStructure(getHeader(), source->getOutputs().front().getHeader(), "QueryPipeline");

    IProcessor::PortNumbers delayed_streams = { pipe.numOutputPorts() };
    pipe.addSource(std::move(source));

    auto processor = std::make_shared<DelayedPortsProcessor>(getHeader(), pipe.numOutputPorts(), delayed_streams);
    addTransform(std::move(processor));
}

void QueryPipeline::addMergingAggregatedMemoryEfficientTransform(AggregatingTransformParamsPtr params, size_t num_merging_processors)
{
    DB::addMergingAggregatedMemoryEfficientTransform(pipe, std::move(params), num_merging_processors);
}

void QueryPipeline::resize(size_t num_streams, bool force, bool strict)
{
    checkInitializedAndNotCompleted();

    if (!force && num_streams == getNumStreams())
        return;

    ProcessorPtr resize;

    if (strict)
        resize = std::make_shared<StrictResizeProcessor>(getHeader(), getNumStreams(), num_streams);
    else
        resize = std::make_shared<ResizeProcessor>(getHeader(), getNumStreams(), num_streams);

    pipe.addTransform(std::move(resize));
}

void QueryPipeline::addTotalsHavingTransform(ProcessorPtr transform)
{
    checkInitializedAndNotCompleted();

    if (!typeid_cast<const TotalsHavingTransform *>(transform.get()))
        throw Exception("TotalsHavingTransform expected for QueryPipeline::addTotalsHavingTransform.",
                ErrorCodes::LOGICAL_ERROR);

    if (pipe.getTotalsPort())
        throw Exception("Totals having transform was already added to pipeline.", ErrorCodes::LOGICAL_ERROR);

    resize(1);

    auto * totals_port = &transform->getOutputs().back();
    pipe.addTransform(std::move(transform));
    pipe.totals_port = totals_port;
}

void QueryPipeline::addDefaultTotals()
{
    checkInitializedAndNotCompleted();

    if (pipe.getTotalsPort())
        throw Exception("Totals having transform was already added to pipeline.", ErrorCodes::LOGICAL_ERROR);

    const auto & current_header = getHeader();
    Columns columns;
    columns.reserve(current_header.columns());

    for (size_t i = 0; i < current_header.columns(); ++i)
    {
        auto column = current_header.getByPosition(i).type->createColumn();
        column->insertDefault();
        columns.emplace_back(std::move(column));
    }

    auto source = std::make_shared<SourceFromSingleChunk>(current_header, Chunk(std::move(columns), 1));
    pipe.addTotalsSource(std::move(source));
}

void QueryPipeline::dropTotalsAndExtremes()
{
    auto drop_port = [&](OutputPort *& port)
    {
        auto null_sink = std::make_shared<NullSink>(port->getHeader());
        connect(*port, null_sink->getPort());
        processors.emplace(std::move(null_sink));
        port = nullptr;
    };

    if (totals_having_port)
        drop_port(totals_having_port);

    if (extremes_port)
        drop_port(extremes_port);
}

void QueryPipeline::addExtremesTransform()
{
    checkInitializedAndNotCompleted();

    if (extremes_port)
        throw Exception("Extremes transform was already added to pipeline.", ErrorCodes::LOGICAL_ERROR);

    std::vector<OutputPort *> extremes;
    extremes.reserve(streams.size());

    for (auto & stream : streams)
    {
        auto transform = std::make_shared<ExtremesTransform>(current_header);
        connect(*stream, transform->getInputPort());

        stream = &transform->getOutputPort();
        extremes.push_back(&transform->getExtremesPort());

        processors.emplace(std::move(transform));
    }

    if (extremes.size() == 1)
        extremes_port = extremes.front();
    else
        extremes_port = uniteExtremes(extremes, current_header, processors);
}

void QueryPipeline::addCreatingSetsTransform(ProcessorPtr transform)
{
    checkInitializedAndNotCompleted();

    if (!typeid_cast<const CreatingSetsTransform *>(transform.get()))
        throw Exception("CreatingSetsTransform expected for QueryPipeline::addExtremesTransform.",
                        ErrorCodes::LOGICAL_ERROR);

    resize(1);

    auto concat = std::make_shared<ConcatProcessor>(current_header, 2);
    connect(transform->getOutputs().front(), concat->getInputs().front());
    connect(*streams.back(), concat->getInputs().back());

    streams.assign({ &concat->getOutputs().front() });
    processors.emplace(std::move(transform));
    processors.emplace(std::move(concat));
}

void QueryPipeline::setOutputFormat(ProcessorPtr output)
{
    checkInitializedAndNotCompleted();

    auto * format = dynamic_cast<IOutputFormat * >(output.get());

    if (!format)
        throw Exception("IOutputFormat processor expected for QueryPipeline::setOutputFormat.", ErrorCodes::LOGICAL_ERROR);

    if (output_format)
        throw Exception("QueryPipeline already has output.", ErrorCodes::LOGICAL_ERROR);

    output_format = format;

    resize(1);

    auto & main = format->getPort(IOutputFormat::PortKind::Main);
    auto & totals = format->getPort(IOutputFormat::PortKind::Totals);
    auto & extremes = format->getPort(IOutputFormat::PortKind::Extremes);

    if (!totals_having_port)
    {
        auto null_source = std::make_shared<NullSource>(totals.getHeader());
        totals_having_port = &null_source->getPort();
        processors.emplace(std::move(null_source));
    }

    if (!extremes_port)
    {
        auto null_source = std::make_shared<NullSource>(extremes.getHeader());
        extremes_port = &null_source->getPort();
        processors.emplace(std::move(null_source));
    }

    processors.emplace(std::move(output));

    connect(*streams.front(), main);
    connect(*totals_having_port, totals);
    connect(*extremes_port, extremes);

    streams.clear();
    current_header.clear();
    extremes_port = nullptr;
    totals_having_port = nullptr;

    initRowsBeforeLimit();
}

void QueryPipeline::unitePipelines(
    std::vector<std::unique_ptr<QueryPipeline>> pipelines, const Block & common_header, size_t max_threads_limit)
{
    /// Should we limit the number of threads for united pipeline. True if all pipelines have max_threads != 0.
    /// If true, result max_threads will be sum(max_threads).
    /// Note: it may be > than settings.max_threads, so we should apply this limit again.
    bool will_limit_max_threads = !initialized() || max_threads != 0;

    if (initialized())
    {
        addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ConvertingTransform>(
                    header, common_header, ConvertingTransform::MatchColumnsMode::Position);
        });
    }

    std::vector<OutputPort *> extremes;
    std::vector<OutputPort *> totals;

    if (extremes_port)
        extremes.push_back(extremes_port);

    if (totals_having_port)
        totals.push_back(totals_having_port);

    for (auto & pipeline_ptr : pipelines)
    {
        auto & pipeline = *pipeline_ptr;
        pipeline.checkInitialized();
        pipeline.processors.setCollectedProcessors(processors.getCollectedProcessors());

        if (!pipeline.isCompleted())
        {
            pipeline.addSimpleTransform([&](const Block & header)
            {
               return std::make_shared<ConvertingTransform>(
                       header, common_header, ConvertingTransform::MatchColumnsMode::Position);
            });
        }

        if (pipeline.extremes_port)
        {
            auto converting = std::make_shared<ConvertingTransform>(
                pipeline.current_header, common_header, ConvertingTransform::MatchColumnsMode::Position);

            connect(*pipeline.extremes_port, converting->getInputPort());
            extremes.push_back(&converting->getOutputPort());
            processors.emplace(std::move(converting));
        }

        /// Take totals only from first port.
        if (pipeline.totals_having_port)
        {
            auto converting = std::make_shared<ConvertingTransform>(
                pipeline.current_header, common_header, ConvertingTransform::MatchColumnsMode::Position);

            connect(*pipeline.totals_having_port, converting->getInputPort());
            totals.push_back(&converting->getOutputPort());
            processors.emplace(std::move(converting));
        }

        auto * collector = processors.setCollectedProcessors(nullptr);
        processors.emplace(pipeline.processors.detach());
        processors.setCollectedProcessors(collector);

        streams.addStreams(pipeline.streams);

        table_locks.insert(table_locks.end(), std::make_move_iterator(pipeline.table_locks.begin()), std::make_move_iterator(pipeline.table_locks.end()));
        interpreter_context.insert(interpreter_context.end(), pipeline.interpreter_context.begin(), pipeline.interpreter_context.end());
        storage_holders.insert(storage_holders.end(), pipeline.storage_holders.begin(), pipeline.storage_holders.end());

        max_threads += pipeline.max_threads;
        will_limit_max_threads = will_limit_max_threads && pipeline.max_threads != 0;

        /// If one of pipelines uses more threads then current limit, will keep it.
        /// It may happen if max_distributed_connections > max_threads
        if (pipeline.max_threads > max_threads_limit)
            max_threads_limit = pipeline.max_threads;
    }

    if (!will_limit_max_threads)
        max_threads = 0;
    else
        limitMaxThreads(max_threads_limit);

    if (!extremes.empty())
    {
        if (extremes.size() == 1)
            extremes_port = extremes.back();
        else
            extremes_port = uniteExtremes(extremes, common_header, processors);
    }

    if (!totals.empty())
    {
        if (totals.size() == 1)
            totals_having_port = totals.back();
        else
            totals_having_port = uniteTotals(totals, common_header, processors);
    }

    current_header = common_header;
}

void QueryPipeline::setProgressCallback(const ProgressCallback & callback)
{
    for (auto & processor : processors.get())
    {
        if (auto * source = dynamic_cast<ISourceWithProgress *>(processor.get()))
            source->setProgressCallback(callback);

        if (auto * source = typeid_cast<CreatingSetsTransform *>(processor.get()))
            source->setProgressCallback(callback);
    }
}

void QueryPipeline::setProcessListElement(QueryStatus * elem)
{
    process_list_element = elem;

    for (auto & processor : processors.get())
    {
        if (auto * source = dynamic_cast<ISourceWithProgress *>(processor.get()))
            source->setProcessListElement(elem);

        if (auto * source = typeid_cast<CreatingSetsTransform *>(processor.get()))
            source->setProcessListElement(elem);
    }
}

void QueryPipeline::initRowsBeforeLimit()
{
    RowsBeforeLimitCounterPtr rows_before_limit_at_least;

    /// TODO: add setRowsBeforeLimitCounter as virtual method to IProcessor.
    std::vector<LimitTransform *> limits;
    std::vector<SourceFromInputStream *> sources;
    std::vector<RemoteSource *> remote_sources;

    std::unordered_set<IProcessor *> visited;

    struct QueuedEntry
    {
        IProcessor * processor;
        bool visited_limit;
    };

    std::queue<QueuedEntry> queue;

    queue.push({ output_format, false });
    visited.emplace(output_format);

    while (!queue.empty())
    {
        auto * processor = queue.front().processor;
        auto visited_limit = queue.front().visited_limit;
        queue.pop();

        if (!visited_limit)
        {
            if (auto * limit = typeid_cast<LimitTransform *>(processor))
            {
                visited_limit = true;
                limits.emplace_back(limit);
            }

            if (auto * source = typeid_cast<SourceFromInputStream *>(processor))
                sources.emplace_back(source);

            if (auto * source = typeid_cast<RemoteSource *>(processor))
                remote_sources.emplace_back(source);
        }
        else if (auto * sorting = typeid_cast<PartialSortingTransform *>(processor))
        {
            if (!rows_before_limit_at_least)
                rows_before_limit_at_least = std::make_shared<RowsBeforeLimitCounter>();

            sorting->setRowsBeforeLimitCounter(rows_before_limit_at_least);

            /// Don't go to children. Take rows_before_limit from last PartialSortingTransform.
            continue;
        }

        /// Skip totals and extremes port for output format.
        if (auto * format = dynamic_cast<IOutputFormat *>(processor))
        {
            auto * child_processor = &format->getPort(IOutputFormat::PortKind::Main).getOutputPort().getProcessor();
            if (visited.emplace(child_processor).second)
                queue.push({ child_processor, visited_limit });

            continue;
        }

        for (auto & child_port : processor->getInputs())
        {
            auto * child_processor = &child_port.getOutputPort().getProcessor();
            if (visited.emplace(child_processor).second)
                queue.push({ child_processor, visited_limit });
        }
    }

    if (!rows_before_limit_at_least && (!limits.empty() || !sources.empty() || !remote_sources.empty()))
    {
        rows_before_limit_at_least = std::make_shared<RowsBeforeLimitCounter>();

        for (auto & limit : limits)
            limit->setRowsBeforeLimitCounter(rows_before_limit_at_least);

        for (auto & source : sources)
            source->setRowsBeforeLimitCounter(rows_before_limit_at_least);

        for (auto & source : remote_sources)
            source->setRowsBeforeLimitCounter(rows_before_limit_at_least);
    }

    /// If there is a limit, then enable rows_before_limit_at_least
    /// It is needed when zero rows is read, but we still want rows_before_limit_at_least in result.
    if (!limits.empty())
        rows_before_limit_at_least->add(0);

    if (rows_before_limit_at_least)
        output_format->setRowsBeforeLimitCounter(rows_before_limit_at_least);
}

Pipe QueryPipeline::getPipe() &&
{
    Pipes pipes(processors.detach(), streams.at(0), totals_having_port, extremes_port);
    pipe.max_parallel_streams = streams.maxParallelStreams();

    for (auto & lock : table_locks)
        pipe.addTableLock(lock);

    for (auto & context : interpreter_context)
        pipe.addInterpreterContext(context);

    for (auto & storage : storage_holders)
        pipe.addStorageHolder(storage);

    if (totals_having_port)
        pipe.setTotalsPort(totals_having_port);

    if (extremes_port)
        pipe.setExtremesPort(extremes_port);

    Pipes pipes;
    pipes.emplace_back(std::move(pipe));

    for (size_t i = 1; i < streams.size(); ++i)
        pipes.emplace_back(Pipe(streams[i]));

    return pipes;
}

PipelineExecutorPtr QueryPipeline::execute()
{
    if (!isCompleted())
        throw Exception("Cannot execute pipeline because it is not completed.", ErrorCodes::LOGICAL_ERROR);

    return std::make_shared<PipelineExecutor>(processors.get(), process_list_element);
}

QueryPipeline & QueryPipeline::operator= (QueryPipeline && rhs)
{
    /// Reset primitive fields
    process_list_element = rhs.process_list_element;
    rhs.process_list_element = nullptr;
    max_threads = rhs.max_threads;
    rhs.max_threads = 0;
    output_format = rhs.output_format;
    rhs.output_format = nullptr;
    extremes_port = rhs.extremes_port;
    rhs.extremes_port = nullptr;
    totals_having_port = rhs.totals_having_port;
    rhs.totals_having_port = nullptr;

    /// Move these fields in destruction order (it's important)
    streams = std::move(rhs.streams);
    processors = std::move(rhs.processors);
    current_header = std::move(rhs.current_header);
    table_locks = std::move(rhs.table_locks);
    storage_holders = std::move(rhs.storage_holders);
    interpreter_context = std::move(rhs.interpreter_context);

    return *this;
}

void QueryPipeline::ProcessorsContainer::emplace(ProcessorPtr processor)
{
    if (collected_processors)
        collected_processors->emplace_back(processor);

    processors.emplace_back(std::move(processor));
}

void QueryPipeline::ProcessorsContainer::emplace(Processors processors_)
{
    for (auto & processor : processors_)
        emplace(std::move(processor));
}

Processors * QueryPipeline::ProcessorsContainer::setCollectedProcessors(Processors * collected_processors_)
{
    if (collected_processors && collected_processors_)
        throw Exception("Cannot set collected processors to QueryPipeline because "
                        "another one object was already created for current pipeline." , ErrorCodes::LOGICAL_ERROR);

    std::swap(collected_processors, collected_processors_);
    return collected_processors_;
}

QueryPipelineProcessorsCollector::QueryPipelineProcessorsCollector(QueryPipeline & pipeline_, IQueryPlanStep * step_)
    : pipeline(pipeline_), step(step_)
{
    pipeline.processors.setCollectedProcessors(&processors);
}

QueryPipelineProcessorsCollector::~QueryPipelineProcessorsCollector()
{
    pipeline.processors.setCollectedProcessors(nullptr);
}

Processors QueryPipelineProcessorsCollector::detachProcessors(size_t group)
{
    for (auto & processor : processors)
        processor->setQueryPlanStep(step, group);

    Processors res;
    res.swap(processors);
    return res;
}

}
