#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/LimitTransform.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <Processors/Transforms/ExtremesTransform.h>
#include <Processors/Transforms/CreatingSetsTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/IJoin.h>
#include <Common/typeid_cast.h>
#include <Common/CurrentThread.h>
#include <Processors/DelayedPortsProcessor.h>
#include <Processors/RowsBeforeLimitCounter.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void QueryPipelineBuilder::addQueryPlan(std::unique_ptr<QueryPlan> plan)
{
    pipe.addQueryPlan(std::move(plan));
}

void QueryPipelineBuilder::checkInitialized()
{
    if (!initialized())
        throw Exception("QueryPipeline is uninitialized", ErrorCodes::LOGICAL_ERROR);
}

void QueryPipelineBuilder::checkInitializedAndNotCompleted()
{
    checkInitialized();

    if (pipe.isCompleted())
        throw Exception("QueryPipeline is already completed", ErrorCodes::LOGICAL_ERROR);
}

static void checkSource(const ProcessorPtr & source, bool can_have_totals)
{
    if (!source->getInputs().empty())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Source for query pipeline shouldn't have any input, but {} has {} inputs",
            source->getName(),
            source->getInputs().size());

    if (source->getOutputs().empty())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Source for query pipeline should have single output, but {} doesn't have any", source->getName());

    if (!can_have_totals && source->getOutputs().size() != 1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Source for query pipeline should have single output, but {} has {} outputs",
            source->getName(),
            source->getOutputs().size());

    if (source->getOutputs().size() > 2)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Source for query pipeline should have 1 or 2 output, but {} has {} outputs",
            source->getName(),
            source->getOutputs().size());
}

void QueryPipelineBuilder::init(Pipe pipe_)
{
    if (initialized())
        throw Exception("Pipeline has already been initialized", ErrorCodes::LOGICAL_ERROR);

    if (pipe_.empty())
        throw Exception("Can't initialize pipeline with empty pipe", ErrorCodes::LOGICAL_ERROR);

    pipe = std::move(pipe_);
}

void QueryPipelineBuilder::init(QueryPipeline pipeline)
{
    if (initialized())
        throw Exception("Pipeline has already been initialized", ErrorCodes::LOGICAL_ERROR);

    if (pipeline.pushing())
        throw Exception("Can't initialize pushing pipeline", ErrorCodes::LOGICAL_ERROR);

    pipe.holder = std::move(pipeline.resources);
    pipe.processors = std::move(pipeline.processors);
    if (pipeline.output)
    {
        pipe.output_ports = {pipeline.output};
        pipe.header = pipeline.output->getHeader();
    }
    else
    {
        pipe.output_ports.clear();
        pipe.header = {};
    }

    pipe.totals_port = pipeline.totals;
    pipe.extremes_port = pipeline.extremes;
    pipe.max_parallel_streams = pipeline.num_threads;
}

void QueryPipelineBuilder::reset()
{
    Pipe pipe_to_destroy(std::move(pipe));
    *this = QueryPipelineBuilder();
}

void QueryPipelineBuilder::addSimpleTransform(const Pipe::ProcessorGetter & getter)
{
    checkInitializedAndNotCompleted();
    pipe.addSimpleTransform(getter);
}

void QueryPipelineBuilder::addSimpleTransform(const Pipe::ProcessorGetterWithStreamKind & getter)
{
    checkInitializedAndNotCompleted();
    pipe.addSimpleTransform(getter);
}

void QueryPipelineBuilder::addTransform(ProcessorPtr transform)
{
    checkInitializedAndNotCompleted();
    pipe.addTransform(std::move(transform));
}

void QueryPipelineBuilder::addTransform(ProcessorPtr transform, InputPort * totals, InputPort * extremes)
{
    checkInitializedAndNotCompleted();
    pipe.addTransform(std::move(transform), totals, extremes);
}

void QueryPipelineBuilder::addChains(std::vector<Chain> chains)
{
    checkInitializedAndNotCompleted();
    pipe.addChains(std::move(chains));
}

void QueryPipelineBuilder::addChain(Chain chain)
{
    checkInitializedAndNotCompleted();
    std::vector<Chain> chains;
    chains.emplace_back(std::move(chain));
    pipe.resize(1);
    pipe.addChains(std::move(chains));
}

void QueryPipelineBuilder::transform(const Transformer & transformer)
{
    checkInitializedAndNotCompleted();
    pipe.transform(transformer);
}

void QueryPipelineBuilder::setSinks(const Pipe::ProcessorGetterWithStreamKind & getter)
{
    checkInitializedAndNotCompleted();
    pipe.setSinks(getter);
}

void QueryPipelineBuilder::addDelayedStream(ProcessorPtr source)
{
    checkInitializedAndNotCompleted();

    checkSource(source, false);
    assertBlocksHaveEqualStructure(getHeader(), source->getOutputs().front().getHeader(), "QueryPipeline");

    IProcessor::PortNumbers delayed_streams = { pipe.numOutputPorts() };
    pipe.addSource(std::move(source));

    auto processor = std::make_shared<DelayedPortsProcessor>(getHeader(), pipe.numOutputPorts(), delayed_streams);
    addTransform(std::move(processor));
}

void QueryPipelineBuilder::addMergingAggregatedMemoryEfficientTransform(AggregatingTransformParamsPtr params, size_t num_merging_processors)
{
    DB::addMergingAggregatedMemoryEfficientTransform(pipe, std::move(params), num_merging_processors);
}

void QueryPipelineBuilder::resize(size_t num_streams, bool force, bool strict)
{
    checkInitializedAndNotCompleted();
    pipe.resize(num_streams, force, strict);
}

void QueryPipelineBuilder::addTotalsHavingTransform(ProcessorPtr transform)
{
    checkInitializedAndNotCompleted();

    if (!typeid_cast<const TotalsHavingTransform *>(transform.get()))
        throw Exception("TotalsHavingTransform is expected for QueryPipeline::addTotalsHavingTransform", ErrorCodes::LOGICAL_ERROR);

    if (pipe.getTotalsPort())
        throw Exception("Totals having transform was already added to pipeline", ErrorCodes::LOGICAL_ERROR);

    resize(1);

    auto * totals_port = &transform->getOutputs().back();
    pipe.addTransform(std::move(transform), totals_port, nullptr);
}

void QueryPipelineBuilder::addDefaultTotals()
{
    checkInitializedAndNotCompleted();

    if (pipe.getTotalsPort())
        throw Exception("Totals having transform was already added to pipeline", ErrorCodes::LOGICAL_ERROR);

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

void QueryPipelineBuilder::dropTotalsAndExtremes()
{
    pipe.dropTotals();
    pipe.dropExtremes();
}

void QueryPipelineBuilder::addExtremesTransform()
{
    checkInitializedAndNotCompleted();

    /// It is possible that pipeline already have extremes.
    /// For example, it may be added from VIEW subquery.
    /// In this case, recalculate extremes again - they should be calculated for different rows.
    if (pipe.getExtremesPort())
        pipe.dropExtremes();

    resize(1);
    auto transform = std::make_shared<ExtremesTransform>(getHeader());
    auto * port = &transform->getExtremesPort();
    pipe.addTransform(std::move(transform), nullptr, port);
}

QueryPipelineBuilder QueryPipelineBuilder::unitePipelines(
    std::vector<std::unique_ptr<QueryPipelineBuilder>> pipelines,
    size_t max_threads_limit,
    Processors * collected_processors)
{
    if (pipelines.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot unite an empty set of pipelines");

    Block common_header = pipelines.front()->getHeader();

    /// Should we limit the number of threads for united pipeline. True if all pipelines have max_threads != 0.
    /// If true, result max_threads will be sum(max_threads).
    /// Note: it may be > than settings.max_threads, so we should apply this limit again.
    bool will_limit_max_threads = true;
    size_t max_threads = 0;
    Pipes pipes;

    for (auto & pipeline_ptr : pipelines)
    {
        auto & pipeline = *pipeline_ptr;
        pipeline.checkInitialized();
        pipeline.pipe.collected_processors = collected_processors;

        pipes.emplace_back(std::move(pipeline.pipe));

        max_threads += pipeline.max_threads;
        will_limit_max_threads = will_limit_max_threads && pipeline.max_threads != 0;

        /// If one of pipelines uses more threads then current limit, will keep it.
        /// It may happen if max_distributed_connections > max_threads
        if (pipeline.max_threads > max_threads_limit)
            max_threads_limit = pipeline.max_threads;
    }

    QueryPipelineBuilder pipeline;
    pipeline.init(Pipe::unitePipes(std::move(pipes), collected_processors, false));

    if (will_limit_max_threads)
    {
        pipeline.setMaxThreads(max_threads);
        pipeline.limitMaxThreads(max_threads_limit);
    }

    return pipeline;
}

std::unique_ptr<QueryPipelineBuilder> QueryPipelineBuilder::joinPipelines(
    std::unique_ptr<QueryPipelineBuilder> left,
    std::unique_ptr<QueryPipelineBuilder> right,
    JoinPtr join,
    size_t max_block_size,
    size_t max_streams,
    bool keep_left_read_in_order,
    Processors * collected_processors)
{
    left->checkInitializedAndNotCompleted();
    right->checkInitializedAndNotCompleted();

    /// Extremes before join are useless. They will be calculated after if needed.
    left->pipe.dropExtremes();
    right->pipe.dropExtremes();

    left->pipe.collected_processors = collected_processors;

    /// Collect the NEW processors for the right pipeline.
    QueryPipelineProcessorsCollector collector(*right);
    /// Remember the last step of the right pipeline.
    ExpressionStep* step = typeid_cast<ExpressionStep*>(right->pipe.processors.back()->getQueryPlanStep());
    if (!step)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The top step of the right pipeline should be ExpressionStep");
    }

    /// In case joined subquery has totals, and we don't, add default chunk to totals.
    bool default_totals = false;
    if (!left->hasTotals() && right->hasTotals())
    {
        left->addDefaultTotals();
        default_totals = true;
    }

    ///                                     (left) ──────┐
    ///                                                  ╞> Joining ─> (joined)
    ///                                     (left) ─┐┌───┘
    ///                                             └┼───┐
    /// (right) ┐                         (totals) ──┼─┐ ╞> Joining ─> (joined)
    ///         ╞> Resize ┐                        ╓─┘┌┼─┘
    /// (right) ┘         │                        ╟──┘└─┐
    ///                   ╞> FillingJoin ─> Resize ╣     ╞> Joining ─> (totals)
    /// (totals) ─────────┘                        ╙─────┘

    size_t num_streams = left->getNumStreams();

    if (join->supportParallelJoin() && !right->hasTotals())
    {
        if (!keep_left_read_in_order)
        {
            left->resize(max_streams);
            num_streams = max_streams;
        }

        right->resize(max_streams);
        auto concurrent_right_filling_transform = [&](OutputPortRawPtrs outports)
        {
            Processors processors;
            for (auto & outport : outports)
            {
                auto adding_joined = std::make_shared<FillingRightJoinSideTransform>(right->getHeader(), join);
                connect(*outport, adding_joined->getInputs().front());
                processors.emplace_back(adding_joined);
            }
            return processors;
        };
        right->transform(concurrent_right_filling_transform);
        right->resize(1);
    }
    else
    {
        right->resize(1);

        auto adding_joined = std::make_shared<FillingRightJoinSideTransform>(right->getHeader(), join);
        InputPort * totals_port = nullptr;
        if (right->hasTotals())
            totals_port = adding_joined->addTotalsPort();

        right->addTransform(std::move(adding_joined), totals_port, nullptr);
    }

    size_t num_streams_including_totals = num_streams + (left->hasTotals() ? 1 : 0);
    right->resize(num_streams_including_totals);

    /// This counter is needed for every Joining except totals, to decide which Joining will generate non joined rows.
    auto finish_counter = std::make_shared<JoiningTransform::FinishCounter>(num_streams);

    auto lit = left->pipe.output_ports.begin();
    auto rit = right->pipe.output_ports.begin();

    for (size_t i = 0; i < num_streams; ++i)
    {
        auto joining = std::make_shared<JoiningTransform>(left->getHeader(), join, max_block_size, false, default_totals, finish_counter);
        connect(**lit, joining->getInputs().front());
        connect(**rit, joining->getInputs().back());
        *lit = &joining->getOutputs().front();

        ++lit;
        ++rit;

        if (collected_processors)
            collected_processors->emplace_back(joining);

        left->pipe.processors.emplace_back(std::move(joining));
    }

    if (left->hasTotals())
    {
        auto joining = std::make_shared<JoiningTransform>(left->getHeader(), join, max_block_size, true, default_totals);
        connect(*left->pipe.totals_port, joining->getInputs().front());
        connect(**rit, joining->getInputs().back());
        left->pipe.totals_port = &joining->getOutputs().front();

        ++rit;

        if (collected_processors)
            collected_processors->emplace_back(joining);

        left->pipe.processors.emplace_back(std::move(joining));
    }

    /// Move the collected processors to the last step in the right pipeline.
    Processors processors = collector.detachProcessors();
    step->appendExtraProcessors(processors);

    left->pipe.processors.insert(left->pipe.processors.end(), right->pipe.processors.begin(), right->pipe.processors.end());
    left->pipe.holder = std::move(right->pipe.holder);
    left->pipe.header = left->pipe.output_ports.front()->getHeader();
    left->pipe.max_parallel_streams = std::max(left->pipe.max_parallel_streams, right->pipe.max_parallel_streams);
    return left;
}

void QueryPipelineBuilder::addCreatingSetsTransform(const Block & res_header, SubqueryForSet subquery_for_set, const SizeLimits & limits, ContextPtr context)
{
    resize(1);

    auto transform = std::make_shared<CreatingSetsTransform>(
            getHeader(),
            res_header,
            std::move(subquery_for_set),
            limits,
            context);

    InputPort * totals_port = nullptr;

    if (pipe.getTotalsPort())
        totals_port = transform->addTotalsPort();

    pipe.addTransform(std::move(transform), totals_port, nullptr);
}

void QueryPipelineBuilder::addPipelineBefore(QueryPipelineBuilder pipeline)
{
    checkInitializedAndNotCompleted();
    if (pipeline.getHeader())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline for CreatingSets should have empty header. Got: {}",
                        pipeline.getHeader().dumpStructure());

    IProcessor::PortNumbers delayed_streams(pipe.numOutputPorts());
    for (size_t i = 0; i < delayed_streams.size(); ++i)
        delayed_streams[i] = i;

    auto * collected_processors = pipe.collected_processors;

    Pipes pipes;
    pipes.emplace_back(std::move(pipe));
    pipes.emplace_back(QueryPipelineBuilder::getPipe(std::move(pipeline)));
    pipe = Pipe::unitePipes(std::move(pipes), collected_processors, true);

    auto processor = std::make_shared<DelayedPortsProcessor>(getHeader(), pipe.numOutputPorts(), delayed_streams, true);
    addTransform(std::move(processor));
}

void QueryPipelineBuilder::setProgressCallback(const ProgressCallback & callback)
{
    for (auto & processor : pipe.processors)
    {
        if (auto * source = dynamic_cast<ISourceWithProgress *>(processor.get()))
            source->setProgressCallback(callback);
    }
}

void QueryPipelineBuilder::setProcessListElement(QueryStatus * elem)
{
    process_list_element = elem;

    for (auto & processor : pipe.processors)
    {
        if (auto * source = dynamic_cast<ISourceWithProgress *>(processor.get()))
            source->setProcessListElement(elem);
    }
}

PipelineExecutorPtr QueryPipelineBuilder::execute()
{
    if (!isCompleted())
        throw Exception("Cannot execute pipeline because it is not completed", ErrorCodes::LOGICAL_ERROR);

    return std::make_shared<PipelineExecutor>(pipe.processors, process_list_element);
}

QueryPipeline QueryPipelineBuilder::getPipeline(QueryPipelineBuilder builder)
{
    QueryPipeline res(std::move(builder.pipe));
    res.setNumThreads(builder.getNumThreads());
    res.setProcessListElement(builder.process_list_element);
    return res;
}

void QueryPipelineBuilder::setCollectedProcessors(Processors * processors)
{
    pipe.collected_processors = processors;
}


QueryPipelineProcessorsCollector::QueryPipelineProcessorsCollector(QueryPipelineBuilder & pipeline_, IQueryPlanStep * step_)
    : pipeline(pipeline_), step(step_)
{
    pipeline.setCollectedProcessors(&processors);
}

QueryPipelineProcessorsCollector::~QueryPipelineProcessorsCollector()
{
    pipeline.setCollectedProcessors(nullptr);
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
