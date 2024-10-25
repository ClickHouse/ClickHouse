#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Core/SortDescription.h>
#include <Core/UUID.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Processors/ConcatProcessor.h>
#include <Processors/DelayedPortsProcessor.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/LimitTransform.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/RowsBeforeStepCounter.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/CreatingSetsTransform.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/ExtremesTransform.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <Processors/Transforms/MergeJoinTransform.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <Processors/Transforms/PasteJoinTransform.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <QueryPipeline/narrowPipe.h>
#include <Common/CurrentThread.h>
#include <Common/iota.h>
#include <Common/typeid_cast.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

void QueryPipelineBuilder::checkInitialized()
{
    if (!initialized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "QueryPipeline is uninitialized");
}

void QueryPipelineBuilder::checkInitializedAndNotCompleted()
{
    checkInitialized();

    if (pipe.isCompleted())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "QueryPipeline is already completed");
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline has already been initialized");

    if (pipe_.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't initialize pipeline with empty pipe");

    pipe = std::move(pipe_);
}

void QueryPipelineBuilder::init(QueryPipeline & pipeline)
{
    if (initialized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline has already been initialized");

    if (pipeline.pushing())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't initialize pushing pipeline");

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

void QueryPipelineBuilder::transform(const Transformer & transformer, bool check_ports)
{
    checkInitializedAndNotCompleted();
    pipe.transform(transformer, check_ports);
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

void QueryPipelineBuilder::narrow(size_t size)
{
    checkInitializedAndNotCompleted();
    narrowPipe(pipe, size);
}

void QueryPipelineBuilder::addTotalsHavingTransform(ProcessorPtr transform)
{
    checkInitializedAndNotCompleted();

    if (!typeid_cast<const TotalsHavingTransform *>(transform.get()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "TotalsHavingTransform is expected for QueryPipeline::addTotalsHavingTransform");

    if (pipe.getTotalsPort())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Totals having transform was already added to pipeline");

    resize(1);

    auto * totals_port = &transform->getOutputs().back();
    pipe.addTransform(std::move(transform), totals_port, nullptr);
}

void QueryPipelineBuilder::addDefaultTotals()
{
    checkInitializedAndNotCompleted();

    if (pipe.getTotalsPort())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Totals having transform was already added to pipeline");

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
    bool concurrency_control = false;
    Pipes pipes;
    QueryPlanResourceHolder resources;

    for (auto & pipeline_ptr : pipelines)
    {
        auto & pipeline = *pipeline_ptr;
        pipeline.checkInitialized();
        resources = std::move(pipeline.resources);
        pipeline.pipe.collected_processors = collected_processors;

        pipes.emplace_back(std::move(pipeline.pipe));

        max_threads += pipeline.max_threads;
        will_limit_max_threads = will_limit_max_threads && pipeline.max_threads != 0;

        /// If one of pipelines uses more threads then current limit, will keep it.
        /// It may happen if max_distributed_connections > max_threads
        max_threads_limit = std::max(pipeline.max_threads, max_threads_limit);

        // Use concurrency control if at least one of pipelines is using it
        concurrency_control = concurrency_control || pipeline.getConcurrencyControl();
    }

    QueryPipelineBuilder pipeline;
    pipeline.init(Pipe::unitePipes(std::move(pipes), collected_processors, false));
    pipeline.addResources(std::move(resources));

    if (will_limit_max_threads)
    {
        pipeline.setMaxThreads(max_threads);
        pipeline.limitMaxThreads(max_threads_limit);
    }
    pipeline.setConcurrencyControl(concurrency_control);

    pipeline.setCollectedProcessors(nullptr);
    return pipeline;
}

QueryPipelineBuilderPtr QueryPipelineBuilder::mergePipelines(
    QueryPipelineBuilderPtr left,
    QueryPipelineBuilderPtr right,
    ProcessorPtr transform,
    Processors * collected_processors)
{
    if (transform->getOutputs().size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Merge transform must have exactly 1 output, got {}", transform->getOutputs().size());

    connect(*left->pipe.output_ports.front(), transform->getInputs().front());
    connect(*right->pipe.output_ports.front(), transform->getInputs().back());

    if (collected_processors)
        collected_processors->emplace_back(transform);

    left->pipe.output_ports.front() = &transform->getOutputs().front();
    left->pipe.processors->emplace_back(transform);

    left->pipe.processors->insert(left->pipe.processors->end(), right->pipe.processors->begin(), right->pipe.processors->end());
    left->pipe.header = left->pipe.output_ports.front()->getHeader();
    left->pipe.max_parallel_streams = std::max(left->pipe.max_parallel_streams, right->pipe.max_parallel_streams);
    return left;
}

std::unique_ptr<QueryPipelineBuilder> QueryPipelineBuilder::joinPipelinesYShaped(
    std::unique_ptr<QueryPipelineBuilder> left,
    std::unique_ptr<QueryPipelineBuilder> right,
    JoinPtr join,
    const Block & out_header,
    size_t max_block_size,
    Processors * collected_processors)
{
    left->checkInitializedAndNotCompleted();
    right->checkInitializedAndNotCompleted();

    left->pipe.dropExtremes();
    right->pipe.dropExtremes();
    if ((left->getNumStreams() != 1 || right->getNumStreams() != 1) && join->getTableJoin().kind() == JoinKind::Paste)
    {
        left->pipe.resize(1, true);
        right->pipe.resize(1, true);
    }
    else if (left->getNumStreams() != 1 || right->getNumStreams() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Join is supported only for pipelines with one output port");

    if (left->hasTotals() || right->hasTotals())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Current join algorithm is supported only for pipelines without totals");

    Blocks inputs = {left->getHeader(), right->getHeader()};

    if (join->getTableJoin().kind() == JoinKind::Paste)
    {
        auto joining = std::make_shared<PasteJoinTransform>(join, inputs, out_header, max_block_size);
        return mergePipelines(std::move(left), std::move(right), std::move(joining), collected_processors);
    }

    auto joining = std::make_shared<MergeJoinTransform>(join, inputs, out_header, max_block_size);
    return mergePipelines(std::move(left), std::move(right), std::move(joining), collected_processors);
}

std::unique_ptr<QueryPipelineBuilder> QueryPipelineBuilder::joinPipelinesRightLeft(
    std::unique_ptr<QueryPipelineBuilder> left,
    std::unique_ptr<QueryPipelineBuilder> right,
    JoinPtr join,
    const Block & output_header,
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
    IQueryPlanStep * step = right->pipe.processors->back()->getQueryPlanStep();

    /// In case joined subquery has totals, and we don't, add default chunk to totals.
    bool default_totals = false;

    if (!join->supportTotals() && (left->hasTotals() || right->hasTotals()))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Current join algorithm is supported only for pipelines without totals");

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


    std::vector<OutputPort *> joined_output_ports;
    std::vector<OutputPort *> delayed_root_output_ports;

    std::shared_ptr<DelayedJoinedBlocksTransform> delayed_root = nullptr;
    if (join->hasDelayedBlocks())
    {
        delayed_root = std::make_shared<DelayedJoinedBlocksTransform>(num_streams, join);
        if (!delayed_root->getInputs().empty() || delayed_root->getOutputs().size() != num_streams)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "DelayedJoinedBlocksTransform should have no inputs and {} outputs, "
                            "but has {} inputs and {} outputs",
                            num_streams, delayed_root->getInputs().size(), delayed_root->getOutputs().size());

        if (collected_processors)
            collected_processors->emplace_back(delayed_root);
        left->pipe.processors->emplace_back(delayed_root);

        for (auto & outport : delayed_root->getOutputs())
            delayed_root_output_ports.emplace_back(&outport);
    }


    Block left_header = left->getHeader();
    for (size_t i = 0; i < num_streams; ++i)
    {
        auto joining = std::make_shared<JoiningTransform>(
            left_header, output_header, join, max_block_size, false, default_totals, finish_counter);

        connect(**lit, joining->getInputs().front());
        connect(**rit, joining->getInputs().back());
        if (delayed_root)
        {
            // Process delayed joined blocks when all JoiningTransform are finished.
            auto delayed = std::make_shared<DelayedJoinedBlocksWorkerTransform>(
                output_header,
                [left_header, output_header, max_block_size, join]()
                { return join->getNonJoinedBlocks(left_header, output_header, max_block_size); });
            if (delayed->getInputs().size() != 1 || delayed->getOutputs().size() != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "DelayedJoinedBlocksWorkerTransform should have one input and one output");

            connect(*delayed_root_output_ports[i], delayed->getInputs().front());

            joined_output_ports.push_back(&joining->getOutputs().front());
            joined_output_ports.push_back(&delayed->getOutputs().front());

            if (collected_processors)
                collected_processors->emplace_back(delayed);
            left->pipe.processors->emplace_back(std::move(delayed));
        }
        else
        {
            *lit = &joining->getOutputs().front();
        }


        ++lit;
        ++rit;
        if (collected_processors)
            collected_processors->emplace_back(joining);

        left->pipe.processors->emplace_back(std::move(joining));
    }

    if (delayed_root)
    {
        // Process DelayedJoinedBlocksTransform after all JoiningTransforms.
        DelayedPortsProcessor::PortNumbers delayed_ports_numbers;
        delayed_ports_numbers.reserve(joined_output_ports.size() / 2);
        for (size_t i = 1; i < joined_output_ports.size(); i += 2)
            delayed_ports_numbers.push_back(i);

        auto delayed_processor = std::make_shared<DelayedPortsProcessor>(output_header, 2 * num_streams, delayed_ports_numbers);
        if (collected_processors)
            collected_processors->emplace_back(delayed_processor);
        left->pipe.processors->emplace_back(delayed_processor);

        // Connect @delayed_processor ports with inputs (JoiningTransforms & DelayedJoinedBlocksTransforms) / pipe outputs
        auto next_delayed_input = delayed_processor->getInputs().begin();
        for (OutputPort * port : joined_output_ports)
            connect(*port, *next_delayed_input++);
        left->pipe.output_ports.clear();
        for (OutputPort & port : delayed_processor->getOutputs())
            left->pipe.output_ports.push_back(&port);
        left->pipe.header = output_header;
        left->resize(num_streams);
    }

    if (left->hasTotals())
    {
        auto joining = std::make_shared<JoiningTransform>(left_header, output_header, join, max_block_size, true, default_totals);
        connect(*left->pipe.totals_port, joining->getInputs().front());
        connect(**rit, joining->getInputs().back());
        left->pipe.totals_port = &joining->getOutputs().front();

        ++rit;

        if (collected_processors)
            collected_processors->emplace_back(joining);

        left->pipe.processors->emplace_back(std::move(joining));
    }

    /// Move the collected processors to the last step in the right pipeline.
    Processors processors = collector.detachProcessors();
    if (step)
        step->appendExtraProcessors(processors);

    left->pipe.processors->insert(left->pipe.processors->end(), right->pipe.processors->begin(), right->pipe.processors->end());
    left->resources = std::move(right->resources);
    left->pipe.header = left->pipe.output_ports.front()->getHeader();
    left->pipe.max_parallel_streams = std::max(left->pipe.max_parallel_streams, right->pipe.max_parallel_streams);
    return left;
}

void QueryPipelineBuilder::addCreatingSetsTransform(
    const Block & res_header,
    SetAndKeyPtr set_and_key,
    StoragePtr external_table,
    const SizeLimits & limits,
    PreparedSetsCachePtr prepared_sets_cache)
{
    dropTotalsAndExtremes();
    resize(1);

    auto transform = std::make_shared<CreatingSetsTransform>(
            getHeader(),
            res_header,
            std::move(set_and_key),
            std::move(external_table),
            limits,
            std::move(prepared_sets_cache));

    pipe.addTransform(std::move(transform));
}

void QueryPipelineBuilder::addPipelineBefore(QueryPipelineBuilder pipeline)
{
    checkInitializedAndNotCompleted();
    if (pipeline.getHeader())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline for CreatingSets should have empty header. Got: {}",
                        pipeline.getHeader().dumpStructure());

    pipeline.dropTotalsAndExtremes();

    bool has_totals = pipe.getTotalsPort();
    bool has_extremes = pipe.getExtremesPort();
    size_t num_extra_ports = (has_totals ? 1 : 0) + (has_extremes ? 1 : 0);
    IProcessor::PortNumbers delayed_streams(pipe.numOutputPorts() + num_extra_ports);
    iota(delayed_streams.data(), delayed_streams.size(), IProcessor::PortNumbers::value_type(0));

    auto * collected_processors = pipe.collected_processors;

    Pipes pipes;
    pipes.emplace_back(std::move(pipe));
    pipes.emplace_back(QueryPipelineBuilder::getPipe(std::move(pipeline), resources));
    pipe = Pipe::unitePipes(std::move(pipes), collected_processors, true);

    auto processor = std::make_shared<DelayedPortsProcessor>(getHeader(), pipe.numOutputPorts() + num_extra_ports, delayed_streams, true);
    auto in = processor->getInputs().begin();
    auto out = processor->getOutputs().begin();
    InputPort * totals_in = has_totals ? &*(in++) : nullptr;
    InputPort * extremes_in = has_extremes ? &*(in++) : nullptr;
    OutputPort * totals_out = has_totals ? &*(out++) : nullptr;
    OutputPort * extremes_out = has_extremes ? &*(out++) : nullptr;
    pipe.addTransform(std::move(processor), totals_in, extremes_in, totals_out, extremes_out);
}

void QueryPipelineBuilder::setProcessListElement(QueryStatusPtr elem)
{
    process_list_element = elem;
}

void QueryPipelineBuilder::setProgressCallback(ProgressCallback callback)
{
    progress_callback = callback;
}

PipelineExecutorPtr QueryPipelineBuilder::execute()
{
    if (!isCompleted())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot execute pipeline because it is not completed");

    return std::make_shared<PipelineExecutor>(pipe.processors, process_list_element);
}

Pipe QueryPipelineBuilder::getPipe(QueryPipelineBuilder pipeline, QueryPlanResourceHolder & resources)
{
    resources = std::move(pipeline.resources);
    return std::move(pipeline.pipe);
}

QueryPipeline QueryPipelineBuilder::getPipeline(QueryPipelineBuilder builder)
{
    QueryPipeline res(std::move(builder.pipe));
    res.addResources(std::move(builder.resources));
    res.setNumThreads(builder.getNumThreads());
    res.setConcurrencyControl(builder.getConcurrencyControl());
    res.setProcessListElement(builder.process_list_element);
    res.setProgressCallback(builder.progress_callback);
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
