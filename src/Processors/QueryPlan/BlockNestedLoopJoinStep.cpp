#include <Core/Block.h>
#include <Interpreters/ExpressionActions.h>
#include <IO/Operators.h>
#include <Processors/DelayedPortsProcessor.h>
#include <Processors/QueryPlan/BlockNestedLoopJoinStep.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/Transforms/BlockNestedLoopJoinData.h>
#include <Processors/Transforms/SquashingTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_JOIN_ON_EXPRESSION;
    extern const int LOGICAL_ERROR;
}

bool BlockNestedLoopJoinStep::isSupportedJoinType(JoinKind kind, JoinStrictness strictness)
{
    /// ASOF and PASTE prescribe the shape of the join condition (one inequality, or none at all),
    /// so an arbitrary predicate is not a condition they can express.
    if (strictness == JoinStrictness::Asof || isPaste(kind))
        return false;

    switch (strictness)
    {
        /// `ANY FULL` is left out on purpose: nothing in ClickHouse implements it (the query tree
        /// rejects it with NOT_IMPLEMENTED), so the operator has no reference semantics to answer
        /// with. `RightAny` is the old `any_join_distinct_right_table_keys` form, which does have
        /// them - one build row joined to every probe row, whatever the kind.
        case JoinStrictness::Any:
            return isInner(kind) || isLeftOrRight(kind) || isCrossOrComma(kind);
        case JoinStrictness::All:
        case JoinStrictness::RightAny:
            return isInner(kind) || isLeftOrRight(kind) || isFull(kind) || isCrossOrComma(kind);
        case JoinStrictness::Semi:
        case JoinStrictness::Anti:
            return isLeftOrRight(kind);
        default:
            return false;
    }
}

/// Where every required column of the condition comes from, by name: the side and its position in
/// that side's header. Resolved against the headers the columns are actually read from rather than
/// once and for all, because a position taken from the plan's header says nothing about the header
/// the pipeline is eventually built on.
static std::vector<BlockNestedLoopPredicate::Source> resolvePredicateInputs(
    const ExpressionActions & actions, const Block & left_header, const Block & right_header)
{
    std::vector<BlockNestedLoopPredicate::Source> inputs;
    for (const auto & required_column : actions.getRequiredColumnsWithTypes())
    {
        const bool in_left = left_header.has(required_column.name);
        const bool in_right = right_header.has(required_column.name);
        if (in_left == in_right)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Block nested loop join condition input {} must come from exactly one input, found in {}",
                required_column.name, in_left ? "both" : "neither");
        if (in_left)
            inputs.push_back({.side = 0, .position = left_header.getPositionByName(required_column.name)});
        else
            inputs.push_back({.side = 1, .position = right_header.getPositionByName(required_column.name)});
    }
    return inputs;
}

BlockNestedLoopJoinStep::BlockNestedLoopJoinStep(
    const SharedHeader & left_header_,
    const SharedHeader & right_header_,
    ExpressionActionsPtr predicate_,
    JoinKind kind_,
    JoinStrictness strictness_,
    const SizeLimits & size_limits_,
    BlockNestedLoopStoreSettings store_settings_,
    size_t max_block_size_,
    size_t max_block_bytes_,
    size_t min_build_block_size_,
    size_t min_build_block_bytes_)
    : kind(kind_)
    , strictness(strictness_)
    , size_limits(size_limits_)
    , store_settings(std::move(store_settings_))
    , max_block_size(max_block_size_)
    , max_block_bytes(max_block_bytes_)
    , min_build_block_size(min_build_block_size_)
    , min_build_block_bytes(min_build_block_bytes_)
{
    if (!isSupportedJoinType(kind, strictness))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Block nested loop join does not support {} {} JOIN",
            toString(strictness), toString(kind));

    if (!predicate_)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Block nested loop join requires a join condition");

    const auto & sample = predicate_->getSampleBlock();
    if (sample.columns() != 1 || !sample.getByPosition(0).type->canBeUsedInBooleanContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Block nested loop join condition must have a single boolean output, got {}",
            sample.dumpStructure());

    /// The condition is evaluated on a batch of candidate pairs and its result is used as a filter
    /// over that batch, so a condition that changes the row count says nothing about any pair.
    if (predicate_->hasArrayJoin())
        throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
            "arrayJoin is not supported in a JOIN ON expression that determines no join key");

    predicate.actions = std::move(predicate_);
    /// Only to reject a condition the operator cannot evaluate while the plan is still being built;
    /// what the probe transform reads is resolved again in `updatePipeline`.
    predicate.inputs = resolvePredicateInputs(*predicate.actions, *left_header_, *right_header_);

    updateInputHeaders({left_header_, right_header_});
}

/// The concatenation of the input columns: what the join operator itself outputs.
static SharedHeader concatHeaders(const SharedHeaders & headers)
{
    Block result;
    for (const auto & header : headers)
        for (const auto & column : *header)
            result.insert(ColumnWithTypeAndName(column.type->createColumn(), column.type, column.name));
    return std::make_shared<const Block>(std::move(result));
}

/// The build rows that matched nothing may be emitted only once every probe stream is done, which
/// is what `DelayedPorts` enforces here: the probe streams are its main ports and the scans over the
/// stored blocks its delayed ones.
void BlockNestedLoopJoinStep::addUnmatchedBuildRowsStage(
    QueryPipelineBuilder & pipeline, const BlockNestedLoopJoinDataPtr & data, size_t max_streams) const
{
    pipeline.transform([&](const OutputPortRawPtrs & probe_ports)
    {
        const size_t num_streams = probe_ports.size();

        VectorWithMemoryTracking<UInt64> delayed_ports;
        delayed_ports.reserve(num_streams);
        for (size_t i = 0; i < num_streams; ++i)
            delayed_ports.push_back(2 * i + 1);

        auto delayed = std::make_shared<DelayedPortsProcessor>(output_header, 2 * num_streams, delayed_ports);
        auto next_input = delayed->getInputs().begin();

        Processors new_processors;
        for (size_t i = 0; i < num_streams; ++i)
        {
            connect(*probe_ports[i], *next_input++);

            auto unmatched = std::make_shared<BlockNestedLoopUnmatchedBuildRowsTransform>(
                output_header, data, max_block_size, max_block_bytes, i, num_streams);
            connect(unmatched->getPort(), *next_input++);
            new_processors.push_back(std::move(unmatched));
        }
        new_processors.push_back(std::move(delayed));
        return new_processors;
    });

    pipeline.resize(max_streams);
}

QueryPipelineBuilderPtr BlockNestedLoopJoinStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings)
{
    if (pipelines.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "BlockNestedLoopJoinStep expects two input pipelines, got {}", pipelines.size());

    ///  (build) ┐                                     (probe) ─> Probe ────────╖
    ///          ╞> Resize ─> BlockNestedLoopBuild ─> Resize(1) ─╖   Unmatched ─╢
    ///  (build) ┘                                               ╠> Delayed     ╠> Delayed ─> (joined)
    ///                                                (probe) ──╢     ports    ║    ports
    ///                                               (totals) ──╜              ║
    ///                                                       ─> Probe ─────────╢
    ///                                                          Unmatched ─────╜
    ///                                                       ─> Totals ─────────> (totals)
    ///
    /// The `Unmatched` sources and the second `Delayed ports` exist only for the kinds that emit the
    /// build rows that matched nothing.

    auto probe_pipeline = std::move(pipelines[0]);
    auto build_pipeline = std::move(pipelines[1]);

    /// Extremes of an input say nothing about the join result; they are recomputed above the join.
    probe_pipeline->dropExtremes();
    build_pipeline->dropExtremes();

    /// The build side's totals row only lends its columns to the joined totals row, so the probe
    /// side needs a totals row to attach them to even when it has none of its own.
    const bool build_has_totals = build_pipeline->hasTotals();
    const bool probe_totals_are_default = build_has_totals && !probe_pipeline->hasTotals();
    if (probe_totals_are_default)
        probe_pipeline->addDefaultTotals();

    auto data = std::make_shared<BlockNestedLoopJoinData>(
        build_pipeline->getSharedHeader(), kind, strictness, size_limits, store_settings);

    /// The condition is evaluated on the columns of these two headers, so this is what its input
    /// positions have to name - not the plan's input headers, which an equivalent header may have
    /// replaced since the step was constructed.
    BlockNestedLoopPredicate probe_predicate = predicate;
    probe_predicate.inputs = resolvePredicateInputs(
        *predicate.actions, *probe_pipeline->getSharedHeader(), *data->getHeader());

    const size_t max_streams = std::max<size_t>(1, settings.max_threads);

    {
        QueryPipelineProcessorsCollector collector(*build_pipeline, this);

        /// One store, one totals row: a single build stream owns the totals port.
        build_pipeline->resize(build_has_totals ? 1 : max_streams);

        /// A tile of candidate pairs never spans two stored blocks, so a right input made of small
        /// blocks costs one evaluation of the condition per block per probe chunk, and a stage that
        /// walks the store emits one chunk per block. Squashing the build side makes both a matter
        /// of the settings rather than of how the right input happened to be written.
        if (min_build_block_size > 0 || min_build_block_bytes > 0)
        {
            build_pipeline->addSimpleTransform(
                [&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
                {
                    if (stream_type != QueryPipelineBuilder::StreamType::Main)
                        return nullptr;
                    return std::make_shared<SimpleSquashingChunksTransform>(header, min_build_block_size, min_build_block_bytes);
                });
        }

        if (build_has_totals)
        {
            auto build_transform = std::make_shared<BlockNestedLoopBuildTransform>(
                build_pipeline->getSharedHeader(), data, std::make_shared<FinishCounter>(1));
            auto * totals_port = build_transform->addTotalsPort();
            build_pipeline->addTransform(std::move(build_transform), totals_port, nullptr);
        }
        else
        {
            auto finish_counter = std::make_shared<FinishCounter>(build_pipeline->getNumStreams());
            build_pipeline->addSimpleTransform([&](const SharedHeader & header)
            {
                return std::make_shared<BlockNestedLoopBuildTransform>(header, data, finish_counter);
            });
        }

        /// The build transforms carry no data downstream, only the signal that the store is closed.
        build_pipeline->resize(1);

        auto build_processors = collector.detachProcessors(static_cast<size_t>(Stage::Build));
        processors.insert(processors.end(), build_processors.begin(), build_processors.end());
    }

    {
        QueryPipelineProcessorsCollector collector(*probe_pipeline, this);

        probe_pipeline->resize(max_streams);
        /// No probe stream, the totals stream included, may pull a row before the store is closed.
        probe_pipeline->addPipelineBefore(std::move(*build_pipeline));

        probe_pipeline->addSimpleTransform([&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type == QueryPipelineBuilder::StreamType::Totals)
                return std::make_shared<BlockNestedLoopTotalsTransform>(header, output_header, data, probe_totals_are_default);
            return std::make_shared<BlockNestedLoopProbeTransform>(
                header, output_header, data, probe_predicate, max_block_size, max_block_bytes);
        });

        if (keepsUnmatchedBuildRows(kind, strictness))
            addUnmatchedBuildRowsStage(*probe_pipeline, data, max_streams);

        auto probe_processors = collector.detachProcessors(static_cast<size_t>(Stage::Probe));
        processors.insert(processors.end(), probe_processors.begin(), probe_processors.end());
    }

    return probe_pipeline;
}

void BlockNestedLoopJoinStep::updateOutputHeader()
{
    output_header = concatHeaders(input_headers);
}

/// The name of the condition column, which for an analyzer plan is the expression that computes it
/// (`less(__table1.x, __table2.y)`).
const String & BlockNestedLoopJoinStep::getConditionName() const
{
    return predicate.actions->getSampleBlock().getByPosition(0).name;
}

void BlockNestedLoopJoinStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;

    settings.out << prefix << "Type: " << toString(kind) << '\n';
    settings.out << prefix << "Strictness: " << toString(strictness) << '\n';

    settings.out << prefix << "Condition: ";
    if (settings.pretty)
    {
        /// The condition is computed inside the operator, so its column name is not in the plan's
        /// pretty-name map; render the sub-DAG the way an `Expression` step's outputs are rendered.
        PrettySetNameMap subquery_set_names;
        settings.out << QueryPlanFormat::formatNodePretty(
            predicate.actions->getActionsDAG().getOutputs().front(),
            settings.pretty_names,
            settings.runtime_filter_names,
            subquery_set_names);
    }
    else
    {
        settings.out << getConditionName();
    }
    settings.out << '\n';

    if (!settings.pretty && !settings.compact)
        predicate.actions->describeActions(settings.out, prefix);
}

void BlockNestedLoopJoinStep::describeActions(JSONBuilder::JSONMap & map) const
{
    map.add("Type", toString(kind));
    map.add("Strictness", toString(strictness));
    map.add("Condition", getConditionName());
    map.add("Expression", predicate.actions->toTree());
}

void BlockNestedLoopJoinStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

std::vector<size_t> BlockNestedLoopJoinStep::getStepGroups() const
{
    return {static_cast<size_t>(Stage::Build), static_cast<size_t>(Stage::Probe)};
}

String BlockNestedLoopJoinStep::getStepGroupName(size_t group) const
{
    switch (static_cast<Stage>(group))
    {
        case Stage::Build: return "build";
        case Stage::Probe: return "probe";
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown block nested loop join stage {}", group);
}

}
