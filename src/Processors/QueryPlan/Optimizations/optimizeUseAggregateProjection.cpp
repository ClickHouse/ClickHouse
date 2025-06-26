#include <Processors/QueryPlan/Optimizations/projectionsCommon.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>

#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Sources/NullSource.h>

#include <AggregateFunctions/AggregateFunctionCount.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/QueryNode.h>

#include <Columns/ColumnAggregateFunction.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Storages/StorageDummy.h>
#include <Storages/VirtualColumnUtils.h>
#include <Planner/PlannerExpressionAnalysis.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/ProjectionsDescription.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool force_optimize_projection;
    extern const SettingsString preferred_optimize_projection_name;
}
}

namespace DB::QueryPlanOptimizations
{

using DAGIndex = std::unordered_map<std::string_view, const ActionsDAG::Node *>;
static DAGIndex buildDAGIndex(const ActionsDAG & dag)
{
    DAGIndex index;
    for (const auto * output : dag.getOutputs())
        index.emplace(output->result_name, output);

    return index;
}

/// Required analysis info from aggregate projection.
struct AggregateProjectionInfo
{
    std::optional<ActionsDAG> before_aggregation;
    Names keys;
    AggregateDescriptions aggregates;

    /// A context copy from interpreter which was used for analysis.
    /// Just in case it is used by some function.
    ContextPtr context;
};

/// Get required info from aggregate projection.
/// Ideally, this should be pre-calculated and stored inside ProjectionDescription.
static AggregateProjectionInfo getAggregatingProjectionInfo(
    const ProjectionDescription & projection,
    const ContextPtr & context,
    const StorageMetadataPtr & metadata_snapshot,
    const Block & key_virtual_columns)
{
    /// This is a bad approach.
    /// We'd better have a separate interpreter for projections.
    /// Now it's not obvious we didn't miss anything here.
    ///
    /// Setting ignoreASTOptimizations is used because some of them are invalid for projections.
    /// Example: 'SELECT min(c0), max(c0), count() GROUP BY -c0' for minmax_count projection can be rewritten to
    /// 'SELECT min(c0), max(c0), count() GROUP BY c0' which is incorrect cause we store a column '-c0' in projection.
    InterpreterSelectQuery interpreter(
        projection.query_ast,
        context,
        Pipe(std::make_shared<SourceFromSingleChunk>(metadata_snapshot->getSampleBlockWithSubcolumns())),
        SelectQueryOptions{QueryProcessingStage::WithMergeableState}.ignoreASTOptimizations().ignoreSettingConstraints());

    const auto & analysis_result = interpreter.getAnalysisResult();
    const auto & query_analyzer = interpreter.getQueryAnalyzer();

    AggregateProjectionInfo info;
    info.context = interpreter.getContext();
    info.before_aggregation = analysis_result.before_aggregation->dag.clone();
    info.keys = query_analyzer->aggregationKeys().getNames();
    info.aggregates = query_analyzer->aggregates();

    /// Add part/partition virtual columns to projection aggregation keys.
    /// We can do it because projection is stored for every part separately.
    for (const auto & virt_column : key_virtual_columns)
    {
        const auto * input = &info.before_aggregation->addInput(virt_column);
        info.before_aggregation->getOutputs().push_back(input);
        info.keys.push_back(virt_column.name);
    }

    return info;
}

struct AggregateFunctionMatch
{
    const AggregateDescription * description = nullptr;
    DataTypes argument_types;
};

using AggregateFunctionMatches = std::vector<AggregateFunctionMatch>;

/// Here we try to match aggregate functions from the query to
/// aggregate functions from projection.
std::optional<AggregateFunctionMatches> matchAggregateFunctions(
    const AggregateProjectionInfo & info,
    const AggregateDescriptions & aggregates,
    const MatchedTrees::Matches & matches,
    const DAGIndex & query_index,
    const DAGIndex & proj_index)
{
    AggregateFunctionMatches res;

    /// Index (projection agg function name) -> pos
    std::unordered_map<std::string, std::vector<size_t>> projection_aggregate_functions;
    for (size_t i = 0; i < info.aggregates.size(); ++i)
        projection_aggregate_functions[info.aggregates[i].function->getName()].push_back(i);

    for (const auto & aggregate : aggregates)
    {
        /// Get a list of candidates by name first.
        auto it = projection_aggregate_functions.find(aggregate.function->getName());
        if (it == projection_aggregate_functions.end())
            return {};

        size_t num_args = aggregate.argument_names.size();

        DataTypes argument_types;
        argument_types.reserve(num_args);

        auto & candidates = it->second;
        bool found_match = false;

        for (size_t idx : candidates)
        {
            argument_types.clear();
            const auto & candidate = info.aggregates[idx];

            /// In some cases it's possible only to check that states are equal,
            /// e.g. for quantile(0.3)(...) and quantile(0.5)(...).
            ///
            /// Note we already checked that aggregate function names are equal,
            /// so that functions sum(...) and sumIf(...) with equal states will
            /// not match.
            if (!candidate.function->getStateType()->equals(*aggregate.function->getStateType()))
                continue;

            /// This is a special case for the function count().
            /// We can assume that 'count(expr) == count()' if expr is not nullable,
            /// which can be verified by simply casting to `AggregateFunctionCount *`.
            if (typeid_cast<const AggregateFunctionCount *>(aggregate.function.get())
                && typeid_cast<const AggregateFunctionCount *>(candidate.function.get()))
            {
                /// we can ignore arguments for count()
                found_match = true;
                res.push_back({&candidate, DataTypes()});
                break;
            }

            /// Now, function names and types matched.
            /// Next, match arguments from DAGs.

            if (num_args != candidate.argument_names.size())
                continue;

            size_t next_arg = 0;
            while (next_arg < num_args)
            {
                const auto & query_name = aggregate.argument_names[next_arg];
                const auto & proj_name = candidate.argument_names[next_arg];

                auto jt = query_index.find(query_name);
                auto kt = proj_index.find(proj_name);

                /// This should not happen ideally.
                if (jt == query_index.end() || kt == proj_index.end())
                    break;

                const auto * query_node = jt->second;
                const auto * proj_node = kt->second;

                auto mt = matches.find(query_node);
                if (mt == matches.end())
                    break;

                const auto & node_match = mt->second;
                if (node_match.node != proj_node || node_match.monotonicity)
                    break;

                argument_types.push_back(query_node->result_type);
                ++next_arg;
            }

            if (next_arg < aggregate.argument_names.size())
                continue;

            found_match = true;
            res.push_back({&candidate, std::move(argument_types)});
            break;
        }

        if (!found_match)
            return {};
    }

    return res;
}

static void appendAggregateFunctions(
    ActionsDAG & proj_dag,
    const AggregateDescriptions & aggregates,
    const AggregateFunctionMatches & matched_aggregates)
{
    std::unordered_map<const AggregateDescription *, const ActionsDAG::Node *> inputs;

    /// Just add all the aggregates to dag inputs.
    auto & proj_dag_outputs =  proj_dag.getOutputs();
    size_t num_aggregates = aggregates.size();
    for (size_t i = 0; i < num_aggregates; ++i)
    {
        const auto & aggregate = aggregates[i];
        const auto & match = matched_aggregates[i];
        auto type = std::make_shared<DataTypeAggregateFunction>(aggregate.function, match.argument_types, aggregate.parameters);

        auto & input = inputs[match.description];
        if (!input)
            input = &proj_dag.addInput(match.description->column_name, type);

        const auto * node = input;

        if (!DataTypeAggregateFunction::strictEquals(type, node->result_type))
            /// Cast to aggregate types specified in query if it's not
            /// strictly the same as the one specified in projection. This
            /// is required to generate correct results during finalization.
            node = &proj_dag.addCast(*node, type, aggregate.column_name);
        else if (node->result_name != aggregate.column_name)
            node = &proj_dag.addAlias(*node, aggregate.column_name);

        proj_dag_outputs.push_back(node);
    }
}

std::optional<ActionsDAG> analyzeAggregateProjection(
    const AggregateProjectionInfo & info,
    const QueryDAG & query,
    const DAGIndex & query_index,
    const Names & keys,
    const AggregateDescriptions & aggregates)
{
    auto proj_index = buildDAGIndex(*info.before_aggregation);

    MatchedTrees::Matches matches = matchTrees(info.before_aggregation->getOutputs(), *query.dag, false /* check_monotonicity */);
    auto matched_aggregates = matchAggregateFunctions(info, aggregates, matches, query_index, proj_index);
    if (!matched_aggregates)
        return {};

    ActionsDAG::NodeRawConstPtrs query_key_nodes;
    std::unordered_set<const ActionsDAG::Node *> proj_key_nodes;

    {
        /// Just, filling the set above.

        for (const auto & key : info.keys)
        {
            auto it = proj_index.find(key);
            /// This should not happen ideally.
            if (it == proj_index.end())
                return {};

            proj_key_nodes.insert(it->second);
        }

        query_key_nodes.reserve(keys.size() + 1);

        /// We need to add filter column to keys set.
        /// It should be computable from projection keys.
        /// It will be removed in FilterStep.
        if (query.filter_node)
            query_key_nodes.push_back(query.filter_node);

        for (const auto & key : keys)
        {
            auto it = query_index.find(key);
            /// This should not happen ideally.
            if (it == query_index.end())
                return {};

            query_key_nodes.push_back(it->second);
        }
    }

    /// Here we want to match query keys with projection keys.
    /// Query key can be any expression depending on projection keys.
    auto new_inputs = resolveMatchedInputs(matches, proj_key_nodes, query_key_nodes);
    if (!new_inputs)
        return {};

    auto proj_dag = ActionsDAG::foldActionsByProjection(*new_inputs, query_key_nodes);
    appendAggregateFunctions(proj_dag, aggregates, *matched_aggregates);
    return proj_dag;
}


/// Aggregate projection analysis result in case it can be applied.
struct AggregateProjectionCandidate : public ProjectionCandidate
{
    AggregateProjectionInfo info;

    /// Actions which need to be applied to columns from projection
    /// in order to get all the columns required for aggregation.
    ActionsDAG dag;
};

struct MinMaxProjectionCandidate
{
    AggregateProjectionCandidate candidate;
    Block block;
};

struct AggregateProjectionCandidates
{
    std::vector<AggregateProjectionCandidate> real;
    std::optional<MinMaxProjectionCandidate> minmax_projection;

    /// This flag means that DAG for projection candidate should be used in FilterStep.
    bool has_filter = false;

    /// If not empty, try to find exact ranges from parts to speed up trivial count queries.
    String only_count_column;
};

AggregateProjectionCandidates getAggregateProjectionCandidates(
    QueryPlan::Node & node,
    AggregatingStep & aggregating,
    ReadFromMergeTree & reading,
    const PartitionIdToMaxBlockPtr & max_added_blocks,
    bool allow_implicit_projections)
{
    const auto & keys = aggregating.getParams().keys;
    const auto & aggregates = aggregating.getParams().aggregates;
    const auto metadata = reading.getStorageMetadata();
    Block key_virtual_columns = reading.getMergeTreeData().getHeaderWithVirtualsForFilter(metadata);

    AggregateProjectionCandidates candidates;

    ContextPtr context = reading.getContext();

    const auto & projections = metadata->projections;
    std::vector<const ProjectionDescription *> agg_projections;

    for (const auto & projection : projections)
        if (projection.type == ProjectionDescription::Type::Aggregate)
            agg_projections.push_back(&projection);

    bool can_use_minmax_projection = allow_implicit_projections && metadata->minmax_count_projection
        && !reading.getMergeTreeData().has_lightweight_delete_parts.load();

    if (!can_use_minmax_projection && agg_projections.empty())
        return candidates;

    QueryDAG dag;
    if (!dag.build(*node.children.front()))
        return candidates;

    auto query_index = buildDAGIndex(*dag.dag);

    candidates.has_filter = dag.filter_node;
    /// We can't use minmax projection if filter has non-deterministic functions.
    if (dag.filter_node && !VirtualColumnUtils::isDeterministicInScopeOfQuery(dag.filter_node))
        can_use_minmax_projection = false;

    if (can_use_minmax_projection)
    {
        const auto * projection = &*(metadata->minmax_count_projection);
        auto info = getAggregatingProjectionInfo(*projection, context, metadata, key_virtual_columns);
        if (auto proj_dag = analyzeAggregateProjection(info, dag, query_index, keys, aggregates))
        {
            AggregateProjectionCandidate candidate{.info = std::move(info), .dag = std::move(*proj_dag)};

            auto block = reading.getMergeTreeData().getMinMaxCountProjectionBlock(
                metadata,
                candidate.dag.getRequiredColumnsNames(),
                (dag.filter_node ? &*dag.dag : nullptr),
                reading.getParts(),
                max_added_blocks.get(),
                context);

            // minmax_count_projection cannot be used when there is no data to process, because
            // it will produce incorrect result during constant aggregation.
            // See https://github.com/ClickHouse/ClickHouse/issues/36728
            if (block)
            {
                MinMaxProjectionCandidate minmax;
                minmax.candidate = std::move(candidate);
                minmax.block = std::move(block);
                minmax.candidate.projection = projection;
                candidates.minmax_projection.emplace(std::move(minmax));
            }
        }
        else
        {
            /// Trivial count optimization only applies after @can_use_minmax_projection.
            if (keys.empty() && aggregates.size() == 1 && typeid_cast<const AggregateFunctionCount *>(aggregates[0].function.get()))
                candidates.only_count_column = aggregates[0].column_name;
        }
    }

    if (!candidates.minmax_projection)
    {
        auto it = std::find_if(
            agg_projections.begin(),
            agg_projections.end(),
            [&](const auto * projection)
            { return projection->name == context->getSettingsRef()[Setting::preferred_optimize_projection_name].value; });

        if (it != agg_projections.end())
        {
            const ProjectionDescription * preferred_projection = *it;
            agg_projections.clear();
            agg_projections.push_back(preferred_projection);
        }

        candidates.real.reserve(agg_projections.size());
        for (const auto * projection : agg_projections)
        {
            auto info = getAggregatingProjectionInfo(*projection, context, metadata, key_virtual_columns);
            if (auto proj_dag = analyzeAggregateProjection(info, dag, query_index, keys, aggregates))
            {
                AggregateProjectionCandidate candidate{.info = std::move(info), .dag = std::move(*proj_dag)};
                candidate.projection = projection;
                candidates.real.emplace_back(std::move(candidate));
            }
        }
    }

    return candidates;
}

static QueryPlan::Node * findReadingStep(QueryPlan::Node & node)
{
    IQueryPlanStep * step = node.step.get();
    if (auto * /*reading*/ _ = typeid_cast<ReadFromMergeTree *>(step))
        return &node;

    if (node.children.size() != 1)
        return nullptr;

    if (typeid_cast<ExpressionStep *>(step) || typeid_cast<FilterStep *>(step))
        return findReadingStep(*node.children.front());

    return nullptr;
}

/// Pseudo projection name used to indicate exact count optimization
static constexpr const char * EXACT_COUNT_PROJECTION_NAME = "_exact_count_projection";

std::optional<String> optimizeUseAggregateProjections(QueryPlan::Node & node, QueryPlan::Nodes & nodes, bool allow_implicit_projections)
{
    if (node.children.size() != 1)
        return {};

    auto * aggregating = typeid_cast<AggregatingStep *>(node.step.get());
    if (!aggregating)
        return {};

    if (!aggregating->canUseProjection())
        return {};

    QueryPlan::Node * reading_node = findReadingStep(*node.children.front());
    if (!reading_node)
        return {};

    auto * reading = typeid_cast<ReadFromMergeTree *>(reading_node->step.get());
    if (!reading)
        return {};

    if (!canUseProjectionForReadingStep(reading))
        return {};

    PartitionIdToMaxBlockPtr max_added_blocks = getMaxAddedBlocks(reading);

    auto candidates = getAggregateProjectionCandidates(node, *aggregating, *reading, max_added_blocks, allow_implicit_projections);

    auto logger = getLogger("optimizeUseAggregateProjections");
    const auto & query_info = reading->getQueryInfo();
    const auto metadata = reading->getStorageMetadata();
    ContextPtr context = reading->getContext();
    MergeTreeDataSelectExecutor reader(reading->getMergeTreeData());
    AggregateProjectionCandidate * best_candidate = nullptr;

    /// Stores row count from exact ranges of parts.
    size_t exact_count = 0;
    ReadFromMergeTree::AnalysisResultPtr parent_reading_select_result;
    ReadFromMergeTree::AnalysisResultPtr inexact_ranges_select_result;
    if (candidates.minmax_projection)
    {
        best_candidate = &candidates.minmax_projection->candidate;
    }
    else if (!candidates.real.empty() || !candidates.only_count_column.empty())
    {
        parent_reading_select_result = reading->getAnalyzedResult();
        bool find_exact_ranges = !candidates.only_count_column.empty();
        if (!parent_reading_select_result || (!parent_reading_select_result->has_exact_ranges && find_exact_ranges))
            parent_reading_select_result = reading->selectRangesToRead(find_exact_ranges);

        bool force_optimize_projection = context->getSettingsRef()[Setting::force_optimize_projection];

        if (!force_optimize_projection)
        {
            /// /// Nothing to read. Ignore projections.
            if (parent_reading_select_result->parts_with_ranges.empty())
                return {};
        }

        /// Try to identify ranges that can be exactly counted using primary key analysis.
        if (!candidates.only_count_column.empty())
        {
            /// Copy parent analysis result to isolate modifications during exact count optimization. This result will
            /// store the remaining inexact ranges (not covered by exact analysis), to be used for for normal reading
            /// and count calculation.
            inexact_ranges_select_result = std::make_shared<ReadFromMergeTree::AnalysisResult>(*parent_reading_select_result);
            auto & parent_parts_with_ranges = inexact_ranges_select_result->parts_with_ranges;
            size_t exact_ranges_count = 0;
            size_t exact_ranges_parts = 0;
            size_t exact_ranges_marks = 0;

            for (auto & part_with_ranges : parent_parts_with_ranges)
            {
                MarkRanges new_ranges;
                auto & ranges = part_with_ranges.ranges;
                const auto & exact_ranges = part_with_ranges.exact_ranges;
                if (exact_ranges.empty())
                    continue;

                size_t i = 0;
                size_t len = exact_ranges.size();
                exact_ranges_count += len;
                exact_ranges_parts += 1;
                exact_ranges_marks += exact_ranges.getNumberOfMarks();
                for (auto & range : ranges)
                {
                    while (i < len && exact_ranges[i].begin < range.end)
                    {
                        chassert(exact_ranges[i].begin >= range.begin);
                        chassert(exact_ranges[i].end <= range.end);

                        /// Found some marks which are not exact
                        if (range.begin < exact_ranges[i].begin)
                            new_ranges.emplace_back(range.begin, exact_ranges[i].begin);

                        range.begin = exact_ranges[i].end;
                        inexact_ranges_select_result->selected_marks -= exact_ranges[i].end - exact_ranges[i].begin;
                        size_t range_rows = part_with_ranges.data_part->index_granularity->getRowsCountInRange(exact_ranges[i]);
                        inexact_ranges_select_result->selected_rows -= range_rows;
                        exact_count += range_rows;
                        ++i;
                    }

                    /// Current range still contains some marks which are not exact
                    if (range.begin < range.end)
                        new_ranges.emplace_back(range);
                }
                chassert(i == len);

                ssize_t range_diff = new_ranges.size() - part_with_ranges.ranges.size();
                inexact_ranges_select_result->selected_ranges += range_diff;
                part_with_ranges.ranges = std::move(new_ranges);
            }

            auto & stat = inexact_ranges_select_result->projection_stats.emplace_back();
            stat.name = EXACT_COUNT_PROJECTION_NAME;
            for (const auto & index_stat : inexact_ranges_select_result->index_stats)
            {
                if (index_stat.type == ReadFromMergeTree::IndexType::PrimaryKey)
                {
                    stat.condition = index_stat.condition;
                    stat.search_algorithm = index_stat.search_algorithm;
                }
            }

            stat.selected_parts = exact_ranges_parts;
            stat.selected_marks = exact_ranges_marks;
            stat.selected_ranges = exact_ranges_count;
            stat.selected_rows = exact_count;
            stat.filtered_parts
                = std::erase_if(parent_parts_with_ranges, [&](const auto & part_with_ranges) { return part_with_ranges.ranges.empty(); });

            stat.description = fmt::format(
                "Exact count optimization is applied: {} ranges with exact row count {}. Filtered parts: {}",
                stat.selected_ranges,
                stat.selected_rows,
                stat.filtered_parts);

            LOG_DEBUG(logger, "{}", stat.description);

            inexact_ranges_select_result->selected_parts = parent_parts_with_ranges.size();
            if (parent_parts_with_ranges.empty())
            {
                chassert(inexact_ranges_select_result->selected_marks == 0);
                chassert(inexact_ranges_select_result->selected_rows == 0);
                chassert(inexact_ranges_select_result->selected_ranges == 0);
            }
        }

        auto empty_mutations_snapshot = reading->getMutationsSnapshot()->cloneEmpty();

        /// If there are remaining parts to read, attempt to select the best candidate.
        if (!parent_reading_select_result->parts_with_ranges.empty() || force_optimize_projection)
        {
            for (auto & candidate : candidates.real)
            {
                auto required_column_names = candidate.dag.getRequiredColumnsNames();

                auto projection_query_info = query_info;
                projection_query_info.prewhere_info = nullptr;
                projection_query_info.filter_actions_dag = std::make_unique<ActionsDAG>(candidate.dag.clone());

                bool analyzed = analyzeProjectionCandidate(
                    candidate,
                    reader,
                    empty_mutations_snapshot,
                    required_column_names,
                    *parent_reading_select_result,
                    projection_query_info,
                    context);

                if (!analyzed)
                    continue;

                auto & stat = parent_reading_select_result->projection_stats.emplace_back();
                stat.name = candidate.projection->name;
                for (const auto & index_stat : candidate.merge_tree_projection_select_result_ptr->index_stats)
                {
                    if (index_stat.type == ReadFromMergeTree::IndexType::PrimaryKey)
                    {
                        stat.condition = index_stat.condition;
                        stat.search_algorithm = index_stat.search_algorithm;
                    }
                }
                stat.selected_parts = candidate.selected_parts;
                stat.selected_marks = candidate.selected_marks;
                stat.selected_ranges = candidate.selected_ranges;
                stat.selected_rows = candidate.selected_rows;
                stat.filtered_parts = candidate.filtered_parts;
                candidate.stat = &stat;

                size_t parent_reading_marks = parent_reading_select_result->selected_marks;
                if (candidate.sum_marks > parent_reading_marks)
                {
                    stat.description = fmt::format(
                        "Projection {} is usable but requires reading {} marks, which is not better than the original table with {} marks",
                        candidate.projection->name,
                        candidate.sum_marks,
                        parent_reading_marks);

                    LOG_DEBUG(logger, "{}", stat.description);
                    continue;
                }

                if (best_candidate == nullptr || best_candidate->sum_marks > candidate.sum_marks)
                    best_candidate = &candidate;
            }
        }

        /// No suitable projection found, and exact count optimization is not used.
        if (!best_candidate && exact_count == 0)
            return {};
    }
    else
    {
        return {};
    }

    /// Identify projections selected as the best candidates and update their stat descriptions with appropriate logging
    if (best_candidate)
    {
        for (const auto & candidate : candidates.real)
        {
            chassert(parent_reading_select_result);

            if (&candidate == best_candidate)
            {
                chassert(candidate.stat);
                chassert(candidate.stat->description.empty());
                candidate.stat->description = fmt::format(
                    "Projection {} is selected as the best with {} marks to read, while the original table requires scanning {} marks",
                    candidate.projection->name,
                    candidate.sum_marks,
                    parent_reading_select_result->selected_marks);
                LOG_DEBUG(logger, "{}", candidate.stat->description);
            }
            else if (candidate.stat && candidate.stat->description.empty())
            {
                candidate.stat->description = fmt::format(
                    "Projection {} is usable but requires reading {} marks, which is less efficient than projection {} with {} marks",
                    candidate.projection->name,
                    candidate.sum_marks,
                    best_candidate->projection->name,
                    best_candidate->sum_marks);
                LOG_DEBUG(logger, "{}", candidate.stat->description);
            }
        }
    }

    QueryPlanStepPtr projection_reading;
    bool has_parent_parts;
    String selected_projection_name;
    if (best_candidate)
        selected_projection_name = best_candidate->projection->name;

    /// Add reading from projection step.
    if (candidates.minmax_projection)
    {
        Pipe pipe(std::make_shared<SourceFromSingleChunk>(std::move(candidates.minmax_projection->block)));
        projection_reading = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
        has_parent_parts = false;
    }
    else if (best_candidate == nullptr)
    {
        /// Exact count optimization.
        chassert(exact_count > 0);
        chassert(inexact_ranges_select_result);

        auto agg_count = std::make_shared<AggregateFunctionCount>(DataTypes{});

        std::vector<char> state(agg_count->sizeOfData());
        AggregateDataPtr place = state.data();
        agg_count->create(place);
        SCOPE_EXIT_MEMORY_SAFE(agg_count->destroy(place));
        AggregateFunctionCount::set(place, exact_count);

        auto column = ColumnAggregateFunction::create(agg_count);
        column->insertFrom(place);

        Block block_with_count{
            {std::move(column),
             std::make_shared<DataTypeAggregateFunction>(agg_count, DataTypes{}, Array{}),
             candidates.only_count_column}};

        Pipe pipe(std::make_shared<SourceFromSingleChunk>(std::move(block_with_count)));
        projection_reading = std::make_unique<ReadFromPreparedSource>(std::move(pipe));

        selected_projection_name = EXACT_COUNT_PROJECTION_NAME;
        has_parent_parts = !inexact_ranges_select_result->parts_with_ranges.empty();
        if (has_parent_parts)
            reading->setAnalyzedResult(std::move(inexact_ranges_select_result));
    }
    else
    {
        chassert(parent_reading_select_result);
        auto storage_snapshot = reading->getStorageSnapshot();
        auto proj_snapshot = std::make_shared<StorageSnapshot>(storage_snapshot->storage, best_candidate->projection->metadata);
        auto projection_query_info = query_info;
        projection_query_info.prewhere_info = nullptr;
        projection_query_info.filter_actions_dag = nullptr;

        projection_reading = reader.readFromParts(
            /* parts = */ {},
            reading->getMutationsSnapshot()->cloneEmpty(),
            best_candidate->dag.getRequiredColumnsNames(),
            proj_snapshot,
            projection_query_info,
            context,
            reading->getMaxBlockSize(),
            reading->getNumStreams(),
            max_added_blocks,
            best_candidate->merge_tree_projection_select_result_ptr,
            reading->isParallelReadingEnabled());

        if (!projection_reading)
        {
            auto header = proj_snapshot->getSampleBlockForColumns(best_candidate->dag.getRequiredColumnsNames());
            Pipe pipe(std::make_shared<NullSource>(std::move(header)));
            projection_reading = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
        }

        /// Filter out parts in parent_ranges that overlap with those already read by the best candidate projection
        filterPartsByProjection(*parent_reading_select_result, best_candidate->parent_parts);
        has_parent_parts = !parent_reading_select_result->parts_with_ranges.empty();
    }

    if (!query_info.is_internal && context->hasQueryContext())
    {
        context->getQueryContext()->addQueryAccessInfo(Context::QualifiedProjectionName
        {
            .storage_id = reading->getMergeTreeData().getStorageID(),
            .projection_name = selected_projection_name,
        });
    }

    projection_reading->setStepDescription(selected_projection_name);
    auto & projection_reading_node = nodes.emplace_back(QueryPlan::Node{.step = std::move(projection_reading)});

    /// Root node of optimized child plan using @projection_name
    QueryPlan::Node * aggregate_projection_node = nullptr;

    if (best_candidate)
    {
        aggregate_projection_node = &nodes.emplace_back();

        if (candidates.has_filter)
        {
            const auto & result_name = best_candidate->dag.getOutputs().front()->result_name;
            aggregate_projection_node->step = std::make_unique<FilterStep>(
                projection_reading_node.step->getOutputHeader(),
                std::move(best_candidate->dag),
                result_name,
                true);
        }
        else
            aggregate_projection_node->step
                = std::make_unique<ExpressionStep>(projection_reading_node.step->getOutputHeader(), std::move(best_candidate->dag));

        aggregate_projection_node->children.push_back(&projection_reading_node);
    }
    else /// trivial count optimization
    {
        aggregate_projection_node = &projection_reading_node;
    }

    if (has_parent_parts)
    {
        node.step = aggregating->convertToAggregatingProjection(aggregate_projection_node->step->getOutputHeader());
        node.children.push_back(aggregate_projection_node);
    }
    else
    {
        /// All parts are taken from projection
        aggregating->requestOnlyMergeForAggregateProjection(aggregate_projection_node->step->getOutputHeader());
        node.children.front() = aggregate_projection_node;
    }

    return selected_projection_name;
}

}
