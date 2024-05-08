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

#include <Common/logger_useful.h>
#include <Storages/StorageDummy.h>
#include <Storages/VirtualColumnUtils.h>
#include <Planner/PlannerExpressionAnalysis.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/ProjectionsDescription.h>
#include <Parsers/queryToString.h>

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
    ActionsDAGPtr before_aggregation;
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
        Pipe(std::make_shared<SourceFromSingleChunk>(metadata_snapshot->getSampleBlock())),
        SelectQueryOptions{QueryProcessingStage::WithMergeableState}.ignoreASTOptimizations().ignoreSettingConstraints());

    const auto & analysis_result = interpreter.getAnalysisResult();
    const auto & query_analyzer = interpreter.getQueryAnalyzer();

    AggregateProjectionInfo info;
    info.context = interpreter.getContext();
    info.before_aggregation = analysis_result.before_aggregation;
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
        {
            // LOG_TRACE(
            //     getLogger("optimizeUseProjections"),
            //     "Cannot match agg func {} by name {}",
            //     aggregate.column_name, aggregate.function->getName());

            return {};
        }

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
            {
                // LOG_TRACE(getLogger("optimizeUseProjections"), "Cannot match agg func {} vs {} by state {} vs {}",
                //     aggregate.column_name, candidate.column_name,
                //     candidate.function->getStateType()->getName(), aggregate.function->getStateType()->getName());
                continue;
            }

            /// This is a special case for the function count().
            /// We can assume that 'count(expr) == count()' if expr is not nullable,
            /// which can be verified by simply casting to `AggregateFunctionCount *`.
            if (typeid_cast<const AggregateFunctionCount *>(aggregate.function.get()))
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
                {
                    // LOG_TRACE(
                    //     getLogger("optimizeUseProjections"),
                    //     "Cannot match agg func {} vs {} : can't match arg {} vs {} : no node in map",
                    //     aggregate.column_name, candidate.column_name, query_name, proj_name);

                    break;
                }

                const auto & node_match = mt->second;
                if (node_match.node != proj_node || node_match.monotonicity)
                {
                    // LOG_TRACE(
                    //     getLogger("optimizeUseProjections"),
                    //     "Cannot match agg func {} vs {} : can't match arg {} vs {} : no match or monotonicity",
                    //     aggregate.column_name, candidate.column_name, query_name, proj_name);

                    break;
                }

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

        if (node->result_name != aggregate.column_name)
        {
            if (DataTypeAggregateFunction::strictEquals(type, node->result_type))
            {
                node = &proj_dag.addAlias(*node, aggregate.column_name);
            }
            else
            {
                /// Cast to aggregate types specified in query if it's not
                /// strictly the same as the one specified in projection. This
                /// is required to generate correct results during finalization.
                node = &proj_dag.addCast(*node, type, aggregate.column_name);
            }
        }

        proj_dag_outputs.push_back(node);
    }
}

ActionsDAGPtr analyzeAggregateProjection(
    const AggregateProjectionInfo & info,
    const QueryDAG & query,
    const DAGIndex & query_index,
    const Names & keys,
    const AggregateDescriptions & aggregates)
{
    auto proj_index = buildDAGIndex(*info.before_aggregation);

    MatchedTrees::Matches matches = matchTrees(info.before_aggregation->getOutputs(), *query.dag, false /* check_monotonicity */);

    // for (const auto & [node, match] : matches)
    // {
    //     LOG_TRACE(getLogger("optimizeUseProjections"), "Match {} {} -> {} {} (with monotonicity : {})",
    //         static_cast<const void *>(node), node->result_name,
    //         static_cast<const void *>(match.node), (match.node ? match.node->result_name : ""), match.monotonicity != std::nullopt);
    // }

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

    struct Frame
    {
        const ActionsDAG::Node * node;
        size_t next_child_to_visit = 0;
    };

    std::stack<Frame> stack;
    std::unordered_set<const ActionsDAG::Node *> visited;
    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> new_inputs;

    for (const auto * key_node : query_key_nodes)
    {
        if (visited.contains(key_node))
            continue;

        stack.push({.node = key_node});

        while (!stack.empty())
        {
            auto & frame = stack.top();

            if (frame.next_child_to_visit == 0)
            {
                auto jt = matches.find(frame.node);
                if (jt != matches.end())
                {
                    auto & match = jt->second;
                    if (match.node && !match.monotonicity && proj_key_nodes.contains(match.node))
                    {
                        visited.insert(frame.node);
                        new_inputs[frame.node] = match.node;
                        stack.pop();
                        continue;
                    }
                }
            }

            if (frame.next_child_to_visit < frame.node->children.size())
            {
                stack.push({.node = frame.node->children[frame.next_child_to_visit]});
                ++frame.next_child_to_visit;
                continue;
            }

            /// Not a match and there is no matched child.
            if (frame.node->type == ActionsDAG::ActionType::INPUT)
            {
                // LOG_TRACE(getLogger("optimizeUseProjections"), "Cannot find match for {}", frame.node->result_name);
                return {};
            }

            /// Not a match, but all children matched.
            visited.insert(frame.node);
            stack.pop();
        }
    }

    // LOG_TRACE(getLogger("optimizeUseProjections"), "Folding actions by projection");

    auto proj_dag = query.dag->foldActionsByProjection(new_inputs, query_key_nodes);
    appendAggregateFunctions(*proj_dag, aggregates, *matched_aggregates);
    return proj_dag;
}


/// Aggregate projection analysis result in case it can be applied.
struct AggregateProjectionCandidate : public ProjectionCandidate
{
    AggregateProjectionInfo info;

    /// Actions which need to be applied to columns from projection
    /// in order to get all the columns required for aggregation.
    ActionsDAGPtr dag;
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
};

AggregateProjectionCandidates getAggregateProjectionCandidates(
    QueryPlan::Node & node,
    AggregatingStep & aggregating,
    ReadFromMergeTree & reading,
    const std::shared_ptr<PartitionIdToMaxBlock> & max_added_blocks,
    bool allow_implicit_projections)
{
    const auto & keys = aggregating.getParams().keys;
    const auto & aggregates = aggregating.getParams().aggregates;
    const auto metadata = reading.getStorageMetadata();
    Block key_virtual_columns = reading.getMergeTreeData().getHeaderWithVirtualsForFilter(metadata);

    AggregateProjectionCandidates candidates;

    const auto & parts = reading.getParts();
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

    // LOG_TRACE(getLogger("optimizeUseProjections"), "Has agg projection");

    QueryDAG dag;
    if (!dag.build(*node.children.front()))
        return candidates;

    auto query_index = buildDAGIndex(*dag.dag);

    // LOG_TRACE(getLogger("optimizeUseProjections"), "Query DAG: {}", dag.dag->dumpDAG());

    candidates.has_filter = dag.filter_node;
    /// We can't use minmax projection if filter has non-deterministic functions.
    if (dag.filter_node && !VirtualColumnUtils::isDeterministicInScopeOfQuery(dag.filter_node))
        can_use_minmax_projection = false;

    if (can_use_minmax_projection)
    {
        const auto * projection = &*(metadata->minmax_count_projection);
        // LOG_TRACE(getLogger("optimizeUseProjections"), "Try projection {}", projection->name);
        auto info = getAggregatingProjectionInfo(*projection, context, metadata, key_virtual_columns);
        // LOG_TRACE(getLogger("optimizeUseProjections"), "Projection DAG {}", info.before_aggregation->dumpDAG());
        if (auto proj_dag = analyzeAggregateProjection(info, dag, query_index, keys, aggregates))
        {
            // LOG_TRACE(getLogger("optimizeUseProjections"), "Projection analyzed DAG {}", proj_dag->dumpDAG());
            AggregateProjectionCandidate candidate{.info = std::move(info), .dag = std::move(proj_dag)};

            // LOG_TRACE(getLogger("optimizeUseProjections"), "Projection sample block {}", sample_block.dumpStructure());
            auto block = reading.getMergeTreeData().getMinMaxCountProjectionBlock(
                metadata,
                candidate.dag->getRequiredColumnsNames(),
                (dag.filter_node ? dag.dag : nullptr),
                parts,
                max_added_blocks.get(),
                context);

            // LOG_TRACE(getLogger("optimizeUseProjections"), "Projection sample block 2 {}", block.dumpStructure());

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
    }

    if (!candidates.minmax_projection)
    {
        auto it = std::find_if(agg_projections.begin(), agg_projections.end(), [&](const auto * projection)
        {
            return projection->name == context->getSettings().preferred_optimize_projection_name.value;
        });

        if (it != agg_projections.end())
        {
            const ProjectionDescription * preferred_projection = *it;
            agg_projections.clear();
            agg_projections.push_back(preferred_projection);
        }

        candidates.real.reserve(agg_projections.size());
        for (const auto * projection : agg_projections)
        {
            // LOG_TRACE(getLogger("optimizeUseProjections"), "Try projection {}", projection->name);
            auto info = getAggregatingProjectionInfo(*projection, context, metadata, key_virtual_columns);
            // LOG_TRACE(getLogger("optimizeUseProjections"), "Projection DAG {}", info.before_aggregation->dumpDAG());
            if (auto proj_dag = analyzeAggregateProjection(info, dag, query_index, keys, aggregates))
            {
                // LOG_TRACE(getLogger("optimizeUseProjections"), "Projection analyzed DAG {}", proj_dag->dumpDAG());
                AggregateProjectionCandidate candidate{.info = std::move(info), .dag = std::move(proj_dag)};
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
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
        return &node;

    if (node.children.size() != 1)
        return nullptr;

    if (typeid_cast<ExpressionStep *>(step) || typeid_cast<FilterStep *>(step))
        return findReadingStep(*node.children.front());

    return nullptr;
}

bool optimizeUseAggregateProjections(QueryPlan::Node & node, QueryPlan::Nodes & nodes, bool allow_implicit_projections)
{
    if (node.children.size() != 1)
        return false;

    auto * aggregating = typeid_cast<AggregatingStep *>(node.step.get());
    if (!aggregating)
        return false;

    if (!aggregating->canUseProjection())
        return false;

    QueryPlan::Node * reading_node = findReadingStep(*node.children.front());
    if (!reading_node)
        return false;

    auto * reading = typeid_cast<ReadFromMergeTree *>(reading_node->step.get());
    if (!reading)
        return false;

    if (!canUseProjectionForReadingStep(reading))
        return false;

    std::shared_ptr<PartitionIdToMaxBlock> max_added_blocks = getMaxAddedBlocks(reading);

    auto candidates = getAggregateProjectionCandidates(node, *aggregating, *reading, max_added_blocks, allow_implicit_projections);

    const auto & parts = reading->getParts();
    const auto & alter_conversions = reading->getAlterConvertionsForParts();
    const auto & query_info = reading->getQueryInfo();
    const auto metadata = reading->getStorageMetadata();
    ContextPtr context = reading->getContext();
    MergeTreeDataSelectExecutor reader(reading->getMergeTreeData());
    AggregateProjectionCandidate * best_candidate = nullptr;
    if (candidates.minmax_projection)
    {
        best_candidate = &candidates.minmax_projection->candidate;
    }
    else if (!candidates.real.empty())
    {
        auto ordinary_reading_select_result = reading->selectRangesToRead(parts, alter_conversions);
        size_t ordinary_reading_marks = ordinary_reading_select_result->selected_marks;

        /// Nothing to read. Ignore projections.
        if (ordinary_reading_marks == 0)
        {
            reading->setAnalyzedResult(std::move(ordinary_reading_select_result));
            return false;
        }

        const auto & parts_with_ranges = ordinary_reading_select_result->parts_with_ranges;

        /// Selecting best candidate.
        for (auto & candidate : candidates.real)
        {
            auto required_column_names = candidate.dag->getRequiredColumnsNames();

            bool analyzed = analyzeProjectionCandidate(
                candidate,
                *reading,
                reader,
                required_column_names,
                parts_with_ranges,
                query_info,
                context,
                max_added_blocks,
                candidate.dag);

            if (!analyzed)
                continue;

            if (candidate.sum_marks > ordinary_reading_marks)
                continue;

            if (best_candidate == nullptr || best_candidate->sum_marks > candidate.sum_marks)
                best_candidate = &candidate;
        }

        if (!best_candidate)
        {
            reading->setAnalyzedResult(std::move(ordinary_reading_select_result));
            return false;
        }
    }
    else
    {
        return false;
    }

    Context::QualifiedProjectionName projection_name;
    chassert(best_candidate != nullptr);

    QueryPlanStepPtr projection_reading;
    bool has_ordinary_parts;

    /// Add reading from projection step.
    if (candidates.minmax_projection)
    {
        // LOG_TRACE(getLogger("optimizeUseProjections"), "Minmax proj block {}",
        //           candidates.minmax_projection->block.dumpStructure());

        Pipe pipe(std::make_shared<SourceFromSingleChunk>(std::move(candidates.minmax_projection->block)));
        projection_reading = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
        has_ordinary_parts = false;

        projection_name = Context::QualifiedProjectionName
        {
            .storage_id = reading->getMergeTreeData().getStorageID(),
            .projection_name = candidates.minmax_projection->candidate.projection->name,
        };
    }
    else
    {
        auto storage_snapshot = reading->getStorageSnapshot();
        auto proj_snapshot = std::make_shared<StorageSnapshot>(storage_snapshot->storage, storage_snapshot->metadata);
        proj_snapshot->addProjection(best_candidate->projection);

        auto projection_query_info = query_info;
        projection_query_info.prewhere_info = nullptr;
        projection_query_info.filter_actions_dag = nullptr;

        projection_reading = reader.readFromParts(
            /* parts = */ {},
            /* alter_conversions = */ {},
            best_candidate->dag->getRequiredColumnsNames(),
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
            auto header = proj_snapshot->getSampleBlockForColumns(best_candidate->dag->getRequiredColumnsNames());
            Pipe pipe(std::make_shared<NullSource>(std::move(header)));
            projection_reading = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
        }

        projection_name = Context::QualifiedProjectionName
        {
            .storage_id = reading->getMergeTreeData().getStorageID(),
            .projection_name = best_candidate->projection->name,
        };

        has_ordinary_parts = best_candidate->merge_tree_ordinary_select_result_ptr != nullptr;
        if (has_ordinary_parts)
            reading->setAnalyzedResult(std::move(best_candidate->merge_tree_ordinary_select_result_ptr));
    }

    if (!query_info.is_internal && context->hasQueryContext())
    {
        context->getQueryContext()->addQueryAccessInfo(Context::QualifiedProjectionName
        {
            .storage_id = reading->getMergeTreeData().getStorageID(),
            .projection_name = best_candidate->projection->name,
        });
    }

    // LOG_TRACE(getLogger("optimizeUseProjections"), "Projection reading header {}",
    //           projection_reading->getOutputStream().header.dumpStructure());

    projection_reading->setStepDescription(best_candidate->projection->name);

    auto & projection_reading_node = nodes.emplace_back(QueryPlan::Node{.step = std::move(projection_reading)});
    auto & expr_or_filter_node = nodes.emplace_back();

    if (candidates.has_filter)
    {
        expr_or_filter_node.step = std::make_unique<FilterStep>(
            projection_reading_node.step->getOutputStream(),
            best_candidate->dag,
            best_candidate->dag->getOutputs().front()->result_name,
            true);
    }
    else
        expr_or_filter_node.step = std::make_unique<ExpressionStep>(
            projection_reading_node.step->getOutputStream(),
            best_candidate->dag);

    expr_or_filter_node.children.push_back(&projection_reading_node);

    if (!has_ordinary_parts)
    {
        /// All parts are taken from projection
        aggregating->requestOnlyMergeForAggregateProjection(expr_or_filter_node.step->getOutputStream());
        node.children.front() = &expr_or_filter_node;
    }
    else
    {
        node.step = aggregating->convertToAggregatingProjection(expr_or_filter_node.step->getOutputStream());
        node.children.push_back(&expr_or_filter_node);
    }

    return true;
}

}
