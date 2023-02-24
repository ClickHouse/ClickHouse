#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionsLogical.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/logger_useful.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Sources/NullSource.h>
#include <Parsers/queryToString.h>
#include <stack>

namespace DB::QueryPlanOptimizations
{

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

/// This is a common DAG which is a merge of DAGs from Filter and Expression steps chain.
/// Additionally, for all the Filter steps, we collect filter conditions into filter_nodes.
/// Flag remove_last_filter_node is set in case if the last step is a Filter step and it should remove filter column.
struct QueryDAG
{
    ActionsDAGPtr dag;
    ActionsDAG::NodeRawConstPtrs filter_nodes;
    bool remove_last_filter_node = false;

    bool build(QueryPlan::Node & node);

private:
    void appendExpression(const ActionsDAGPtr & expression)
    {
        if (dag)
            dag->mergeInplace(std::move(*expression->clone()));
        else
            dag = expression->clone();
    }
};

bool QueryDAG::build(QueryPlan::Node & node)
{
    IQueryPlanStep * step = node.step.get();
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
    {
        if (const auto * prewhere_info = reading->getPrewhereInfo())
        {
            if (prewhere_info->row_level_filter)
            {
                remove_last_filter_node = false;
                appendExpression(prewhere_info->row_level_filter);
                if (const auto * filter_node = dag->tryFindInOutputs(prewhere_info->row_level_column_name))
                    filter_nodes.push_back(filter_node);
                else
                    return false;
            }

            if (prewhere_info->prewhere_actions)
            {
                remove_last_filter_node = prewhere_info->remove_prewhere_column;
                appendExpression(prewhere_info->prewhere_actions);
                if (const auto * filter_node = dag->tryFindInOutputs(prewhere_info->prewhere_column_name))
                    filter_nodes.push_back(filter_node);
                else
                    return false;
            }
        }
        return true;
    }

    if (node.children.size() != 1)
        return false;

    if (!build(*node.children.front()))
        return false;

    if (auto * expression = typeid_cast<ExpressionStep *>(step))
    {
        const auto & actions = expression->getExpression();
        if (actions->hasArrayJoin())
            return false;

        appendExpression(actions);
        remove_last_filter_node = false;
        return true;
    }

    if (auto * filter = typeid_cast<FilterStep *>(step))
    {
        const auto & actions = filter->getExpression();
        if (actions->hasArrayJoin())
            return false;

        appendExpression(actions);
        remove_last_filter_node = filter->removesFilterColumn();
        const auto * filter_expression = dag->tryFindInOutputs(filter->getFilterColumnName());
        if (!filter_expression)
            return false;

        filter_nodes.push_back(filter_expression);
        return true;
    }

    return false;
}

bool canUseProjectionForReadingStep(ReadFromMergeTree * reading)
{
    /// Probably some projection already was applied.
    if (reading->hasAnalyzedResult())
        return false;

    if (reading->isQueryWithFinal())
        return false;

    if (reading->isQueryWithSampling())
        return false;

    if (reading->isParallelReadingEnabled())
        return false;

    // Currently projection don't support deduplication when moving parts between shards.
    if (reading->getContext()->getSettingsRef().allow_experimental_query_deduplication)
        return false;

    return true;
}


/// Required analysis info from aggregate projection.
struct AggregateProjectionInfo
{
    ActionsDAGPtr before_aggregation;
    NamesAndTypesList keys;
    AggregateDescriptions aggregates;

    /// A context copy from interpreter which was used for analysis.
    /// Just in case it is used by some function.
    ContextPtr context;
};

struct ProjectionCandidate
{
    const ProjectionDescription * projection;

    /// The number of marks we are going to read
    size_t sum_marks = 0;

    /// Analysis result, separate for parts with and without projection.
    /// Analysis is done in order to estimate the number of marks we are going to read.
    /// For chosen projection, it is reused for reading step.
    MergeTreeDataSelectAnalysisResultPtr merge_tree_projection_select_result_ptr;
    MergeTreeDataSelectAnalysisResultPtr merge_tree_normal_select_result_ptr;
};

/// Aggregate projection analysis result in case it can be applied.
struct AggregateProjectionCandidate : public ProjectionCandidate
{
    AggregateProjectionInfo info;

    /// Actions which need to be applied to columns from projection
    /// in order to get all the columns required for aggregation.
    ActionsDAGPtr dag;
};

/// Normal projection analysis result in case it can be applied.
/// For now, it is empty.
/// Normal projection can be used only if it contains all required source columns.
/// It would not be hard to support pre-computed expressions and filtration.
struct NormalProjectionCandidate : public ProjectionCandidate
{
};

/// Get required info from aggregate projection.
/// Ideally, this should be pre-calculated and stored inside ProjectionDescription.
static AggregateProjectionInfo getAggregatingProjectionInfo(
    const ProjectionDescription & projection,
    const ContextPtr & context,
    const StorageMetadataPtr & metadata_snapshot,
    const Block & key_virtual_columns)
{
    // LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Projection query : {}", queryToString(projection.query_ast));

    /// This is a bad approach.
    /// We'd better have a separate interpreter for projections.
    /// Now it's not obvious we didn't miss anything here.
    InterpreterSelectQuery interpreter(
        projection.query_ast,
        context,
        Pipe(std::make_shared<SourceFromSingleChunk>(metadata_snapshot->getSampleBlock())),
        SelectQueryOptions{QueryProcessingStage::WithMergeableState});

    const auto & analysis_result = interpreter.getAnalysisResult();
    const auto & query_analyzer = interpreter.getQueryAnalyzer();

    AggregateProjectionInfo info;
    info.context = interpreter.getContext();
    info.before_aggregation = analysis_result.before_aggregation;
    info.keys = query_analyzer->aggregationKeys();
    info.aggregates = query_analyzer->aggregates();

    /// Add part/partition virtual columns to projection aggregation keys.
    /// We can do it because projection is stored for every part separately.
    for (const auto & virt_column : key_virtual_columns)
    {
        const auto * input = &info.before_aggregation->addInput(virt_column);
        info.before_aggregation->getOutputs().push_back(input);
        info.keys.push_back(NameAndTypePair{virt_column.name, virt_column.type});
    }

    return info;
}

using DAGIndex = std::unordered_map<std::string_view, const ActionsDAG::Node *>;
static DAGIndex buildDAGIndex(const ActionsDAG & dag)
{
    DAGIndex index;
    for (const auto * output : dag.getOutputs())
        index.emplace(output->result_name, output);

    return index;
}

static bool hasNullableOrMissingColumn(const DAGIndex & index, const Names & names)
{
    for (const auto & query_name : names)
    {
        auto jt = index.find(query_name);
        if (jt == index.end() || jt->second->result_type->isNullable())
            return true;
    }

    return false;
}

/// Here we try to match aggregate functions from the query to
/// aggregate functions from projection.
bool areAggregatesMatch(
    const AggregateProjectionInfo & info,
    const AggregateDescriptions & aggregates,
    const MatchedTrees::Matches & matches,
    const DAGIndex & query_index,
    const DAGIndex & proj_index)
{
    /// Index (projection agg function name) -> pos
    std::unordered_map<std::string, std::list<size_t>> projection_aggregate_functions;
    for (size_t i = 0; i < info.aggregates.size(); ++i)
        projection_aggregate_functions[info.aggregates[i].function->getName()].push_back(i);

    for (const auto & aggregate : aggregates)
    {
        /// Get a list of candidates by name first.
        auto it = projection_aggregate_functions.find(aggregate.function->getName());
        if (it == projection_aggregate_functions.end())
        {
            // LOG_TRACE(
            //     &Poco::Logger::get("optimizeUseProjections"),
            //     "Cannot match agg func {} by name {}",
            //     aggregate.column_name, aggregate.function->getName());

            return false;
        }

        auto & candidates = it->second;
        bool found_match = false;

        for (size_t idx : candidates)
        {
            const auto & candidate = info.aggregates[idx];

            /// Note: this check is a bit strict.
            /// We check that aggregate function names, argument types and parameters are equal.
            /// In some cases it's possible only to check that states are equal,
            /// e.g. for quantile(0.3)(...) and quantile(0.5)(...).
            /// But also functions sum(...) and sumIf(...) will have equal states,
            /// and we can't replace one to another from projection.
            if (!candidate.function->getStateType()->equals(*aggregate.function->getStateType()))
            {
                LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Cannot match agg func {} vs {} by state {} vs {}",
                    aggregate.column_name, candidate.column_name,
                    candidate.function->getStateType()->getName(), aggregate.function->getStateType()->getName());
                continue;
            }

            /// This is a special case for the function count().
            /// We can assume that 'count(expr) == count()' if expr is not nullable.
            if (typeid_cast<const AggregateFunctionCount *>(candidate.function.get()))
            {
                bool has_nullable_or_missing_arg = false;
                has_nullable_or_missing_arg |= hasNullableOrMissingColumn(query_index, aggregate.argument_names);
                has_nullable_or_missing_arg |= hasNullableOrMissingColumn(proj_index, candidate.argument_names);

                if (!has_nullable_or_missing_arg)
                {
                    /// we can ignore arguments for count()
                    found_match = true;
                    break;
                }
            }

            /// Now, function names and types matched.
            /// Next, match arguments from DAGs.

            size_t num_args = aggregate.argument_names.size();
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
                    //     &Poco::Logger::get("optimizeUseProjections"),
                    //     "Cannot match agg func {} vs {} : can't match arg {} vs {} : no node in map",
                    //     aggregate.column_name, candidate.column_name, query_name, proj_name);

                    break;
                }

                const auto & node_match = mt->second;
                if (node_match.node != proj_node || node_match.monotonicity)
                {
                    // LOG_TRACE(
                    //     &Poco::Logger::get("optimizeUseProjections"),
                    //     "Cannot match agg func {} vs {} : can't match arg {} vs {} : no match or monotonicity",
                    //     aggregate.column_name, candidate.column_name, query_name, proj_name);

                    break;
                }

                ++next_arg;
            }

            if (next_arg < aggregate.argument_names.size())
                continue;

            found_match = true;
            break;
        }

        if (!found_match)
            return false;
    }

    return true;
}

struct AggregateQueryDAG
{
    ActionsDAGPtr dag;
    const ActionsDAG::Node * filter_node = nullptr;

    bool build(QueryPlan::Node & node)
    {
        QueryDAG query;
        if (!query.build(node))
            return false;

        dag = std::move(query.dag);
        auto filter_nodes = std::move(query.filter_nodes);

        if (!filter_nodes.empty())
        {
            filter_node = filter_nodes.front();
            if (filter_nodes.size() > 1)
            {
                FunctionOverloadResolverPtr func_builder_and =
                    std::make_unique<FunctionToOverloadResolverAdaptor>(
                        std::make_shared<FunctionAnd>());

                filter_node = &dag->addFunction(func_builder_and, std::move(filter_nodes), {});
            }

            dag->getOutputs().push_back(filter_node);
        }

        return true;
    }
};

struct NormalQueryDAG
{
    ActionsDAGPtr dag;
    bool need_remove_column = false;
    const ActionsDAG::Node * filter_node = nullptr;

    bool build(QueryPlan::Node & node)
    {
        QueryDAG query;
        if (!query.build(node))
            return false;

        dag = std::move(query.dag);
        auto filter_nodes = std::move(query.filter_nodes);
        need_remove_column = query.remove_last_filter_node;

        if (!filter_nodes.empty())
        {
            auto & outputs = dag->getOutputs();
            filter_node = filter_nodes.back();

            if (filter_nodes.size() > 1)
            {
                /// Add a conjunction of all the filters.
                if (need_remove_column)
                {
                    /// Last filter column is not needed; remove it right here
                    size_t pos = 0;
                    while (pos < outputs.size() && outputs[pos] != filter_node)
                        ++pos;

                    if (pos < outputs.size())
                        outputs.erase(outputs.begin() + pos);
                }
                else
                {
                    /// Last filter is needed; we must replace it to constant 1,
                    /// As well as FilterStep does to make a compatible header.
                    for (auto & output : outputs)
                    {
                        if (output == filter_node)
                        {
                            ColumnWithTypeAndName col;
                            col.name = filter_node->result_name;
                            col.type = filter_node->result_type;
                            col.column = col.type->createColumnConst(1, 1);
                            output = &dag->addColumn(std::move(col));
                        }
                    }
                }

                FunctionOverloadResolverPtr func_builder_and =
                    std::make_unique<FunctionToOverloadResolverAdaptor>(
                        std::make_shared<FunctionAnd>());

                filter_node = &dag->addFunction(func_builder_and, std::move(filter_nodes), {});
                outputs.insert(outputs.begin(), filter_node);
                need_remove_column = true;
            }
        }

        if (dag)
        {
            dag->removeUnusedActions();
            // LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Header {}, Query DAG: {}", header.dumpStructure(), dag->dumpDAG());
        }

        return true;
    }
};

ActionsDAGPtr analyzeAggregateProjection(
    const AggregateProjectionInfo & info,
    const AggregateQueryDAG & query,
    const Names & keys,
    const AggregateDescriptions & aggregates)
{
    auto query_index = buildDAGIndex(*query.dag);
    auto proj_index = buildDAGIndex(*info.before_aggregation);

    MatchedTrees::Matches matches = matchTrees(*info.before_aggregation, *query.dag);

    // for (const auto & [node, match] : matches)
    // {
    //     LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Match {} {} -> {} {} (with monotonicity : {})",
    //         static_cast<const void *>(node), node->result_name,
    //         static_cast<const void *>(match.node), (match.node ? match.node->result_name : ""), match.monotonicity != std::nullopt);
    // }

    if (!areAggregatesMatch(info, aggregates, matches, query_index, proj_index))
        return {};

    ActionsDAG::NodeRawConstPtrs query_key_nodes;
    std::unordered_set<const ActionsDAG::Node *> proj_key_nodes;

    {
        /// Just, filling the set above.

        for (const auto & key : info.keys)
        {
            auto it = proj_index.find(key.name);
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
    std::unordered_map<const ActionsDAG::Node *, std::string> new_inputs;

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
                        new_inputs[frame.node] = match.node->result_name;
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
                // LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Cannot find match for {}", frame.node->result_name);
                return {};
            }

            /// Not a match, but all children matched.
            visited.insert(frame.node);
            stack.pop();
        }
    }

    // LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Folding actions by projection");

    auto proj_dag = query.dag->foldActionsByProjection(new_inputs, query_key_nodes);

    /// Just add all the aggregates to dag inputs.
    auto & proj_dag_outputs =  proj_dag->getOutputs();
    for (const auto & aggregate : aggregates)
        proj_dag_outputs.push_back(&proj_dag->addInput(aggregate.column_name, aggregate.function->getResultType()));

    return proj_dag;
}

struct MinMaxProjectionCandidate
{
    AggregateProjectionCandidate candidate;
    Block minmax_count_projection_block;
    MergeTreeData::DataPartsVector minmax_projection_normal_parts;
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
    const std::shared_ptr<PartitionIdToMaxBlock> & max_added_blocks)
{
    const auto & keys = aggregating.getParams().keys;
    const auto & aggregates = aggregating.getParams().aggregates;
    Block key_virtual_columns = reading.getMergeTreeData().getSampleBlockWithVirtualColumns();

    AggregateProjectionCandidates candidates;

    const auto & parts = reading.getParts();
    const auto & query_info = reading.getQueryInfo();

    const auto metadata = reading.getStorageMetadata();
    ContextPtr context = reading.getContext();

    const auto & projections = metadata->projections;
    std::vector<const ProjectionDescription *> agg_projections;
    for (const auto & projection : projections)
        if (projection.type == ProjectionDescription::Type::Aggregate)
            agg_projections.push_back(&projection);

    bool can_use_minmax_projection = metadata->minmax_count_projection && !reading.getMergeTreeData().has_lightweight_delete_parts.load();

    if (!can_use_minmax_projection && agg_projections.empty())
        return candidates;

    // LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Has agg projection");

    AggregateQueryDAG dag;
    if (!dag.build(*node.children.front()))
        return candidates;

    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Query DAG: {}", dag.dag->dumpDAG());

    candidates.has_filter = dag.filter_node;

    if (can_use_minmax_projection)
    {
        const auto * projection = &*(metadata->minmax_count_projection);
        // LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Try projection {}", projection->name);
        auto info = getAggregatingProjectionInfo(*projection, context, metadata, key_virtual_columns);
        // LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Projection DAG {}", info.before_aggregation->dumpDAG());
        if (auto proj_dag = analyzeAggregateProjection(info, dag, keys, aggregates))
        {
            // LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Projection analyzed DAG {}", proj_dag->dumpDAG());
            AggregateProjectionCandidate candidate{.info = std::move(info), .dag = std::move(proj_dag)};
            MergeTreeData::DataPartsVector minmax_projection_normal_parts;

            auto block = reading.getMergeTreeData().getMinMaxCountProjectionBlock(
                metadata,
                candidate.dag->getRequiredColumnsNames(),
                dag.filter_node != nullptr,
                query_info,
                parts,
                minmax_projection_normal_parts,
                max_added_blocks.get(),
                context);

            if (block)
            {
                MinMaxProjectionCandidate minmax;
                minmax.candidate = std::move(candidate);
                minmax.minmax_count_projection_block = std::move(block);
                minmax.minmax_projection_normal_parts = std::move(minmax_projection_normal_parts);
                minmax.candidate.projection = projection;
                candidates.minmax_projection.emplace(std::move(minmax));
            }
        }
    }

    if (!candidates.minmax_projection)
    {
        candidates.real.reserve(agg_projections.size());
        for (const auto * projection : agg_projections)
        {
            // LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Try projection {}", projection->name);
            auto info = getAggregatingProjectionInfo(*projection, context, metadata, key_virtual_columns);
            // LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Projection DAG {}", info.before_aggregation->dumpDAG());
            if (auto proj_dag = analyzeAggregateProjection(info, dag, keys, aggregates))
            {
                // LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Projection analyzed DAG {}", proj_dag->dumpDAG());
                AggregateProjectionCandidate candidate{.info = std::move(info), .dag = std::move(proj_dag)};
                candidate.projection = projection;
                candidates.real.emplace_back(std::move(candidate));
            }
        }
    }

    return candidates;
}

bool optimizeUseAggProjections(QueryPlan::Node & node, QueryPlan::Nodes & nodes)
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

    const auto metadata = reading->getStorageMetadata();
    ContextPtr context = reading->getContext();

    std::shared_ptr<PartitionIdToMaxBlock> max_added_blocks;
    if (context->getSettingsRef().select_sequential_consistency)
    {
        if (const StorageReplicatedMergeTree * replicated = dynamic_cast<const StorageReplicatedMergeTree *>(&reading->getMergeTreeData()))
            max_added_blocks = std::make_shared<PartitionIdToMaxBlock>(replicated->getMaxAddedBlocks());
    }

    auto candidates = getAggregateProjectionCandidates(node, *aggregating, *reading, max_added_blocks);

    AggregateProjectionCandidate * best_candidate = nullptr;
    if (candidates.minmax_projection)
        best_candidate = &candidates.minmax_projection->candidate;
    else if (candidates.real.empty())
        return false;

    MergeTreeDataSelectExecutor reader(reading->getMergeTreeData());
    const auto & parts = reading->getParts();
    const auto & query_info = reading->getQueryInfo();

    for (auto & candidate : candidates.real)
    {
        MergeTreeData::DataPartsVector projection_parts;
        MergeTreeData::DataPartsVector normal_parts;
        for (const auto & part : parts)
        {
            const auto & created_projections = part->getProjectionParts();
            auto it = created_projections.find(candidate.projection->name);
            if (it != created_projections.end())
                projection_parts.push_back(it->second);
            else
                normal_parts.push_back(part);
        }

        if (projection_parts.empty())
            continue;

        ActionDAGNodes added_filter_nodes;
        if (candidates.has_filter)
            added_filter_nodes.nodes.push_back(candidate.dag->getOutputs().front());

        auto projection_result_ptr = reader.estimateNumMarksToRead(
            std::move(projection_parts),
            nullptr,
            candidate.dag->getRequiredColumnsNames(),
            metadata,
            candidate.projection->metadata,
            query_info, /// How it is actually used? I hope that for index we need only added_filter_nodes
            added_filter_nodes,
            context,
            context->getSettingsRef().max_threads,
            max_added_blocks);

        if (projection_result_ptr->error())
            continue;

        candidate.merge_tree_projection_select_result_ptr = std::move(projection_result_ptr);
        candidate.sum_marks += candidate.merge_tree_projection_select_result_ptr->marks();

        if (!normal_parts.empty())
        {
            auto normal_result_ptr = reading->selectRangesToRead(std::move(normal_parts));

            if (normal_result_ptr->error())
                continue;

            if (normal_result_ptr->marks() != 0)
            {
                candidate.sum_marks += normal_result_ptr->marks();
                candidate.merge_tree_normal_select_result_ptr = std::move(normal_result_ptr);
            }
        }

        if (best_candidate == nullptr || best_candidate->sum_marks > candidate.sum_marks)
            best_candidate = &candidate;
    }

    if (!best_candidate)
        return false;

    QueryPlanStepPtr projection_reading;
    bool has_nornal_parts;

    if (candidates.minmax_projection)
    {
        LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Minmax proj block {}", candidates.minmax_projection->minmax_count_projection_block.dumpStructure());

        Pipe pipe(std::make_shared<SourceFromSingleChunk>(std::move(candidates.minmax_projection->minmax_count_projection_block)));
        projection_reading = std::make_unique<ReadFromPreparedSource>(std::move(pipe));

        has_nornal_parts = !candidates.minmax_projection->minmax_projection_normal_parts.empty();
        if (has_nornal_parts)
            reading->resetParts(std::move(candidates.minmax_projection->minmax_projection_normal_parts));
    }
    else
    {
        auto storage_snapshot = reading->getStorageSnapshot();
        auto proj_snapshot = std::make_shared<StorageSnapshot>(
            storage_snapshot->storage, storage_snapshot->metadata, storage_snapshot->object_columns); //, storage_snapshot->data);
        proj_snapshot->addProjection(best_candidate->projection);

        LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Proj snapshot {}", proj_snapshot->getColumns(GetColumnsOptions::Kind::All).toString());

        auto query_info_copy = query_info;
        query_info_copy.prewhere_info = nullptr;

        projection_reading = reader.readFromParts(
            {},
            best_candidate->dag->getRequiredColumnsNames(),
            proj_snapshot,
            query_info_copy,
            context,
            reading->getMaxBlockSize(),
            reading->getNumStreams(),
            max_added_blocks,
            best_candidate->merge_tree_projection_select_result_ptr,
            reading->isParallelReadingEnabled());

        if (!projection_reading)
        {
            Pipe pipe(std::make_shared<NullSource>(proj_snapshot->getSampleBlockForColumns(best_candidate->dag->getRequiredColumnsNames())));
            projection_reading = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
        }

        has_nornal_parts = best_candidate->merge_tree_normal_select_result_ptr != nullptr;
        if (has_nornal_parts)
            reading->setAnalyzedResult(std::move(best_candidate->merge_tree_normal_select_result_ptr));
    }

    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Projection reading header {}", projection_reading->getOutputStream().header.dumpStructure());

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

    if (!has_nornal_parts)
    {
        /// All parts are taken from projection

        LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Expr stream {}", expr_or_filter_node.step->getOutputStream().header.dumpStructure());
        aggregating->requestOnlyMergeForAggregateProjection(expr_or_filter_node.step->getOutputStream());
        node.children.front() = &expr_or_filter_node;

        //optimizeAggregationInOrder(node, nodes);
    }
    else
    {
        node.step = aggregating->convertToAggregatingProjection(expr_or_filter_node.step->getOutputStream());
        node.children.push_back(&expr_or_filter_node);
    }

    return true;
}

ActionsDAGPtr makeMaterializingDAG(const Block & proj_header, const Block main_header)
{
    /// Materialize constants in case we don't have it in output header.
    /// This may happen e.g. if we have PREWHERE.

    size_t num_columns = main_header.columns();
    /// This is a error; will have block structure mismatch later.
    if (proj_header.columns() != num_columns)
        return nullptr;

    std::vector<size_t> const_positions;
    for (size_t i = 0; i < num_columns; ++i)
    {
        auto col_proj = proj_header.getByPosition(i).column;
        auto col_main = main_header.getByPosition(i).column;
        bool is_proj_const = col_proj && isColumnConst(*col_proj);
        bool is_main_proj = col_main && isColumnConst(*col_main);
        if (is_proj_const && !is_main_proj)
            const_positions.push_back(i);
    }

    if (const_positions.empty())
        return nullptr;

    ActionsDAGPtr dag = std::make_unique<ActionsDAG>(proj_header.getColumnsWithTypeAndName());
    for (auto pos : const_positions)
    {
        auto & output = dag->getOutputs()[pos];
        output = &dag->materializeNode(*output);
    }

    return dag;
}


bool optimizeUseNormalProjections(Stack & stack, QueryPlan::Nodes & nodes)
{
    const auto & frame = stack.back();

    auto * reading = typeid_cast<ReadFromMergeTree *>(frame.node->step.get());
    if (!reading)
        return false;

    if (!canUseProjectionForReadingStep(reading))
        return false;

    auto iter = stack.rbegin();
    while (iter != stack.rend())
    {
        auto next = std::next(iter);
        if (next == stack.rend())
            break;

        if (!typeid_cast<FilterStep *>(next->node->step.get()) &&
            !typeid_cast<ExpressionStep *>(next->node->step.get()))
            break;

        iter = next;
    }

    if (iter == stack.rbegin())
        return false;

    const auto metadata = reading->getStorageMetadata();
    const auto & projections = metadata->projections;

    std::vector<const ProjectionDescription *> normal_projections;
    for (const auto & projection : projections)
        if (projection.type == ProjectionDescription::Type::Normal)
            normal_projections.push_back(&projection);

    if (normal_projections.empty())
        return false;

    NormalQueryDAG query;
    {
        if (!query.build(*iter->node->children.front()))
            return false;
    }

    std::list<NormalProjectionCandidate> candidates;
    NormalProjectionCandidate * best_candidate = nullptr;

    const Names & required_columns = reading->getRealColumnNames();
    const auto & parts = reading->getParts();
    const auto & query_info = reading->getQueryInfo();
    ContextPtr context = reading->getContext();
    MergeTreeDataSelectExecutor reader(reading->getMergeTreeData());

    auto ordinary_reading_select_result = reading->selectRangesToRead(parts);
    size_t ordinary_reading_marks = ordinary_reading_select_result->marks();

    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Marks for ordinary reading {}", ordinary_reading_marks);

    std::shared_ptr<PartitionIdToMaxBlock> max_added_blocks;
    if (context->getSettingsRef().select_sequential_consistency)
    {
        if (const StorageReplicatedMergeTree * replicated = dynamic_cast<const StorageReplicatedMergeTree *>(&reading->getMergeTreeData()))
            max_added_blocks = std::make_shared<PartitionIdToMaxBlock>(replicated->getMaxAddedBlocks());
    }

    for (const auto * projection : normal_projections)
    {
        bool has_all_columns = true;
        for (const auto & col : required_columns)
        {
            if (!projection->sample_block.has(col))
            {
                has_all_columns = false;
                break;
            }
        }

        if (!has_all_columns)
            continue;

        MergeTreeData::DataPartsVector projection_parts;
        MergeTreeData::DataPartsVector normal_parts;
        for (const auto & part : parts)
        {
            const auto & created_projections = part->getProjectionParts();
            auto it = created_projections.find(projection->name);
            if (it != created_projections.end())
                projection_parts.push_back(it->second);
            else
                normal_parts.push_back(part);
        }

        if (projection_parts.empty())
            continue;

        ActionDAGNodes added_filter_nodes;
        if (query.filter_node)
            added_filter_nodes.nodes.push_back(query.filter_node);

        auto projection_result_ptr = reader.estimateNumMarksToRead(
            std::move(projection_parts),
            nullptr,
            required_columns,
            metadata,
            projection->metadata,
            query_info, /// How it is actually used? I hope that for index we need only added_filter_nodes
            added_filter_nodes,
            context,
            context->getSettingsRef().max_threads,
            max_added_blocks);

        if (projection_result_ptr->error())
            continue;

        auto & candidate = candidates.emplace_back();
        candidate.projection = projection;
        candidate.merge_tree_projection_select_result_ptr = std::move(projection_result_ptr);
        candidate.sum_marks += candidate.merge_tree_projection_select_result_ptr->marks();

        if (!normal_parts.empty())
        {
            auto normal_result_ptr = reading->selectRangesToRead(std::move(normal_parts));

            if (normal_result_ptr->error())
                continue;

            if (normal_result_ptr->marks() != 0)
            {
                candidate.sum_marks += normal_result_ptr->marks();
                candidate.merge_tree_normal_select_result_ptr = std::move(normal_result_ptr);
            }
        }

        LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Marks for projection {} {}", projection->name ,candidate.sum_marks);
        if (candidate.sum_marks < ordinary_reading_marks && (best_candidate == nullptr || candidate.sum_marks < best_candidate->sum_marks))
            best_candidate = &candidate;
    }

    if (!best_candidate)
    {
        reading->setAnalyzedResult(std::move(ordinary_reading_select_result));
        return false;
    }

    auto storage_snapshot = reading->getStorageSnapshot();
    auto proj_snapshot = std::make_shared<StorageSnapshot>(
        storage_snapshot->storage, storage_snapshot->metadata, storage_snapshot->object_columns); //, storage_snapshot->data);
    proj_snapshot->addProjection(best_candidate->projection);

    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Proj snapshot {}", proj_snapshot->getColumns(GetColumnsOptions::Kind::All).toString());

    auto query_info_copy = query_info;
    query_info_copy.prewhere_info = nullptr;

    auto projection_reading = reader.readFromParts(
        {},
        required_columns,
        proj_snapshot,
        query_info_copy,
        context,
        reading->getMaxBlockSize(),
        reading->getNumStreams(),
        max_added_blocks,
        best_candidate->merge_tree_projection_select_result_ptr,
        reading->isParallelReadingEnabled());

    if (!projection_reading)
    {
        Pipe pipe(std::make_shared<NullSource>(proj_snapshot->getSampleBlockForColumns(required_columns)));
        projection_reading = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
    }

    bool has_nornal_parts = best_candidate->merge_tree_normal_select_result_ptr != nullptr;
    if (has_nornal_parts)
        reading->setAnalyzedResult(std::move(best_candidate->merge_tree_normal_select_result_ptr));

    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Projection reading header {}", projection_reading->getOutputStream().header.dumpStructure());

    projection_reading->setStepDescription(best_candidate->projection->name);

    auto & projection_reading_node = nodes.emplace_back(QueryPlan::Node{.step = std::move(projection_reading)});
    auto * next_node = &projection_reading_node;

    if (query.dag)
    {
        auto & expr_or_filter_node = nodes.emplace_back();

        if (query.filter_node)
        {
            expr_or_filter_node.step = std::make_unique<FilterStep>(
                projection_reading_node.step->getOutputStream(),
                query.dag,
                query.filter_node->result_name,
                query.need_remove_column);
        }
        else
            expr_or_filter_node.step = std::make_unique<ExpressionStep>(
                projection_reading_node.step->getOutputStream(),
                query.dag);

        expr_or_filter_node.children.push_back(&projection_reading_node);
        next_node = &expr_or_filter_node;
    }

    if (!has_nornal_parts)
    {
        /// All parts are taken from projection
        iter->node->children.front() = next_node;
    }
    else
    {
        const auto & main_stream = iter->node->children.front()->step->getOutputStream();
        const auto * proj_stream = &next_node->step->getOutputStream();

        if (auto materializing = makeMaterializingDAG(proj_stream->header, main_stream.header))
        {
            // auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            //     proj_stream->header.getColumnsWithTypeAndName(),
            //     main_stream.header.getColumnsWithTypeAndName(),
            //     ActionsDAG::MatchColumnsMode::Name,
            //     true);

            auto converting = std::make_unique<ExpressionStep>(*proj_stream, materializing);
            proj_stream = &converting->getOutputStream();
            auto & expr_node = nodes.emplace_back();
            expr_node.step = std::move(converting);
            expr_node.children.push_back(next_node);
            next_node = &expr_node;
        }

        auto & union_node = nodes.emplace_back();
        DataStreams input_streams = {main_stream, *proj_stream};
        union_node.step = std::make_unique<UnionStep>(std::move(input_streams));
        union_node.children = {iter->node->children.front(), next_node};
        iter->node->children.front() = &union_node;

        iter->next_child = 0;
        stack.resize(iter.base() - stack.begin() + 1);
    }

    return true;
}

}
