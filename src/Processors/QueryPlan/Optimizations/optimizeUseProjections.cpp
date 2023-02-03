#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/matchTrees.h>
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

static void appendExpression(ActionsDAGPtr & dag, const ActionsDAGPtr & expression)
{
    if (dag)
        dag->mergeInplace(std::move(*expression->clone()));
    else
        dag = expression->clone();
}


/// This function builds a common DAG which is a gerge of DAGs from Filter and Expression steps chain.
static bool buildAggregatingDAG(QueryPlan::Node & node, ActionsDAGPtr & dag, ActionsDAG::NodeRawConstPtrs & filter_nodes)
{
    IQueryPlanStep * step = node.step.get();
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
    {
        if (const auto * prewhere_info = reading->getPrewhereInfo())
        {
            if (prewhere_info->row_level_filter)
            {
                appendExpression(dag, prewhere_info->row_level_filter);
                if (const auto * filter_node = dag->tryFindInOutputs(prewhere_info->row_level_column_name))
                    filter_nodes.push_back(filter_node);
                else
                    return false;
            }

            if (prewhere_info->prewhere_actions)
            {
                appendExpression(dag, prewhere_info->prewhere_actions);
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

    if (!buildAggregatingDAG(*node.children.front(), dag, filter_nodes))
        return false;

    if (auto * expression = typeid_cast<ExpressionStep *>(step))
    {
        const auto & actions = expression->getExpression();
        if (actions->hasArrayJoin())
            return false;

        appendExpression(dag, actions);
        return true;
    }

    if (auto * filter = typeid_cast<FilterStep *>(step))
    {
        const auto & actions = filter->getExpression();
        if (actions->hasArrayJoin())
            return false;

        appendExpression(dag, actions);
        const auto * filter_expression = dag->tryFindInOutputs(filter->getFilterColumnName());
        if (!filter_expression)
            return false;

        filter_nodes.push_back(filter_expression);
        return true;
    }

    return false;
}

struct AggregateProjectionInfo
{
    ActionsDAGPtr before_aggregation;
    NamesAndTypesList keys;
    AggregateDescriptions aggregates;

    /// A context copy from interpreter which was used for analysis.
    /// Just in case it is used by some function.
    ContextPtr context;
};

AggregateProjectionInfo getAggregatingProjectionInfo(
    const ProjectionDescription & projection,
    const ContextPtr & context,
    const StorageMetadataPtr & metadata_snapshot)
{
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

    return info;
}

struct AggregateProjectionCandidate
{
    AggregateProjectionInfo info;
    const ProjectionDescription * projection;
    ActionsDAGPtr dag;

    MergeTreeDataSelectAnalysisResultPtr merge_tree_projection_select_result_ptr;
    MergeTreeDataSelectAnalysisResultPtr merge_tree_normal_select_result_ptr;
};

ActionsDAGPtr analyzeAggregateProjection(
    const AggregateProjectionInfo & info,
    ActionsDAG & query_dag,
    const Names & keys,
    const AggregateDescriptions & aggregates)
{

    ActionsDAG::NodeRawConstPtrs key_nodes;
    std::unordered_set<const ActionsDAG::Node *> aggregate_args;

    std::unordered_map<std::string, const ActionsDAG::Node *> index;
    for (const auto * output : query_dag.getOutputs())
        index.emplace(output->result_name, output);

    std::unordered_map<std::string, const ActionsDAG::Node *> proj_index;
    for (const auto * output : info.before_aggregation->getOutputs())
        proj_index.emplace(output->result_name, output);

    key_nodes.reserve(keys.size());
    for (const auto & key : keys)
    {
        auto it = index.find(key);
        /// This should not happen ideally.
        if (it == index.end())
            return {};

        key_nodes.push_back(it->second);
    }

    for (const auto & aggregate : aggregates)
    {
        for (const auto & argument : aggregate.argument_names)
        {
            auto it = index.find(argument);
            /// This should not happen ideally.
            if (it == index.end())
                return {};

            aggregate_args.insert(it->second);
        }
    }

    MatchedTrees::Matches matches = matchTrees(*info.before_aggregation, query_dag);

    std::unordered_map<std::string, std::list<size_t>> projection_aggregate_functions;
    for (size_t i = 0; i < info.aggregates.size(); ++i)
        projection_aggregate_functions[info.aggregates[i].function->getName()].push_back(i);

    std::unordered_set<const ActionsDAG::Node *> split_nodes;

    struct AggFuncMatch
    {
        /// idx in projection
        size_t idx;
        /// nodes in query DAG
        ActionsDAG::NodeRawConstPtrs args;
    };

    std::vector<AggFuncMatch> aggregate_function_matches;
    aggregate_function_matches.reserve(aggregates.size());

    for (const auto & aggregate : aggregates)
    {
        auto it = projection_aggregate_functions.find(aggregate.function->getName());
        if (it == projection_aggregate_functions.end())
            return {};
        auto & candidates = it->second;

        std::optional<AggFuncMatch> match;

        for (size_t idx : candidates)
        {
            const auto & candidate = info.aggregates[idx];

            /// Note: this check is a bit strict.
            /// We check that aggregate function names, arguemnt types and parameters are equal.
            /// In some cases it's possilbe only to check that states are equal,
            /// e.g. for quantile(0.3)(...) and quantile(0.5)(...).
            /// But also functions sum(...) and sumIf(...) will have equal states,
            /// and we can't replace one to another from projection.
            if (!candidate.function->getStateType()->equals(*aggregate.function->getStateType()))
                continue;

            ActionsDAG::NodeRawConstPtrs args;
            size_t num_args = aggregate.argument_names.size();
            args.reserve(num_args);
            for (size_t arg = 0; arg < num_args; ++arg)
            {
                const auto & query_name = aggregate.argument_names[arg];
                const auto & proj_name = candidate.argument_names[arg];

                auto jt = index.find(query_name);
                /// This should not happen ideally.
                if (jt == index.end())
                    break;

                const auto * query_node = jt->second;

                auto kt = proj_index.find(proj_name);
                /// This should not happen ideally.
                if (kt == proj_index.end())
                    break;

                const auto * proj_node = kt->second;

                auto mt = matches.find(query_node);
                if (mt == matches.end())
                    break;

                const auto & node_match = mt->second;
                if (node_match.node != proj_node || node_match.monotonicity)
                    break;

                args.push_back(query_node);
            }

            if (args.size() < aggregate.argument_names.size())
                continue;

            for (const auto * node : args)
                split_nodes.insert(node);

            match = AggFuncMatch{idx, std::move(args)};
        }

        if (!match)
            return {};

        aggregate_function_matches.emplace_back(std::move(*match));
    }

    std::unordered_set<const ActionsDAG::Node *> proj_key_nodes;
    for (const auto & key : info.keys)
    {
        auto it = proj_index.find(key.name);
        /// This should not happen ideally.
        if (it == proj_index.end())
            break;

        proj_key_nodes.insert(it->second);
    }

    std::unordered_set<const ActionsDAG::Node *> visited;

    struct Frame
    {
        const ActionsDAG::Node * node;
        size_t next_child_to_visit = 0;
    };

    std::stack<Frame> stack;
    for (const auto & key : keys)
    {
        auto it = index.find(key);
        /// This should not happen ideally.
        if (it == index.end())
            break;

        const auto * key_node = it->second;
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
                        split_nodes.insert(frame.node);
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
            if (frame.node->children.empty())
                return {};

            /// Not a match, but all children matched.
            visited.insert(frame.node);
            stack.pop();
        }
    }

    std::unordered_map<const ActionsDAG::Node *, std::string> new_inputs;
    for (const auto * node : split_nodes)
        new_inputs[node] = matches[node].node->result_name;

    return query_dag.foldActionsByProjection(new_inputs);
}

void optimizeUseProjections(QueryPlan::Node & node, QueryPlan::Nodes & nodes)
{
    if (node.children.size() != 1)
        return;

    auto * aggregating = typeid_cast<AggregatingStep *>(node.step.get());
    if (!aggregating)
        return;

    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Try optimize projection 2");
    if (!aggregating->canUseProjection())
        return;

    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Try optimize projection 3");
    QueryPlan::Node * reading_node = findReadingStep(*node.children.front());
    if (!reading_node)
        return;

    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Try optimize projection 4");

    auto * reading = typeid_cast<ReadFromMergeTree *>(reading_node->step.get());
    if (!reading)
        return;

    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Try optimize projection 5");

    const auto metadata = reading->getStorageMetadata();
    const auto & projections = metadata->projections;

    std::vector<const ProjectionDescription *> agg_projections;
    for (const auto & projection : projections)
        if (projection.type == ProjectionDescription::Type::Aggregate)
            agg_projections.push_back(&projection);

    if (agg_projections.empty())
        return;

    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Has agg projection");

    ActionsDAGPtr dag;
    ActionsDAG::NodeRawConstPtrs filter_nodes;
    if (!buildAggregatingDAG(*node.children.front(), dag, filter_nodes))
        return;

    LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Query DAG: {}", dag->dumpDAG());

    const ActionsDAG::Node * filter_node = nullptr;
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

        dag->getOutputs().insert(dag->getOutputs().begin(), filter_node);
    }

    ContextPtr context = reading->getContext();

    const auto & keys = aggregating->getParams().keys;
    const auto & aggregates = aggregating->getParams().aggregates;

    std::vector<AggregateProjectionCandidate> candidates;
    candidates.reserve(agg_projections.size());
    for (const auto * projection : agg_projections)
    {

        LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Try projection {}", projection->name);
        auto info = getAggregatingProjectionInfo(*projection, context, metadata);
        LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Projection DAG {}", info.before_aggregation->dumpDAG());
        if (auto proj_dag = analyzeAggregateProjection(info, *dag, keys, aggregates))
        {
            LOG_TRACE(&Poco::Logger::get("optimizeUseProjections"), "Projection analyzed DAG {}", proj_dag->dumpDAG());
            candidates.emplace_back(AggregateProjectionCandidate{
                .info = std::move(info),
                .projection = projection,
                .dag = std::move(proj_dag),
            });
        }
    }

    if (candidates.empty())
        return;

    AggregateProjectionCandidate * best_candidate = nullptr;
    size_t best_candidate_marks = 0;

    const auto & parts = reading->getParts();
    const auto & query_info = reading->getQueryInfo();

    MergeTreeDataSelectExecutor reader(reading->getMergeTreeData());

    std::shared_ptr<PartitionIdToMaxBlock> max_added_blocks;
    if (context->getSettingsRef().select_sequential_consistency)
    {
        if (const StorageReplicatedMergeTree * replicated = dynamic_cast<const StorageReplicatedMergeTree *>(&reading->getMergeTreeData()))
            max_added_blocks = std::make_shared<PartitionIdToMaxBlock>(replicated->getMaxAddedBlocks());
    }

    for (auto & candidate : candidates)
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
        if (filter_node)
            added_filter_nodes.nodes.push_back(candidate.dag->getOutputs().front());

        auto projection_result_ptr = reader.estimateNumMarksToRead(
            projection_parts,
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

        size_t sum_marks = projection_result_ptr->marks();

        if (!normal_parts.empty())
        {
            auto normal_result_ptr = reading->selectRangesToRead(std::move(normal_parts));

            if (normal_result_ptr->error())
                continue;

            if (normal_result_ptr->marks() != 0)
            {
                sum_marks += normal_result_ptr->marks();
                candidate.merge_tree_normal_select_result_ptr = std::move(normal_result_ptr);
            }
        }

        candidate.merge_tree_projection_select_result_ptr = std::move(projection_result_ptr);

        if (best_candidate == nullptr || best_candidate_marks > sum_marks)
        {
            best_candidate = &candidate;
            best_candidate_marks = sum_marks;
        }
    }

    if (!best_candidate)
        return;

    auto storage_snapshot = reading->getStorageSnapshot();
    auto proj_snapshot = std::make_shared<StorageSnapshot>(
        storage_snapshot->storage, storage_snapshot->metadata, storage_snapshot->object_columns); //, storage_snapshot->data);
    proj_snapshot->addProjection(best_candidate->projection);

    auto projection_reading = reader.readFromParts(
        {},
        best_candidate->dag->getRequiredColumnsNames(),
        proj_snapshot,
        query_info,
        context,
        reading->getMaxBlockSize(),
        reading->getNumStreams(),
        max_added_blocks,
        best_candidate->merge_tree_projection_select_result_ptr,
        reading->isParallelReadingEnabled());

    projection_reading->setStepDescription(best_candidate->projection->name);

    auto & projection_reading_node = nodes.emplace_back(QueryPlan::Node{.step = std::move(projection_reading)});
    auto & expr_or_filter_node = nodes.emplace_back();

    if (filter_node)
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

    if (!best_candidate->merge_tree_normal_select_result_ptr)
    {
        /// All parts are taken from projection


        aggregating->requestOnlyMergeForAggregateProjection(expr_or_filter_node.step->getOutputStream());
        node.children.front() = &expr_or_filter_node;

        //optimizeAggregationInOrder(node, nodes);
    }
    else
    {
        node.step = aggregating->convertToAggregatingProjection(expr_or_filter_node.step->getOutputStream());
        node.children.push_back(&expr_or_filter_node);
    }
}

}
