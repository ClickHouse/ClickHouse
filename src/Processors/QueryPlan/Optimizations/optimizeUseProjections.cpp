#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/matchTrees.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Interpreters/InterpreterSelectQuery.h>

namespace DB::QueryPlanOptimizations
{

QueryPlan::Node * findReadingStep(QueryPlan::Node & node)
{
    IQueryPlanStep * step = node.step.get();\
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
    {
        /// Already read-in-order, skip.
        if (reading->getQueryInfo().input_order_info)
            return nullptr;

        const auto & sorting_key = reading->getStorageMetadata()->getSortingKey();
        if (sorting_key.column_names.empty())
            return nullptr;

        return &node;
    }

    if (node.children.size() != 1)
        return nullptr;

    if (typeid_cast<ExpressionStep *>(step) || typeid_cast<FilterStep *>(step))
        return findReadingStep(*node.children.front());

    return nullptr;
}

void appendExpression(ActionsDAGPtr & dag, const ActionsDAGPtr & expression)
{
    if (dag)
        dag->mergeInplace(std::move(*expression->clone()));
    else
        dag = expression->clone();
}


/// This function builds a common DAG which is a gerge of DAGs from Filter and Expression steps chain.
bool buildAggregatingDAG(QueryPlan::Node & node, ActionsDAGPtr & dag, ActionsDAG::NodeRawConstPtrs & filter_nodes)
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
    }

    if (auto * filter = typeid_cast<FilterStep *>(step))
    {
        const auto & actions = filter->getExpression();
        if (actions->hasArrayJoin())
            return false;

        appendExpression(dag, actions);
        if (const auto * filter_expression = dag->tryFindInOutputs(filter->getFilterColumnName()))
            filter_nodes.push_back(filter_expression);
        else
            return false;
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
    StoragePtr & storage,
    const StorageMetadataPtr & metadata_snapshot)
{
    /// This is a bad approach.
    /// We'd better have a separate interpreter for projections.
    /// Now it's not obvious we didn't miss anything here.
    InterpreterSelectQuery interpreter(
        projection.query_ast,
        context,
        storage,
        metadata_snapshot,
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
    ProjectionDescription * projection;
};

std::optional<AggregateProjectionCandidate> analyzeAggregateProjection(
    ProjectionDescription & projection,
    AggregateProjectionInfo info,
    ActionsDAG & query_dag,
    const Names & keys,
    const AggregateDescriptions & aggregates)
{

    ActionsDAG::NodeRawConstPtrs key_nodes;
    std::unordered_set<const ActionsDAG::Node *> aggregate_args;

    std::unordered_map<std::string, const ActionsDAG::Node *> index;
    for (const auto * output : query_dag.getOutputs())
        index.emplace(output->result_name, output);

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
            args.reserve(aggregate.argument_names.size());
            for (const auto & name : aggregate.argument_names)
            {
                auto jt = index.find(name);
                /// This should not happen ideally.
                if (jt == index.end())
                    break;

                const auto * outer_node = jt->second;
                auto kt = matches.find(outer_node);
                if (kt == matches.end())
                    break;

                const auto & node_match = kt->second;
                if (!node_match.node || node_match.monotonicity)
                    break;

                args.push_back(node_match.node);
            }

            if (args.size() < aggregate.argument_names.size())
                continue;

            match = AggFuncMatch{idx, std::move(args)};
        }

        if (!match)
            return {};

        aggregate_function_matches.emplace_back(std::move(*match));
    }


}

void optimizeUseProjections(QueryPlan::Node & node, QueryPlan::Nodes &)
{
    if (node.children.size() != 1)
        return;

    auto * aggregating = typeid_cast<AggregatingStep *>(node.step.get());
    if (!aggregating)
        return;

    QueryPlan::Node * reading_node = findReadingStep(node);
    if (!reading_node)
        return;

    ActionsDAGPtr dag;
    ActionsDAG::NodeRawConstPtrs filter_nodes;
    if (!buildAggregatingDAG(node, dag, filter_nodes))
        return;

    const auto & keys = aggregating->getParams().keys;
    const auto & aggregates = aggregating->getParams().aggregates;

    auto * reading = typeid_cast<ReadFromMergeTree *>(reading_node->step.get());
    if (!reading)
        return;

    // const auto metadata = reading->getStorageMetadata();
    // const auto & projections = metadata->projections;


}

}
