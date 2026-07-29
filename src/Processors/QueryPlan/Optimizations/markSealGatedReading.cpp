#include <Columns/ColumnConst.h>
#include <Functions/IFunction.h>
#include <Interpreters/IJoin.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

namespace DB
{
namespace QueryPlanOptimizations
{

namespace
{

/// Steps a join and the reading steps of its inputs may be separated by.
bool isTransparentForSealGating(IQueryPlanStep * step)
{
    return typeid_cast<ExpressionStep *>(step) || typeid_cast<FilterStep *>(step);
}

/// Find `__applyFilter(<key>, key_column)` planted by the runtime filter optimization among
/// the filter conjuncts pushed down to the reading step. Returns {filter key, key column name}.
std::optional<std::pair<String, String>> findAppliedRuntimeFilter(const ActionsDAG & dag)
{
    for (const auto & dag_node : dag.getNodes())
    {
        if (dag_node.type != ActionsDAG::ActionType::FUNCTION || !dag_node.function_base)
            continue;
        if (dag_node.function_base->getName() != "__applyFilter" || dag_node.children.size() != 2)
            continue;

        /// Argument 0: const String whose VALUE is the runtime filter rendezvous key.
        const auto * label = dag_node.children[0];
        if (!label->column || !isColumnConst(*label->column) || !isString(label->result_type))
            continue;
        String filter_key(label->column->getDataAt(0));

        /// Argument 1: the probe key column, possibly wrapped in a CAST.
        const auto * key_arg = dag_node.children[1];
        while (key_arg->type == ActionsDAG::ActionType::FUNCTION && key_arg->function_base
               && (key_arg->function_base->getName() == "CAST" || key_arg->function_base->getName() == "_CAST")
               && !key_arg->children.empty())
            key_arg = key_arg->children.front();

        if (key_arg->type != ActionsDAG::ActionType::INPUT)
            continue;

        return std::make_pair(std::move(filter_key), key_arg->result_name);
    }

    return {};
}

struct ProbeSide
{
    ReadFromMergeTree * reading = nullptr;
    /// {filter key, key column name} of the first `__applyFilter` conjunct found on the way.
    std::optional<std::pair<String, String>> runtime_filter;
};

/// The runtime filter is planted as a FilterStep conjunct above the reading step (it is not
/// pushed into the reading step's own filter, which is populated earlier in the second pass),
/// so look for it while walking down. The reading step's own filter and prewhere are checked
/// too in case a later optimization moved the conjunct there.
ProbeSide findProbeSide(QueryPlan::Node * node)
{
    ProbeSide res;
    while (node)
    {
        if (!res.runtime_filter)
        {
            if (const auto * filter_step = typeid_cast<FilterStep *>(node->step.get()))
                res.runtime_filter = findAppliedRuntimeFilter(filter_step->getExpression());
        }

        if (auto * reading = typeid_cast<ReadFromMergeTree *>(node->step.get()))
        {
            res.reading = reading;
            if (!res.runtime_filter)
            {
                if (const auto & dag = reading->getFilterActionsDAG())
                    res.runtime_filter = findAppliedRuntimeFilter(*dag);
                if (!res.runtime_filter && reading->getQueryInfo().prewhere_info)
                    res.runtime_filter = findAppliedRuntimeFilter(reading->getQueryInfo().prewhere_info->prewhere_actions);
            }
            return res;
        }

        if (!isTransparentForSealGating(node->step.get()) || node->children.size() != 1)
            return res;

        node = node->children.front();
    }
    return res;
}

/// Find the build-side runtime filter step with the given rendezvous key.
bool buildSideHasRuntimeFilter(QueryPlan::Node * node, const String & filter_key)
{
    while (node)
    {
        IQueryPlanStep * step = node->step.get();

        if (auto * build_step = typeid_cast<BuildRuntimeFilterStep *>(step))
        {
            if (build_step->getFilterKey() == filter_key)
                return true;
        }
        else if (!isTransparentForSealGating(step))
        {
            return false;
        }

        if (node->children.size() != 1)
            return false;
        node = node->children.front();
    }
    return false;
}

void tryMarkJoin(QueryPlan::Node & node)
{
    auto * join_step = typeid_cast<JoinStep *>(node.step.get());
    if (!join_step || node.children.size() != 2)
        return;

    /// Only the hash-join pipeline (build the right side first) wires the seal.
    if (join_step->getJoin()->pipelineType() != JoinPipelineType::FillRightFirst)
        return;

    const size_t probe_idx = join_step->swap_streams ? 1 : 0;

    auto [reading, match] = findProbeSide(node.children[probe_idx]);
    if (!reading || !match)
        return;

    if (reading->isQueryWithFinal() || reading->isParallelReadingEnabled())
        return;

    if (!buildSideHasRuntimeFilter(node.children[1 - probe_idx], match->first))
        return;

    /// The filter can prune ranges only through the primary key.
    const auto & primary_key_columns = reading->getStorageMetadata()->getPrimaryKey().column_names;
    if (std::find(primary_key_columns.begin(), primary_key_columns.end(), match->second) == primary_key_columns.end())
        return;

    reading->enableSealGatedReading(match->second);
    join_step->enableSealGatedProbeReading(match->first);
}

}

void markSealGatedReading(QueryPlan::Node & root)
{
    std::vector<QueryPlan::Node *> stack = {&root};
    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();

        tryMarkJoin(*node);

        for (auto * child : node->children)
            stack.push_back(child);
    }
}

}
}
