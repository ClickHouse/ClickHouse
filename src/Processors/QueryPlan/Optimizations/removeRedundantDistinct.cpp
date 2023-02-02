#include <memory>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/IntersectOrExceptStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

namespace
{
    constexpr bool debug_logging_enabled = false;
    void logActionsDAG(const String & prefix, const ActionsDAGPtr & actions)
    {
        if constexpr (debug_logging_enabled)
        {
            LOG_DEBUG(&Poco::Logger::get("redundantDistinct"), "{}: {}", prefix, actions->dumpDAG());
        }
    }

    std::set<std::string_view> getDistinctColumns(const DistinctStep * distinct)
    {
        /// find non-const columns in DISTINCT
        const ColumnsWithTypeAndName & distinct_columns = distinct->getOutputStream().header.getColumnsWithTypeAndName();
        std::set<std::string_view> non_const_columns;
        for (const auto & column : distinct_columns)
        {
            if (!isColumnConst(*column.column))
                non_const_columns.emplace(column.name);
        }
        return non_const_columns;
    }

    bool canRemoveDistinct(const QueryPlan::Node * distinct_node)
    {
        const DistinctStep * distinct_step = typeid_cast<DistinctStep *>(distinct_node->step.get());
        chassert(distinct_step);

        std::vector<ActionsDAGPtr> dag_stack;
        const DistinctStep * inner_distinct_step = nullptr;
        const QueryPlan::Node * node = distinct_node;
        while (!node->children.empty())
        {
            const IQueryPlanStep * current_step = node->step.get();

            /// don't try to remove DISTINCT after union or join
            if (typeid_cast<const UnionStep *>(current_step) || typeid_cast<const JoinStep *>(current_step)
                || typeid_cast<const IntersectOrExceptStep *>(current_step))
                break;

            if (const auto * const expr = typeid_cast<const ExpressionStep *>(current_step); expr)
                dag_stack.push_back(expr->getExpression());
            if (const auto * const filter = typeid_cast<const FilterStep *>(current_step); filter)
                dag_stack.push_back(filter->getExpression());

            node = node->children.front();
            inner_distinct_step = typeid_cast<DistinctStep *>(node->step.get());
            if (inner_distinct_step)
                break;
        }
        if (!inner_distinct_step)
            return false;

        /// possible cases (outer distinct -> inner distinct):
        /// final -> preliminary => do nothing
        /// preliminary -> final => try remove preliminary
        /// final -> final => try remove final
        /// preliminary -> preliminary => logical error?
        if (inner_distinct_step->isPreliminary())
            return false;

        const auto distinct_columns = getDistinctColumns(distinct_step);
        auto inner_distinct_columns = getDistinctColumns(inner_distinct_step);
        if (distinct_columns.size() != inner_distinct_columns.size())
            return false;

        ActionsDAGPtr path_actions;
        if (!dag_stack.empty())
        {
            /// build actions DAG to find original column names
            path_actions = dag_stack.back();
            dag_stack.pop_back();
            while (!dag_stack.empty())
            {
                ActionsDAGPtr clone = dag_stack.back()->clone();
                dag_stack.pop_back();
                path_actions->mergeInplace(std::move(*clone));
            }

            logActionsDAG("merged DAG:\n{}", path_actions);

            /// compare columns of two DISTINCTs
            for (const auto & column : distinct_columns)
            {
                const auto * alias_node = path_actions->getOriginalNodeForOutputAlias(String(column));
                if (!alias_node)
                    return false;

                auto it = inner_distinct_columns.find(alias_node->result_name);
                if (it == inner_distinct_columns.end())
                    return false;

                inner_distinct_columns.erase(it);
            }
        }
        else
        {
            if (distinct_columns != inner_distinct_columns)
                return false;
        }

        return true;
    }
}

///
/// DISTINCT is redundant if DISTINCT on the same columns was executed before
/// Trivial example: SELECT DISTINCT * FROM (SELECT DISTINCT * FROM numbers(3))
///
size_t tryRemoveRedundantDistinct(QueryPlan::Node * parent_node, QueryPlan::Nodes & /* nodes*/)
{
    bool applied = false;
    for (const auto * node : parent_node->children)
    {
        /// check if it is distinct node
        const DistinctStep * distinct_step = typeid_cast<DistinctStep *>(node->step.get());
        if (!distinct_step)
            continue;

        if (canRemoveDistinct(node))
        {
            /// remove current distinct
            chassert(!node->children.empty());
            parent_node->children[0] = node->children.front();
            applied = true;
        }
    }

    return applied;
}

}
