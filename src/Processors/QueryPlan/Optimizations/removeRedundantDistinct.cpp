#include <memory>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/IntersectOrExceptStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/RollupStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

namespace
{
    constexpr bool debug_logging_enabled = false;

    template <typename T>
    void logDebug(String key, const T & value, const char * separator = " : ")
    {
        if constexpr (debug_logging_enabled)
        {
            WriteBufferFromOwnString ss;
            if constexpr (std::is_pointer_v<T>)
                ss << *value;
            else
                ss << value;

            LOG_DEBUG(getLogger("redundantDistinct"), "{}{}{}", key, separator, ss.str());
        }
    }

    void logActionsDAG(const String & prefix, const ActionsDAG & actions)
    {
        if constexpr (debug_logging_enabled)
            LOG_DEBUG(getLogger("redundantDistinct"), "{} :\n{}", prefix, actions.dumpDAG());
    }

    using DistinctColumns = std::set<std::string_view>;
    DistinctColumns getDistinctColumns(const DistinctStep * distinct)
    {
        /// find non-const columns in DISTINCT
        const ColumnsWithTypeAndName & distinct_columns = distinct->getOutputHeader().getColumnsWithTypeAndName();
        std::set<std::string_view> non_const_columns;
        std::unordered_set<std::string_view> column_names(cbegin(distinct->getColumnNames()), cend(distinct->getColumnNames()));
        for (const auto & column : distinct_columns)
        {
            if (!isColumnConst(*column.column) && column_names.contains(column.name))
                non_const_columns.emplace(column.name);
        }
        return non_const_columns;
    }

    /// build actions DAG from stack of steps
    std::optional<ActionsDAG> buildActionsForPlanPath(std::vector<const ActionsDAG *> & dag_stack)
    {
        if (dag_stack.empty())
            return {};

        ActionsDAG path_actions = dag_stack.back()->clone();
        dag_stack.pop_back();
        while (!dag_stack.empty())
        {
            ActionsDAG clone = dag_stack.back()->clone();
            logActionsDAG("DAG to merge", clone);
            dag_stack.pop_back();
            path_actions.mergeInplace(std::move(clone));
        }
        return path_actions;
    }

    bool compareAggregationKeysWithDistinctColumns(
        const Names & aggregation_keys, const DistinctColumns & distinct_columns, std::vector<std::vector<const ActionsDAG *>> actions_chain)
    {
        logDebug("aggregation_keys", aggregation_keys);
        logDebug("aggregation_keys size", aggregation_keys.size());
        logDebug("distinct_columns size", distinct_columns.size());

        std::set<String> current_columns(begin(distinct_columns), end(distinct_columns));
        std::set<String> source_columns;
        for (auto & actions : actions_chain)
        {
            auto tmp_actions = buildActionsForPlanPath(actions);
            FindOriginalNodeForOutputName original_node_finder(*tmp_actions);
            for (const auto & column : current_columns)
            {
                logDebug("distinct column name", column);
                const auto * alias_node = original_node_finder.find(String(column));
                if (!alias_node)
                {
                    logDebug("original name for alias is not found", column);
                    source_columns.insert(String(column));
                }
                else
                {
                    logDebug("alias result name", alias_node->result_name);
                    source_columns.insert(alias_node->result_name);
                }
            }

            current_columns = std::move(source_columns);
            source_columns.clear();
        }
        /// if aggregation keys are part of distinct columns then rows already distinct
        for (const auto & key : aggregation_keys)
        {
            if (!current_columns.contains(key))
            {
                logDebug("aggregation key NOT found", key);
                return false;
            }
        }
        return true;
    }

    bool checkStepToAllowOptimization(const IQueryPlanStep * step)
    {
        if (typeid_cast<const DistinctStep *>(step))
            return true;

        if (const auto * const expr = typeid_cast<const ExpressionStep *>(step); expr)
            return !expr->getExpression().hasArrayJoin();

        if (const auto * const filter = typeid_cast<const FilterStep *>(step); filter)
            return !filter->getExpression().hasArrayJoin();

        if (typeid_cast<const LimitStep *>(step) || typeid_cast<const LimitByStep *>(step) || typeid_cast<const SortingStep *>(step)
            || typeid_cast<const WindowStep *>(step))
            return true;

        /// those steps can be only after AggregatingStep, so we skip them here but check AggregatingStep separately
        if (typeid_cast<const CubeStep *>(step) || typeid_cast<const RollupStep *>(step) || typeid_cast<const TotalsHavingStep *>(step))
            return true;

        return false;
    }

    bool passTillAggregation(const QueryPlan::Node * distinct_node)
    {
        const DistinctStep * distinct_step = typeid_cast<DistinctStep *>(distinct_node->step.get());
        chassert(distinct_step);

        std::vector<const ActionsDAG *> dag_stack;
        std::vector<std::vector<const ActionsDAG *>> actions_chain;
        const DistinctStep * inner_distinct_step = nullptr;
        const IQueryPlanStep * aggregation_before_distinct = nullptr;
        const QueryPlan::Node * node = distinct_node;
        while (!node->children.empty())
        {
            const IQueryPlanStep * current_step = node->step.get();
            if (typeid_cast<const AggregatingStep *>(current_step) || typeid_cast<const MergingAggregatedStep *>(current_step))
            {
                aggregation_before_distinct = current_step;
                break;
            }
            if (!checkStepToAllowOptimization(current_step))
            {
                logDebug("aggregation pass: stopped by allow check on step", current_step->getName());
                break;
            }

            if (typeid_cast<const WindowStep *>(current_step))
            {
                /// it can be empty in case of 2 WindowSteps following one another
                if (!dag_stack.empty())
                {
                    actions_chain.push_back(std::move(dag_stack));
                    dag_stack.clear();
                }
            }

            if (const auto * const expr = typeid_cast<const ExpressionStep *>(current_step); expr)
                dag_stack.push_back(&expr->getExpression());
            else if (const auto * const filter = typeid_cast<const FilterStep *>(current_step); filter)
                dag_stack.push_back(&filter->getExpression());

            node = node->children.front();
            if (inner_distinct_step = typeid_cast<DistinctStep *>(node->step.get()); inner_distinct_step)
                break;
        }
        if (inner_distinct_step)
            return false;

        if (aggregation_before_distinct)
        {
            if (actions_chain.empty())
                actions_chain.push_back(std::move(dag_stack));

            const auto distinct_columns = getDistinctColumns(distinct_step);

            if (const auto * aggregating_step = typeid_cast<const AggregatingStep *>(aggregation_before_distinct); aggregating_step)
            {
                return compareAggregationKeysWithDistinctColumns(
                    aggregating_step->getParams().keys, distinct_columns, std::move(actions_chain));
            }
            if (const auto * merging_aggregated_step = typeid_cast<const MergingAggregatedStep *>(aggregation_before_distinct);
                merging_aggregated_step)
            {
                return compareAggregationKeysWithDistinctColumns(
                    merging_aggregated_step->getParams().keys, distinct_columns, std::move(actions_chain));
            }
        }

        return false;
    }

    bool passTillDistinct(const QueryPlan::Node * distinct_node)
    {
        const DistinctStep * distinct_step = typeid_cast<DistinctStep *>(distinct_node->step.get());
        chassert(distinct_step);
        const auto distinct_columns = getDistinctColumns(distinct_step);

        std::vector<const ActionsDAG *> dag_stack;
        const DistinctStep * inner_distinct_step = nullptr;
        const QueryPlan::Node * node = distinct_node;
        while (!node->children.empty())
        {
            const IQueryPlanStep * current_step = node->step.get();
            if (!checkStepToAllowOptimization(current_step))
            {
                logDebug("distinct pass: stopped by allow check on step", current_step->getName());
                break;
            }

            if (const auto * const expr = typeid_cast<const ExpressionStep *>(current_step); expr)
                dag_stack.push_back(&expr->getExpression());
            else if (const auto * const filter = typeid_cast<const FilterStep *>(current_step); filter)
                dag_stack.push_back(&filter->getExpression());

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

        auto inner_distinct_columns = getDistinctColumns(inner_distinct_step);
        if (distinct_columns.size() != inner_distinct_columns.size())
            return false;

        ActionsDAG path_actions;
        if (!dag_stack.empty())
        {
            /// build actions DAG to find original column names
            path_actions = std::move(*buildActionsForPlanPath(dag_stack));
            logActionsDAG("distinct pass: merged DAG", path_actions);

            /// compare columns of two DISTINCTs
            FindOriginalNodeForOutputName original_node_finder(path_actions);
            for (const auto & column : distinct_columns)
            {
                const auto * alias_node = original_node_finder.find(String(column));
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

    bool canRemoveDistinct(const QueryPlan::Node * distinct_node)
    {
        if (passTillAggregation(distinct_node))
            return true;

        if (passTillDistinct(distinct_node))
            return true;

        return false;
    }
}

///
/// DISTINCT is redundant if DISTINCT on the same columns was executed before
/// Trivial example: SELECT DISTINCT * FROM (SELECT DISTINCT * FROM numbers(3))
///
size_t tryRemoveRedundantDistinct(QueryPlan::Node * parent_node, QueryPlan::Nodes & /* nodes*/)
{
    bool applied = false;
    for (auto & node : parent_node->children)
    {
        /// check if it is distinct node
        if (typeid_cast<const DistinctStep *>(node->step.get()) == nullptr)
            continue;

        if (canRemoveDistinct(node))
        {
            /// remove current distinct
            chassert(!node->children.empty());
            node = node->children.front();
            applied = true;
        }
    }

    return applied;
}
}
