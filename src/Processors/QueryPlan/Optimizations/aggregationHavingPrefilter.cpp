#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <Functions/IFunction.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

using Op = Aggregator::Params::HavingPrefilterOp;

Op mirror(Op op)
{
    switch (op)
    {
        case Op::Greater: return Op::Less;
        case Op::GreaterOrEqual: return Op::LessOrEqual;
        case Op::Less: return Op::Greater;
        case Op::LessOrEqual: return Op::GreaterOrEqual;
        case Op::Equal: return Op::Equal;
        case Op::Disabled: return Op::Disabled;
    }
    return Op::Disabled;
}

Op opFromFunctionName(const String & name)
{
    if (name == "greater") return Op::Greater;
    if (name == "greaterOrEquals") return Op::GreaterOrEqual;
    if (name == "less") return Op::Less;
    if (name == "lessOrEquals") return Op::LessOrEqual;
    if (name == "equals") return Op::Equal;
    return Op::Disabled;
}

bool exactCountThreshold(const Field & field, UInt64 & threshold)
{
    if (field.getType() == Field::Types::UInt64)
    {
        threshold = field.safeGet<UInt64>();
        return true;
    }
    if (field.getType() == Field::Types::Int64)
    {
        const Int64 value = field.safeGet<Int64>();
        if (value < 0)
            return false;
        threshold = static_cast<UInt64>(value);
        return true;
    }
    return false;
}

const ActionsDAG::Node * unwrapAlias(const ActionsDAG::Node * node)
{
    while (node && node->type == ActionsDAG::ActionType::ALIAS && !node->children.empty())
        node = node->children.front();
    return node;
}

/// Each conjunct of an `and` is a necessary condition for the row to survive the filter.
void collectConjuncts(const ActionsDAG::Node * node, std::vector<const ActionsDAG::Node *> & out)
{
    node = unwrapAlias(node);
    if (!node)
        return;
    if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base
        && node->function_base->getName() == "and")
    {
        for (const auto * child : node->children)
            collectConjuncts(child, out);
        return;
    }
    out.push_back(node);
}

/// A materialized group always has a count of at least one, so these bounds keep every group.
bool boundKeepsEverything(Op op, UInt64 threshold)
{
    return (op == Op::Greater && threshold == 0) || (op == Op::GreaterOrEqual && threshold <= 1);
}

}

/// A `FilterStep` directly above a final aggregation is HAVING, QUALIFY or an outer WHERE, all of which are
/// per-group. This pass reads the settled plan, so that filter is the one that will execute.
size_t tryPushHavingPrefilterIntoAggregation(
    QueryPlan::Node * parent_node, QueryPlan::Nodes &, const Optimization::ExtraSettings & settings)
{
    /// A serialized or distributed plan does not carry these fields to whoever executes it.
    if (settings.make_distributed_plan || settings.serialize_query_plan)
        return 0;

    const auto * filter = typeid_cast<FilterStep *>(parent_node->step.get());
    if (!filter || parent_node->children.size() != 1)
        return 0;

    auto * aggregating = typeid_cast<AggregatingStep *>(parent_node->children.front()->step.get());
    if (!aggregating)
        return 0;

    const auto & params = aggregating->getParams();
    /// None of these counts is the group's own: a non-final aggregation's is partial, `overflow_row`'s stands for
    /// everything past `max_rows_to_group_by`, and a grouping-sets level is derived from this conversion's output.
    if (!aggregating->isFinal() || aggregating->isGroupingSets() || params.overflow_row || params.keys_size == 0)
        return 0;
    if (params.only_merge || aggregating->inOrder())
        return 0;
    /// `skip_merging` squashes the per-bucket chunks by row and byte thresholds, so a sparser chunk lands more
    /// buckets in one output block, which `rowNumberInBlock` can see.
    if (aggregating->isMergingSkipped())
        return 0;
    if (params.bucket_top_k || params.top_k || params.having_prefilter_op != Op::Disabled)
        return 0;

    const auto & expression = filter->getExpression();
    if (isSensitiveToEvaluationCount(expression) || expression.hasArrayJoin())
        return 0;

    const auto * filter_node = expression.tryFindInOutputs(filter->getFilterColumnName());
    if (!filter_node)
        return 0;

    std::vector<const ActionsDAG::Node *> conjuncts;
    collectConjuncts(filter_node, conjuncts);

    for (const auto * conjunct : conjuncts)
    {
        if (conjunct->type != ActionsDAG::ActionType::FUNCTION || conjunct->children.size() != 2
            || !conjunct->function_base)
            continue;

        Op op = opFromFunctionName(conjunct->function_base->getName());
        if (op == Op::Disabled)
            continue;

        const auto * lhs = unwrapAlias(conjunct->children[0]);
        const auto * rhs = unwrapAlias(conjunct->children[1]);
        if (!lhs || !rhs)
            continue;

        /// `4 < count()` is the same bound read from the other side.
        if (lhs->type == ActionsDAG::ActionType::COLUMN && rhs->type == ActionsDAG::ActionType::INPUT)
        {
            std::swap(lhs, rhs);
            op = mirror(op);
        }
        if (lhs->type != ActionsDAG::ActionType::INPUT || rhs->type != ActionsDAG::ActionType::COLUMN)
            continue;
        if (!rhs->column || !isColumnConst(*rhs->column))
            continue;

        UInt64 threshold = 0;
        if (!exactCountThreshold((*rhs->column)[0], threshold) || boundKeepsEverything(op, threshold))
            continue;

        for (size_t i = 0; i < params.aggregates.size(); ++i)
        {
            const auto & aggregate = params.aggregates[i];
            /// The filter's input header is the aggregation's output header, so this name match is exact.
            if (aggregate.column_name != lhs->result_name)
                continue;
            /// Only a no-argument `count()` has the bare UInt64 state the conversion reads: `count(x)` can acquire
            /// a nullable adapter and a combinator changes the layout.
            if (aggregate.function->getName() != "count" || !aggregate.argument_names.empty()
                || !aggregate.parameters.empty())
                break;

            aggregating->enableHavingPrefilter(op, threshold, i);
            return 0;
        }
    }

    return 0;
}

}
