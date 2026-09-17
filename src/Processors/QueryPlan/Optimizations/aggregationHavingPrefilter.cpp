#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnConst.h>
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

/// The conversion compares a UInt64 count, so a threshold that does not mean the same thing over the
/// integers is refused rather than rounded; the FilterStep still evaluates such a bound as before.
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

/// Pre-filtering on one conjunct of an `and` is sound because each conjunct is a necessary condition
/// for the row to survive the FilterStep, which remains the authoritative filter.
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

/// A materialized group always has a count of at least one, so these bounds keep every group and
/// the selection scan would be pure overhead.
bool boundKeepsEverything(Op op, UInt64 threshold)
{
    return (op == Op::Greater && threshold == 0) || (op == Op::GreaterOrEqual && threshold <= 1);
}

}

/// This pass runs LAST, from the final traversal of `optimizeTreeSecondPass`, so the shape it reads is
/// the one that will execute: the filter may already have been merged with an outer WHERE, split, or
/// had conjuncts pushed below the aggregation, and the aggregation's own flags have settled.
///
/// The shape it looks for is a per-group filter directly above a final aggregation, requiring a bound on
/// that aggregation's own no-argument `count()`. That is usually HAVING, but a `FilterStep` in that slot
/// can also be QUALIFY or an outer WHERE - all per-group, so all sound.
size_t tryPushHavingPrefilterIntoAggregation(
    QueryPlan::Node * parent_node, QueryPlan::Nodes &, const Optimization::ExtraSettings & settings)
{
    /// A serialized or distributed plan carries none of these fields to whoever executes it, so
    /// annotating one would only make EXPLAIN advertise an optimization that does not run.
    if (settings.make_distributed_plan || settings.serialize_query_plan)
        return 0;

    const auto * filter = typeid_cast<FilterStep *>(parent_node->step.get());
    if (!filter || parent_node->children.size() != 1)
        return 0;

    auto * aggregating = typeid_cast<AggregatingStep *>(parent_node->children.front()->step.get());
    if (!aggregating)
        return 0;

    const auto & params = aggregating->getParams();
    /// A non-final aggregation's counts are still partial; `overflow_row` emits one row standing
    /// for everything past `max_rows_to_group_by`, whose count is not this group's count; and a
    /// grouping-sets/ROLLUP level is derived from the conversion's output rather than beside it,
    /// so dropping a row here would change a coarser level's aggregate.
    if (!aggregating->isFinal() || aggregating->isGroupingSets() || params.overflow_row || params.keys_size == 0)
        return 0;
    if (params.only_merge || aggregating->inOrder())
        return 0;
    /// `skip_merging` routes the pipeline through a squashing transform that re-packs the per-bucket
    /// chunks by row and byte thresholds, so a sparser chunk lands more buckets in one output block -
    /// which `rowNumberInBlock` above the retained filter can see.
    if (aggregating->isMergingSkipped())
        return 0;
    if (params.bucket_top_k || params.top_k || params.having_prefilter_op != Op::Disabled)
        return 0;

    /// The retained filter cannot undo this: it is the thing being fed the shorter input.
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
            /// The filter is the aggregation's direct parent, so its input header is the aggregation's
            /// output header and this name match is exact: no intervening step can have renamed it.
            if (aggregate.column_name != lhs->result_name)
                continue;
            /// Only a no-argument `count()`: its state is the bare UInt64 the conversion reads.
            /// `count(x)` can acquire a nullable adapter and a combinator changes the layout, so
            /// neither is read directly.
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
