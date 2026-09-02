#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>

#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/NegativeLimitByStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>

#include <Functions/IFunction.h>
#include <Interpreters/FullSortingMergeJoin.h>
#include <Interpreters/TableJoin.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace QueryPlanOptimizations
{

struct SortingProperty
{
    /// Sorting scope.
    enum class SortScope : uint8_t
    {
        Stream = 0, /// Each data steam is sorted
        Global = 1, /// Data is globally sorted
    };

    SortDescription sort_description = {};
    SortScope sort_scope = SortScope::Stream;
};

/// A full sorting merge join emits its result in the order of one of its sorted inputs: `MergeJoinAlgorithm`
/// walks both inputs with cursors that only move forward, so for an INNER or LEFT join the output rows follow
/// the left input (an INNER join emits a subsequence of it, a LEFT join emits every left row, repeated for
/// each match), and for a RIGHT join they follow the right input. A FULL join interleaves the non-matched rows
/// of both sides, whose key columns of the other side are defaults, so neither side's keys stay sorted.
///
/// Only the leading run of join-key columns is advertised. Within one group of equal keys the rows of an ALL
/// join are emitted as a cross product, which keeps the order of the keys but not of any other column.
static SortingProperty applyOrderToJoin(const JoinStep & join_step, const SortingProperty * children_properties)
{
    const auto * merge_join = typeid_cast<const FullSortingMergeJoin *>(join_step.getJoin().get());
    if (!merge_join)
        return {};

    /// `parallel_full_sorting_merge` is sharded by the hash of the join keys afterwards
    /// (`optimizeParallelFullSortingMergeJoin`), and that pass only scatters a plain full sort. Advertising the
    /// order would turn the merge-join sort above into `FinishSorting` and leave the next join in a chain
    /// unsharded, trading its parallelism for a saved sort. Keep the parallel variant hash-sharded at every
    /// level; its output is unordered by design.
    if (merge_join->isParallel())
        return {};

    const auto & table_join = merge_join->getTableJoin();
    const auto kind = table_join.kind();
    if (!isInner(kind) && !isLeft(kind) && !isRight(kind))
        return {};

    /// With `swap_streams` the plan's right child feeds the algorithm's left input and vice versa.
    const size_t algorithm_left_child = join_step.swap_streams ? 1 : 0;
    const size_t ordered_child = isRight(kind) ? 1 - algorithm_left_child : algorithm_left_child;
    const auto & clause = table_join.getOnlyClause();
    const Names & key_names = isRight(kind) ? clause.key_names_right : clause.key_names_left;

    auto sort_description = getCollationAwareSortPrefixInColumns(children_properties[ordered_child].sort_description, key_names);

    /// Keep the key columns that reach the output under their own, unambiguous name. The legacy planner lets
    /// both inputs carry a column of the same name and renames the right one on the way out, so a name found in
    /// the other input may denote a different column in the output.
    const auto & output_header = *join_step.getOutputHeader();
    const auto & other_input_header = *join_step.getInputHeaders()[1 - ordered_child];
    size_t num_columns_in_output = 0;
    for (; num_columns_in_output < sort_description.size(); ++num_columns_in_output)
    {
        const auto & name = sort_description[num_columns_in_output].column_name;
        if (!output_header.has(name) || other_input_header.has(name))
            break;
    }
    sort_description.resize(num_columns_in_output);

    if (sort_description.empty())
        return {};

    /// A single merge join is resized to `max_streams` outputs, and a sharded one has one output per shard,
    /// so the order holds within every stream rather than globally.
    return {std::move(sort_description), SortingProperty::SortScope::Stream};
}

static SortingProperty applyOrder(QueryPlan::Node * parent, SortingProperty * properties, const QueryPlanOptimizationSettings & optimization_settings)
{
    if (const auto * read_from_merge_tree = typeid_cast<ReadFromMergeTree *>(parent->step.get()))
        return {read_from_merge_tree->getSortDescription(), SortingProperty::SortScope::Stream};

    if (const auto * aggregating_step = typeid_cast<AggregatingStep *>(parent->step.get()))
    {
        /// TODO: here we can apply aggregation-in-order after some sorting.

        auto sort_description = aggregating_step->getSortDescription();
        if (!sort_description.empty())
            return {std::move(sort_description), SortingProperty::SortScope::Global};
    }

    if (auto * mergine_aggeregated = typeid_cast<MergingAggregatedStep *>(parent->step.get()))
    {
        enableMemoryBoundMerging(*parent);

        auto sort_description = mergine_aggeregated->getSortDescription();
        if (!sort_description.empty())
            return {std::move(sort_description), SortingProperty::SortScope::Global};
    }

    if (auto * distinct_step = typeid_cast<DistinctStep *>(parent->step.get()))
    {
        /// Do not apply distinct-in-order second time.
        /// Also, prefer sorting from propertires against Distinct sorting description,
        /// cause the last one might be shorter, or may haver additional monotonic functions.
        if (optimization_settings.distinct_in_order && distinct_step->getSortDescription().empty() &&
            (properties->sort_scope == SortingProperty::SortScope::Global
            || (distinct_step->isPreliminary() && properties->sort_scope == SortingProperty::SortScope::Stream)))
        {
            distinct_step->applyOrder(getCollationAwareSortPrefixInColumns(properties->sort_description, distinct_step->getColumnNames()));
        }

        /// Distinct never breaks global order
        if (properties->sort_scope == SortingProperty::SortScope::Global)
            return *properties;

        /// Preliminary Distinct also does not break stream order
        if (distinct_step->isPreliminary() && properties->sort_scope == SortingProperty::SortScope::Stream)
            return *properties;
    }

    if (auto * expression_step = typeid_cast<ExpressionStep *>(parent->step.get()))
    {
        applyActionsToSortDescription(properties->sort_description, expression_step->getExpression());
        return std::move(*properties);
    }

    if (auto * filter_step = typeid_cast<FilterStep *>(parent->step.get()))
    {
        const auto & expr = filter_step->getExpression();
        const ActionsDAG::Node * out_to_skip = nullptr;
        if (filter_step->removesFilterColumn())
        {
            out_to_skip = expr.tryFindInOutputs(filter_step->getFilterColumnName());
            if (!out_to_skip)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Output nodes for ActionsDAG do not contain filter column name {}. DAG:\n{}",
                    filter_step->getFilterColumnName(),
                    expr.dumpDAG());
        }

        applyActionsToSortDescription(properties->sort_description, expr, out_to_skip);
        return std::move(*properties);
    }

    if (auto * sorting_step = typeid_cast<SortingStep *>(parent->step.get()))
    {
        if (optimization_settings.optimize_sorting_by_input_stream_properties
            && !sorting_step->hasPartitions() && sorting_step->getType() == SortingStep::Type::Full)
        {
            /// Convert Sorting to FinishSorting based on plan's sorting properties.
            auto common_prefix = commonPrefix(properties->sort_description, sorting_step->getSortDescription());
            if (!common_prefix.empty())
                /// Buffering is useful for reading from MergeTree, and it is applied in optimizeReadInOrder only.
                sorting_step->convertToFinishSorting(common_prefix, /*use_buffering*/ false, false);
        }

        auto scope = sorting_step->hasPartitions() ? SortingProperty::SortScope::Stream : SortingProperty::SortScope::Global;
        return {sorting_step->getSortDescription(), scope};
    }

    if (auto * limit_by_step = typeid_cast<LimitByStep *>(parent->step.get()))
    {
        if (properties->sort_scope != SortingProperty::SortScope::Global)
            return {};

        auto prefix = getCollationAwareSortPrefixInColumns(properties->sort_description, limit_by_step->getColumns());
        if (prefix.size() == limit_by_step->getColumns().size())
            limit_by_step->applyOrder(prefix);

        return std::move(*properties);
    }

    if (auto * negative_limit_by_step = typeid_cast<NegativeLimitByStep *>(parent->step.get()))
    {
        if (properties->sort_scope != SortingProperty::SortScope::Global)
            return {};

        auto prefix = getCollationAwareSortPrefixInColumns(properties->sort_description, negative_limit_by_step->getColumns());
        if (prefix.size() == negative_limit_by_step->getColumns().size())
            negative_limit_by_step->applyOrder(prefix);

        return std::move(*properties);
    }

    if (const auto * join_step = typeid_cast<const JoinStep *>(parent->step.get()); join_step && parent->children.size() == 2)
        return applyOrderToJoin(*join_step, properties);

    if (auto * transforming = dynamic_cast<ITransformingStep *>(parent->step.get()))
    {
        if (transforming->getDataStreamTraits().preserves_sorting)
            return std::move(*properties);
    }

    if (auto * union_step = typeid_cast<UnionStep *>(parent->step.get()))
    {
        SortDescription common_sort_description = std::move(properties->sort_description);

        for (size_t i = 1; i < parent->children.size(); ++i)
            common_sort_description = commonPrefix(common_sort_description, properties[i].sort_description);

        if (!common_sort_description.empty())
        {
            /// We are about to advertise per-stream sortedness to steps above the union
            /// (which may convert Sorting to FinishSorting or enable DISTINCT-in-order).
            /// Narrowing the union pipeline would concatenate sorted streams and silently
            /// invalidate this property, so forbid it.
            union_step->disableNarrowing();

            /// `UnionStep` concatenates child pipelines without a sorted merge, so with multiple
            /// children each stream stays sorted by the common prefix.
            auto sort_scope = parent->children.size() == 1 ? properties->sort_scope : SortingProperty::SortScope::Stream;
            return {std::move(common_sort_description), sort_scope};
        }
    }

    return {};
}

void applyOrder(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root)
{
    Stack stack;
    stack.push_back({.node = &root});

    using SortingPropertyStack = std::vector<SortingProperty>;
    SortingPropertyStack properties;

    while (!stack.empty())
    {
        auto & frame = stack.back();

        /// Traverse all children first.
        if (frame.next_child < frame.node->children.size())
        {
            auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
            ++frame.next_child;
            stack.push_back(next_frame);
            continue;
        }

        auto * node = frame.node;
        stack.pop_back();

        auto it = properties.begin() + (properties.size() - node->children.size());
        auto property = applyOrder(node, (it == properties.end()) ? nullptr : &*it, optimization_settings);
        properties.erase(it, properties.end());
        properties.push_back(std::move(property));
    }
}

}

}
