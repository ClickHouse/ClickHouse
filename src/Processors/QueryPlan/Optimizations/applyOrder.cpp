#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>

#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/LimitRangeStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/NegativeLimitByStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>

#include <Functions/IFunction.h>
#include <Interpreters/FullSortingMergeJoin.h>
#include <Interpreters/TableJoin.h>

#include <algorithm>
#include <unordered_set>


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

    /// The number of leading columns of `sort_description` that are computed from the current time for every block
    /// (see `getCurrentTimeOrderDirection`). Such a column does not decrease within a stream whatever the other
    /// columns are, so any of them can be put first, and a stream sorted by `sort_description` is sorted as well by
    /// any subset of these columns followed by the rest of the description.
    size_t current_time_prefix = 0;
};

/// If `node` is the current time computed for every block (`nowInBlock`, `nowInBlock64`), or a monotonic function of it,
/// returns the direction of the order of its values in every stream: 1 for ascending and -1 for descending. Otherwise 0.
///
/// The time is computed separately for every block by the transform that executes the expression, and the blocks of a
/// stream are processed in order, so the time does not decrease within a stream. The exception is the system clock set
/// back, which only makes the rows of the following blocks be counted in the group of a later time (the aggregation
/// in order does not go back), or produce a group for the same time twice.
static int getCurrentTimeOrderDirection(const ActionsDAG::Node * node)
{
    int direction = 1;
    while (true)
    {
        if (node->type == ActionsDAG::ActionType::ALIAS)
        {
            node = node->children.at(0);
            continue;
        }

        if (node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base)
            return 0;

        const auto & name = node->function_base->getName();
        if (name == "nowInBlock" || name == "nowInBlock64")
        {
            /// The arguments are the scale and the time zone, which do not change the order.
            for (const auto * child : node->children)
                if (child->type != ActionsDAG::ActionType::COLUMN)
                    return 0;
            return direction;
        }

        if (!node->function_base->hasInformationAboutMonotonicity())
            return 0;

        const ActionsDAG::Node * non_const_child = nullptr;
        for (const auto * child : node->children)
        {
            if (child->type == ActionsDAG::ActionType::COLUMN)
                continue;
            if (non_const_child)
                return 0;
            non_const_child = child;
        }

        if (!non_const_child)
            return 0;

        /// Monotonic on the whole range of the argument.
        const Field unbounded{};
        auto monotonicity = node->function_base->getMonotonicityForRange(*non_const_child->result_type, unbounded, unbounded);
        if (!monotonicity.is_monotonic)
            return 0;

        if (!monotonicity.is_positive)
            direction = -direction;

        node = non_const_child;
    }
}

/// Updates the sorting property by the actions of an expression or a filter, including the columns of the current time
/// the actions compute.
static void applyActionsToSortingProperty(SortingProperty & property, const ActionsDAG & dag, const ActionsDAG::Node * output_to_skip, bool with_current_time)
{
    applyActionsToSortDescription(property.sort_description, dag, output_to_skip);
    property.current_time_prefix = std::min(property.current_time_prefix, property.sort_description.size());

    /// A global order of several streams does not hold together with the current time, which is only ordered within
    /// every stream. Keep the global order, which is the more useful one.
    if (!with_current_time || (property.sort_scope == SortingProperty::SortScope::Global && !property.sort_description.empty()))
        return;

    SortDescription current_time_columns;
    for (const auto * output : dag.getOutputs())
    {
        if (output == output_to_skip)
            continue;

        int direction = getCurrentTimeOrderDirection(output);
        if (!direction)
            continue;

        const auto & name = output->result_name;
        auto has_name = [&](const SortColumnDescription & column) { return column.column_name == name; };
        if (std::ranges::any_of(current_time_columns, has_name) || std::ranges::any_of(property.sort_description, has_name))
            continue;

        current_time_columns.emplace_back(name, direction);
    }

    if (current_time_columns.empty())
        return;

    property.current_time_prefix += current_time_columns.size();
    current_time_columns.insert(
        current_time_columns.end(),
        std::make_move_iterator(property.sort_description.begin()),
        std::make_move_iterator(property.sort_description.end()));
    property.sort_description = std::move(current_time_columns);
    property.sort_scope = SortingProperty::SortScope::Stream;
}

/// Whether the plan reads a table with `STREAM`, which is an unbounded input.
static bool readsStream(const QueryPlan::Node & node)
{
    std::vector<const QueryPlan::Node *> stack{&node};
    while (!stack.empty())
    {
        const auto * current = stack.back();
        stack.pop_back();

        if (const auto * reading = typeid_cast<const ReadFromMergeTree *>(current->step.get()); reading && reading->getQueryInfo().isStream())
            return true;

        for (const auto * child : current->children)
            stack.push_back(child);
    }
    return false;
}

/// Turns on the aggregation in order if a key of `GROUP BY` is the current time (see `getCurrentTimeOrderDirection`).
/// It is useful for streaming queries, which receive the result of every group as soon as the time changes, instead
/// of at the end of the infinite input.
static void applyCurrentTimeOrderToAggregation(QueryPlan::Node & node, AggregatingStep & aggregating_step, const SortingProperty & property)
{
    if (!property.current_time_prefix || aggregating_step.inOrder() || aggregating_step.explicitSortingRequired()
        || aggregating_step.isGroupingSets() || aggregating_step.hasUniqExactInOrder())
        return;

    const auto & params = aggregating_step.getParams();
    /// The same conditions as in `optimizeAggregationInOrder`.
    if (params.keys.empty() || params.overflow_row)
        return;

    std::unordered_set<std::string_view> keys(params.keys.begin(), params.keys.end());

    /// The columns of the current time that are not the keys are skipped, see `SortingProperty::current_time_prefix`.
    SortDescription order;
    for (size_t i = 0; i < property.current_time_prefix; ++i)
        if (keys.contains(property.sort_description[i].column_name))
            order.push_back(property.sort_description[i]);

    if (order.empty())
        return;

    const size_t num_current_time_keys = order.size();
    order.insert(order.end(), property.sort_description.begin() + property.current_time_prefix, property.sort_description.end());

    order = getCollationAwareSortPrefixInColumns(order, params.keys, *aggregating_step.getInputHeaders().front());
    if (order.size() < num_current_time_keys)
        return;

    std::unordered_set<std::string_view> used_keys;
    for (const auto & column : order)
        used_keys.insert(column.column_name);

    SortDescription group_by_sort_description = order;
    for (const auto & key : params.keys)
        if (used_keys.emplace(key).second)
            group_by_sort_description.push_back(SortColumnDescription(key));

    aggregating_step.applyOrder(std::move(order), std::move(group_by_sort_description));
    aggregating_step.enableFlushOnOrderKeyChange(readsStream(node));
}

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

    const auto & ordered_input_header = *join_step.getInputHeaders()[ordered_child];
    auto sort_description
        = getCollationAwareSortPrefixInColumns(children_properties[ordered_child].sort_description, key_names, ordered_input_header);

    /// Keep the key columns that reach the output under their own, unambiguous name. The legacy planner lets
    /// both inputs carry a column of the same name - `JOIN ... USING (k)` is the common shape - and renames
    /// the copy of the right side out of the way (`TableJoin::renamedRightColumnName`), so a name carried by
    /// both inputs denotes the column of the left side in the output. That is the ordered column itself when
    /// the left side is the ordered one, and a different column when the right side is: there the ordered key
    /// reaches the output under its renamed name, which is not the name we are sorted by.
    const bool ordered_side_is_left = ordered_child == algorithm_left_child;
    const auto & output_header = *join_step.getOutputHeader();
    const auto & other_input_header = *join_step.getInputHeaders()[1 - ordered_child];

    /// A name carried by both inputs does denote the ordered column of the right side in one case: when the
    /// renamed copy of the right key is not selected, the legacy planner merges the two key columns of a
    /// `USING` clause into the single output column of the left side, and `MergeJoinAlgorithm` fills it from
    /// the right key for the rows that have no match on the left (`TableJoin::leftToRightKeyRemap`). For a
    /// RIGHT join the left key column then holds the value of the right key on every row, so the ordered key
    /// does reach the output - only under the name of the left key, while the renamed right copy is the
    /// column that carries defaults there.
    NameToNameMap right_key_to_merged_output;
    if (isRight(kind))
    {
        for (const auto & [left_key, right_key] : table_join.leftToRightKeyRemap())
            right_key_to_merged_output.emplace(right_key, left_key);
    }

    /// Every row of an INNER join has equal keys on both sides, so a key of the other side carries the same
    /// order as the ordered key it is compared with. That matters when the ordered key itself is not needed
    /// above the join and only the other one is kept, as in `a JOIN b ON a.id = b.id JOIN c ON b.id = c.id`.
    /// It does not hold for the last key of an ASOF join, which is not compared for equality.
    NameToNameMap ordered_key_to_equal_key;
    if (isInner(kind) && table_join.strictness() != JoinStrictness::Asof)
    {
        for (size_t i = 0; i < clause.key_names_left.size(); ++i)
            ordered_key_to_equal_key.emplace(clause.key_names_left[i], clause.key_names_right[i]);
    }

    size_t num_columns_in_output = 0;
    for (; num_columns_in_output < sort_description.size(); ++num_columns_in_output)
    {
        auto & name = sort_description[num_columns_in_output].column_name;

        if (auto it = right_key_to_merged_output.find(name); it != right_key_to_merged_output.end())
        {
            /// The merged column is the ordered one under the name of the other side, so the check for a key
            /// shadowed by that side does not apply to it.
            if (!output_header.has(it->second))
                break;
            name = it->second;
            continue;
        }

        if (!output_header.has(name))
        {
            /// The equal key comes from the other side, so it must not be shadowed by the ordered side.
            auto it = ordered_key_to_equal_key.find(name);
            if (it == ordered_key_to_equal_key.end() || !output_header.has(it->second) || ordered_input_header.has(it->second))
                break;
            name = it->second;
            continue;
        }
        if (other_input_header.has(name) && (!ordered_side_is_left || table_join.renamedRightColumnName(name) == name))
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
    if (const auto * source_step = dynamic_cast<const ISourceStep*>(parent->step.get()))
        return {source_step->getSortDescription(), SortingProperty::SortScope::Stream};

    if (auto * aggregating_step = typeid_cast<AggregatingStep *>(parent->step.get()))
    {
        /// TODO: here we can apply aggregation-in-order after some sorting.
        if (optimization_settings.aggregation_in_order_by_current_time)
            applyCurrentTimeOrderToAggregation(*parent, *aggregating_step, *properties);

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
            distinct_step->applyOrder(getCollationAwareSortPrefixInColumns(
                properties->sort_description, distinct_step->getColumnNames(), *distinct_step->getInputHeaders().front()));
        }

        /// Distinct never breaks global order: the steps above may rely on it, so the final `DISTINCT`,
        /// which may spill, has to restore the order after the spill (see
        /// `DistinctStep::preserveInputOrder`). The preliminary `DISTINCT` never spills, and an empty
        /// description carries no order to preserve.
        if (properties->sort_scope == SortingProperty::SortScope::Global)
        {
            if (!distinct_step->isPreliminary() && !properties->sort_description.empty())
                distinct_step->preserveInputOrder();
            return *properties;
        }

        /// Preliminary Distinct also does not break stream order
        if (distinct_step->isPreliminary() && properties->sort_scope == SortingProperty::SortScope::Stream)
            return *properties;
    }

    if (auto * expression_step = typeid_cast<ExpressionStep *>(parent->step.get()))
    {
        applyActionsToSortingProperty(
            *properties, expression_step->getExpression(), nullptr, optimization_settings.aggregation_in_order_by_current_time);
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

        applyActionsToSortingProperty(*properties, expr, out_to_skip, optimization_settings.aggregation_in_order_by_current_time);
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

        /// A partitioned `FinishSorting` scatters its merged stream into the window partitions and streams the
        /// rows out of every shard as soon as their prefix group is complete (see `SortingStep::transformPipeline`).
        /// A `MergingSortedTransform` above it would pull from the shards selectively, while the scatter has to
        /// push to all of them, which can deadlock. So do not advertise this order: the sorting above stays a full
        /// sort, which consumes every shard independently.
        if (sorting_step->hasPartitions() && sorting_step->getType() == SortingStep::Type::FinishSorting)
            return {};

        auto scope = sorting_step->hasPartitions() ? SortingProperty::SortScope::Stream : SortingProperty::SortScope::Global;
        return {sorting_step->getSortDescription(), scope};
    }

    if (auto * limit_by_step = typeid_cast<LimitByStep *>(parent->step.get()))
    {
        if (properties->sort_scope != SortingProperty::SortScope::Global)
            return {};

        auto prefix = getCollationAwareSortPrefixInColumns(
            properties->sort_description, limit_by_step->getColumns(), *limit_by_step->getInputHeaders().front());
        if (prefix.size() == limit_by_step->getColumns().size())
            limit_by_step->applyOrder(prefix);

        return std::move(*properties);
    }

    if (auto * negative_limit_by_step = typeid_cast<NegativeLimitByStep *>(parent->step.get()))
    {
        if (properties->sort_scope != SortingProperty::SortScope::Global)
            return {};

        auto prefix = getCollationAwareSortPrefixInColumns(
            properties->sort_description, negative_limit_by_step->getColumns(), *negative_limit_by_step->getInputHeaders().front());
        if (prefix.size() == negative_limit_by_step->getColumns().size())
            negative_limit_by_step->applyOrder(prefix);

        return std::move(*properties);
    }

    if (const auto * join_step = typeid_cast<const JoinStep *>(parent->step.get()); join_step && parent->children.size() == 2)
        return applyOrderToJoin(*join_step, properties);

    if (typeid_cast<LimitRangeStep *>(parent->step.get()))
    {
        /// The range is evaluated over a single stream, so several per-stream-sorted inputs are
        /// concatenated without a merge and only a global order survives the step.
        if (properties->sort_scope != SortingProperty::SortScope::Global)
            return {};

        return std::move(*properties);
    }

    if (auto * transforming = dynamic_cast<ITransformingStep *>(parent->step.get()))
    {
        if (transforming->getDataStreamTraits().preserves_sorting)
            return std::move(*properties);
    }

    if (auto * union_step = typeid_cast<UnionStep *>(parent->step.get()))
    {
        SortDescription common_sort_description = std::move(properties->sort_description);
        size_t current_time_prefix = properties->current_time_prefix;

        for (size_t i = 1; i < parent->children.size(); ++i)
        {
            common_sort_description = commonPrefix(common_sort_description, properties[i].sort_description);
            current_time_prefix = std::min(current_time_prefix, properties[i].current_time_prefix);
        }
        current_time_prefix = std::min(current_time_prefix, common_sort_description.size());

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
            return {std::move(common_sort_description), sort_scope, current_time_prefix};
        }
    }

    return {};
}

void applyOrder(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root)
{
    Stack stack;
    stack.push_back({.node = &root});

    using SortingPropertyStack = VectorWithMemoryTracking<SortingProperty>;
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
