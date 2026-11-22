#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>

#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
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
#include <Storages/StorageMerge.h>

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
    /// Columns equal on every row (the keys of the INNER merge joins below), so the data is sorted by any column
    /// equal to a sort column as well.
    EquivalenceClasses<String> equivalences = {};
};

/// Maps the property through `dag`. Where the sort description is cut at a column that `dag` does not carry, the
/// columns from there on are replaced by equal columns that `dag` passes on unchanged, if that keeps more of it.
static void applyActionsToProperty(SortingProperty & property, const ActionsDAG & dag, const ActionsDAG::Node * output_to_skip)
{
    auto substituted = property.sort_description;
    applyActionsToSortDescription(property.sort_description, dag, output_to_skip);
    if (property.equivalences.getMemberToClassMap().empty())
        return;

    auto identity_outputs = identityOutputNames(dag, output_to_skip);
    if (property.sort_description.size() < substituted.size())
    {
        for (size_t i = property.sort_description.size(); i < substituted.size(); ++i)
        {
            auto & column = substituted[i];
            if (identity_outputs.contains(column.column_name))
                continue;
            if (auto equal = property.equivalences.getClass(column.column_name))
                if (auto it = std::ranges::find_if(*equal, [&](const String & member) { return identity_outputs.contains(member); });
                    it != equal->end())
                    column.column_name = *it;
        }
        applyActionsToSortDescription(substituted, dag, output_to_skip);
        if (substituted.size() > property.sort_description.size())
            property.sort_description = std::move(substituted);
    }
    property.equivalences = remapEquivalences(property.equivalences, identity_outputs);
}

/// Whether `step` is a join that emits the rows of its probe side (`children.front()`) in their
/// original order, with the same conditions as `findReadingStep` in `optimizeReadInOrder`.
static bool isOrderPreservingJoinStep(const IQueryPlanStep & step)
{
    const IJoin * join = nullptr;
    if (const auto * join_step = typeid_cast<const JoinStep *>(&step))
        join = join_step->getJoin().get();
    else if (const auto * filled_join_step = typeid_cast<const FilledJoinStep *>(&step))
        join = filled_join_step->getJoin().get();

    if (!join)
        return false;

    const auto & table_join = join->getTableJoin();
    auto strictness = table_join.strictness();
    return (strictness == JoinStrictness::Any || strictness == JoinStrictness::All) && isInnerOrLeft(table_join.kind())
        && !join->hasDelayedBlocks() && join->preservesLeftBlockOrder();
}

/// Marks the reading step below `node` as preferring multiple streams, so that per-stream work
/// installed above it (the pre-distinct transforms of distinct-in-order, the per-stream `LIMIT BY`
/// pre-filter of `pushLimitByIntoSort`) is not collapsed into a single stream and serialized by
/// `PrefetchingConcatProcessor`.
///
/// Distinct-in-order can be installed either here (from plan sorting properties) or by
/// `optimizeDistinctInOrder`. The latter sets `prefer_multiple_streams` on the underlying
/// `ReadFromMergeTree` itself. When distinct order is applied from sorting properties instead, the
/// reading step does not get that signal, so we propagate it here by descending through
/// order-preserving steps (into every branch of a `UnionStep`, and into the probe side of an
/// order-preserving join) to the reading steps.
void preferMultipleStreamsForReadingBelow(QueryPlan::Node * node)
{
    while (node)
    {
        IQueryPlanStep * step = node->step.get();

        if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
        {
            reading->setPreferMultipleStreams();
            return;
        }

        /// A `Merge` table hides the actual reads inside child plans - forward the opt-out to them.
        if (auto * merge = typeid_cast<ReadFromMerge *>(step))
        {
            merge->setPreferMultipleStreams();
            return;
        }

        if (node->children.empty())
            return;

        /// A set-building step keeps the main pipeline in `children.front()` and adds one more child
        /// per set subquery, so it usually has several children. Follow only the main child, the same
        /// way `findReadingStep` in `optimizeReadInOrder` does.
        if (typeid_cast<CreatingSetsStep *>(step) || typeid_cast<DelayedCreatingSetsStep *>(step))
        {
            node = node->children.front();
            continue;
        }

        /// Each branch of a `UnionStep` has its own reading step, and `optimizeReadInOrder` reads every
        /// branch in order, so each of them may choose `PrefetchingConcatProcessor` - forward the opt-out
        /// to all branches.
        if (typeid_cast<UnionStep *>(step))
        {
            for (auto * child : node->children)
                preferMultipleStreamsForReadingBelow(child);
            return;
        }

        /// A `JoinStep` or a `FilledJoinStep` that keeps the order of its probe side runs one
        /// `JoiningTransform` per probe-side stream, and `optimizeReadInOrder` reads the probe side in
        /// order through it (see `findReadingStep`), so the per-stream work above the join depends on
        /// the probe-side read staying parallel. Follow the probe side, which is `children.front()`.
        if (isOrderPreservingJoinStep(*step))
        {
            node = node->children.front();
            continue;
        }

        if (node->children.size() != 1)
            return;

        /// Only descend through steps that keep a single underlying reading pipeline
        /// and preserve its per-stream order, mirroring `findReadingStep` in `optimizeReadInOrder`.
        if (!typeid_cast<ExpressionStep *>(step)
            && !typeid_cast<FilterStep *>(step)
            && !typeid_cast<ArrayJoinStep *>(step)
            && !typeid_cast<DistinctStep *>(step))
            return;

        node = node->children.front();
    }
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
    /// It does not hold for the last key of an ASOF join, which is not compared for equality. The equalities of
    /// the inputs are carried as well.
    const bool keys_are_equal = isInner(kind) && table_join.strictness() != JoinStrictness::Asof;
    NameToNameMap ordered_key_to_equal_key;
    if (keys_are_equal)
    {
        for (size_t i = 0; i < clause.key_names_left.size(); ++i)
            ordered_key_to_equal_key.emplace(clause.key_names_left[i], clause.key_names_right[i]);
    }

    /// A name carried by both inputs is ambiguous (see above), so it is never taken as equal to anything.
    UnorderedMapWithMemoryTracking<String, Names> unambiguous_inputs;
    for (const auto * header : {&ordered_input_header, &other_input_header})
        for (const auto & column : *header)
            if (!ordered_input_header.has(column.name) || !other_input_header.has(column.name))
                unambiguous_inputs.emplace(column.name, Names{column.name});

    auto equivalences = remapEquivalences(children_properties[ordered_child].equivalences, unambiguous_inputs);
    if (keys_are_equal)
    {
        equivalences.merge(remapEquivalences(children_properties[1 - ordered_child].equivalences, unambiguous_inputs));
        for (const auto & [ordered_key, equal_key] : ordered_key_to_equal_key)
            if (unambiguous_inputs.contains(ordered_key) && unambiguous_inputs.contains(equal_key))
                equivalences.add(ordered_key, equal_key);
    }

    UnorderedMapWithMemoryTracking<String, Names> unambiguous_outputs;
    for (const auto & column : output_header)
        if (unambiguous_inputs.contains(column.name))
            unambiguous_outputs.emplace(column.name, Names{column.name});

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
            {
                auto equal = equivalences.getClass(name);
                if (!equal)
                    break;
                auto member = std::ranges::find_if(*equal, [&](const String & column) { return unambiguous_outputs.contains(column); });
                if (member == equal->end())
                    break;
                name = *member;
                continue;
            }
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
    return {std::move(sort_description), SortingProperty::SortScope::Stream, remapEquivalences(equivalences, unambiguous_outputs)};
}

static SortingProperty applyOrder(QueryPlan::Node * parent, SortingProperty * properties, const QueryPlanOptimizationSettings & optimization_settings)
{
    if (const auto * source_step = dynamic_cast<const ISourceStep*>(parent->step.get()))
        return {source_step->getSortDescription(), SortingProperty::SortScope::Stream};

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
            distinct_step->applyOrder(getCollationAwareSortPrefixInColumns(
                properties->sort_description, distinct_step->getColumnNames(), *distinct_step->getInputHeaders().front()));

            /// Only preliminary distinct performs per-stream deduplication. A final distinct merges
            /// its input into one stream, so keeping the read parallel would only disable the
            /// `PrefetchingConcatProcessor` fast path without preserving parallel work.
            if (distinct_step->isPreliminary())
                preferMultipleStreamsForReadingBelow(parent);
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
        applyActionsToProperty(*properties, expression_step->getExpression(), nullptr);
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

        applyActionsToProperty(*properties, expr, out_to_skip);
        return std::move(*properties);
    }

    if (auto * sorting_step = typeid_cast<SortingStep *>(parent->step.get()))
    {
        if (optimization_settings.optimize_sorting_by_input_stream_properties
            && !sorting_step->hasPartitions() && sorting_step->getType() == SortingStep::Type::Full)
        {
            /// A column equal on every row to a sort column of the input is sorted the same way.
            auto input_description = properties->sort_description;
            const auto & sort_description = sorting_step->getSortDescription();
            for (size_t i = 0; i < std::min(input_description.size(), sort_description.size()); ++i)
                if (auto equal = properties->equivalences.getClass(input_description[i].column_name);
                    equal && std::ranges::find(*equal, sort_description[i].column_name) != equal->end())
                    input_description[i].column_name = sort_description[i].column_name;

            /// Convert Sorting to FinishSorting based on plan's sorting properties.
            auto common_prefix = commonPrefix(input_description, sort_description);
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
        return {sorting_step->getSortDescription(), scope, std::move(properties->equivalences)};
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
        /// A step may add rows (`WITH FILL`), so the equalities do not survive it.
        properties->equivalences = {};
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
