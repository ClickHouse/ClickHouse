#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/Optimizations/optimizeReadInOrder.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/typeid_cast.h>

#include <optional>
#include <utility>

namespace DB::QueryPlanOptimizations
{

namespace
{

/// Walk down a single-child chain looking for a `ReadFromMergeTree` step. Used for the
/// parallel-replicas guard below.
const ReadFromMergeTree * findMergeTreeRead(const QueryPlan::Node * node)
{
    while (node)
    {
        if (const auto * reading = typeid_cast<const ReadFromMergeTree *>(node->step.get()))
            return reading;
        if (node->children.size() != 1)
            return nullptr;
        node = node->children.front();
    }
    return nullptr;
}

/// Build `length(c1) > 0 OR ... OR length(cn) > 0` over the ARRAY JOIN input header, which is
/// exactly the emptiness condition an inner `ARRAY JOIN` applies to its input rows.
///
/// Why the condition spans *all* joined columns rather than just `length(c1) > 0`: with several
/// joined arrays of different sizes an aligned inner `ARRAY JOIN` throws
/// `SIZES_OF_ARRAYS_DONT_MATCH` (`ArrayJoinResultIterator::next` checks `hasEqualOffsets`).
/// A guard on one column alone would filter the row `(c1 = [], c2 = [1])` out before it ever
/// reaches the `ARRAY JOIN`, turning a query that throws today into one that silently succeeds.
/// The disjunction keeps every row where any array is non-empty - those still throw - and drops
/// only rows where all arrays are empty, which produce no output and never throw.
///
/// Only `length`, `greater` and `or` are used, all of which build without a `Context` (which plan
/// optimizations do not have): `FunctionComparison::create` handles a null context explicitly and
/// `FunctionAnyArityLogical::create` ignores it. `greatest` would read
/// `least_greatest_legacy_null_behavior` from the context and cannot be used here.
///
/// Returns `std::nullopt` when the guard cannot be built or would be a constant (all joined
/// columns are constants, e.g. `ARRAY JOIN [1, 2]`), in which case no guard is needed.
std::optional<std::pair<ActionsDAG, String>> buildEmptinessGuard(const Block & input_header, const Names & array_join_columns)
{
    if (array_join_columns.empty())
        return {};

    ActionsDAG dag(input_header.getColumnsWithTypeAndName());

    auto length_function = FunctionFactory::instance().get("length", nullptr);
    auto greater_function = FunctionFactory::instance().get("greater", nullptr);

    DataTypePtr zero_type = std::make_shared<DataTypeUInt8>();
    const auto * zero = &dag.addColumn(zero_type->createColumnConst(0, Field(UInt64(0))), zero_type, "0");

    ActionsDAG::NodeRawConstPtrs non_empty;
    non_empty.reserve(array_join_columns.size());

    for (const auto & column_name : array_join_columns)
    {
        const auto * input = dag.tryFindInOutputs(column_name);
        if (!input)
            return {};

        /// A joined column that is not an array or a map cannot appear here (`ArrayJoinAction::prepare`
        /// would have thrown), but the guard is only valid for those two, so check explicitly.
        const auto & type = input->result_type;
        if (!typeid_cast<const DataTypeArray *>(type.get()) && !typeid_cast<const DataTypeMap *>(type.get()))
            return {};

        const auto & length = dag.addFunction(length_function, {input}, {});
        non_empty.push_back(&dag.addFunction(greater_function, {&length, zero}, {}));
    }

    const auto * guard = non_empty.front();
    if (non_empty.size() > 1)
    {
        auto or_function = FunctionFactory::instance().get("or", nullptr);
        guard = &dag.addFunction(or_function, std::move(non_empty), {});
    }

    /// All joined columns were constants (e.g. `ARRAY JOIN [1, 2]`) and the condition folded to a
    /// constant. Filtering by it would be pointless work.
    if (guard->column)
        return {};

    dag.getOutputs().push_back(guard);

    return std::make_pair(std::move(dag), guard->result_name);
}

}

/// Restrict the input of an `ARRAY JOIN` to the rows a `LIMIT n` on top of an `ORDER BY` can
/// possibly need, by grafting a second `Sort(K, limit = n) + Limit(n)` below the `ARRAY JOIN`.
///
/// Soundness sketch
/// ----------------
/// Consider `Limit(n) <- Sort(K) <- ArrayJoin(c)` where `K` does not reference `c`. Every output
/// row of the `ARRAY JOIN` inherits its `K` value from the input row it was expanded from, so the
/// top-n output rows by `K` are drawn from the input rows with the n smallest (or largest) `K`
/// values - that is, the top-n input rows by `K`. Restricting the input to its own top-n by `K`
/// before the expansion therefore cannot change the final result. The outer `Sort + Limit` is
/// kept, because the expansion multiplies each surviving input row into several output rows.
///
/// This relies on every input row producing at least one output row. `LEFT ARRAY JOIN` satisfies
/// it by construction (`emptyArrayToSingle` in `ArrayJoinResultIterator`'s constructor gives every
/// empty array one default element). An inner `ARRAY JOIN` drops rows whose arrays are all empty,
/// so for it we first insert an emptiness guard below the new sort: the sort then picks the top-n
/// among the rows that survive the `ARRAY JOIN`, and each of those expands into at least one row.
///
/// Note that we duplicate the `Sort + Limit` instead of moving the existing one below the
/// `ARRAY JOIN`. Moving would avoid the second sort, but it would also move the
/// `PartialSortingTransform` that carries the `rows_before_limit_at_least` counter (see
/// `initRowsBeforeLimit`) below the expansion, silently changing that value from the number of
/// expanded rows to the number of input rows.
///
/// Pattern matched: `LimitStep -> SortingStep -> [ExpressionStep] -> ArrayJoinStep`.
/// The optional `ExpressionStep`s are allowed only when every sort key passes through them
/// unchanged (see `peelPassThroughExpressions`).
size_t tryTopKThroughArrayJoin(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings)
{
    auto * limit_step = typeid_cast<LimitStep *>(parent_node->step.get());
    if (!limit_step)
        return 0;

    /// LIMIT WITH TIES needs to know how many rows have the threshold value, so we cannot
    /// restrict the input to n rows.
    if (limit_step->withTies())
        return 0;

    /// Skip when `always_read_till_end` is set (e.g. `WITH TOTALS`, `exact_rows_before_limit`).
    /// Both require the upstream to keep processing past the limit, which the inserted `Limit`
    /// would prevent.
    if (limit_step->alwaysReadTillEnd())
        return 0;

    if (parent_node->children.size() != 1)
        return 0;

    auto * sort_node = parent_node->children.front();
    auto * sort_step = typeid_cast<SortingStep *>(sort_node->step.get());
    if (!sort_step)
        return 0;

    /// Only Full sort is meaningful here. FinishSorting/MergingSorted mean the input is already
    /// (partially) sorted, in which case the pipeline already stops early and there is nothing
    /// to gain. A partitioned sort produces a per-partition order, so a plain row-count limit
    /// below it would not correspond to the top-n of any partition.
    if (sort_step->getType() != SortingStep::Type::Full || sort_step->hasPartitions())
        return 0;

    if (sort_node->children.size() != 1)
        return 0;

    SortDescription description = sort_step->getSortDescription();
    QueryPlan::Node * array_join_node = sort_node->children.front();
    if (!peelPassThroughExpressions(array_join_node, description))
        return 0;

    auto * array_join_step = typeid_cast<ArrayJoinStep *>(array_join_node->step.get());
    if (!array_join_step)
        return 0;
    if (array_join_node->children.size() != 1)
        return 0;

    const auto & array_join_columns = array_join_step->getColumns();
    const NameSet array_join_column_names(array_join_columns.begin(), array_join_columns.end());
    const auto & array_join_input_header = array_join_step->getInputHeaders().front();

    /// Every sort key must be carried through the `ARRAY JOIN` unchanged. A joined column keeps
    /// its *name* across the step and only changes its type (`ArrayJoinAction::prepare` replaces
    /// the column in place), so checking that the name is present in the input header is not
    /// enough - `ORDER BY arr` would pass that check while sorting by a completely different
    /// value below the step.
    for (const auto & sort_column : description)
    {
        if (array_join_column_names.contains(sort_column.column_name))
            return 0;
        if (!array_join_input_header->has(sort_column.column_name))
            return 0;
    }

    const size_t n = limit_step->getLimitForSorting();
    if (n == 0)
        return 0;

    /// Reuse the cap that already gates `tryOptimizeTopK`. If the user disabled large-N TopK
    /// optimization there, do not work around it here.
    if (settings.max_limit_for_top_k_optimization && n > settings.max_limit_for_top_k_optimization)
        return 0;

    QueryPlan::Node * array_join_input_node = array_join_node->children.front();

    /// Avoid re-applying: if the immediate child is already a LimitStep with a limit no larger
    /// than `n`, the optimization has already fired (or there is a user-supplied LIMIT we should
    /// not weaken).
    if (const auto * existing_limit = typeid_cast<const LimitStep *>(array_join_input_node->step.get()))
    {
        if (existing_limit->getLimit() <= n && existing_limit->getOffset() == 0)
            return 0;
    }

    /// Do not insert a `Sort + Limit` when the input is read with parallel replicas. The inserted
    /// `Sort` would let `optimizeReadInOrder` turn the scan into `WithOrder` mode, conflicting
    /// with the coordination mode the other replicas pick ("Replica decided to read in Default
    /// mode, not in WithOrder").
    if (const auto * reading = findMergeTreeRead(array_join_input_node))
    {
        if (reading->isParallelReadingFromReplicas())
            return 0;
    }

    /// Defer to `optimizeReadInOrder` (second pass) when the input can stream rows in the
    /// requested order straight from the storage's sorting key. That path scans only the rows the
    /// limit keeps, without materializing a sort, so it is strictly better than what we would do
    /// here. This is the steady state for `LEFT ARRAY JOIN ... ORDER BY <primary key>`, which
    /// already reads `InOrder` today.
    if (settings.read_in_order)
    {
        SortingStep probe_sort_step(
            array_join_input_node->step->getOutputHeader(),
            description,
            n,
            sort_step->getSettings());

        if (wouldReadInOrderBeUseful(
                probe_sort_step,
                *array_join_input_node,
                settings.read_in_order_through_join,
                settings.read_in_order_through_spilling_join))
            return 0;
    }

    /// An inner ARRAY JOIN drops input rows whose arrays are all empty. Filter them out below the
    /// new sort so that each of the n rows it keeps expands into at least one output row.
    QueryPlan::Node * new_child_node = array_join_input_node;
    if (!array_join_step->isLeft())
    {
        auto guard = buildEmptinessGuard(*array_join_input_node->step->getOutputHeader(), array_join_columns);
        if (!guard)
            return 0;

        auto & guard_node = nodes.emplace_back();
        guard_node.children.push_back(new_child_node);
        guard_node.step = std::make_unique<FilterStep>(
            new_child_node->step->getOutputHeader(),
            std::move(guard->first),
            guard->second,
            /*remove_filter_column_=*/true);
        guard_node.step->setStepDescription("Non-empty arrays for ARRAY JOIN");
        new_child_node = &guard_node;
    }

    auto & new_sort_node = nodes.emplace_back();
    new_sort_node.children.push_back(new_child_node);
    new_sort_node.step = std::make_unique<SortingStep>(
        new_child_node->step->getOutputHeader(),
        description,
        n,
        sort_step->getSettings());

    auto & new_limit_node = nodes.emplace_back();
    new_limit_node.children.push_back(&new_sort_node);
    new_limit_node.step = std::make_unique<LimitStep>(new_sort_node.step->getOutputHeader(), n, /*offset_=*/0);

    array_join_node->children[0] = &new_limit_node;
    array_join_step->updateInputHeader(new_limit_node.step->getOutputHeader());

    /// Re-run optimizations on the modified subtree. The new `Limit -> Sort -> [Filter] -> Read`
    /// shape is exactly what `tryOptimizeTopK` matches (it can never fire on the original plan,
    /// because the `ArrayJoinStep` sits between the sort and the reading step), and the guard is
    /// a candidate for `tryPushDownFilter`.
    return 4;
}

}
