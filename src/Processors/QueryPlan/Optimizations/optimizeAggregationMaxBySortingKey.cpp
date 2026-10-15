#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Range.h>
#include <Core/Settings.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/TypeTree.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/Optimizations/projectionsCommon.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 merge_tree_min_bytes_for_seek;
    extern const SettingsUInt64 merge_tree_min_rows_for_seek;
}

}

namespace DB::QueryPlanOptimizations
{

/// Skip granules that cannot contain `max(k)` or `min(k)` for any group of `GROUP BY <keys>`, where `k`
/// is a sorting key column and every key depends only on the sorting key columns before `k` (the prefix).
///
/// Within a part, rows are sorted by `(<prefix>, k)`, so each value of the prefix forms a run of rows sorted by `k`.
/// The primary index stores the first row of every granule. If granules `j` and `j + 1` start with the same prefix,
/// then every row of granule `j` belongs to that run, and its `k` is not greater than `k` of the first row of
/// granule `j + 1`. So granule `j` holds neither the start of a new run nor the maximum of an existing one, and can
/// be skipped. A group is a union of whole runs, because its keys are functions of the prefix, so the aggregation
/// over the remaining rows still finds the maximum of every group: it is the largest maximum of its runs.
/// For example, with `ORDER BY (exchange, symbol, ts)`, `GROUP BY exchange` with `max(ts)` keeps the last granule
/// of every `(exchange, symbol)` run, and `WHERE exchange = 'X' GROUP BY symbol` keeps the last granule of every
/// symbol of that exchange.
///
/// For the minimum, granule `j` must also not hold the first row of its run. That is guaranteed when
/// granule `j - 1` starts with the same prefix too, because then the run already started at or before the
/// first row of granule `j - 1`. So with `min`, granule `j` is skipped only when granules `j - 1`, `j` and
/// `j + 1` all start with the same prefix. This condition implies the one for the maximum, so it also serves
/// queries with both.
///
/// Filters. The filter is split into conjuncts:
/// - A conjunct that depends only on the prefix columns removes whole runs, so it does not matter.
/// - The other conjuncts must be certainly true for the row that proves the granule unnecessary: the first row of
///   granule `j + 1` for the maximum, and the first row of granule `j - 1` for the minimum. These rows are
///   real rows of the same run, and their `k` bounds every `k` in granule `j`; if they pass the filter, the
///   run's answer is not in granule `j`. They are checked on the primary index with a `KeyCondition`, which
///   must be exact (not relaxed): a conjunct on a column outside the primary index, or on a function the
///   analysis does not know, disables the optimization.
///
/// Supported aggregates: `max(k)`, `argMax(x, k)`, `min(k)` and `argMin(x, k)`, all on the same `k`, plus aggregates
/// whose result depends only on the set of distinct values of arguments that depend only on the prefix (`min`, `max`,
/// `any`, `uniq*`, `groupUniqArray`, ...), which only need every run to be present.
///
/// Without `max(k)`-like aggregates (`GROUP BY` with only such aggregates or none, and `DISTINCT`), the prefix is the
/// shortest sorting key prefix that covers the keys and arguments, and the last granule of each run is kept, as for
/// `max`: then every run is present.
///
/// `ORDER BY <keys>, k DESC LIMIT n BY <keys>` needs the last `n` rows of each run (the first `n` with `ASC`), plus the
/// offset: granule `j` is skipped when at least that many rows of its run are known to follow it (or precede it).
///
/// The rule assumes that every row of the part reaches the aggregation unless the filter removes it, and that
/// the stored order is the order the aggregation uses. So it is not applied with `FINAL`, sampling, reverse
/// sorting keys or key types whose order or equality differ from the aggregation's, and parts with deleted rows,
/// patch parts or on-the-fly mutations are read in full.

/// The order and equality of the primary index must agree with the aggregation. `Nullable`: `max` and `min`
/// ignore NULLs, which sort at an end of the run. Floating point: NaN has no consistent order, and `-0` and `+0`
/// sort as equal but form different groups. `Variant`, `Dynamic` and `JSON` can hold such values at runtime.
static bool isSupportedKeyType(const IDataType & type)
{
    return !type.isNullable() && !type.isLowCardinalityNullable() && !isFloat(type) && !isVariant(type)
        && !isDynamic(type) && !isObject(type);
}

static bool isSupportedKeyTypeRecursive(const DataTypePtr & type)
{
    return !anyInTypeTree(*type, [](const IDataType & node) { return !isSupportedKeyType(node); });
}

/// Follows `name` from the outputs of the expressions between the aggregation and the reading step down to a
/// column of the reading step, through aliases only. Without expressions, the name is already a column.
static std::optional<String> mapToReadingColumn(const std::optional<ActionsDAG> & dag, const String & name)
{
    if (!dag)
        return name;

    const auto * node = dag->tryFindInOutputs(name);
    if (!node)
        return {};
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    if (node->type != ActionsDAG::ActionType::INPUT)
        return {};
    return node->result_name;
}

/// Whether `predicate` is deterministic and depends only on the columns in `columns`.
static bool dependsOnlyOn(const ActionsDAG::Node * predicate, const NameSet & columns)
{
    std::vector<const ActionsDAG::Node *> stack{predicate};
    std::unordered_set<const ActionsDAG::Node *> visited;
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();
        if (!visited.insert(node).second)
            continue;
        if (!node->isDeterministic() || node->type == ActionsDAG::ActionType::ARRAY_JOIN)
            return false;
        if (node->type == ActionsDAG::ActionType::INPUT && !columns.contains(node->result_name))
            return false;
        for (const auto * child : node->children)
            stack.push_back(child);
    }
    return true;
}

/// Aggregate functions whose result depends only on the set of distinct values of their arguments, not on how
/// many rows have each value. Over arguments that depend only on the prefix columns, they only need every run to
/// be present, which keeping at least one granule per run guarantees.
static bool dependsOnlyOnDistinctValues(const String & function_name)
{
    static const std::unordered_set<String> names{
        "min", "max", "any", "uniq", "uniqExact", "uniqCombined", "uniqCombined64", "uniqHLL12", "uniqTheta",
        "groupUniqArray", "groupBitOr", "groupBitAnd", "sumDistinct", "avgDistinct"};
    return names.contains(function_name);
}

/// The inputs (columns of the reading step) that the output `name` of the expressions depends on.
static std::optional<NameSet> getRequiredColumns(const std::optional<ActionsDAG> & dag, const String & name)
{
    if (!dag)
        return NameSet{name};

    const auto * output = dag->tryFindInOutputs(name);
    if (!output)
        return {};

    NameSet columns;
    std::vector<const ActionsDAG::Node *> stack{output};
    std::unordered_set<const ActionsDAG::Node *> visited;
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();
        if (!visited.insert(node).second)
            continue;
        if (node->type == ActionsDAG::ActionType::INPUT)
            columns.insert(node->result_name);
        for (const auto * child : node->children)
            stack.push_back(child);
    }
    return columns;
}

/// Whether the output `name` of the expressions is a deterministic function of `columns`.
static bool outputDependsOnlyOn(const std::optional<ActionsDAG> & dag, const String & name, const NameSet & columns)
{
    if (!dag)
        return columns.contains(name);
    const auto * output = dag->tryFindInOutputs(name);
    return output && dependsOnlyOn(output, columns);
}

/// The longest gap between kept granules, in marks, that is read through instead of skipped.
///
/// Every range of a read has a fixed cost (a seek, a compressed block decompressed again, and on remote disks a request
/// that may stall a stream). On S3, it is larger than reading one granule of the columns read for queries that do
/// little work per row, so reading many ranges split by single skipped granules is slower than reading everything.
/// Measured on 100M rows with 10k groups of about 1.2 granules (every gap one granule long), cold reads from S3: `GROUP BY`
/// with `argMax` 241 ms without the optimization, 252 ms with it, 233 ms when single-granule gaps are read through.
/// On local disks, a range is cheap, and skipping single granules is still faster (101 ms, 87 ms, 102 ms warm); so is
/// a remote disk behind a filesystem cache, once the cache is warm. Longer gaps are worth skipping on all of them. So,
/// on remote disks without a cache, gaps of one granule are read through, except for `LIMIT BY`: it sorts every row it
/// reads, so each skipped row saves more than the cost of a range.
///
/// `merge_tree_min_rows_for_seek` and `merge_tree_min_bytes_for_seek` raise it, as they do for the ranges of the index
/// analysis: a gap shorter than that many rows, or bytes in every file of the columns read, is read through. The bytes
/// of a gap are estimated with the average compressed size of a granule of each column in the part.
static size_t minMarksForSeek(
    const IMergeTreeDataPart & part, const Names & columns, size_t num_granules, const Settings & settings, bool for_limit_by)
{
    const size_t min_rows_for_seek = settings[Setting::merge_tree_min_rows_for_seek];
    const size_t min_bytes_for_seek = settings[Setting::merge_tree_min_bytes_for_seek];

    size_t marks = 0;
    const auto & storage = part.getDataPartStorage();
    if (!for_limit_by && storage.isStoredOnRemoteDisk() && !storage.getCacheName())
        marks = 1;

    if (min_rows_for_seek && num_granules)
        marks = std::max<size_t>(marks, min_rows_for_seek / std::max<size_t>(part.rows_count / num_granules, 1));

    if (min_bytes_for_seek && num_granules)
    {
        size_t max_bytes_per_mark = 0;
        for (const auto & column : columns)
            max_bytes_per_mark = std::max<size_t>(max_bytes_per_mark, part.getColumnSize(column).data_compressed / num_granules);
        /// No column read has a file (e.g. only virtual columns): the gaps cost nothing to read.
        marks = std::max(marks, max_bytes_per_mark ? min_bytes_for_seek / max_bytes_per_mark : num_granules);
    }

    return marks;
}

/// For `ORDER BY <keys>, k [DESC] LIMIT n BY <keys>`: the column `k` (a name in the output of the input node), whether
/// it is sorted descending (then the last rows of each run are needed) or ascending (the first rows), and the number
/// of rows needed per group (`n` plus the offset).
struct LimitByOrder
{
    String column;
    bool descending = false;
    size_t rows = 0;
};

/// `input_node` is the input of an aggregation (`keys`, `aggregates`), of a `DISTINCT` (`keys`, no aggregates), or of
/// the sorting below a `LIMIT BY` (`keys`, no aggregates, `limit_by`).
static void tryApply(
    QueryPlan::Node & input_node, const Names & keys, const AggregateDescriptions & aggregates, const std::optional<LimitByOrder> & limit_by = {})
{
    /// The expressions and filters between the aggregation and the reading step: only `ExpressionStep` and
    /// `FilterStep`, plus `PREWHERE` and row policies of the reading step.
    QueryDAG query;
    if (!query.build(input_node))
        return;

    QueryPlan::Node * node = &input_node;
    while (node->children.size() == 1)
        node = node->children.front();
    auto * reading = typeid_cast<ReadFromMergeTree *>(node->step.get());
    if (!reading)
        return;

    if (reading->isQueryWithFinal() || reading->isQueryWithSampling() || reading->isParallelReadingFromReplicas()
        || reading->isParallelReadingEnabled())
        return;

    const auto & metadata = reading->getStorageMetadata();
    const auto & sorting_key = metadata->getSortingKey();
    const auto & primary_key = metadata->getPrimaryKey();

    auto sorting_key_position = [&](const std::optional<String> & column) -> std::optional<size_t>
    {
        if (!column)
            return {};
        auto it = std::find(sorting_key.column_names.begin(), sorting_key.column_names.end(), *column);
        if (it == sorting_key.column_names.end())
            return {};
        return it - sorting_key.column_names.begin();
    };

    /// The argument of `max`/`min` or the second argument of `argMax`/`argMin`, if it is a column of the reading step.
    auto order_column = [&](const AggregateDescription & aggregate) -> std::optional<String>
    {
        const String name = aggregate.function->getName();
        if ((name == "max" || name == "min") && aggregate.argument_names.size() == 1)
            return mapToReadingColumn(query.dag, aggregate.argument_names[0]);
        if ((name == "argMax" || name == "argMin") && aggregate.argument_names.size() == 2)
            return mapToReadingColumn(query.dag, aggregate.argument_names[1]);
        return {};
    };

    /// The aggregated column `k` is the sorting key column furthest in the sorting key among the arguments of `max`,
    /// `min`, `argMax` and `argMin`. `min` and `max` of earlier columns depend only on the runs that are present.
    std::optional<size_t> aggregated_position;
    for (const auto & aggregate : aggregates)
        if (auto position = sorting_key_position(order_column(aggregate)))
            aggregated_position = std::max(aggregated_position.value_or(0), *position);

    /// For `LIMIT BY`, the column of the `ORDER BY` after the keys plays the role of the aggregated column.
    if (limit_by)
    {
        aggregated_position = sorting_key_position(mapToReadingColumn(query.dag, limit_by->column));
        if (!aggregated_position)
            return;
        const NameSet columns_before(sorting_key.column_names.begin(), sorting_key.column_names.begin() + *aggregated_position);
        for (const auto & key : keys)
            if (!outputDependsOnlyOn(query.dag, key, columns_before))
                return;
    }

    /// If a key or the argument of another aggregate needs that column or a later one (e.g. `min(k2)` with
    /// `uniqExact(k2)`), there is no aggregated column: `min` and `max` then also depend only on the runs that are
    /// present, with a longer prefix. `argMin` and `argMax` need the aggregated column.
    if (aggregated_position)
    {
        const NameSet columns_before(sorting_key.column_names.begin(), sorting_key.column_names.begin() + *aggregated_position);
        bool fits = std::ranges::all_of(keys, [&](const String & key) { return outputDependsOnlyOn(query.dag, key, columns_before); });
        bool has_arg_function = false;
        for (const auto & aggregate : aggregates)
        {
            const String name = aggregate.function->getName();
            has_arg_function = has_arg_function || name == "argMin" || name == "argMax";
            if (sorting_key_position(order_column(aggregate)) == aggregated_position)
                continue;
            for (const auto & argument : aggregate.argument_names)
                fits = fits && outputDependsOnlyOn(query.dag, argument, columns_before);
        }
        if (!fits)
        {
            if (has_arg_function)
                return;
            aggregated_position.reset();
        }
    }

    /// Whether the last row of each run (`max`, `argMax`) or the first one (`min`, `argMin`) is needed. Without
    /// them, only the presence of every run matters, which keeping the last granule of each run guarantees.
    bool need_group_end = limit_by && limit_by->descending;
    bool need_group_start = limit_by && !limit_by->descending;
    /// How many of the last (or first) rows of each run must be read: more than one only for `LIMIT n BY`.
    const size_t rows_needed = limit_by ? limit_by->rows : 1;
    Names set_arguments;
    for (const auto & aggregate : aggregates)
    {
        const String name = aggregate.function->getName();
        if (aggregated_position && sorting_key_position(order_column(aggregate)) == aggregated_position)
        {
            if (name == "min" || name == "argMin")
                need_group_start = true;
            else
                need_group_end = true;
        }
        else if (dependsOnlyOnDistinctValues(name))
        {
            set_arguments.insert(set_arguments.end(), aggregate.argument_names.begin(), aggregate.argument_names.end());
        }
        else
            return;
    }

    /// The prefix: the sorting key columns before the aggregated one or, without it, the shortest sorting key prefix
    /// that covers all columns of the keys and of the other aggregates' arguments.
    size_t prefix_size = 0;
    if (aggregated_position)
    {
        prefix_size = *aggregated_position;
    }
    else
    {
        need_group_end = true;
        Names outputs = keys;
        outputs.insert(outputs.end(), set_arguments.begin(), set_arguments.end());
        for (const auto & output : outputs)
        {
            auto columns = getRequiredColumns(query.dag, output);
            if (!columns)
                return;
            for (const auto & column : *columns)
            {
                auto position = sorting_key_position(column);
                if (!position)
                    return;
                prefix_size = std::max(prefix_size, *position + 1);
            }
        }
    }

    if (primary_key.column_names.size() < prefix_size)
        return;

    /// The prefix columns and the aggregated column must be sorted ascending, with types whose order the aggregation shares.
    const size_t checked_columns = prefix_size + (aggregated_position ? 1 : 0);
    for (size_t i = 0; i < checked_columns; ++i)
    {
        if (i < sorting_key.reverse_flags.size() && sorting_key.reverse_flags[i])
            return;
        if (!isSupportedKeyTypeRecursive(sorting_key.data_types[i]))
            return;
    }

    /// Every key and every argument of the other aggregates must be a deterministic function of the prefix columns
    /// (usually one of them), so that each group is a union of whole runs.
    const NameSet prefix_columns(sorting_key.column_names.begin(), sorting_key.column_names.begin() + prefix_size);
    for (const auto & key : keys)
        if (!outputDependsOnlyOn(query.dag, key, prefix_columns))
            return;
    for (const auto & argument : set_arguments)
        if (!outputDependsOnlyOn(query.dag, argument, prefix_columns))
            return;

    /// Split the filter: conjuncts on the prefix columns only remove whole runs; the other ones must hold for
    /// the witness rows, which is checked with an exact `KeyCondition` on the primary index.
    ActionsDAG::NodeRawConstPtrs witness_conjuncts;
    if (query.filter_node)
    {
        const auto * predicate = query.filter_node;
        while (predicate->type == ActionsDAG::ActionType::ALIAS)
            predicate = predicate->children.front();
        for (const auto * conjunct : ActionsDAG::extractConjunctionAtoms(predicate))
            if (!dependsOnlyOn(conjunct, prefix_columns))
                witness_conjuncts.push_back(conjunct);
    }

    const auto context = reading->getContext();
    std::optional<ActionsDAG> witness_dag;
    std::optional<ActionsDAGWithInversionPushDown> witness_filter;
    std::optional<KeyCondition> witness_condition;
    if (!witness_conjuncts.empty())
    {
        witness_dag = ActionsDAG::buildFilterActionsDAG(witness_conjuncts);
        if (!witness_dag)
            return;
        witness_filter.emplace(witness_dag->getOutputs().front(), context, /*boolean_context=*/ true);
        witness_condition.emplace(*witness_filter, context, primary_key);
        witness_condition->relaxRangeAtomsOverNaNHidingTupleColumns(primary_key.data_types);
        if (witness_condition->isRelaxed())
            return;
    }

    auto analysis = reading->getAnalyzedResult();
    if (!analysis)
        analysis = reading->selectRangesToRead();
    /// A read of a normal projection carries the projection's metadata, so the sorting key and the primary index above
    /// are the projection's, and its parts are sorted by them. The projection is never chosen here: only the rule is
    /// applied to the parts of a projection that was already chosen.
    if (!analysis)
        return;

    auto result = std::make_shared<ReadFromMergeTree::AnalysisResult>(*analysis);
    const auto & mutations_snapshot = reading->getMutationsSnapshot();

    /// Rows of such parts can be hidden or changed while reading, after the primary index was written:
    /// lightweight deletes, patch parts (e.g. a `DELETE` in lightweight-update mode) and mutations applied on the fly.
    auto may_hide_rows = [&](const MergeTreeData::DataPartPtr & data_part)
    {
        return data_part->hasLightweightDelete()
            || (mutations_snapshot
                && (!mutations_snapshot->getPatchesForPart(data_part).empty()
                    || !mutations_snapshot->getOnFlyMutationCommandsForPart(data_part).empty()));
    };

    size_t sum_marks = 0;
    size_t sum_ranges = 0;
    size_t sum_rows = 0;
    for (auto & part : result->parts_with_ranges)
    {
        const auto & data_part = part.data_part;
        auto index = data_part->getIndex();
        /// Rows hidden in the parent part are hidden in its projection parts too.
        const bool hides_rows = may_hide_rows(data_part)
            || (data_part->isProjectionPart() && may_hide_rows(data_part->getParentPart()->shared_from_this()));
        if (index->size() >= prefix_size && !index->empty() && !hides_rows)
        {
            const auto & columns = *index;
            const size_t index_rows = columns.front()->size();
            const size_t num_granules = data_part->index_granularity->getMarksCountWithoutFinal();

            auto same_prefix = [&](size_t lhs, size_t rhs)
            {
                for (size_t i = 0; i < prefix_size; ++i)
                    if (columns[i]->compareAt(lhs, rhs, *columns[i], 1) != 0)
                        return false;
                return true;
            };

            /// Whether the first row of granule `row` certainly passes the filter. Primary key columns that are not
            /// loaded in the index are unknown, so a conjunct on them is not certainly true.
            auto witness_passes = [&](size_t row)
            {
                if (!witness_condition)
                    return true;
                Hyperrectangle point;
                point.reserve(primary_key.column_names.size());
                for (size_t i = 0; i < primary_key.column_names.size(); ++i)
                {
                    if (i < columns.size())
                        point.emplace_back(FieldRef((*columns[i])[row]));
                    else
                        point.emplace_back(Range::createWholeUniverse());
                }
                return !witness_condition->checkInHyperrectangle(point, primary_key.data_types).can_be_false;
            };

            /// The rows of granule `granule` of the current run that certainly pass the filter: with a filter, only its
            /// first row is known; without one, all its rows if the granule lies entirely in the run.
            auto known_rows = [&](size_t granule, bool whole_granule) -> size_t
            {
                if (witness_condition)
                    return witness_passes(granule) ? 1 : 0;
                return whole_granule ? data_part->index_granularity->getMarkRows(granule) : 1;
            };

            /// Whether at least `rows_needed` such rows of the run of granule `mark` follow it. Every one of them has
            /// a value of the aggregated column not smaller than any in granule `mark`.
            auto enough_rows_after = [&](size_t mark)
            {
                size_t rows = 0;
                for (size_t granule = mark + 1; granule < num_granules && granule < index_rows && same_prefix(mark, granule); ++granule)
                {
                    bool whole_granule = granule + 1 < num_granules && granule + 1 < index_rows && same_prefix(mark, granule + 1);
                    rows += known_rows(granule, whole_granule);
                    if (rows >= rows_needed)
                        return true;
                }
                return false;
            };

            /// Whether at least `rows_needed` such rows of the run of granule `mark` precede it. A granule before `mark`
            /// that starts with the same prefix lies entirely in the run, because the next one does too.
            auto enough_rows_before = [&](size_t mark)
            {
                size_t rows = 0;
                for (size_t granule = mark; granule > 0 && same_prefix(granule - 1, mark); --granule)
                {
                    rows += known_rows(granule - 1, /*whole_granule=*/ true);
                    if (rows >= rows_needed)
                        return true;
                }
                return false;
            };

            const size_t min_marks_for_seek = minMarksForSeek(
                *data_part, reading->getAllColumnNames(), num_granules, context->getSettingsRef(), limit_by.has_value());

            MarkRanges new_ranges;
            for (const auto & range : part.ranges)
            {
                /// Gaps between the ranges selected by the index analysis are left as they are.
                const size_t first_new_range = new_ranges.size();
                for (size_t mark = range.begin; mark < range.end; ++mark)
                {
                    /// Granule `mark` must lie entirely in one run: the next granule starts with the same prefix.
                    bool can_skip = mark + 1 < num_granules && mark + 1 < index_rows && same_prefix(mark, mark + 1);
                    if (can_skip && need_group_end)
                        can_skip = enough_rows_after(mark);
                    if (can_skip && need_group_start)
                        can_skip = enough_rows_before(mark);
                    if (can_skip)
                        continue;
                    if (new_ranges.size() > first_new_range && mark - new_ranges.back().end <= min_marks_for_seek)
                        new_ranges.back().end = mark + 1;
                    else
                        new_ranges.push_back(MarkRange(mark, mark + 1));
                }
            }
            part.ranges = std::move(new_ranges);
            part.exact_ranges.clear();
        }

        sum_ranges += part.ranges.size();
        sum_marks += part.getMarksCount();
        sum_rows += part.getRowsCount();
    }

    std::erase_if(result->parts_with_ranges, [](const auto & part) { return part.ranges.empty(); });

    LOG_DEBUG(getLogger("optimizeAggregationMaxBySortingKey"), "Kept {} of {} marks", sum_marks, result->selected_marks);

    result->selected_parts = result->parts_with_ranges.size();
    result->selected_ranges = sum_ranges;
    result->selected_marks = sum_marks;
    result->selected_rows = sum_rows;
    result->index_stats.emplace_back(ReadFromMergeTree::IndexStat{
        .type = ReadFromMergeTree::IndexType::MaxBySortingKey,
        .description = limit_by             ? "Skip granules that cannot contain the first rows of LIMIT BY"
            : !aggregated_position          ? "Skip granules whose groups are all present in later granules"
            : need_group_start              ? "Skip granules that cannot contain the minimum or maximum of the aggregated sorting key column"
                                            : "Skip granules that cannot contain the maximum of the aggregated sorting key column",
        .num_parts_after = result->selected_parts,
        .num_granules_after = sum_marks});

    /// The remaining ranges are short and scattered: size the read tasks by their average length, so that more streams
    /// read them in parallel instead of few streams reading many of them one after another.
    if (sum_ranges)
        reading->limitMinMarksForConcurrentRead(sum_marks / sum_ranges);

    reading->setAnalyzedResult(std::move(result));
}

/// `ORDER BY <keys>, k [DESC] LIMIT n BY <keys>`: `LimitByStep`, expressions, `SortingStep`. The `ORDER BY` must be
/// some of the `LIMIT BY` columns, then one more column, and nothing after it: a tie-breaker could prefer a row with
/// the same value of `k` in a skipped granule.
static void tryApplyToLimitBy(const LimitByStep & limit_by, QueryPlan::Node & limit_by_node)
{
    std::vector<const ExpressionStep *> expressions;
    QueryPlan::Node * node = limit_by_node.children.front();
    while (const auto * expression = typeid_cast<const ExpressionStep *>(node->step.get()))
    {
        if (node->children.size() != 1)
            return;
        expressions.push_back(expression);
        node = node->children.front();
    }

    const auto * sorting = typeid_cast<const SortingStep *>(node->step.get());
    if (!sorting || node->children.size() != 1)
        return;

    /// The `LIMIT BY` columns, as names of the sorting's output, through aliases only.
    Names keys;
    for (String name : limit_by.getColumns())
    {
        for (const auto * expression : expressions)
        {
            const auto * output = expression->getExpression().tryFindInOutputs(name);
            if (!output)
                return;
            while (output->type == ActionsDAG::ActionType::ALIAS)
                output = output->children.front();
            if (output->type != ActionsDAG::ActionType::INPUT)
                return;
            name = output->result_name;
        }
        keys.push_back(std::move(name));
    }

    const auto & description = sorting->getSortDescription();
    if (description.empty())
        return;
    const NameSet key_set(keys.begin(), keys.end());
    for (size_t i = 0; i + 1 < description.size(); ++i)
        if (!key_set.contains(description[i].column_name))
            return;

    const auto & last = description.back();
    if (last.collator || key_set.contains(last.column_name))
        return;

    const size_t rows = limit_by.getGroupLength() + limit_by.getGroupOffset();
    if (rows == 0)
        return;

    tryApply(*node->children.front(), keys, {}, LimitByOrder{.column = last.column_name, .descending = last.direction < 0, .rows = rows});
}

void optimizeAggregationMaxBySortingKey(QueryPlan::Node & root)
{
    Stack stack;
    stack.push_back({.node = &root});

    while (!stack.empty())
    {
        auto & frame = stack.back();

        if (frame.next_child < frame.node->children.size())
        {
            stack.push_back({.node = frame.node->children[frame.next_child++]});
            continue;
        }

        auto * node = frame.node;
        stack.pop_back();

        if (node->children.size() != 1)
            continue;

        if (auto * aggregating = typeid_cast<AggregatingStep *>(node->step.get()))
        {
            const auto & params = aggregating->getParams();
            if (!aggregating->isGroupingSets() && !params.only_merge && !params.overflow_row)
                tryApply(*node->children.front(), params.keys, params.aggregates);
        }
        else if (auto * distinct = typeid_cast<DistinctStep *>(node->step.get()))
        {
            /// The lowest `DISTINCT` above the reading step: a final `DISTINCT` above a preliminary one has a
            /// `DistinctStep` below it, so `QueryDAG` rejects it.
            tryApply(*node->children.front(), distinct->getColumnNames(), {});
        }
        else if (const auto * limit_by = typeid_cast<const LimitByStep *>(node->step.get()))
        {
            tryApplyToLimitBy(*limit_by, *node);
        }
    }
}

}
