#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Range.h>
#include <Core/Settings.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/TypeTree.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
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

#include <limits>

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

/// Skips granules that cannot hold the answer of `GROUP BY`, `DISTINCT` or `LIMIT BY`, using only the primary index.
///
/// Example:
///
///     CREATE TABLE trades (symbol String, ts DateTime64(3), price Float64) ENGINE = MergeTree ORDER BY (symbol, ts);
///     SELECT symbol, argMax(price, ts) FROM trades GROUP BY symbol;  -- the latest price of every symbol
///
/// The rows of each symbol are stored together, sorted by `ts`. The primary index holds the first row of every granule:
///
///     granule:      0           1           2           3           4           5
///     first row:    AAPL 09:00  AAPL 10:00  AAPL 11:00  AAPL 12:00  MSFT 10:30  MSFT 11:30
///
/// Granule 0 is followed by a granule that starts with `AAPL` at a later time, so it cannot hold the latest `AAPL`
/// trade. The same holds for granules 1, 2 and 4. Only granules 3 and 5 are read: the last granule of each symbol.
///
/// Terms. `k` is the column of `max`/`min` (`ts`), or the last `ORDER BY` column for `LIMIT BY`. The prefix is the
/// sorting key columns before `k` (`symbol`). A run is the rows of one prefix value (all `AAPL` rows), sorted by `k`.
/// The `GROUP BY` keys must depend only on the prefix, so that each group is made of whole runs: `GROUP BY symbol`
/// works, `GROUP BY toDate(ts)` does not.
///
/// `max`. Skip granule `j` if granule `j + 1` starts with the same prefix: all of `j` is in that run, and every `k` in
/// `j` is at most the first `k` of `j + 1`. The last granule of each run is always read.
///
/// `min`. Skip granule `j` only if granules `j - 1`, `j` and `j + 1` all start with the same prefix, so that `j` holds
/// neither the start nor the end of its run. In the example, `argMin(price, ts)` reads granules 0, 3, 4 and 5.
///
/// Other aggregates. Allowed if their result depends only on which distinct values they see (`any`, `uniq*`, ...) and
/// their arguments depend only on the prefix: one row per run is then enough, e.g. `any(lower(symbol))`. Aggregates
/// like `sum` or `count` need every row, so they disable the optimization.
///
/// `DISTINCT` and `GROUP BY` without `max`/`min`. The last granule of each run is kept, so every run is seen:
/// `SELECT DISTINCT symbol` and `SELECT count(DISTINCT symbol)` read granules 3 and 5.
///
/// `LIMIT BY`. `ORDER BY symbol, ts DESC LIMIT n BY symbol` needs the last `n` rows of each run (the first `n` with
/// `ASC`), plus the offset. Granule `j` is skipped if at least that many rows of its run are known to follow it (or
/// precede it). A granule fully in its run counts all its rows; the last granule of a run counts only its first row.
/// With `LIMIT 2 BY`, the example reads granules 2, 3, 4 and 5.
///
/// Filters. A condition on prefix columns only (`WHERE symbol IN (...)`) removes whole runs and does not matter. Any
/// other condition must hold for the row that justifies the skip (the first row of `j + 1`, or of `j - 1` for `min`),
/// checked exactly on the primary index; otherwise the optimization is not applied. With `WHERE ts < '11:30'`, granule
/// 2 is also read, because the first row of granule 3, `AAPL 12:00`, does not pass. A condition on `price` disables the
/// optimization, because `price` is not in the primary index.
///
/// Not applied with:
/// - `FINAL`, sampling, parallel replicas, reverse sorting keys, or key types such as `Nullable` and floats (see
///   `isSupportedKeyType`);
/// - `UNIQUE KEY` tables; parts with deleted rows, patch parts or on-the-fly mutations are read in full;
/// - `argMin`/`argMax` of a `Nullable` value;
/// - a filter on a non-prefix column when a primary key column can contain NULL;
/// - `LIMIT BY` whose limit plus offset overflows, or with `arrayJoin` above the sorting;
/// - stateful functions (`rowNumberInAllBlocks`, ...) before the aggregation or `LIMIT BY`;
/// - a `DEFAULT` or `MATERIALIZED` column whose expression calls a function.

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

/// Whether the expression calls a function. Only the parsed expression is inspected: no function is built.
static bool hasFunctionCall(const ASTPtr & ast)
{
    if (ast->as<ASTFunction>())
        return true;
    for (const auto & child : ast->children)
        if (hasFunctionCall(child))
            return true;
    return false;
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
    /// Stateful functions (`rowNumberInAllBlocks`, `runningDifference`, ...) depend on all rows they process, so their
    /// values would change for the kept rows too.
    if (query.dag && query.dag->hasStatefulFunctions())
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
    /// A table with `UNIQUE KEY` hides overwritten rows with a delete bitmap, so an index row may be a hidden row,
    /// and a granule may hold fewer rows than its marks say. The read of a projection carries the projection's
    /// metadata, so the table's own metadata is checked.
    const auto table_metadata = reading->getMergeTreeData().getInMemoryMetadataPtr(reading->getContext(), /*bypass_metadata_cache=*/ false);
    if (metadata->hasUniqueKey() || table_metadata->hasUniqueKey())
        return;
    /// The reader computes `DEFAULT` and `MATERIALIZED` expressions for the rows of parts that do not have the column
    /// (e.g. added by `ALTER TABLE ... ADD COLUMN`), or that are converted to a new type, and a function there may depend
    /// on all rows read (`rowNumberInAllBlocks`). Constants cannot, so only defaults that call a function disable the
    /// optimization. `ALIAS` columns are expanded in the query, so they are covered by `query.dag`.
    for (const auto & column : table_metadata->getColumns())
        if (column.default_desc.expression
            && (column.default_desc.kind == ColumnDefaultKind::Default || column.default_desc.kind == ColumnDefaultKind::Materialized)
            && hasFunctionCall(column.default_desc.expression))
            return;
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
            /// With a `Nullable` value argument, `argMin` and `argMax` ignore the rows where it is NULL, so the row
            /// that proves a granule unnecessary may not take part, and the answer can be in a skipped granule.
            if (name == "argMin" || name == "argMax")
            {
                const auto & value_type = aggregate.function->getArgumentTypes().front();
                if (value_type->isNullable() || value_type->isLowCardinalityNullable())
                    return;
            }
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
        /// The witness rows are checked with their values from the primary index, but `KeyCondition` expects a NULL
        /// of a key column (also one nested in a `Tuple`) in the form the index analysis converts it to, so a NULL
        /// could pass a condition like `isNotNull`. Key columns that may contain NULL are not supported here.
        for (const auto & key_type : primary_key.data_types)
            if (anyInTypeTree(*key_type, [](const IDataType & type) { return type.isNullable() || type.isLowCardinalityNullable(); }))
                return;
        witness_dag = ActionsDAG::buildFilterActionsDAG(witness_conjuncts);
        if (!witness_dag)
            return;
        witness_filter.emplace(witness_dag->getOutputs().front(), context, /*boolean_context=*/ true);
        witness_condition.emplace(*witness_filter, context, primary_key);
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

            /// The selected granules that have an index row, in order. Only they are looked at, so that the planning
            /// time is proportional to the selected granules, not to the whole part, and stays linear even when no run
            /// has enough rows (e.g. `LIMIT n BY` with `n` larger than every run).
            const size_t indexed_granules = std::min(num_granules, index_rows);
            std::vector<size_t> selected;
            for (const auto & range : part.ranges)
                for (size_t mark = range.begin; mark < std::min<size_t>(range.end, indexed_granules); ++mark)
                    selected.push_back(mark);

            /// Whether granule `g` lies entirely in one run: granule `g + 1` starts with the same prefix.
            auto continues_run = [&](size_t granule) { return granule + 1 < indexed_granules && same_prefix(granule, granule + 1); };

            /// Granules that the index analysis did not select hold no such rows: with a filter, their first row does not
            /// pass it, otherwise they would be selected, and without one, all granules of a run are selected. So the
            /// counts below skip them, and a run continues from one selected granule to the next one when both start with
            /// the same prefix: the part is sorted, so all granules between them do too.
            /// `rows_after[i]`: such rows of the run of granule `selected[i]` in the granules after it. The last granule
            /// of a run may hold rows of the next run, so only its first row counts. Every one of them has a value of the
            /// aggregated column not smaller than any in granule `selected[i]`.
            /// `rows_before[i]`: such rows of the run of granule `selected[i]` in the granules before it. A granule before
            /// it that starts with the same prefix lies entirely in the run, because the next one does too.
            const size_t num_selected = selected.size();
            std::vector<size_t> rows_after;
            std::vector<size_t> rows_before;
            if (need_group_end)
            {
                rows_after.assign(num_selected, 0);
                size_t rows_from_next = 0;
                for (size_t i = num_selected; i-- > 0;)
                {
                    const size_t granule = selected[i];
                    rows_after[i] = (i + 1 < num_selected && same_prefix(granule, selected[i + 1])) ? rows_from_next : 0;
                    rows_from_next = known_rows(granule, continues_run(granule)) + rows_after[i];
                }
            }
            if (need_group_start)
            {
                rows_before.assign(num_selected, 0);
                for (size_t i = 1; i < num_selected; ++i)
                    if (same_prefix(selected[i - 1], selected[i]))
                        rows_before[i] = rows_before[i - 1] + known_rows(selected[i - 1], /*whole_granule=*/ true);
            }

            /// Whether at least `rows_needed` such rows of the run of granule `selected[i]` follow (precede) it.
            auto enough_rows_after = [&](size_t i) { return rows_after[i] >= rows_needed; };
            auto enough_rows_before = [&](size_t i) { return rows_before[i] >= rows_needed; };

            const size_t min_marks_for_seek = minMarksForSeek(
                *data_part, reading->getAllColumnNames(), num_granules, context->getSettingsRef(), limit_by.has_value());

            MarkRanges new_ranges;
            size_t position = 0;
            for (const auto & range : part.ranges)
            {
                /// Gaps between the ranges selected by the index analysis are left as they are.
                const size_t first_new_range = new_ranges.size();
                for (size_t mark = range.begin; mark < range.end; ++mark)
                {
                    /// Granule `mark` must lie entirely in one run: the next granule starts with the same prefix.
                    const bool indexed = mark < indexed_granules;
                    const size_t i = position;
                    position += indexed ? 1 : 0;
                    bool can_skip = indexed && continues_run(mark);
                    if (can_skip && need_group_end)
                        can_skip = enough_rows_after(i);
                    if (can_skip && need_group_start)
                        can_skip = enough_rows_before(i);
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
        /// `arrayJoin` above the sort (e.g. with `query_plan_lower_array_join_function = 0`) changes the number of
        /// rows: a row of the run that proves a granule unnecessary may produce none. Stateful functions depend on all
        /// sorted rows, so they would change for the kept rows too.
        if (node->children.size() != 1 || expression->getExpression().hasArrayJoin()
            || expression->getExpression().hasStatefulFunctions())
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

    /// `LIMIT -n OFFSET m BY` is planned as a `LimitByStep` with the largest length and offset `m`, followed by a
    /// `NegativeLimitByStep`, so the sum may not fit: then every row may be needed.
    const UInt64 length = limit_by.getGroupLength();
    const UInt64 offset = limit_by.getGroupOffset();
    if (length == 0 || length > std::numeric_limits<UInt64>::max() - offset)
        return;
    const size_t rows = length + offset;

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
