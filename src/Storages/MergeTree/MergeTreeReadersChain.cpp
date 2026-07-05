#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeReadersChain.h>
#include <Storages/MergeTree/PatchParts/PatchPartsUtils.h>
#include <Common/logger_useful.h>
#include <Columns/ColumnConst.h>
#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>

#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static NameSet collectColumnsConsumedByChainActions(const RangeReaders & range_readers);

MergeTreeReadersChain::MergeTreeReadersChain(RangeReaders range_readers_, MergeTreePatchReaders patch_readers_)
    : range_readers(std::move(range_readers_))
    , patch_readers(std::move(patch_readers_))
    , patches_results(patch_readers.size())
    , columns_consumed_by_chain_actions(collectColumnsConsumedByChainActions(range_readers))
    , is_initialized(true)
{
}

size_t MergeTreeReadersChain::numReadRowsInCurrentGranule() const
{
    return range_readers.empty() ? 0 : range_readers.front().numReadRowsInCurrentGranule();
}

size_t MergeTreeReadersChain::numPendingRowsInCurrentGranule() const
{
    return range_readers.empty() ? 0 : range_readers.front().numPendingRowsInCurrentGranule();
}

size_t MergeTreeReadersChain::numRowsInCurrentGranule() const
{
    return range_readers.empty() ? 0 : range_readers.back().numRowsInCurrentGranule();
}

size_t MergeTreeReadersChain::currentMark() const
{
    return range_readers.empty() ? 0 : range_readers.back().currentMark();
}

const Block & MergeTreeReadersChain::getSampleBlock() const
{
    static const Block empty_block;
    return range_readers.empty() ? empty_block : range_readers.back().getSampleBlock();
}

bool MergeTreeReadersChain::isCurrentRangeFinished() const
{
    return range_readers.empty() ? true : range_readers.front().isCurrentRangeFinished();
}

static std::optional<UInt64> getMaxPatchVersionForStep(const MergeTreeRangeReader & reader)
{
    const auto * prewhere_info = reader.getPrewhereInfo();
    return prewhere_info ? prewhere_info->mutation_version : std::nullopt;
}

/// Name of the internal `_CAST` function (`CastOverloadResolver`); the outer/inner casts an
/// on-fly `UPDATE` wraps its assignment in are emitted under this name. `CAST` is the
/// user-facing alias of the same resolver, accepted here for robustness.
static bool isCastFunctionNode(const ActionsDAG::Node & node)
{
    if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function_base)
        return false;
    const auto & name = node.function_base->getName();
    return name == "_CAST" || name == "CAST";
}

/// Whether an `if` condition selects the THEN branch for every row at runtime, i.e. it is a
/// constant that is neither NULL nor false. Then `FunctionIf` returns the THEN branch and never
/// reads the keep-old fallback, so that fallback's on-disk value need not be converted. Prefers
/// the node's own propagated constant and descends the `_CAST(..., UInt8)` the mutation builder
/// may wrap the condition in (the cast preserves the 0/non-0 truthiness).
static bool ifConditionIsConstantTrue(const ActionsDAG::Node * node)
{
    while (true)
    {
        if (node->column)
            return !node->column->isNullAt(0) && node->column->getBool(0);
        if (isCastFunctionNode(*node) && !node->children.empty())
            node = node->children.front();
        else
            return false;
    }
}

/// Storage names of overwritten columns whose on-disk value must STILL be converted to the
/// post-`MODIFY` metadata type, because an on-fly mutation step genuinely consumes it as a
/// function input before any step overwrites it.
///
/// Background: a column the on-fly chain overwrites is normally exempt from
/// `performRequiredConversions` (its on-disk value is about to be discarded, and pre-casting
/// could fail on a value the UPDATE replaces, e.g. `CAST('x', UInt64)` before
/// `UPDATE v = '100'`). But conversion happens once, in the reader that first reads the column
/// from disk. A step that consumes the column as a function input expects it in the
/// post-`MODIFY` type its DAG was built against; if that consumption is reached before the
/// column is overwritten, the on-disk value the consumer reads is the real one and must be
/// converted.
///
/// `MutationsInterpreter` lowers `UPDATE col = expr WHERE cond` to
/// `_CAST(if(cond, _CAST(expr, type), col), type)`. The bare `col` argument of that `if` is a
/// keep-old-value fallback: it is the value for unmatched rows. The `if` node is declared at the
/// post-`MODIFY` `type`, so whenever `FunctionIf` actually reads that fallback at runtime the
/// on-disk `col` must already be in `type`, or the executor aborts with
/// `Unexpected return type from if`. `FunctionIf` skips the fallback only when `cond` is a
/// constant that selects the THEN branch for every row; only then is the fallback edge safe to
/// ignore (its on-disk value, possibly unconvertible like `'x'` before `UPDATE v = '100'`, is
/// never materialized). For a non-constant or constant-false `cond` the fallback IS read, so it
/// counts as a genuine consume and forces conversion. Any OTHER reference to the column (inside
/// `cond`, or inside `expr`, e.g. `UPDATE col = materialize(col)`) is always a genuine read.
///
/// The decision is per column and order-sensitive. Walk the on-fly mutation steps in chain
/// (== mutation version) order; the FIRST step that references a column decides it:
///   - the step GENUINELY CONSUMES it as a function input -> force its conversion (this wins
///     over a self-overwrite in the same step: `UPDATE col = f(col)` reads the old `col` and
///     needs it in the post-`MODIFY` type the DAG was built against);
///   - the step only OVERWRITES it (its sole reference is the synthetic fallback) -> its
///     on-disk value is discarded there, leave it skipped.
/// A later step never overrides an earlier decision: it reads the value the earlier step
/// produced, not the on-disk one.
///
/// Query PREWHERE / row-level-filter steps (`perform_alter_conversions == true`) are not
/// scanned: they run after the whole mutation chain has produced the final column and convert
/// at their own turn. Counting their consumption would force a premature conversion of an
/// about-to-be-discarded on-disk value (e.g. `PREWHERE v > 50` before `UPDATE v = '100'`).
///
/// Keys are `NameAndTypePair::getNameInStorage`, matching the skip set in
/// `executeActionsBeforePrewhere`. A physical column whose name contains dots (`info.name`)
/// keeps its exact name; a real subcolumn (`info.name` of a Tuple/Nested `info`) maps to its
/// parent `info`. Splitting on the first dot unconditionally would key a physical `info.name`
/// as `info` and miss it in the skip set, reintroducing the type mismatch.
static NameSet collectColumnsConsumedByChainActions(const RangeReaders & range_readers)
{
    /// Map each column's read name to its storage-name key, so a name from an action DAG
    /// resolves to the same key the skip set uses.
    std::unordered_map<String, String> name_in_storage_by_read_name;
    for (const auto & reader : range_readers)
    {
        if (const auto * merge_tree_reader = reader.getReader())
            for (const auto & name_and_type : merge_tree_reader->getColumns())
                name_in_storage_by_read_name.emplace(name_and_type.name, name_and_type.getNameInStorage());
    }
    auto to_storage_key = [&](const String & read_name) -> String
    {
        auto it = name_in_storage_by_read_name.find(read_name);
        return it != name_in_storage_by_read_name.end() ? it->second : read_name;
    };

    NameSet must_convert;
    NameSet decided;
    for (const auto & reader : range_readers)
    {
        const auto * prewhere_info = reader.getPrewhereInfo();
        if (!prewhere_info || !prewhere_info->actions)
            continue;

        /// Query PREWHERE / row-level-filter steps convert at their own turn; never force here.
        if (prewhere_info->perform_alter_conversions)
            continue;

        const auto & dag = prewhere_info->actions->getActionsDAG();

        /// Columns this step overwrites: an assignment target the step produces (a DAG output
        /// that is not a plain passthrough INPUT node). For each, locate the keep-old fallback
        /// `(INPUT col)` edge of an `if(cond, _CAST(expr, type), col)` WHOSE CONDITION IS
        /// CONSTANT-TRUE, so the consume scan can ignore exactly that one (never-read) edge while
        /// still counting genuine reads of the column.
        NameSet overwritten_here;
        std::unordered_set<const ActionsDAG::Node *> synthetic_fallback_if_nodes;
        for (const auto * output : dag.getOutputs())
        {
            if (output->type == ActionsDAG::ActionType::INPUT)
                continue;
            auto key = to_storage_key(output->result_name);
            if (!prewhere_info->columns_overwritten_by_chain.contains(key))
                continue;
            overwritten_here.insert(key);

            /// Descend the assignment output to its `if`: strip the named ALIAS, then any
            /// outer `_CAST(..., type)` wrappers (their first child is the wrapped value).
            const auto * node = output;
            while (node->type == ActionsDAG::ActionType::ALIAS && node->children.size() == 1)
                node = node->children.front();
            while (isCastFunctionNode(*node) && !node->children.empty())
                node = node->children.front();

            /// Recognize the keep-old fallback shape: `if(cond, then, INPUT col)` whose third
            /// argument is the bare on-disk input of this very column. Ignore the fallback edge
            /// ONLY when `cond` is constant-true: then `FunctionIf` returns the then-branch and
            /// never reads the on-disk `col`, so its (possibly unconvertible) value is safely
            /// left unconverted. For a non-constant or constant-false `cond` the fallback is read
            /// with the post-`MODIFY`-declared type, so it must count as a genuine consume.
            /// A user `if` nested elsewhere in the expression is reached through the then-branch
            /// and is always counted as a genuine consume.
            if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base
                && node->function_base->getName() == "if" && node->children.size() == 3
                && ifConditionIsConstantTrue(node->children[0]))
            {
                const auto * fallback = node->children[2];
                if (fallback->type == ActionsDAG::ActionType::INPUT && to_storage_key(fallback->result_name) == key)
                    synthetic_fallback_if_nodes.insert(node);
            }
        }

        /// Columns this step genuinely consumes as a function input, ignoring the synthetic
        /// keep-old fallback edge identified above.
        NameSet consumed_here;
        for (const auto & node : dag.getNodes())
        {
            /// `arrayJoin` is rejected for mutations in `MutationsInterpreter` (it changes the
            /// row count), so a mutation step's DAG never contains an ARRAY_JOIN node.
            chassert(node.type != ActionsDAG::ActionType::ARRAY_JOIN);

            if (node.type != ActionsDAG::ActionType::FUNCTION)
                continue;

            const bool is_synthetic_fallback_if = synthetic_fallback_if_nodes.contains(&node);
            for (size_t i = 0; i < node.children.size(); ++i)
            {
                /// Skip the `if`'s third argument: it is the keep-old fallback, not a read.
                if (is_synthetic_fallback_if && i == 2)
                    continue;

                const auto * arg = node.children[i];

                /// A mutation step's DAG binds a function argument directly to its source node
                /// (INPUT, another FUNCTION, or COLUMN); ALIAS nodes only appear as a step's
                /// named output, never as a function argument.
                chassert(arg->type != ActionsDAG::ActionType::ALIAS);

                if (arg->type == ActionsDAG::ActionType::INPUT)
                    consumed_here.insert(to_storage_key(arg->result_name));
            }
        }

        /// First reference wins across steps; within a step a genuine consume wins over the
        /// overwrite, so `UPDATE col = f(col)` forces `col` to its post-`MODIFY` type.
        for (const auto & col : consumed_here)
            if (decided.insert(col).second)
                must_convert.insert(col);
        for (const auto & col : overwritten_here)
            decided.insert(col);
    }
    return must_convert;
}

/// Builds `ColumnsWithTypeAndName` using the on-disk column descriptions (from `IMergeTreeReader::getColumnsToRead`).
/// This is important when columns have not yet been converted, i.e. their types with differ those contained in `getReadSampleBlock`.
static ColumnsWithTypeAndName toColumnsWithTypeAndName(const Columns & columns, const NamesAndTypes & on_disk_columns)
{
    if (columns.size() != on_disk_columns.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Number of columns doesn't match number of on-disk columns, columns size: {}, on_disk_columns size: {}",
            columns.size(),
            on_disk_columns.size());

    ColumnsWithTypeAndName res;
    res.reserve(columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        /// Columns might be null, e.g. not yet filled by `fillMissingColumns`
        if (columns[i])
            res.emplace_back(columns[i], on_disk_columns[i].type, on_disk_columns[i].name);
    }
    return res;
}

MergeTreeReadersChain::ReadResult MergeTreeReadersChain::read(
    size_t max_rows,
    MarkRanges & ranges,
    std::vector<MarkRanges> & patch_ranges,
    const DataflowCacheUpdateCallback & dataflow_cache_update_cb)
{
    if (max_rows == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected at least 1 row to read, got 0.");

    ReadResult read_result{log};

    if (range_readers.empty())
        return read_result;

    auto & first_reader = range_readers.front();

    try
    {
        read_result = first_reader.startReadingChain(max_rows, ranges);
        LOG_TEST(log, "First reader returned: {}, requested columns: {}", read_result.dumpInfo(), first_reader.getSampleBlock().dumpNames());
    }
    catch (Exception & e)
    {
        e.addMessage("While reading part {}", first_reader.getReader()->data_part_info_for_read->getPartName());
        throw;
    }

    std::optional<bool> should_continue_sampling;
    if (read_result.num_rows != 0)
    {
        first_reader.getReader()->fillVirtualColumns(read_result.columns, read_result.num_rows);
        readPatches(first_reader.getReadSampleBlock(), patch_ranges, read_result);

        if (dataflow_cache_update_cb)
            dataflow_cache_update_cb(
                toColumnsWithTypeAndName(read_result.columns, first_reader.getReader()->getColumnsToRead()),
                read_result.num_bytes_read,
                should_continue_sampling);

        executeActionsBeforePrewhere(read_result, read_result.columns, first_reader, {}, read_result.num_rows);

        executePrewhereActions(first_reader, read_result, {}, range_readers.size() == 1);
        addPatchVirtuals(read_result, first_reader.getSampleBlock());
    }

    for (size_t i = 1; i < range_readers.size(); ++i)
    {
        const size_t num_bytes_read_so_far = read_result.num_bytes_read;
        size_t num_read_rows = 0;
        auto columns = range_readers[i].continueReadingChain(read_result, num_read_rows);

        /// Even if number of read rows is 0 we need to apply all steps to produce a block with correct structure.
        /// It's also needed to properly advancing streams in later steps.
        if (read_result.num_rows == 0)
            continue;

        const auto & previous_header = range_readers[i - 1].getSampleBlock();
        applyPatchesAfterReader(read_result, i - 1);

        if (!columns.empty())
        {
            /// If all requested columns are absent in part num_read_rows will be 0.
            /// In this case we need to use number of rows in the result to fill the default values and don't filter block.
            if (num_read_rows == 0)
                num_read_rows = read_result.num_rows;

            if (dataflow_cache_update_cb)
            {
                chassert(read_result.num_bytes_read >= num_bytes_read_so_far);
                // It is important that we call `recordInputColumns` here even if `should_continue_sampling`
                // is already set to false, because we still need to update the total bytes seen.
                dataflow_cache_update_cb(
                    toColumnsWithTypeAndName(columns, range_readers[i].getReader()->getColumnsToRead()),
                    read_result.num_bytes_read - num_bytes_read_so_far,
                    should_continue_sampling);
            }

            executeActionsBeforePrewhere(read_result, columns, range_readers[i], previous_header, num_read_rows);
            read_result.columns.insert(read_result.columns.end(), columns.begin(), columns.end());
        }

        executePrewhereActions(range_readers[i], read_result, previous_header, i + 1 == range_readers.size());
    }

    applyPatchesAfterReader(read_result, range_readers.size() - 1);
    return read_result;
}

void MergeTreeReadersChain::executeActionsBeforePrewhere(
    ReadResult & result,
    Columns & read_columns,
    MergeTreeRangeReader & range_reader,
    const Block & previous_header,
    size_t num_read_rows) const
{
    auto * merge_tree_reader = range_reader.getReader();
    const auto * prewhere_info = range_reader.getPrewhereInfo();

    merge_tree_reader->fillVirtualColumns(read_columns, num_read_rows);

    /// fillMissingColumns() must be called after reading but before any filterings because
    /// some columns (e.g. arrays) might be only partially filled and thus not be valid and
    /// fillMissingColumns() fixes this.
    bool should_evaluate_missing_defaults = false;
    merge_tree_reader->fillMissingColumns(read_columns, should_evaluate_missing_defaults, num_read_rows);

    if (result.total_rows_per_granule != num_read_rows)
    {
        /// This can only happen when all columns are missing during the current read step,
        /// and num_read_rows is inferred from a previous read.
        if (result.num_rows != num_read_rows)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Mismatch in expected row count: total_rows_per_granule={}, num_rows={}, num_read_rows={}. "
                "This indicates an inconsistency in row filling logic when all columns are missing",
                result.total_rows_per_granule,
                result.num_rows,
                num_read_rows);
    }
    else if (result.num_rows != num_read_rows)
    {
        /// We have filter applied from the previous step
        /// So we need to apply it to the newly read rows
        if (!result.final_filter.present() || result.final_filter.countBytesInFilter() != result.num_rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Final filter is missing or has mistaching size, read_result: {}", result.dumpInfo());

        MergeTreeRangeReader::filterColumns(read_columns, result.final_filter);
    }

    auto patch_max_version = getMaxPatchVersionForStep(range_reader);
    const auto & result_header = range_reader.getReadSampleBlock();
    auto columns_for_patches = getColumnsForPatches(result_header, read_columns);

    auto apply_patches = [&](ColumnForPatch::Order order)
    {
        /// Apply patches without min version for new columns because
        /// if column is read on this step, it is not used by previous steps.
        applyPatches(
            result_header,
            read_columns,
            result.patch_versions_block,
            /*min_version=*/ {},
            patch_max_version,
            columns_for_patches,
            {order},
            result.columns_for_patches);
    };

    apply_patches(ColumnForPatch::Order::BeforeConversions);

    /// Apply alter conversions for columns read by this step.
    ///
    /// On-fly UPDATE/DELETE steps that precede a pending `ALTER MODIFY COLUMN` are built
    /// by `AlterConversions::getMutationSteps` with `perform_alter_conversions = false`,
    /// because the on-fly action's DAG embeds its own CAST for the columns the mutation
    /// overwrites. Pre-casting the on-disk value would fail on values the UPDATE is
    /// about to replace (e.g. `CAST('x' AS UInt64)` before `UPDATE v = '100'`).
    ///
    /// The same step also produces pass-through columns the mutation never touches.
    /// Skipping conversion for those leaves the block advertising the post-MODIFY
    /// metadata type over the on-disk column class, and downstream operators
    /// (`MergingSortedTransform`'s `ColumnLowCardinality::insertFrom`, `NativeWriter`'s
    /// `typeid_cast<ColumnLowCardinality>`, etc.) trip on the type vs. storage mismatch.
    ///
    /// `prewhere_info->columns_overwritten_by_chain` is the set of columns the chain
    /// will overwrite. Null those out around `performRequiredConversions` so they are
    /// skipped, then restore them so the step's action sees them in their on-disk form.
    /// An empty skip set means the chain has nothing to skip, so the conversion is a
    /// no-op for this step and we leave the columns untouched.
    ///
    /// A column is exempt from conversion ONLY when its on-disk value is about to be
    /// discarded and replaced before anything genuinely reads it. If a mutation step consumes
    /// the column as a function input first (e.g. `materialize(a)` from
    /// `UPDATE b = isNotNull(materialize(a))` before `UPDATE a = ...`, or a self-read like
    /// `UPDATE a = materialize(a)`), that action expects it in the post-`MODIFY` type, so it
    /// must still be converted. `columns_consumed_by_chain_actions` holds exactly those
    /// columns; keep them out of the skip set so the conversion still runs.
    if (!prewhere_info || prewhere_info->perform_alter_conversions)
    {
        merge_tree_reader->performRequiredConversions(read_columns);
    }
    else if (!prewhere_info->columns_overwritten_by_chain.empty())
    {
        const auto & reader_columns = merge_tree_reader->getColumns();
        const auto & skip = prewhere_info->columns_overwritten_by_chain;

        /// Skip conversion for overwritten columns, EXCEPT those a mutation step consumes
        /// before any step overwrites them (`columns_consumed_by_chain_actions`): those must
        /// arrive in the post-`MODIFY` type their consuming action was built against.
        Columns saved(read_columns.size());
        size_t pos = 0;
        for (const auto & name_and_type : reader_columns)
        {
            const auto name_in_storage = name_and_type.getNameInStorage();
            if (skip.contains(name_in_storage) && !columns_consumed_by_chain_actions.contains(name_in_storage))
            {
                saved[pos] = read_columns[pos];
                read_columns[pos] = nullptr;
            }
            ++pos;
        }

        merge_tree_reader->performRequiredConversions(read_columns);

        pos = 0;
        for (const auto & name_and_type : reader_columns)
        {
            const auto name_in_storage = name_and_type.getNameInStorage();
            if (skip.contains(name_in_storage) && !columns_consumed_by_chain_actions.contains(name_in_storage))
                read_columns[pos] = std::move(saved[pos]);
            ++pos;
        }
    }

    apply_patches(ColumnForPatch::Order::AfterConversions);

    /// If some columns absent in part, then evaluate default values
    if (should_evaluate_missing_defaults)
    {
        Block additional_columns;
        if (!previous_header.empty())
            additional_columns = previous_header.cloneWithColumns(result.columns);

        for (const auto & col : result.additional_columns)
            additional_columns.insert(col);

        addDummyColumnWithRowCount(additional_columns, result.num_rows);
        merge_tree_reader->evaluateMissingDefaults(additional_columns, read_columns);
    }

    apply_patches(ColumnForPatch::Order::AfterEvaluatingDefaults);
}

void MergeTreeReadersChain::executePrewhereActions(MergeTreeRangeReader & reader, ReadResult & result, const Block & previous_header, bool is_last_reader)
{
    reader.executePrewhereActionsAndFilterColumns(result, previous_header, is_last_reader);
    result.checkInternalConsistency();

    if (!result.can_return_prewhere_column_without_filtering && is_last_reader)
    {
        if (!result.filterWasApplied())
        {
            /// TODO: another solution might be to set all 0s from final filter into the prewhere column
            /// and not filter all the columns here but rely on filtering in WHERE.
            result.applyFilter(result.final_filter);
            result.checkInternalConsistency();
        }

        result.can_return_prewhere_column_without_filtering = true;
    }

    const auto & sample_block = reader.getSampleBlock();
    if (result.num_rows != 0 && result.columns.size() != sample_block.columns())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Number of columns in result doesn't match number of columns in sample block, read_result: {}, sample block: {}",
            result.dumpInfo(), sample_block.dumpStructure());
}

void MergeTreeReadersChain::addPatchVirtuals(ReadResult & result, const Block & header) const
{
    if (patch_readers.empty() || result.num_rows == 0)
        return;

    auto result_block = header.cloneWithColumns(result.columns);
    addPatchVirtuals(result.columns_for_patches, result_block);
}

void MergeTreeReadersChain::addPatchVirtuals(Block & to, const Block & from) const
{
    const auto & system_columns = getPatchPartSystemColumns();
    for (const auto & column : system_columns)
    {
        /// All system columns must be read on previous steps.
        if (!to.has(column.name) && from.has(column.name))
            to.insert(from.getByName(column.name));
    }
}

void MergeTreeReadersChain::readPatches(const Block & result_header, std::vector<MarkRanges> & patch_ranges, ReadResult & read_result)
{
    for (size_t i = 0; i < patches_results.size(); ++i)
    {
        auto & patch_results = patches_results[i];

        /// Remove patches that are not needed for current block anymore.
        while (!patch_results.empty() && !patch_readers[i]->needOldPatch(read_result, *patch_results.front()))
        {
            patch_results.pop_front();
        }

        const auto * last_read_patch = patch_results.empty() ? nullptr : patch_results.back().get();
        auto new_patches = patch_readers[i]->readPatches(patch_ranges[i], read_result, result_header, last_read_patch);
        patch_results.insert(patch_results.end(), new_patches.begin(), new_patches.end());
    }
}

ColumnsForPatches MergeTreeReadersChain::getColumnsForPatches(const Block & header, const Columns & columns) const
{
    ColumnsForPatches res;
    auto block = header.cloneWithColumns(columns);
    using enum ColumnForPatch::Order;

    for (const auto & patch_reader : patch_readers)
    {
        const auto & patch = patch_reader->getPatchPart();
        const auto & patch_columns = patch.part->getColumnsDescription();
        const auto & alter_conversions = patch.part->getAlterConversions();
        auto & columns_for_patch = res.emplace_back();

        for (const auto & column : block)
        {
            if (isPatchPartSystemColumn(column.name))
                continue;

            String column_name_in_patch = column.name;
            if (alter_conversions && alter_conversions->isColumnRenamed(column.name))
                column_name_in_patch = alter_conversions->getColumnOldName(column.name);

            if (!patch_columns.hasColumnOrSubcolumn(GetColumnsOptions::All, column_name_in_patch))
                continue;

            /// Add requested column name, not the column name in patch, for correct applying patches.
            /// This column name is translated to the column name in patch in MergeTree reader.
            /// If columns is not created at this stage, it will be filled with defaults at the latest stage of reading.
            if (!column.column)
            {
                columns_for_patch.emplace_back(column.name, AfterEvaluatingDefaults);
            }
            else if (patch.perform_alter_conversions)
            {
                columns_for_patch.emplace_back(column.name, AfterConversions);
            }
            else
            {
                columns_for_patch.emplace_back(column.name, BeforeConversions);
            }
        }
    }

    return res;
}

void MergeTreeReadersChain::applyPatchesAfterReader(ReadResult & result, size_t reader_index)
{
    if (patch_readers.empty() || result.num_rows == 0 || result.columns.empty())
        return;

    auto & current_reader = range_readers.at(reader_index);

    std::optional<UInt64> min_version = getMaxPatchVersionForStep(current_reader);
    std::optional<UInt64> max_version;

    if (reader_index + 1 != range_readers.size())
        max_version = getMaxPatchVersionForStep(range_readers[reader_index + 1]);

    if (min_version == max_version)
        return;

    if (min_version.has_value())
    {
        for (auto & column : result.patch_versions_block)
            column.column = ColumnUInt64::create(result.patch_versions_block.rows(), *min_version);
    }

    const auto & result_header = current_reader.getSampleBlock();
    auto columns_for_patches = getColumnsForPatches(result_header, result.columns);

    using enum ColumnForPatch::Order;
    std::set<ColumnForPatch::Order> suitable_orders = {AfterConversions, AfterEvaluatingDefaults};

    applyPatches(
        result_header,
        result.columns,
        result.patch_versions_block,
        min_version,
        max_version,
        columns_for_patches,
        suitable_orders,
        result.columns_for_patches);
}

void MergeTreeReadersChain::applyPatches(
    const Block & result_header,
    Columns & result_columns,
    Block & versions_block,
    std::optional<UInt64> min_version,
    std::optional<UInt64> max_version,
    const ColumnsForPatches & columns_for_patches,
    const std::set<ColumnForPatch::Order> & suitable_orders,
    const Block & additional_columns) const
{
    if (patch_readers.empty() || result_columns.empty())
        return;

    auto result_block = result_header.cloneWithColumns(result_columns);
    addPatchVirtuals(result_block, additional_columns);

    /// Combine patches with the same structure.
    std::unordered_map<Names, PatchesToApply, NamesHash> patches_to_apply;
    UInt64 source_data_version = patch_readers.front()->getPatchPart().source_data_version;

    for (size_t i = 0; i < patch_readers.size(); ++i)
    {
        const auto & patch = patch_readers[i]->getPatchPart();
        const auto & patch_results = patches_results[i];

        if (static_cast<UInt64>(patch.source_data_version) != source_data_version)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Patches for one reader must have the same source data version. Got: {}, expected: {}",
                patch.source_data_version, source_data_version);
        }

        if (min_version.has_value() && !patchHasHigherDataVersion(*patch.part, *min_version))
            continue;

        if (max_version.has_value() && patchHasHigherDataVersion(*patch.part, *max_version))
            continue;

        Names updated_columns;
        for (const auto & columns_for_patch : columns_for_patches[i])
        {
            if (suitable_orders.contains(columns_for_patch.order))
                updated_columns.push_back(columns_for_patch.column_name);
        }

        if (updated_columns.empty())
            continue;

        std::sort(updated_columns.begin(), updated_columns.end());

        for (const auto & patch_result : patch_results)
        {
            /// TODO: build indices once and filter them in MergeTreeRangeReader.
            auto patches = patch_readers[i]->applyPatch(result_block, *patch_result);

            for (auto & patch_to_apply : patches)
            {
                if (!patch_to_apply->empty())
                    patches_to_apply[updated_columns].push_back(std::move(patch_to_apply));
            }
        }
    }

    if (min_version.has_value())
        source_data_version = std::max(source_data_version, *min_version);

    for (const auto & [updated_columns, patches] : patches_to_apply)
        applyPatchesToBlock(result_block, versions_block, patches, updated_columns, source_data_version);

    result_columns = result_block.getColumns();
    result_columns.resize(result_header.columns());
}

}
