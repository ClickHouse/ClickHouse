#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeReadersChain.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeReadersChain::MergeTreeReadersChain(
    RangeReaders range_readers_, MergeTreePatchReaders patch_readers_)
    : range_readers(std::move(range_readers_))
    , patch_readers(std::move(patch_readers_))
    , patches_results(patch_readers.size())
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
    auto columns_for_patches = getColumnsForPatches(result_header, read_columns, patch_readers);

    auto apply_patches = [&](ColumnForPatch::Order order)
    {
        /// Apply patches without min version for new columns because
        /// if column is read on this step, it is not used by previous steps.
        applyPatchesToColumns(
            patch_readers,
            patches_results,
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

    /// If columns not empty, then apply on-fly alter conversions if any required
    if (!prewhere_info || prewhere_info->perform_alter_conversions)
    {
        merge_tree_reader->performRequiredConversions(read_columns);
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

void MergeTreeReadersChain::executePrewhereActions(MergeTreeRangeReader & reader, ReadResult & result, const Block & previous_header, bool is_last_reader) const
{
    reader.executePrewhereActionsAndFilterColumns(result, previous_header);
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
    addPatchVirtualColumns(result.columns_for_patches, result_block);
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
    auto columns_for_patches = getColumnsForPatches(result_header, result.columns, patch_readers);

    using enum ColumnForPatch::Order;
    std::set<ColumnForPatch::Order> suitable_orders = {AfterConversions, AfterEvaluatingDefaults};

    applyPatchesToColumns(
        patch_readers,
        patches_results,
        result_header,
        result.columns,
        result.patch_versions_block,
        min_version,
        max_version,
        columns_for_patches,
        suitable_orders,
        result.columns_for_patches);
}

}
