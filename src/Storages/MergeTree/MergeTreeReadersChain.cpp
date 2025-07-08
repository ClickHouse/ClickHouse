#include <Storages/MergeTree/MergeTreeReadersChain.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Common/logger_useful.h>
#include <Storages/MergeTree/PatchParts/PatchPartsUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static size_t getTotalBytesInColumns(const Columns & columns)
{
    size_t total_bytes = 0;
    for (const auto & column : columns)
        if (column)
            total_bytes += column->byteSize();
    return total_bytes;
}

MergeTreeReadersChain::MergeTreeReadersChain(RangeReaders range_readers_, MergeTreePatchReaders patch_readers_)
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

Block MergeTreeReadersChain::getSampleBlock() const
{
    return range_readers.empty() ? Block{} : range_readers.back().getSampleBlock();
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

MergeTreeReadersChain::ReadResult MergeTreeReadersChain::read(size_t max_rows, MarkRanges & ranges, std::vector<MarkRanges> & patch_ranges)
{
    if (max_rows == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected at least 1 row to read, got 0.");

    if (range_readers.empty())
        return ReadResult{log};

    auto & first_reader = range_readers.front();
    auto read_result = first_reader.startReadingChain(max_rows, ranges);
    read_result.addNumBytesRead(getTotalBytesInColumns(read_result.columns));

    LOG_TEST(log, "First reader returned: {}, requested columns: {}", read_result.dumpInfo(), first_reader.getSampleBlock().dumpNames());

    if (read_result.num_rows != 0)
    {
        readPatches(first_reader.getReadSampleBlock(), patch_ranges, read_result);
        executeActionsBeforePrewhere(read_result, read_result.columns, first_reader, {}, read_result.num_rows);

        executePrewhereActions(first_reader, read_result, {}, range_readers.size() == 1);
        addPatchVirtuals(read_result, first_reader.getSampleBlock());
    }

    for (size_t i = 1; i < range_readers.size(); ++i)
    {
        size_t num_read_rows = 0;
        auto columns = range_readers[i].continueReadingChain(read_result, num_read_rows);
        read_result.addNumBytesRead(getTotalBytesInColumns(columns));

        /// Even if number of read rows is 0 we need to apply all steps to produce a block with correct structure.
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

    if (result.total_rows_per_granule == num_read_rows && result.num_rows != num_read_rows)
    {
        /// We have filter applied from the previous step
        /// So we need to apply it to the newly read rows
        if (!result.final_filter.present() || result.final_filter.countBytesInFilter() != result.num_rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Final filter is missing or has mistaching size, read_result: {}", result.dumpInfo());

        MergeTreeRangeReader::filterColumns(read_columns, result.final_filter);
    }

    auto patch_max_version = getMaxPatchVersionForStep(range_reader);

    /// Apply patches without min version for new columns because
    /// if column is read on this step, it is not used by previous steps.
    applyPatches(
        range_reader.getReadSampleBlock(),
        read_columns,
        result.patch_versions_block,
        /*min_version=*/ {},
        patch_max_version,
        /*after_conversions=*/ false,
        result.columns_for_patches);

    /// If columns not empty, then apply on-fly alter conversions if any required
    if (!prewhere_info || prewhere_info->perform_alter_conversions)
    {
        merge_tree_reader->performRequiredConversions(read_columns);
    }

    applyPatches(
        range_reader.getReadSampleBlock(),
        read_columns,
        result.patch_versions_block,
        /*min_version=*/ {},
        patch_max_version,
        /*after_conversions=*/ true,
        result.columns_for_patches);

    /// If some columns absent in part, then evaluate default values
    if (should_evaluate_missing_defaults)
    {
        Block additional_columns;
        if (previous_header)
            additional_columns = previous_header.cloneWithColumns(result.columns);

        for (const auto & col : result.additional_columns)
            additional_columns.insert(col);

        MergeTreeRangeReader::addDummyColumnWithRowCount(additional_columns, result.num_rows);
        merge_tree_reader->evaluateMissingDefaults(additional_columns, read_columns);
    }
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

bool MergeTreeReadersChain::needApplyPatch(const Block & block, const IMergeTreeDataPartInfoForReader & patch) const
{
    const auto & patch_columns = patch.getColumnsDescription();
    const auto & alter_conversions = patch.getAlterConversions();

    for (const auto & column : block)
    {
        if (isPatchPartSystemColumn(column.name))
            continue;

        String column_name = column.name;
        if (alter_conversions && alter_conversions->isColumnRenamed(column_name))
            column_name = alter_conversions->getColumnOldName(column_name);

        if (patch_columns.hasColumnOrSubcolumn(GetColumnsOptions::All, column_name))
            return true;
    }

    return false;
}

void MergeTreeReadersChain::readPatches(const Block & result_header, std::vector<MarkRanges> & patch_ranges, ReadResult & read_result)
{
    if (patches_results.empty())
        return;

    auto result_block = result_header.cloneWithColumns(read_result.columns);

    for (size_t i = 0; i < patches_results.size(); ++i)
    {
        auto & patch_results = patches_results[i];

        /// Remove patches that are not needed for current block anymore.
        while (!patch_results.empty() && !patch_readers[i]->needOldPatch(read_result, *patch_results.front()))
        {
            patch_results.pop_front();
        }

        while (!patch_ranges[i].empty() && (patch_results.empty() || patch_readers[i]->needNewPatch(read_result, *patch_results.back())))
        {
            patch_results.emplace_back(patch_readers[i]->readPatch(patch_ranges[i]));
        }
    }
}

void MergeTreeReadersChain::applyPatchesAfterReader(ReadResult & result, size_t reader_index)
{
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

    applyPatches(
        current_reader.getSampleBlock(),
        result.columns,
        result.patch_versions_block,
        min_version,
        max_version,
        /*after_conversions=*/ true,
        result.columns_for_patches);
}

void MergeTreeReadersChain::applyPatches(
    const Block & result_header,
    Columns & result_columns,
    Block & versions_block,
    std::optional<UInt64> min_version,
    std::optional<UInt64> max_version,
    bool after_conversions,
    const Block & additional_columns) const
{
    if (patch_readers.empty() || result_columns.empty())
        return;

    auto result_block = result_header.cloneWithColumns(result_columns);
    addPatchVirtuals(result_block, additional_columns);

    /// Combine patches for same partitions.
    /// But we cannot combine patches for different partitions.
    std::unordered_map<String, PatchesToApply> patches_to_apply;
    UInt64 source_data_version = patch_readers.front()->getPatchPart().source_data_version;

    for (size_t i = 0; i < patch_readers.size(); ++i)
    {
        const auto & patch = patch_readers[i]->getPatchPart();
        const auto & patch_results = patches_results[i];

        if (min_version.has_value() && !patchHasHigherDataVersion(*patch.part, *min_version))
            continue;

        if (max_version.has_value() && patchHasHigherDataVersion(*patch.part, *max_version))
            continue;

        if (after_conversions != patch.perform_alter_conversions)
            continue;

        if (!needApplyPatch(result_block, *patch.part))
            continue;

        if (static_cast<UInt64>(patch.source_data_version) != source_data_version)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Patches for one reader must have the same source data version. Got: {}, expected: {}",
                patch.source_data_version, source_data_version);
        }

        for (const auto & patch_result : patch_results)
        {
            /// TODO: build indices once and filter them in MergeTreeRangeReader.
            auto patch_to_apply = patch_readers[i]->applyPatch(result_block, *patch_result);

            if (!patch_to_apply->empty())
            {
                const auto & partition_id = patch.part->getPartInfo().getPartitionId();
                patches_to_apply[partition_id].push_back(std::move(patch_to_apply));
            }
        }
    }

    if (min_version.has_value())
        source_data_version = std::max(source_data_version, *min_version);

    for (const auto & [_, patches] : patches_to_apply)
        applyPatchesToBlock(result_block, versions_block, patches, source_data_version);

    result_columns = result_block.getColumns();
    result_columns.resize(result_header.columns());
}

}
