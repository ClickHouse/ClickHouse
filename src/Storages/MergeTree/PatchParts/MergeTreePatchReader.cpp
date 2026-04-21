#include <Storages/MergeTree/PatchParts/MergeTreePatchReader.h>
#include <Storages/MergeTree/PatchParts/RangesInPatchParts.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/KeyDescription.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnLowCardinality.h>
#include <Interpreters/ExpressionActions.h>
#include <base/range.h>
#include <Common/Stopwatch.h>
#include <Common/SipHash.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>

namespace ProfileEvents
{
    extern const Event ReadPatchesMicroseconds;
    extern const Event PatchesReadRows;
    extern const Event PatchesReadUncompressedBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int NOT_IMPLEMENTED;
}

/// When perform_alter_conversions is false, performRequiredConversions was skipped
/// so the actual column data has on-disk types that may differ from current schema types.
/// Fix declared types to match the actual data, enabling correct castColumn later.
static void fixPatchBlockTypes(Block & block, const IMergeTreeReader & patch_reader)
{
    const auto & requested = patch_reader.getColumns();
    const auto & on_disk = patch_reader.getColumnsToRead();

    auto req_it = requested.begin();
    auto disk_it = on_disk.begin();
    for (; req_it != requested.end() && disk_it != on_disk.end(); ++req_it, ++disk_it)
    {
        if (isPatchPartSystemColumn(req_it->name) || !block.has(req_it->name))
            continue;
        if (!req_it->type->equals(*disk_it->type))
            block.getByName(req_it->name).type = disk_it->type;
    }
}

MergeTreePatchReader::MergeTreePatchReader(PatchPartInfoForReader patch_part_, MergeTreeReaderPtr reader_)
    : patch_part(std::move(patch_part_))
    , reader(std::move(reader_))
    , range_reader(reader.get(), {}, nullptr, std::make_shared<ReadStepPerformanceCounters>(), false, reader->canReadIncompleteGranules())
{
}

MergeTreePatchReader::ReadResult MergeTreePatchReader::readPatchRanges(MarkRanges ranges)
{
    Stopwatch watch;

    size_t max_rows = std::numeric_limits<UInt64>::max();
    auto read_result = range_reader.startReadingChain(max_rows, ranges);

    if (!ranges.empty())
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read the full ranges ({}) for patch part {}", ranges.describe(), patch_part.part->getPartName());

    for (auto & column : read_result.columns)
        column = removeSpecialRepresentations(column);

    if (patch_part.perform_alter_conversions)
        range_reader.getReader()->performRequiredConversions(read_result.columns);

    ProfileEvents::increment(ProfileEvents::ReadPatchesMicroseconds, watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::PatchesReadRows, read_result.num_rows);
    ProfileEvents::increment(ProfileEvents::PatchesReadUncompressedBytes, read_result.numBytesRead());

    return read_result;
}

MergeTreePatchReaderMerge::MergeTreePatchReaderMerge(PatchPartInfoForReader patch_part_, MergeTreeReaderPtr reader_)
    : MergeTreePatchReader(std::move(patch_part_), std::move(reader_))
{
    if (patch_part.mode != PatchMode::Merge)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected patch with mode Merge, got {}", patch_part.mode);
}

PatchReadResultPtr MergeTreePatchReaderMerge::readPatch(const MarkRange & range)
{
    MarkRanges ranges_to_read = {range};
    auto read_result = readPatchRanges(ranges_to_read);
    auto patch_read_result = std::make_shared<PatchMergeReadResult>();

    const auto & sample_block = range_reader.getReadSampleBlock();
    patch_read_result->block = sample_block.cloneWithColumns(read_result.columns);

    if (!patch_part.perform_alter_conversions)
        fixPatchBlockTypes(patch_read_result->block, *reader);

    patch_read_result->min_part_offset = 0;
    patch_read_result->max_part_offset = 0;

    if (read_result.num_rows == 0)
        return patch_read_result;

    size_t offset_pos = sample_block.getPositionByName("_part_offset");
    size_t part_name_pos = sample_block.getPositionByName("_part");

    const auto & offset_data = assert_cast<const ColumnUInt64 &>(*read_result.columns[offset_pos]).getData();
    const auto & part_name_col = assert_cast<const ColumnLowCardinality &>(*read_result.columns[part_name_pos]);

    auto [patch_begin, patch_end] = getPartNameRange(part_name_col, patch_part.source_parts.front());

    if (patch_begin != part_name_col.size() && patch_end != 0)
    {
        patch_read_result->min_part_offset = offset_data[patch_begin];
        patch_read_result->max_part_offset = offset_data[patch_end - 1];
    }

    return patch_read_result;
}

std::vector<PatchReadResultPtr> MergeTreePatchReaderMerge::readPatches(
    MarkRanges & ranges,
    const ReadResult & main_result,
    const Block & /*result_header*/,
    const PatchReadResult * last_read_patch)
{
    std::vector<PatchReadResultPtr> results;

    while (!ranges.empty() && (!last_read_patch || needNewPatch(main_result, *last_read_patch)))
    {
        auto result = readPatch(ranges.front());
        ranges.pop_front();
        last_read_patch = result.get();
        results.push_back(std::move(result));
    }

    return results;
}

std::vector<PatchToApplyPtr> MergeTreePatchReaderMerge::applyPatch(const Block & result_block, const PatchReadResult & patch_result) const
{
    const auto & patch_merge_data = typeid_cast<const PatchMergeReadResult &>(patch_result);
    return {applyPatchMerge(result_block, patch_merge_data.block, patch_part)};
}

bool MergeTreePatchReaderMerge::needNewPatch(const ReadResult & main_result, const PatchReadResult & old_patch) const
{
    const auto & old_patch_result = typeid_cast<const PatchMergeReadResult &>(old_patch);

    if (!main_result.max_part_offset.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Min/max part offset must be set in RangeReader for reading patch parts");

    return *main_result.max_part_offset > old_patch_result.max_part_offset;
}

bool MergeTreePatchReaderMerge::needOldPatch(const ReadResult & main_result, const PatchReadResult & old_patch, const Block & /*result_header*/) const
{
    const auto & old_patch_result = typeid_cast<const PatchMergeReadResult &>(old_patch);

    if (!main_result.min_part_offset.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Min/max part offset must be set in RangeReader for reading patch parts");

    return *main_result.min_part_offset <= old_patch_result.max_part_offset;
}

MergeTreePatchReaderJoin::MergeTreePatchReaderJoin(PatchPartInfoForReader patch_part_, MergeTreeReaderPtr reader_, PatchJoinCache * patch_join_cache_)
    : MergeTreePatchReader(std::move(patch_part_), std::move(reader_))
    , patch_join_cache(patch_join_cache_)
{
    if (patch_part.mode != PatchMode::Join)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected patch with mode Join, got {}", patch_part.mode);
}

static MinMaxStat getResultBlockStat(const Block & result_block, const String & column_name)
{
    const auto & column = result_block.getByName(column_name).column;

    Field min_value;
    Field max_value;

    column->getExtremes(min_value, max_value, 0, column->size());
    return {min_value.safeGet<UInt64>(), max_value.safeGet<UInt64>()};
}

static void filterReadRanges(MarkRanges & all_ranges, const MarkRanges & read_ranges)
{
    std::unordered_set<MarkRange, MarkRangeHash> read_ranges_set(read_ranges.begin(), read_ranges.end());

    for (auto * it = all_ranges.begin(); it != all_ranges.end();)
    {
        if (read_ranges_set.contains(*it))
            it = all_ranges.erase(it);
        else
            ++it;
    }
}

std::vector<PatchReadResultPtr> MergeTreePatchReaderJoin::readPatches(
    MarkRanges & ranges,
    const ReadResult & main_result,
    const Block & result_header,
    const PatchReadResult * /*last_read_patch*/)
{
    std::vector<PatchReadResultPtr> results;
    const auto & sample_block = range_reader.getSampleBlock();

    if (ranges.empty())
        return results;

    MarkRanges ranges_to_read = ranges;
    auto result_block = result_header.cloneWithColumns(main_result.columns);
    auto patch_read_result = std::make_shared<PatchJoinReadResult>();

    if (!patch_join_cache)
    {
        ranges.clear();
        auto read_result = readPatchRanges(ranges_to_read);
        auto & entry = patch_read_result->entries.emplace_back(std::make_shared<PatchJoinCache::Entry>());

        auto block = sample_block.cloneWithColumns(read_result.columns);
        if (!patch_part.perform_alter_conversions)
            fixPatchBlockTypes(block, *reader);
        entry->addBlock(std::move(block));
        results.push_back(std::move(patch_read_result));
        return results;
    }

    const auto * loaded_part_info = dynamic_cast<const LoadedMergeTreeDataPartInfoForReader *>(patch_part.part.get());
    if (!loaded_part_info)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Applying patch parts is supported only for loaded data parts");

    auto reader_settings = range_reader.getReader()->getMergeTreeReaderSettings();
    auto stats_entry = patch_join_cache->getStatsEntry(loaded_part_info->getDataPart(), reader_settings);

    if (!stats_entry->stats.empty())
    {
        PatchStats result_stats;
        result_stats.block_number_stat = getResultBlockStat(result_block, BlockNumberColumn::name);
        result_stats.block_offset_stat = getResultBlockStat(result_block, BlockOffsetColumn::name);
        ranges_to_read = filterPatchRanges(ranges_to_read, stats_entry->stats, result_stats);
    }

    if (ranges_to_read.empty())
        return results;

    auto block_reader = [this, &sample_block](const MarkRanges & task_ranges)
    {
        auto read_result = readPatchRanges(task_ranges);
        auto block = sample_block.cloneWithColumns(read_result.columns);
        if (!patch_part.perform_alter_conversions)
            fixPatchBlockTypes(block, *reader);
        return block;
    };

    filterReadRanges(ranges, ranges_to_read);
    patch_read_result->entries = patch_join_cache->getEntries(patch_part.part->getPartName(), ranges_to_read, std::move(block_reader));
    results.push_back(std::move(patch_read_result));
    return results;
}

std::vector<PatchToApplyPtr> MergeTreePatchReaderJoin::applyPatch(const Block & result_block, const PatchReadResult & patch_result) const
{
    const auto & patch_join_result = typeid_cast<const PatchJoinReadResult &>(patch_result);
    std::vector<PatchToApplyPtr> patches;

    for (const auto & entry : patch_join_result.entries)
        patches.push_back(applyPatchJoin(result_block, *entry));

    return patches;
}

MergeTreePatchReaderMergeOnKey::MergeTreePatchReaderMergeOnKey(PatchPartInfoForReader patch_part_, MergeTreeReaderPtr reader_)
    : MergeTreePatchReader(std::move(patch_part_), std::move(reader_))
{
    if (patch_part.mode != PatchMode::MergeOnKey)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected patch with mode MergeOnKey, got {}", patch_part.mode);
}

PatchReadResultPtr MergeTreePatchReaderMergeOnKey::readPatch(const MarkRange & range)
{
    MarkRanges ranges_to_read = {range};
    auto read_result = readPatchRanges(ranges_to_read);

    auto patch_read_result = std::make_shared<PatchMergeOnKeyReadResult>();
    const auto & sample_block = range_reader.getReadSampleBlock();
    patch_read_result->block = sample_block.cloneWithColumns(read_result.columns);

    if (!patch_part.perform_alter_conversions)
        fixPatchBlockTypes(patch_read_result->block, *reader);

    if (read_result.num_rows == 0)
        return patch_read_result;

    /// Materialize the sort-key result columns on the patch block in place so downstream callers
    /// (`applyPatchMergeOnKey`, `needOldPatch`, `needNewPatch`) can look them up by name without
    /// re-executing the expression. Safe because `sorting_key.expression` is built with
    /// `project_result=false` — it *adds* output columns without dropping input columns the apply
    /// path needs. `patch_part.sorting_key` is the shared semantic-prefix `KeyDescription` built
    /// once per `SourcePartsSetForPatch`; its `column_names` are the sort-key *result* columns.
    const auto & sorting_key = *patch_part.sorting_key;
    if (sorting_key.expression)
        sorting_key.expression->execute(patch_read_result->block);

    return patch_read_result;
}

std::vector<PatchReadResultPtr> MergeTreePatchReaderMergeOnKey::readPatches(
    MarkRanges & ranges,
    const ReadResult & main_result,
    const Block & result_header,
    const PatchReadResult * last_read_patch)
{
    std::vector<PatchReadResultPtr> results;

    /// When the caller's `patch_results` deque is empty — typically right after `MergeTreeReadersChain`
    /// evicted every accumulated block on the main cursor advancing — `last_read_patch` is null and
    /// this loop has to read ranges until `needNewPatch` says the last block's max sort-key has
    /// caught up with the current main block's max. For a fresh patch reader whose sort-key range
    /// starts far below the main cursor, that means traversing *every* range whose max is still
    /// below `main_max`. Retaining each of those blocks in `results` pins ~8 marks of patch data
    /// per traversed range; for a patch a few million rows wide this alone can hold hundreds of
    /// MB per patch per thread until the next `readPatches` call gets to evict them from the
    /// front of the deque (the blow-up the user hit at 96 patches, 60 GiB peak). The blocks are
    /// also useless — they live entirely below `main_min`, so `MergeTreeReadersChain::readPatches`
    /// would call `needOldPatch` → false → pop on the very next iteration.
    ///
    /// Drop such blocks inline. We still need to track `last_read_patch` so `needNewPatch` can
    /// decide when to stop, so keep the most recent discarded block alive in a local holder
    /// across iterations (its dtor runs when this function returns).
    PatchReadResultPtr last_discarded;

    while (!ranges.empty() && (!last_read_patch || needNewPatch(main_result, *last_read_patch, result_header)))
    {
        auto result = readPatch(ranges.front());
        ranges.pop_front();

        /// `needOldPatch` returns false iff the block's max sort-key is strictly below `main_min`,
        /// which is exactly the "useless to retain" case.
        const bool keep = needOldPatch(main_result, *result, result_header);
        last_read_patch = result.get();

        if (keep)
            results.push_back(std::move(result));
        else
            last_discarded = std::move(result);  // kept alive only to anchor `last_read_patch`
    }

    return results;
}

std::vector<PatchToApplyPtr> MergeTreePatchReaderMergeOnKey::applyPatch(const Block & result_block, const PatchReadResult & patch_result) const
{
    const auto & patch_data = typeid_cast<const PatchMergeOnKeyReadResult &>(patch_result);
    return {applyPatchMergeOnKey(result_block, patch_data.block, *patch_part.sorting_key)};
}

static int compareMainVsPatch(
    const Block & main_block,
    size_t main_row,
    const Block & patch_block,
    size_t patch_row,
    const Names & sorting_key_names,
    const std::vector<bool> & reverse_flags)
{
    /// Compares sort-key tuples at two positions: `main_block[main_row]` vs `patch_block[patch_row]`.
    for (size_t i = 0; i < sorting_key_names.size(); ++i)
    {
        const auto & main_column = *main_block.getByName(sorting_key_names[i]).column;
        const auto & patch_column = *patch_block.getByName(sorting_key_names[i]).column;

        int cmp = main_column.compareAt(main_row, patch_row, patch_column, /*nan_direction_hint=*/ 1);
        if (cmp != 0)
            return (i < reverse_flags.size() && reverse_flags[i]) ? -cmp : cmp;
    }
    return 0;
}

/// Build a block from `main_result` whose sorting key *result* columns are materialized via `patch.sorting_key_expression`.
static Block buildMainBlockWithSortKey(const Block & result_header, const Columns & columns, const ExpressionActionsPtr & sorting_key_expression)
{
    Block block = result_header.cloneWithColumns(columns);

    if (sorting_key_expression)
        sorting_key_expression->execute(block);

    return block;
}

bool MergeTreePatchReaderMergeOnKey::needNewPatch(const ReadResult & main_result, const PatchReadResult & old_patch, const Block & result_header) const
{
    /// Need a new patch block if main's max sort-key has advanced past the last-read patch block's
    /// max sort-key. For an empty sort key, every block is a single run; still follow the same
    /// logic with a zero-column compare that always returns 0 → needNewPatch=false → we stream
    /// the patch mark by mark as the main side advances.
    const auto & old = typeid_cast<const PatchMergeOnKeyReadResult &>(old_patch);

    /// An empty patch block contributes nothing — always read the next mark if there is one.
    if (old.block.rows() == 0)
        return true;

    const auto & sorting_key = *patch_part.sorting_key;
    if (sorting_key.column_names.empty())
        return true;  /// No key — always read next mark if one exists.

    if (main_result.num_rows == 0)
        return false;

    auto result_block = buildMainBlockWithSortKey(result_header, main_result.columns, sorting_key.expression);

    int cmp = compareMainVsPatch(
        result_block,
        main_result.num_rows - 1,
        old.block,
        old.block.rows() - 1,
        sorting_key.column_names,
        sorting_key.reverse_flags);

    return cmp > 0;
}

bool MergeTreePatchReaderMergeOnKey::needOldPatch(const ReadResult & main_result, const PatchReadResult & old_patch, const Block & result_header) const
{
    /// Keep the old patch block if main's min sort-key is still at-or-before patch's max.
    const auto & old = typeid_cast<const PatchMergeOnKeyReadResult &>(old_patch);

    /// An empty patch result can never contribute rows to apply — safe to evict immediately.
    if (old.block.rows() == 0)
        return false;

    const auto & sorting_key = *patch_part.sorting_key;
    if (sorting_key.column_names.empty())
        return true;  /// Single global run — never evict.

    if (main_result.num_rows == 0)
        return true;

    auto result_block = buildMainBlockWithSortKey(result_header, main_result.columns, sorting_key.expression);

    int cmp = compareMainVsPatch(
        result_block,
        /*main_row=*/ 0,  // first row = min sort-key on main side
        old.block,
        old.block.rows() - 1,
        sorting_key.column_names,
        sorting_key.reverse_flags);

    return cmp <= 0;
}

MergeTreePatchReaderPtr getPatchReader(PatchPartInfoForReader patch_part, MergeTreeReaderPtr reader, PatchJoinCache * read_join_cache)
{
    if (patch_part.mode == PatchMode::Merge)
        return std::make_unique<MergeTreePatchReaderMerge>(std::move(patch_part), std::move(reader));

    if (patch_part.mode == PatchMode::Join)
        return std::make_unique<MergeTreePatchReaderJoin>(std::move(patch_part), std::move(reader), read_join_cache);

    if (patch_part.mode == PatchMode::MergeOnKey)
        return std::make_unique<MergeTreePatchReaderMergeOnKey>(std::move(patch_part), std::move(reader));

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected patch parts mode {}", patch_part.mode);
}

}
