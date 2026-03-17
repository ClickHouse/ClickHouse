#include <Storages/MergeTree/PatchParts/MergeTreePatchReader.h>
#include <Storages/MergeTree/PatchParts/RangesInPatchParts.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnLowCardinality.h>
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

MergeTreePatchReader::MergeTreePatchReader(PatchPartInfoForReader patch_part_, MergeTreeReaderPtr reader_)
    : patch_part(std::move(patch_part_))
    , reader(std::move(reader_))
    , range_reader(reader.get(), {}, nullptr, std::make_shared<ReadStepPerformanceCounters>(), false)
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

bool MergeTreePatchReaderMerge::needOldPatch(const ReadResult & main_result, const PatchReadResult & old_patch) const
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

        entry->addBlock(sample_block.cloneWithColumns(read_result.columns));
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

    auto reader = [this, &sample_block](const MarkRanges & task_ranges)
    {
        auto read_result = readPatchRanges(task_ranges);
        return sample_block.cloneWithColumns(read_result.columns);
    };

    filterReadRanges(ranges, ranges_to_read);
    patch_read_result->entries = patch_join_cache->getEntries(patch_part.part->getPartName(), ranges_to_read, std::move(reader));
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

MergeTreePatchReaderPtr getPatchReader(PatchPartInfoForReader patch_part, MergeTreeReaderPtr reader, PatchJoinCache * read_join_cache)
{
    if (patch_part.mode == PatchMode::Merge)
        return std::make_unique<MergeTreePatchReaderMerge>(std::move(patch_part), std::move(reader));

    if (patch_part.mode == PatchMode::Join)
        return std::make_unique<MergeTreePatchReaderJoin>(std::move(patch_part), std::move(reader), read_join_cache);

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected patch parts mode {}", patch_part.mode);
}

}
