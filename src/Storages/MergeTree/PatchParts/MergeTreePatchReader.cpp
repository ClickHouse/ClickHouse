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

MergeTreePatchReader::ReadResult MergeTreePatchReader::readPatchRange(MarkRanges ranges)
{
    Stopwatch watch;

    size_t max_rows = std::numeric_limits<UInt64>::max();
    auto read_result = range_reader.startReadingChain(max_rows, ranges);

    if (!ranges.empty())
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read the full ranges ({}) for patch part {}", ranges.describe(), patch_part.part->getPartName());

    for (auto & column : read_result.columns)
        column = recursiveRemoveSparse(column);

    if (patch_part.perform_alter_conversions)
        range_reader.getReader()->performRequiredConversions(read_result.columns);

    ProfileEvents::increment(ProfileEvents::ReadPatchesMicroseconds, watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::PatchesReadUncompressedBytes, read_result.numBytesRead());
    return read_result;
}

MergeTreePatchReaderMerge::MergeTreePatchReaderMerge(PatchPartInfoForReader patch_part_, MergeTreeReaderPtr reader_)
    : MergeTreePatchReader(std::move(patch_part_), std::move(reader_))
{
    if (patch_part.mode != PatchMode::Merge)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected patch with mode Merge, got {}", patch_part.mode);
}

PatchReadResultPtr MergeTreePatchReaderMerge::readPatch(MarkRanges & ranges, const Block & /*result_block*/)
{
    auto patch_read_result = std::make_shared<PatchMergeReadResult>();

    if (ranges.empty())
        return patch_read_result;

    MarkRanges ranges_to_read = {ranges.front()};
    ranges.pop_front();

    auto read_result = readPatchRange(ranges_to_read);

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

std::vector<PatchToApplyPtr> MergeTreePatchReaderMerge::applyPatch(const Block & result_block, const PatchReadResult & patch_result) const
{
    const auto * patch_merge_data = typeid_cast<const PatchMergeReadResult *>(&patch_result);
    if (!patch_merge_data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PatchMergeReadResult is expected");

    return {applyPatchMerge(result_block, patch_merge_data->block, patch_part)};
}

bool MergeTreePatchReaderMerge::needNewPatch(const ReadResult & main_result, const PatchReadResult & old_patch) const
{
    const auto * old_patch_result = typeid_cast<const PatchMergeReadResult *>(&old_patch);
    if (!old_patch_result)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PatchMergeReadResult is expected");

    if (!main_result.max_part_offset.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Min/max part offset must be set in RangeReader for reading patch parts");

    return *main_result.max_part_offset > old_patch_result->max_part_offset;
}

bool MergeTreePatchReaderMerge::needOldPatch(const ReadResult & main_result, const PatchReadResult & old_patch) const
{
    const auto * old_patch_result = typeid_cast<const PatchMergeReadResult *>(&old_patch);
    if (!old_patch_result)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PatchMergeReadResult is expected");

    if (!main_result.min_part_offset.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Min/max part offset must be set in RangeReader for reading patch parts");

    return *main_result.min_part_offset <= old_patch_result->max_part_offset;
}

MergeTreePatchReaderJoin::MergeTreePatchReaderJoin(PatchPartInfoForReader patch_part_, MergeTreeReaderPtr reader_, PatchJoinCache * patch_join_cache_)
    : MergeTreePatchReader(std::move(patch_part_), std::move(reader_))
    , patch_join_cache(patch_join_cache_)
{
    if (patch_part.mode != PatchMode::Join)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected patch with mode Join, got {}", patch_part.mode);
}

static PatchRangeStats getResultBlockStats(const Block & result_block, const String & column_name)
{
    const auto & column = result_block.getByName(column_name).column;

    Field min_value;
    Field max_value;

    column->getExtremes(min_value, max_value);
    return {min_value.safeGet<UInt64>(), max_value.safeGet<UInt64>()};
}

void MergeTreePatchReaderJoin::filterRangesByMinMaxIndex(MarkRanges & ranges, const Block & result_block, const String & column_name)
{
    const auto * loaded_part_info = dynamic_cast<const LoadedMergeTreeDataPartInfoForReader *>(patch_part.part.get());
    if (!loaded_part_info)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Applying patch parts is supported only for loaded data parts");

    auto stats = getPatchRangesStats(loaded_part_info->getDataPart(), ranges, column_name);

    if (stats.has_value())
    {
        auto result_stats = getResultBlockStats(result_block, column_name);
        ranges = filterPatchRanges(ranges, stats.value(), result_stats);
    }
}

PatchReadResultPtr MergeTreePatchReaderJoin::readPatch(MarkRanges & ranges, const Block & result_block)
{
    auto patch_read_result = std::make_shared<PatchJoinReadResult>();
    const auto & sample_block = range_reader.getSampleBlock();

    MarkRanges all_ranges = ranges;
    ranges.clear();

    if (all_ranges.empty())
        return patch_read_result;

    filterRangesByMinMaxIndex(all_ranges, result_block, BlockNumberColumn::name);
    if (!all_ranges.empty())
        filterRangesByMinMaxIndex(all_ranges, result_block, BlockOffsetColumn::name);

    if (all_ranges.empty())
        return patch_read_result;

    if (!patch_join_cache)
    {
        auto read_result = readPatchRange(all_ranges);
        auto & entry = patch_read_result->entries.emplace_back(std::make_shared<PatchJoinCache::Entry>());
        entry->addBlock(sample_block.cloneWithColumns(read_result.columns));
        return patch_read_result;
    }

    std::mutex reader_mutex;

    auto reader = [this, &sample_block, &reader_mutex](const MarkRanges & ranges_to_read)
    {
        std::lock_guard lock(reader_mutex);
        auto read_result = readPatchRange(ranges_to_read);
        return sample_block.cloneWithColumns(read_result.columns);
    };

    patch_read_result->entries = patch_join_cache->getEntries(patch_part.part->getPartName(), all_ranges, std::move(reader));
    return patch_read_result;
}

std::vector<PatchToApplyPtr> MergeTreePatchReaderJoin::applyPatch(const Block & result_block, const PatchReadResult & patch_result) const
{
    const auto * patch_join_result = typeid_cast<const PatchJoinReadResult *>(&patch_result);
    if (!patch_join_result)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PatchJoinReadResult is expected");

    std::vector<PatchToApplyPtr> patches;
    patches.reserve(patch_join_result->entries.size());

    for (const auto & entry : patch_join_result->entries)
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
