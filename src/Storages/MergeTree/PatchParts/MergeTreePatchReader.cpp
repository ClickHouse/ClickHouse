#include <Storages/MergeTree/PatchParts/MergeTreePatchReader.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
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
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_READ_ALL_DATA;
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
    return read_result;
}

MergeTreePatchReaderMerge::MergeTreePatchReaderMerge(PatchPartInfoForReader patch_part_, MergeTreeReaderPtr reader_)
    : MergeTreePatchReader(std::move(patch_part_), std::move(reader_))
{
    if (patch_part.mode != PatchMode::Merge)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected patch with mode Merge, got {}", patch_part.mode);
}

MergeTreePatchReader::PatchReadResultPtr MergeTreePatchReaderMerge::readPatch(MarkRanges & ranges)
{
    if (ranges.empty())
        return std::make_shared<PatchReadResult>(ReadResult(nullptr), std::make_shared<PatchMergeSharedData>());

    MarkRanges ranges_to_read = {ranges.front()};
    ranges.pop_front();

    auto read_result = readPatchRange(ranges_to_read);

    size_t offset_pos = range_reader.getReadSampleBlock().getPositionByName("_part_offset");
    size_t part_name_pos = range_reader.getReadSampleBlock().getPositionByName("_part");

    read_result.min_part_offset = 0;
    read_result.max_part_offset = 0;

    if (read_result.num_rows == 0)
        return std::make_shared<PatchReadResult>(std::move(read_result), std::make_shared<PatchMergeSharedData>());

    const auto & offset_data = assert_cast<const ColumnUInt64 &>(*read_result.columns[offset_pos]).getData();
    const auto & part_name_col = assert_cast<const ColumnLowCardinality &>(*read_result.columns[part_name_pos]);

    auto [patch_begin, patch_end] = getPartNameRange(part_name_col, patch_part.source_parts.front());

    if (patch_begin != part_name_col.size() && patch_end != 0)
    {
        read_result.min_part_offset = offset_data[patch_begin];
        read_result.max_part_offset = offset_data[patch_end - 1];
    }

    return std::make_shared<PatchReadResult>(std::move(read_result), std::make_shared<PatchMergeSharedData>());
}

PatchToApplyPtr MergeTreePatchReaderMerge::applyPatch(const Block & result_block, const PatchReadResult & patch_result) const
{
    const auto & sample_block = range_reader.getSampleBlock();
    auto patch_block = sample_block.cloneWithColumns(patch_result.read_result.columns);
    return applyPatchMerge(result_block, patch_block, patch_part);
}

bool MergeTreePatchReaderMerge::needNewPatch(const ReadResult & main_result, const PatchReadResult & old_patch) const
{
    if (!main_result.max_part_offset.has_value() || !old_patch.read_result.max_part_offset.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Min/max part offset must be set in RangeReader for reading patch parts");

    return *main_result.max_part_offset > *old_patch.read_result.max_part_offset;
}

bool MergeTreePatchReaderMerge::needOldPatch(const ReadResult & main_result, const PatchReadResult & old_patch) const
{
    if (!main_result.min_part_offset.has_value() || !old_patch.read_result.max_part_offset.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Min/max part offset must be set in RangeReader for reading patch parts");

    return *main_result.min_part_offset <= *old_patch.read_result.max_part_offset;
}

/// We don't need eviction of this cache and use it just as a convenient
/// interface to execute tasks which result can be shared between threads.
PatchReadResultCache::PatchReadResultCache()
    : CacheBase<UInt128, MergeTreePatchReader::PatchReadResult>(CurrentMetrics::end(), CurrentMetrics::end(), std::numeric_limits<UInt64>::max())
{
}

UInt128 PatchReadResultCache::hash(const String & patch_name, const MarkRanges & ranges)
{
    SipHash hash;
    hash.update(patch_name);
    hash.update(ranges.size());

    for (const auto & range : ranges)
    {
        hash.update(range.begin);
        hash.update(range.end);
    }

    return hash.get128();
}

MergeTreePatchReaderJoin::MergeTreePatchReaderJoin(PatchPartInfoForReader patch_part_, MergeTreeReaderPtr reader_, PatchReadResultCache * read_result_cache_)
    : MergeTreePatchReader(std::move(patch_part_), std::move(reader_))
    , read_result_cache(read_result_cache_)
{
    if (patch_part.mode != PatchMode::Join)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected patch with mode Join, got {}", patch_part.mode);
}

MergeTreePatchReader::PatchReadResultPtr MergeTreePatchReaderJoin::readPatch(MarkRanges & ranges)
{
    MarkRanges ranges_to_read = ranges;
    ranges.clear();

    auto read_patch = [&]
    {
        auto read_result = readPatchRange(ranges_to_read);

        auto patch_block = range_reader.getSampleBlock().cloneWithColumns(read_result.columns);
        auto data = buildPatchJoinData(patch_block);

        return std::make_shared<PatchReadResult>(std::move(read_result), std::move(data));
    };

    if (read_result_cache)
    {
        auto key = PatchReadResultCache::hash(patch_part.part->getPartName(), ranges_to_read);
        return read_result_cache->getOrSet(key, read_patch).first;
    }

    return read_patch();
}

PatchToApplyPtr MergeTreePatchReaderJoin::applyPatch(const Block & result_block, const PatchReadResult & patch_result) const
{
    const auto * patch_join_data = typeid_cast<const PatchJoinSharedData *>(patch_result.data.get());
    if (!patch_join_data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Join data for patch is not set");

    const auto & sample_block = range_reader.getSampleBlock();
    auto patch_block = sample_block.cloneWithColumns(patch_result.read_result.columns);
    return applyPatchJoin(result_block, patch_block, *patch_join_data);
}

MergeTreePatchReaderPtr getPatchReader(PatchPartInfoForReader patch_part, MergeTreeReaderPtr reader, PatchReadResultCache * read_result_cache)
{
    if (patch_part.mode == PatchMode::Merge)
        return std::make_unique<MergeTreePatchReaderMerge>(std::move(patch_part), std::move(reader));

    if (patch_part.mode == PatchMode::Join)
        return std::make_unique<MergeTreePatchReaderJoin>(std::move(patch_part), std::move(reader), read_result_cache);

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected patch parts mode {}", patch_part.mode);
}

}
