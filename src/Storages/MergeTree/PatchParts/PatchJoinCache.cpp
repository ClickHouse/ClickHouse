#include <shared_mutex>
#include <utility>
#include <Storages/MergeTree/PatchParts/PatchJoinCache.h>
#include <Storages/MergeTree/PatchParts/RangesInPatchParts.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

#include <Common/ProfileEvents.h>
#include <Columns/ColumnsNumber.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>

namespace ProfileEvents
{
    extern const Event BuildPatchesJoinMicroseconds;
    extern const Event PatchesJoinRowsAddedToHashTable;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_ROWS;
}

static const PaddedPODArray<UInt64> & getColumnUInt64Data(const Block & block, const String & column_name)
{
    return assert_cast<const ColumnUInt64 &>(*block.getByName(column_name).column).getData();
}

void PatchJoinCache::init(const RangesInPatchParts & ranges_in_patches)
{
    std::lock_guard lock(mutex);
    const auto & all_ranges = ranges_in_patches.getRanges();

    for (const auto & [patch_name, ranges] : all_ranges)
    {
        if (ranges.empty())
            continue;

        cache[patch_name] = std::make_shared<PatchJoinCache::Entry>();
        all_ranges_by_name[patch_name] = ranges;
    }
}

PatchJoinCache::PatchStatsEntryPtr PatchJoinCache::getStatsEntry(const DataPartPtr & patch_part, const MergeTreeReaderSettings & settings)
{
    auto stats_entry = getOrCreatePatchStats(patch_part->name);
    std::lock_guard lock(stats_entry->mutex);

    if (stats_entry->initialized)
        return stats_entry;

    auto it = all_ranges_by_name.find(patch_part->name);

    if (it == all_ranges_by_name.end())
    {
        stats_entry->initialized = true;
        return stats_entry;
    }

    const auto & all_patch_ranges = it->second;

    auto block_number_stats = getPatchMinMaxStats(patch_part, all_patch_ranges, BlockNumberColumn::name, settings);
    auto block_offset_stats = getPatchMinMaxStats(patch_part, all_patch_ranges, BlockOffsetColumn::name, settings);

    if (block_number_stats && block_offset_stats)
    {
        chassert(block_number_stats->size() == all_patch_ranges.size());
        chassert(block_offset_stats->size() == all_patch_ranges.size());

        for (size_t i = 0; i < all_patch_ranges.size(); ++i)
        {
            auto & range_stats = stats_entry->stats[all_patch_ranges[i]];
            range_stats.block_number_stat = (*block_number_stats)[i];
            range_stats.block_offset_stat = (*block_offset_stats)[i];
        }
    }

    stats_entry->initialized = true;
    return stats_entry;
}

PatchJoinCache::PatchStatsEntryPtr PatchJoinCache::getOrCreatePatchStats(const String & patch_name)
{
    std::lock_guard lock(mutex);
    auto & entry = stats_cache[patch_name];
    if (!entry)
        entry = std::make_shared<PatchStatsEntry>();
    return entry;
}

PatchJoinCache::EntryPtr PatchJoinCache::getEntry(const String & patch_name)
{
    std::lock_guard lock(mutex);
    auto it = cache.find(patch_name);
    if (it == cache.end())
        return nullptr;
    return it->second;
}

MarkRanges PatchJoinCache::Entry::getUnreadRanges(const MarkRanges & ranges) const
{
    std::shared_lock lock(mutex);
    std::unordered_set<MarkRange, MarkRangeHash> read_set(read_ranges.begin(), read_ranges.end());

    MarkRanges unread;
    for (const auto & range : ranges)
    {
        if (!read_set.contains(range))
            unread.push_back(range);
    }
    return unread;
}

void PatchJoinCache::Entry::addBlock(Block read_block, const MarkRanges & new_ranges)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::BuildPatchesJoinMicroseconds);

    size_t num_read_rows = read_block.rows();
    if (num_read_rows == 0)
    {
        std::lock_guard lock(mutex);
        read_ranges.insert(read_ranges.end(), new_ranges.begin(), new_ranges.end());
        return;
    }

    ProfileEvents::increment(ProfileEvents::PatchesJoinRowsAddedToHashTable, num_read_rows);

    const auto & block_number_column = getColumnUInt64Data(read_block, BlockNumberColumn::name);
    const auto & block_offset_column = getColumnUInt64Data(read_block, BlockOffsetColumn::name);
    const auto & data_version_column = getColumnUInt64Data(read_block, PartDataVersionColumn::name);

    /// Build a data block without system columns used only for the hash map.
    Block data_block(read_block);
    data_block.erase(BlockNumberColumn::name);
    data_block.erase(BlockOffsetColumn::name);
    size_t version_column_position = data_block.getPositionByName(PartDataVersionColumn::name);

    std::lock_guard lock(mutex);

    size_t base_row_offset = block.rows();

    if (base_row_offset == 0)
    {
        block = std::move(data_block);
    }
    else
    {
#ifdef DEBUG_OR_SANITIZER_BUILD
        assertCompatibleHeader(data_block, block, "patch join cache");
#endif
        auto mutable_columns = block.mutateColumns();
        for (size_t col = 0; col < mutable_columns.size(); ++col)
            mutable_columns[col]->insertRangeFrom(*data_block.getByPosition(col).column, 0, num_read_rows);
        block.setColumns(std::move(mutable_columns));
    }

    if (num_read_rows > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::TOO_MANY_ROWS, "Too many rows ({}) in patch ranges", num_read_rows);

    if (base_row_offset + num_read_rows > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Too large row offset ({}) in patch join cache", base_row_offset + num_read_rows);

    PatchOffsetsMap * current_offsets = nullptr;
    UInt64 prev_block_number = std::numeric_limits<UInt64>::max();
    /// Offsets are mostly sorted, so use the iterator to latest inserted offset as a hint
    /// to optimize the insertion. It gives average complexity of O(1) instead of O(log n).
    PatchOffsetsMap::const_iterator last_inserted_it;

    for (size_t i = 0; i < num_read_rows; ++i)
    {
        UInt64 block_number = block_number_column[i];
        UInt64 block_offset = block_offset_column[i];

        if (block_number != prev_block_number)
        {
            prev_block_number = block_number;
            current_offsets = &hash_map[block_number];

            min_block = std::min(min_block, block_number);
            max_block = std::max(max_block, block_number);
            last_inserted_it = current_offsets->end();
        }

        /// try_emplace overload with hint doesn't return 'inserted' flag,
        /// so we need to check size before and after emplace.
        size_t old_size = current_offsets->size();
        auto it = current_offsets->try_emplace(last_inserted_it, block_offset);
        last_inserted_it = it;
        bool inserted = current_offsets->size() > old_size;

        if (inserted)
        {
            it->second = static_cast<UInt32>(base_row_offset + i);
        }
        else
        {
            UInt32 existing_row = it->second;
            const auto & existing_version_column = block.getByPosition(version_column_position).column;

            UInt64 current_version = data_version_column[i];
            UInt64 existing_version = assert_cast<const ColumnUInt64 &>(*existing_version_column).getData()[existing_row];
            chassert(current_version != existing_version);

            /// Keep only the row with the highest version.
            if (current_version > existing_version)
                it->second = static_cast<UInt32>(base_row_offset + i);
        }
    }

    read_ranges.insert(read_ranges.end(), new_ranges.begin(), new_ranges.end());
}

}
