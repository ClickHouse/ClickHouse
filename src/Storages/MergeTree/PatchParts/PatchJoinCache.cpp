#include <Storages/MergeTree/PatchParts/PatchJoinCache.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>

#include <Common/ProfileEvents.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnSparse.h>

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

PatchJoinCache::PatchJoinCache() = default;
PatchJoinCache::~PatchJoinCache() = default;

static const PaddedPODArray<UInt64> & getColumnUInt64Data(const Block & block, const String & column_name)
{
    return assert_cast<const ColumnUInt64 &>(*block.getByName(column_name).column).getData();
}

void PatchJoinCache::init(
    const RangesInPatchParts & ranges_in_patches,
    size_t num_buckets_,
    UInt64 min_block,
    UInt64 max_block)
{
    num_buckets = num_buckets_;
    min_block_value = min_block;
    block_range = (max_block >= min_block) ? (max_block - min_block + 1) : 1;

    const auto & all_ranges = ranges_in_patches.getRanges();
    for (const auto & [patch_name, ranges] : all_ranges)
    {
        if (ranges.empty())
            continue;

        auto & entries = cache[patch_name];
        entries.resize(num_buckets);
        for (size_t i = 0; i < num_buckets; ++i)
            entries[i] = std::make_shared<Entry>();

        all_ranges_by_name[patch_name] = ranges;
    }
}

size_t PatchJoinCache::getBucket(UInt64 block_number) const
{
    if (num_buckets <= 1)
        return 0;
    if (block_number < min_block_value)
        return 0;
    return std::min(
        static_cast<size_t>((block_number - min_block_value) * num_buckets / block_range),
        num_buckets - 1);
}

static const PatchJoinCache::Entries empty_entries;

const PatchJoinCache::Entries & PatchJoinCache::getEntries(const String & patch_name) const
{
    auto it = cache.find(patch_name);
    if (it == cache.end())
        return empty_entries;
    return it->second;
}

void PatchJoinCache::Entry::addBlock(Block read_block)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::BuildPatchesJoinMicroseconds);

    size_t num_read_rows = read_block.rows();
    if (num_read_rows == 0)
        return;

    ProfileEvents::increment(ProfileEvents::PatchesJoinRowsAddedToHashTable, num_read_rows);

    const auto & block_number_column = getColumnUInt64Data(read_block, BlockNumberColumn::name);
    const auto & block_offset_column = getColumnUInt64Data(read_block, BlockOffsetColumn::name);
    const auto & data_version_column = getColumnUInt64Data(read_block, PartDataVersionColumn::name);

    /// Build a data block without system columns used only for the hash map.
    Block data_block(read_block);
    data_block.erase(BlockNumberColumn::name);
    data_block.erase(BlockOffsetColumn::name);
    size_t version_column_position = data_block.getPositionByName(PartDataVersionColumn::name);

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

            if (current_version > existing_version)
                it->second = static_cast<UInt32>(base_row_offset + i);
        }
    }
}

}
