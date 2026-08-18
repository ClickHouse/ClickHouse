#include <Storages/MergeTree/PatchParts/PatchRangesCache.h>

#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>

namespace ProfileEvents
{
    extern const Event PatchRangesCacheHits;
    extern const Event PatchRangesCachePartialHits;
    extern const Event PatchRangesCacheMisses;
    extern const Event PatchRangesCacheEvictedBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

PatchRangesCache::PatchRangesCache(size_t max_bytes_)
    : max_bytes(max_bytes_)
{
}

size_t PatchRangesCache::KeyHash::operator()(const Key & key) const
{
    SipHash hash;
    hash.update(key.patch_name);
    hash.update(key.columns_fingerprint);
    return hash.get64();
}

Block PatchRangesCache::getOrRead(
    const String & patch_name,
    UInt128 columns_fingerprint,
    MarkRange range,
    const MergeTreeIndexGranularity & granularity,
    const Reader & reader)
{
    chassert(range.begin < range.end);

    Key key{patch_name, columns_fingerprint};
    std::vector<Piece> pieces;
    MarkRanges missing_ranges;
    bool had_partial_coverage = false;

    {
        std::lock_guard lock(mutex);
        bool has_cached_pieces = false;
        auto & entry_map = entries[key];

        auto it = entry_map.upper_bound(range.begin);
        /// The previous entry may span over the beginning of the requested range.
        if (it != entry_map.begin() && std::prev(it)->second.range.end > range.begin)
            it = std::prev(it);

        std::vector<LRUList::iterator> used_entries;

        size_t next_mark = range.begin;
        for (; it != entry_map.end() && it->second.range.begin < range.end; ++it)
        {
            auto & entry = it->second;

            if (next_mark < entry.range.begin)
            {
                missing_ranges.emplace_back(next_mark, entry.range.begin);
                pieces.push_back(Piece{.covered = MarkRange(next_mark, entry.range.begin), .block = {}, .offset = 0, .num_rows = 0, .cached = false});
            }

            MarkRange covered(std::max(entry.range.begin, range.begin), std::min(entry.range.end, range.end));
            size_t offset = entry.first_row + granularity.getRowsCountInRange(entry.range.begin, covered.begin);
            size_t num_rows = granularity.getRowsCountInRange(covered);

            if (offset + num_rows > entry.block.rows())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Cached entry for range {} of patch part {} has {} rows, expected at least {}",
                    entry.range, patch_name, entry.block.rows(), offset + num_rows);

            used_entries.push_back(entry.lru_it);
            pieces.push_back(Piece{.covered = covered, .block = entry.block, .offset = offset, .num_rows = num_rows, .cached = true});
            has_cached_pieces = true;
            next_mark = covered.end;
        }

        if (next_mark < range.end)
        {
            missing_ranges.emplace_back(next_mark, range.end);
            pieces.push_back(Piece{.covered = MarkRange(next_mark, range.end), .block = {}, .offset = 0, .num_rows = 0, .cached = false});
        }

        /// Serve from the cache only when it covers the whole range. Stitching around
        /// partially covering pieces copies all requested rows into a new block, which
        /// costs more than re-reading the covered fraction, so re-read the whole range.
        if (has_cached_pieces && !missing_ranges.empty())
        {
            pieces.clear();
            pieces.push_back(Piece{.covered = range, .block = {}, .offset = 0, .num_rows = 0, .cached = false});
            missing_ranges = MarkRanges{range};
            had_partial_coverage = true;
        }
        else
        {
            for (auto lru_it : used_entries)
                lru_list.splice(lru_list.end(), lru_list, lru_it);
        }
    }

    if (missing_ranges.empty())
    {
        ProfileEvents::increment(ProfileEvents::PatchRangesCacheHits);

        /// One entry covers the whole range. Return its block or a cut of it without concatenation.
        if (pieces.size() == 1)
        {
            const auto & piece = pieces.front();
            if (piece.offset == 0 && piece.num_rows == piece.block.rows())
                return piece.block;
            return piece.block.cloneWithCutColumns(piece.offset, piece.num_rows);
        }

        return concatPieces(pieces);
    }

    ProfileEvents::increment(had_partial_coverage ? ProfileEvents::PatchRangesCachePartialHits : ProfileEvents::PatchRangesCacheMisses);

    Block read_block = reader(missing_ranges);

    /// Distribute rows of the read block among the missing pieces.
    size_t read_offset = 0;
    for (auto & piece : pieces)
    {
        if (piece.cached)
            continue;

        piece.block = read_block;
        piece.offset = read_offset;
        piece.num_rows = granularity.getRowsCountInRange(piece.covered);
        read_offset += piece.num_rows;
    }

    if (read_offset != read_block.rows())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Read {} rows from ranges {} of patch part {}, expected {} rows by index granularity",
            read_block.rows(), missing_ranges.describe(), patch_name, read_offset);

    insertPieces(key, pieces, read_block, granularity);
    return read_block;
}

Block PatchRangesCache::concatPieces(const std::vector<Piece> & pieces)
{
    size_t total_rows = 0;
    for (const auto & piece : pieces)
        total_rows += piece.num_rows;

    if (pieces.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot concatenate pieces of patch part ranges: no pieces");

    /// All pieces are cached here and cached blocks are never empty.
    Block header = pieces.front().block.cloneEmpty();
    MutableColumns columns = header.cloneEmptyColumns();

    for (auto & column : columns)
        column->reserve(total_rows);

    /// Pieces may have different column orders (they come from different readers), so align by name.
    for (const auto & piece : pieces)
    {
        if (piece.num_rows == 0)
            continue;

        for (size_t i = 0; i < columns.size(); ++i)
        {
            const auto & source = piece.block.getByName(header.getByPosition(i).name).column;
            columns[i]->insertRangeFrom(*source, piece.offset, piece.num_rows);
        }
    }

    return header.cloneWithColumns(std::move(columns));
}

void PatchRangesCache::insertPieces(
    const Key & key,
    const std::vector<Piece> & pieces,
    const Block & read_block,
    const MergeTreeIndexGranularity & granularity)
{
    /// Empty blocks have a narrower header and contribute nothing.
    if (read_block.rows() == 0)
        return;

    /// Entries reference slices of the whole read block, so each is charged its full size.
    size_t entry_bytes = read_block.allocatedBytes() + ENTRY_OVERHEAD;
    /// Never admit an entry heavier than the whole budget.
    if (entry_bytes > max_bytes)
        return;

    std::lock_guard lock(mutex);
    auto & entry_map = entries[key];

    for (const auto & piece : pieces)
    {
        if (piece.cached || piece.num_rows == 0)
            continue;

        /// Insert only the subranges that were not inserted by other threads meanwhile,
        /// to keep the entries non-overlapping.
        size_t next_mark = piece.covered.begin;
        auto it = entry_map.upper_bound(piece.covered.begin);
        if (it != entry_map.begin() && std::prev(it)->second.range.end > next_mark)
            next_mark = std::min(std::prev(it)->second.range.end, piece.covered.end);

        std::vector<MarkRange> gaps;
        for (; it != entry_map.end() && it->second.range.begin < piece.covered.end; ++it)
        {
            if (next_mark < it->second.range.begin)
                gaps.emplace_back(next_mark, it->second.range.begin);
            next_mark = std::max(next_mark, std::min(it->second.range.end, piece.covered.end));
        }

        if (next_mark < piece.covered.end)
            gaps.emplace_back(next_mark, piece.covered.end);

        for (const auto & gap : gaps)
        {
            size_t first_row = piece.offset + granularity.getRowsCountInRange(piece.covered.begin, gap.begin);
            size_t num_rows = granularity.getRowsCountInRange(gap);

            if (num_rows == 0)
                continue;

            auto lru_it = lru_list.insert(lru_list.end(), LRUItem{key, gap.begin});
            auto [entry_it, inserted] = entry_map.emplace(gap.begin, Entry{gap, read_block, first_row, entry_bytes, lru_it});

            if (!inserted)
            {
                lru_list.erase(lru_it);
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Entry for mark {} of patch part {} already exists in the patch ranges cache", gap.begin, key.patch_name);
            }

            total_bytes += entry_bytes;
        }
    }

    evictOverflow();
}

void PatchRangesCache::evictOverflow()
{
    while (total_bytes > max_bytes && !lru_list.empty())
    {
        const auto & item = lru_list.front();
        auto map_it = entries.find(item.key);
        auto entry_it = map_it != entries.end() ? map_it->second.find(item.begin_mark) : EntryMap::iterator{};

        if (map_it == entries.end() || entry_it == map_it->second.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Entry for mark {} of patch part {} is in the LRU list but not in the patch ranges cache",
                item.begin_mark, item.key.patch_name);

        total_bytes -= entry_it->second.bytes;
        ProfileEvents::increment(ProfileEvents::PatchRangesCacheEvictedBytes, entry_it->second.bytes);

        map_it->second.erase(entry_it);
        if (map_it->second.empty())
            entries.erase(map_it);

        lru_list.pop_front();
    }
}

}
