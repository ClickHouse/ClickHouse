#pragma once
#include <Core/Block.h>
#include <Storages/MergeTree/MarkRange.h>
#include <base/defines.h>
#include <boost/core/noncopyable.hpp>
#include <list>
#include <map>
#include <mutex>
#include <unordered_map>

namespace DB
{

class MergeTreeIndexGranularity;

/** A query-level cache of blocks read from patch parts applied in MergeOnKey mode.
  * One patch part typically covers many original parts, and readers for different
  * original parts (running in different threads) request overlapping, differently
  * aligned mark ranges of the same patch part. The cache avoids re-reading and
  * re-postprocessing the same ranges.
  *
  * Entries are whole-mark-aligned blocks with materialized sorting key columns.
  * Entries for one key never overlap. A request is served from the cache only when
  * entries cover the whole range: by returning the single covering block (cut if
  * needed) or by concatenating adjacent entries in mark order (patch parts are
  * sorted by the sorting key, so the order is preserved). A partially covered range
  * is re-read whole: stitching around cached pieces copies all requested rows and
  * costs more than re-reading the covered fraction.
  * The total size of blocks is limited and entries are evicted in LRU order.
  */
class PatchRangesCache : private boost::noncopyable
{
public:
    explicit PatchRangesCache(size_t max_bytes_);

    /// Reads the ranges from the patch part and returns one post-processed
    /// block with rows of all the ranges concatenated in their order.
    using Reader = std::function<Block(const MarkRanges &)>;

    /// Returns a block with rows of exactly the requested range.
    /// Reads (via reader) only the subranges that are not in the cache.
    /// The fingerprint must identify the names and types of the block's columns.
    Block getOrRead(
        const String & patch_name,
        UInt128 columns_fingerprint,
        MarkRange range,
        const MergeTreeIndexGranularity & granularity,
        const Reader & reader);

private:
    struct Key
    {
        String patch_name;
        UInt128 columns_fingerprint;

        bool operator==(const Key & other) const = default;
    };

    struct KeyHash
    {
        size_t operator()(const Key & key) const;
    };

    struct LRUItem
    {
        Key key;
        size_t begin_mark = 0;
    };

    using LRUList = std::list<LRUItem>;

    struct Entry
    {
        MarkRange range{};
        /// The block may be wider than the range: it holds rows of the whole read that
        /// produced this entry, and the range's rows start at first_row. This makes the
        /// insertion zero-copy; bytes is charged for the whole block (a safe overestimate).
        Block block;
        size_t first_row = 0;
        size_t bytes = 0;
        LRUList::iterator lru_it;
    };

    /// Entries keyed by the first mark of their range; ranges of entries don't overlap.
    using EntryMap = std::map<size_t, Entry>;

    /// A part of the requested range: a view into a cached or newly read block.
    struct Piece
    {
        MarkRange covered{};
        Block block;
        size_t offset = 0;
        size_t num_rows = 0;
        bool cached = false;
    };

    static Block concatPieces(const std::vector<Piece> & pieces);

    void insertPieces(
        const Key & key,
        const std::vector<Piece> & pieces,
        const Block & read_block,
        const MergeTreeIndexGranularity & granularity);

    void evictOverflow() TSA_REQUIRES(mutex);

    const size_t max_bytes;
    static constexpr size_t ENTRY_OVERHEAD = 256;

    std::mutex mutex;
    std::unordered_map<Key, EntryMap, KeyHash> entries TSA_GUARDED_BY(mutex);
    /// The least recently used entry is at the front.
    LRUList lru_list TSA_GUARDED_BY(mutex);
    size_t total_bytes TSA_GUARDED_BY(mutex) = 0;
};

using PatchRangesCachePtr = std::shared_ptr<PatchRangesCache>;

}
