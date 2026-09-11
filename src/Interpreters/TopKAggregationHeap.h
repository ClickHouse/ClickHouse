#pragma once

#include <atomic>
#include <cstring>
#include <memory>
#include <mutex>
#include <string_view>
#include <type_traits>
#include <vector>

#include <base/defines.h>
#include <base/PackedStringRef.h>
#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/assert_cast.h>
#include <Core/CompareHelper.h>
#include <Core/TypeId.h>

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>

namespace DB
{

/** The tightest skip boundary any aggregation thread has published, shared by all
  * per-thread top-K sets of one `Aggregator`. A set at capacity K with boundary B proves
  * K distinct groups strictly better than any key worse than B - globally, not just for
  * the publishing thread - so every thread may skip against it (the same argument that
  * makes per-replica skipping sound). Publications only ever tighten the boundary.
  *
  * Publish and refresh both happen at most once per block per thread (a trim only marks
  * the local boundary as pending), so a mutex around the key plus a version counter for
  * the cheap no-change check is enough. Deferring the publications to the block boundary
  * caps the sharing overhead when the local boundaries tighten on every trim while the
  * shared one never produces a skip - e.g. a DESC ranking over data that ascends in step
  * across all threads - at the cost of at most one block of staleness.
  */
struct SharedTopKBoundary
{
    std::mutex mutex;
    std::atomic<UInt64> version{0};
    MutableColumnPtr key;   /// single row, same structure as the tracking column; null until the first publication
};

/** The part of `TopKAggregationHeap` that does not depend on the hash-table key type.
  *
  * Everything here concerns the tracked key *values* - a column and the ranking over
  * it - and none of it varies with the `Key` the hash table is indexed by, so it is
  * compiled once in `TopKAggregationHeap.cpp` rather than once per aggregation method.
  * `AggregationMethod.h` embeds a heap in every method, and the ranking machinery
  * instantiates a comparator, an `nth_element` and a `partition` for each of the eleven
  * numeric heap column types, so leaving the algorithms in the header multiplied them by
  * the number of methods and dominated the cost of `Aggregator.cpp`.
  *
  * The per-row typed skip test stays inline: it is the innermost check of the aggregation
  * loop, and being key-independent it is instantiated once per translation unit instead of
  * once per method.
  */
struct TopKAggregationHeapBase
{
    MutableColumnPtr heap_column;           /// the tracked key values, one row per key (`ColumnTuple` for composite); null while not running
    bool is_composite = false;              /// `heap_column` is a `ColumnTuple`
    bool is_prefix_mode = false;            /// ranks only a key prefix, which identifies many groups: skip-only, the caller must not erase
    bool frozen = false;                    /// abandoned at runtime; aggregation proceeds as if the optimization were off
    std::vector<AggregateDataPtr> free_states;  /// state slots of pruned groups, reused by later inserts (arena memory is never returned)

    TopKAggregationHeapBase() = default;
    TopKAggregationHeapBase(const TopKAggregationHeapBase &) = delete;
    TopKAggregationHeapBase & operator=(const TopKAggregationHeapBase &) = delete;
    TopKAggregationHeapBase(TopKAggregationHeapBase &&) noexcept = default;
    TopKAggregationHeapBase & operator=(TopKAggregationHeapBase &&) noexcept = default;

    size_t size() const { return heap_indices.size(); }

    void recordRows(UInt64 observed, UInt64 skipped)
    {
        observed_rows += observed;
        skipped_rows += skipped;
    }

    bool everRejected() const { return skipped_rows > 0 || evicted_keys > 0; }

    bool needsTrim() const { return heap_indices.size() > next_trim_size; }

    bool shouldFreeze() const;

    /// A skip boundary exists once the local set has filled to K, or earlier if another
    /// thread has published a shared boundary this thread can borrow.
    bool hasBoundary() const { return boundary_is_shared || boundary_row != invalid_row; }

    /// Publishes the local boundary if a trim tightened it since the last exchange, and
    /// re-reads the shared one if another thread has tightened it; one atomic load when
    /// nothing changed on either side. Called once per block.
    void exchangeSharedBoundary();

    /// Per-row on the paths the typed fast path does not cover (`String`, composite, and
    /// anything the numeric dispatch rejects), hence inline.
    bool shouldSkip(const ColumnRawPtrs & source_columns, size_t source_row) const
    {
        chassert(!frozen);
        chassert(hasBoundary());
        if (is_composite)
            return sourceAboveBoundaryComposite(source_columns, source_row);
        return sourceAboveBoundary(*source_columns[0], source_row);
    }

    /// The innermost check of the aggregation loop, hence inline.
    bool shouldSkipTyped(const void * source_typed_data, const ColumnRawPtrs & source_columns, size_t source_row) const
    {
        if (source_typed_data)
        {
            bool skip = false;
            const bool typed = dispatchNumericKeyType(typed_key_type, [&]<typename T>()
            {
                skip = shouldSkipNumeric<T>(source_typed_data, source_row);
            });
            if (typed)
                return skip;
        }
        return shouldSkip(source_columns, source_row);
    }

    const UInt8 * fillSkipBitmap(const void * source_typed_data, size_t begin, size_t end);

protected:
    static constexpr size_t invalid_row = static_cast<size_t>(-1);

    /// The tracked set.
    std::vector<size_t> heap_indices;       /// row indices into `heap_column`, in no particular order
    size_t boundary_row = invalid_row;      /// `heap_column` row of the worst kept key, i.e. the local skip boundary; fixed between trims
    std::unique_ptr<Arena> key_arena;       /// owned bytes of pointer-bearing keys (`emplaceKey` may return a pointer into the source block); rebuilt from survivors at every trim
    size_t k = 0;                           /// the query's `LIMIT K`; trims shrink the set back to it

    /// The cross-thread boundary. The skip paths compare against `boundaryColumn()[boundaryRow()]`,
    /// which is the tighter of the local boundary and the shared one. Skipping against a foreign
    /// boundary is sound (K globally better groups exist), but local trims and evictions keep
    /// ranking against the local set only.
    SharedTopKBoundary * shared_boundary = nullptr; /// one per `Aggregator`; null when the sharing is disabled
    UInt64 shared_version_seen = 0;         /// last `SharedTopKBoundary::version` copied into `shared_cache_column`
    MutableColumnPtr shared_cache_column;   /// single-row local copy of the shared boundary key; null until first refresh
    bool boundary_is_shared = false;        /// the effective boundary is `shared_cache_column[0]`, not `heap_column[boundary_row]`
    bool publish_pending = false;           /// a trim tightened the local boundary since the last exchange

    /// Ranking configuration.
    std::vector<int> directions;            /// per ranked column: +1/-1 for ASC/DESC
    std::vector<int> nulls_directions;      /// per ranked column: NULLs/NaNs placement for `compareAt`/`CompareHelper`
    TypeIndex typed_key_type = TypeIndex::Nothing;  /// single numeric key type for the inlined fast paths; `Nothing` means the virtual `compareAt` path

    std::vector<size_t> low_cardinality_columns;    /// positions of `ColumnLowCardinality` heap columns, for dictionary compaction after a trim

    /// Growth and trim control.
    static constexpr size_t max_tie_rows = 1ULL << 16;    /// rows an untrimmable boundary tie-set may add past `k` before the set freezes
    size_t trim_slack = 0;                  /// `k / 2` (the 1.5 load factor), at least 1; amortizes the O(size) trim
    size_t next_trim_size = 0;              /// the `needsTrim` threshold; raised when a tie-set blocks trimming
    size_t tie_overflow_limit = 0;          /// `k + max_tie_rows`; growing past it from an untrimmable tie-set sets `tie_overflow`
    bool tie_overflow = false;              /// sticky; makes `shouldFreeze` true regardless of the profitability window

    /// Profitability accounting.
    UInt64 observed_rows = 0;               /// fed by `recordRows`; the skip ratio drives the freeze decision
    UInt64 skipped_rows = 0;
    UInt64 evicted_keys = 0;                /// with `skipped_rows` defines `everRejected`, which suppresses hash-table size statistics
    UInt64 profitability_window = 0;        /// rows to observe before the freeze check; 0 disables it

    /// Scratch buffers; members only to avoid per-batch/per-trim allocation.
    std::vector<UInt8> skip_bitmap;         /// per-row skip decisions for the typed batch path
    std::vector<size_t> evicted_rows;       /// `heap_column` rows dropped by the last `trimToK`
    IColumn::Filter trim_filter;            /// which `heap_column` rows survive a trim
    std::vector<size_t> trim_old_to_new;    /// old row index -> compacted row index, to remap `heap_indices`

    /// Sets up the ranked columns and the trim thresholds, and returns the number of rows
    /// the derived class should reserve for its per-row hash-table key array.
    size_t initBase(
        const ColumnRawPtrs & key_columns,
        size_t heap_key_count,
        size_t total_group_by_keys,
        size_t query_k,
        const std::vector<int> & dirs,
        const std::vector<int> & null_dirs,
        UInt64 observation_rows);

    void freezeBase();

    /// Appends the row to `heap_column` and admits it to the set. The derived class must
    /// append the matching hash-table key first, so that the two arrays stay index-aligned.
    /// Runs once per admitted row, hence inline; the one-shot boundary search it triggers when
    /// the set first fills is not, which keeps the comparators out of the including unit.
    void pushHeapRow(const ColumnRawPtrs & source_columns, size_t source_row)
    {
        size_t new_idx = 0;

        if (is_composite)
        {
            auto & tuple = assert_cast<ColumnTuple &>(*heap_column);
            chassert(source_columns.size() == tuple.tupleSize());
            new_idx = tuple.size();

            for (size_t i = 0; i < source_columns.size(); ++i)
                tuple.getColumn(i).insertFrom(*source_columns[i], source_row);

            tuple.addSize(1);
        }
        else
        {
            new_idx = heap_column->size();
            heap_column->insertFrom(*source_columns[0], source_row);
        }

        heap_indices.push_back(new_idx);

        if (boundary_row == invalid_row && heap_indices.size() >= k)
            initBoundary();
    }

    /// Ranks the set, moves the boundary to the k-th best key and drops everything below it.
    /// The dropped rows are left in `evictedRows`; they stay addressable in `heap_column` and
    /// in the derived per-row key array until `finishCompaction` runs.
    void trimToK();

    const std::vector<size_t> & evictedRows() const { return evicted_rows; }

    /// Marks the surviving rows of `heap_column`. Returns false when nothing has to be
    /// compacted, in which case `finishCompaction` must not be called.
    bool startCompaction();

    /// Set for every row of `heap_column` between `startCompaction` and `finishCompaction`.
    /// The surviving rows keep their relative order, so a derived per-row array is compacted
    /// by copying its marked entries down in one forward pass.
    const IColumn::Filter & survivingRows() const { return trim_filter; }

    /// Drops the dead rows from `heap_column` and remaps the row indices. The derived class
    /// must have compacted its own per-row key array first.
    void finishCompaction();

    /// Both of these run once per admitted row on the pointer-bearing key paths, so they stay
    /// inline; being key-independent they cost one instantiation per translation unit.
    Arena & keyArena()
    {
        if (!key_arena)
            key_arena = std::make_unique<Arena>();
        return *key_arena;
    }

    /// The string hash table dispatches on keys by reading whole 8-byte words, touching up to
    /// 7 bytes past either end of the key: for a 1..8-byte key it may read the word that ends
    /// at the key's last byte, i.e. before `data()`, where a plain `Arena::insert` at the head
    /// of a chunk has nothing readable. The leading pad keeps that read in bounds; the trailing
    /// pad covers the forward reads without relying on the arena chunk's tail padding.
    static const char * copyKeyBytes(std::string_view bytes, Arena & arena)
    {
        char * buf = arena.alloc(bytes.size() + 16);
        memcpy(buf + 8, bytes.data(), bytes.size());
        return buf + 8;
    }

private:
    struct GenericComparator
    {
        const TopKAggregationHeapBase * owner;

        bool operator()(size_t a, size_t b) const;
    };

    template <typename T>
    struct TypedComparator
    {
        const T * data;
        int direction;
        int nulls_direction;

        explicit TypedComparator(const TopKAggregationHeapBase * owner)
            : data(assert_cast<const ColumnVector<T> &>(*owner->heap_column).getData().data())
            , direction(owner->directions[0])
            , nulls_direction(owner->nulls_directions[0])
        {
        }

        ALWAYS_INLINE bool operator()(size_t a, size_t b) const
        {
            return direction * CompareHelper<T>::compare(data[a], data[b], nulls_direction) < 0;
        }
    };

    template <typename F>
    static ALWAYS_INLINE bool dispatchNumericKeyType(TypeIndex type, F && f)
    {
        switch (type)
        {
            case TypeIndex::UInt8:     f.template operator()<UInt8>(); return true;
            case TypeIndex::UInt16:    f.template operator()<UInt16>(); return true;
            case TypeIndex::UInt32:    f.template operator()<UInt32>(); return true;
            case TypeIndex::UInt64:    f.template operator()<UInt64>(); return true;
            case TypeIndex::Int8:      f.template operator()<Int8>(); return true;
            case TypeIndex::Int16:     f.template operator()<Int16>(); return true;
            case TypeIndex::Int32:     f.template operator()<Int32>(); return true;
            case TypeIndex::Int64:     f.template operator()<Int64>(); return true;
            case TypeIndex::Float32:   f.template operator()<Float32>(); return true;
            case TypeIndex::Float64:   f.template operator()<Float64>(); return true;
            case TypeIndex::IPv4:      f.template operator()<IPv4>(); return true;
            default: return false;
        }
    }

    /// Instantiated only from `TopKAggregationHeap.cpp`; the comparators and the algorithms
    /// they drive are the bulk of the code and must not reach the including translation units.
    template <typename F>
    ALWAYS_INLINE void withComparator(F && f) const
    {
        const bool typed = dispatchNumericKeyType(typed_key_type, [&]<typename T>()
        {
            f(TypedComparator<T>(this));
        });
        if (!typed)
            f(GenericComparator{this});
    }

    template <typename T>
    ALWAYS_INLINE bool shouldSkipNumeric(const void * source_data, size_t source_row) const
    {
        chassert(hasBoundary());
        const auto * src = reinterpret_cast<const T *>(source_data);
        const auto & boundary_data = assert_cast<const ColumnVector<T> &>(boundaryColumn()).getData();
        return directions[0] * CompareHelper<T>::compare(src[source_row], boundary_data[boundaryRow()], nulls_directions[0]) > 0;
    }

    /// The effective skip boundary: the shared one when it is strictly tighter than the local.
    const IColumn & boundaryColumn() const { return boundary_is_shared ? *shared_cache_column : *heap_column; }
    size_t boundaryRow() const { return boundary_is_shared ? 0 : boundary_row; }

    /// Recomputes `boundary_is_shared` after either boundary moved.
    void updateBoundaryChoice();

    /// Ranked comparison of two single-key rows (composite-aware); negative when `lhs` is better.
    int compareRanked(const IColumn & lhs, size_t lhs_row, const IColumn & rhs, size_t rhs_row) const;

    void setK(size_t query_k);

    void init(const IColumn & source_column, size_t query_k, int direction, int nulls_direction);

    void init(
        const ColumnRawPtrs & source_columns,
        size_t query_k,
        const std::vector<int> & dirs,
        const std::vector<int> & null_dirs);

    void findLowCardinalityColumns();

    void compactDictionaries();

    /// Sets `boundary_row` to the worst key once the set first reaches `k`; runs once per heap.
    void initBoundary();

    bool sourceAboveBoundary(const IColumn & source_column, size_t source_row) const
    {
        const int cmp = compareColumns(source_column, source_row, boundaryColumn(), boundaryRow(), 0);
        return directions[0] * cmp > 0;
    }

    bool sourceAboveBoundaryComposite(const ColumnRawPtrs & source_columns, size_t source_row) const
    {
        const auto & tuple = assert_cast<const ColumnTuple &>(boundaryColumn());
        const size_t boundary_idx = boundaryRow();
        for (size_t i = 0; i < source_columns.size(); ++i)
        {
            const int cmp = compareColumns(*source_columns[i], source_row, tuple.getColumn(i), boundary_idx, i);
            if (cmp != 0)
                return directions[i] * cmp > 0;
        }
        return false;
    }

    int compareHeapRowsComposite(size_t a, size_t b) const;

    int compareColumns(const IColumn & lhs, size_t lhs_row, const IColumn & rhs, size_t rhs_row, size_t column_index) const
    {
        return lhs.compareAt(lhs_row, rhs_row, rhs, nulls_directions[column_index]);
    }
};

/** A bounded set tracking the top-K best keys by the query's `ORDER BY`.
  * Supports single-column and composite (`ColumnTuple`) keys.
  *
  * No heap is maintained: only the boundary (the worst kept key) is ever
  * consulted, and it cannot change between trims - everything admitted is
  * strictly better than it - so admission is a plain `push_back` and a trim
  * is one `nth_element` past `next_trim_size` (~1.5x capacity).  Boundary
  * ties are never evicted, so a tie-set can grow the set; past
  * `tie_overflow_limit` it freezes.
  *
  * Adds to `TopKAggregationHeapBase` only what the hash-table key type touches:
  * the per-row key captured at admission, which lets an eviction erase the group.
  */
template <typename Key>
struct TopKAggregationHeap : public TopKAggregationHeapBase
{
    void initIfNeeded(
        const ColumnRawPtrs & key_columns,
        size_t heap_key_count,
        size_t total_group_by_keys,
        size_t query_k,
        const std::vector<int> & dirs,
        const std::vector<int> & null_dirs,
        UInt64 observation_rows,
        SharedTopKBoundary * shared)
    {
        if (heap_column)
            return;

        shared_boundary = shared;

        const size_t reserve_hint
            = initBase(key_columns, heap_key_count, total_group_by_keys, query_k, dirs, null_dirs, observation_rows);

        hash_table_keys.clear();
        hash_table_keys.reserve(reserve_hint);
    }

    void freeze()
    {
        freezeBase();
        hash_table_keys = {};
    }

    void push(const ColumnRawPtrs & source_columns, size_t source_row) { push(source_columns, source_row, Key{}); }

    void push(const ColumnRawPtrs & source_columns, size_t source_row, const Key & hash_table_key)
    {
        if constexpr (keys_hold_pointers)
            hash_table_keys.push_back(persistHashTableKey(hash_table_key, keyArena()));
        else
            hash_table_keys.push_back(hash_table_key);

        pushHeapRow(source_columns, source_row);
    }

    template <typename EvictCallback>
    size_t trimAndCompact(EvictCallback && on_evict)
    {
        trimToK();

        for (size_t evicted_row : evictedRows())
            on_evict(evicted_row);

        const size_t evicted_count = evictedRows().size();

        if (startCompaction())
        {
            compactHashTableKeys();
            finishCompaction();
        }

        return evicted_count;
    }

    size_t trimAndCompact()
    {
        return trimAndCompact([](size_t) { });
    }

    const Key & hashTableKeyAt(size_t heap_row) const { return hash_table_keys[heap_row]; }

private:
    static constexpr bool keys_hold_pointers
        = std::is_same_v<Key, std::string_view> || std::is_same_v<Key, PackedStringRef>;

    std::vector<Key> hash_table_keys;       /// per `heap_column` row: the hash-table key captured at admission, for `erase` on eviction

    static Key persistHashTableKey(const Key & key, Arena & arena)
    {
        if constexpr (std::is_same_v<Key, std::string_view>)
        {
            if (key.empty())
                return {};
            return {copyKeyBytes(key, arena), key.size()};
        }
        else if constexpr (std::is_same_v<Key, PackedStringRef>)
        {
            /// These keys live in a plain `HashMap` compared by exact `memcmp`,
            /// so the ordinary unpadded persistence applies.
            ArenaPackedStringHolder holder{key, arena};
            keyHolderPersistKey(holder);
            return holder.key;
        }
        else
            return key;
    }

    void compactHashTableKeys()
    {
        std::unique_ptr<Arena> compacted_arena;
        if constexpr (keys_hold_pointers)
            compacted_arena = std::make_unique<Arena>();

        const auto & surviving = survivingRows();
        size_t new_idx = 0;
        for (size_t i = 0, rows = surviving.size(); i < rows; ++i)
        {
            if (!surviving[i])
                continue;
            if constexpr (keys_hold_pointers)
                hash_table_keys[new_idx] = persistHashTableKey(hash_table_keys[i], *compacted_arena);
            else
                hash_table_keys[new_idx] = hash_table_keys[i];
            ++new_idx;
        }
        hash_table_keys.resize(new_idx);

        if constexpr (keys_hold_pointers)
            key_arena = std::move(compacted_arena);
    }
};

}
