#pragma once

#include <algorithm>
#include <memory>
#include <string_view>
#include <type_traits>
#include <vector>

#include <base/defines.h>
#include <base/PackedStringRef.h>
#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Common/Arena.h>
#include <Common/assert_cast.h>
#include <Core/CompareHelper.h>
#include <Core/TypeId.h>

#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>

namespace DB
{

/** A bounded heap tracking the top-K best keys by the query's `ORDER BY`.
  * Supports single-column and composite (`ColumnTuple`) keys; the worst kept
  * key sits at the front of `heap_indices`.  Trimming runs past
  * `next_trim_size` (~1.5x capacity).  Boundary ties are never evicted, so a
  * tie-set can grow the heap; past `tie_overflow_limit` the heap freezes.
  */
template <typename Key>
struct TopKAggregationHeap
{
    MutableColumnPtr heap_column;           /// the tracked key values, one row per key (`ColumnTuple` for composite); null while not running
    bool is_composite = false;              /// `heap_column` is a `ColumnTuple`
    bool is_prefix_mode = false;            /// ranks only a key prefix, which identifies many groups: skip-only, the caller must not erase
    bool frozen = false;                    /// abandoned at runtime; aggregation proceeds as if the optimization were off
    std::vector<AggregateDataPtr> free_states;  /// state slots of pruned groups, reused by later inserts (arena memory is never returned)

private:
    /// The heap proper.
    std::vector<size_t> heap_indices;       /// binary max-heap of row indices into `heap_column`; `front` is the worst kept key, i.e. the skip boundary
    std::vector<Key> hash_table_keys;       /// per `heap_column` row: the hash-table key captured at admission, for `erase` on eviction
    std::unique_ptr<Arena> key_arena;       /// owned bytes of pointer-bearing keys (`emplaceKey` may return a pointer into the source block); rebuilt from survivors at every trim
    size_t k = 0;                           /// the query's `LIMIT K`; trims shrink the heap back to it

    /// Ranking configuration.
    std::vector<int> directions;            /// per ranked column: +1/-1 for ASC/DESC
    std::vector<int> nulls_directions;      /// per ranked column: NULLs/NaNs placement for `compareAt`/`CompareHelper`

    /// Typed fast paths for a single numeric key, resolved once from the column's `TypeIndex`; null means the virtual `compareAt` path.
    using ShouldSkipNumericFn = bool (*)(const TopKAggregationHeap &, const void *, size_t);
    using NumericCmpFn = bool (*)(const TopKAggregationHeap &, size_t, size_t);
    using FillSkipBitmapFn = void (*)(const TopKAggregationHeap &, const void *, size_t, size_t, UInt8 *);
    NumericCmpFn numeric_cmp_fn = nullptr;
    ShouldSkipNumericFn should_skip_numeric_fn = nullptr;
    FillSkipBitmapFn fill_skip_bitmap_fn = nullptr;

    std::vector<size_t> low_cardinality_columns;    /// positions of `ColumnLowCardinality` heap columns, for dictionary compaction after a trim

    /// Growth and trim control.
    static constexpr size_t max_tie_rows = 1ULL << 20;    /// rows an untrimmable boundary tie-set may add past `k` before the heap freezes
    size_t trim_slack = 0;                  /// `k / 2` (the 1.5 load factor), at least 1; amortizes the O(heap) compaction
    size_t next_trim_size = 0;              /// the `needsTrim` threshold; raised when a tie plateau blocks trimming
    size_t tie_overflow_limit = 0;          /// `k + max_tie_rows`; growing past it from an untrimmable tie-set sets `tie_overflow`
    bool tie_overflow = false;              /// sticky; makes `shouldFreeze` true regardless of the profitability window
    size_t tie_scan_size = 0;               /// heap size at the last failed O(heap) plateau scan; suppresses rescans until the heap ~doubles

    /// Profitability accounting.
    UInt64 observed_rows = 0;               /// fed by `recordRows`; the skip ratio drives the freeze decision
    UInt64 skipped_rows = 0;
    UInt64 evicted_keys = 0;                /// with `skipped_rows` defines `everRejected`, which suppresses hash-table size statistics
    UInt64 profitability_window = 0;        /// rows to observe before the freeze check; 0 disables it

    /// Scratch buffers; members only to avoid per-batch/per-trim allocation.
    std::vector<UInt8> skip_bitmap;         /// per-row skip decisions for the typed batch path
    IColumn::Filter trim_filter;            /// which `heap_column` rows survive a trim
    std::vector<size_t> trim_old_to_new;    /// old row index -> compacted row index, to remap `heap_indices`

public:
    TopKAggregationHeap() = default;
    TopKAggregationHeap(const TopKAggregationHeap &) = delete;
    TopKAggregationHeap & operator=(const TopKAggregationHeap &) = delete;
    TopKAggregationHeap(TopKAggregationHeap &&) noexcept = default;
    TopKAggregationHeap & operator=(TopKAggregationHeap &&) noexcept = default;

    void initIfNeeded(
        const ColumnRawPtrs & key_columns,
        size_t heap_key_count,
        size_t total_group_by_keys,
        size_t query_k,
        const std::vector<int> & dirs,
        const std::vector<int> & null_dirs,
        UInt64 observation_rows)
    {
        if (heap_column)
            return;

        is_prefix_mode = heap_key_count < total_group_by_keys;

        if (heap_key_count == 1)
        {
            init(
                *key_columns[0],
                query_k,
                dirs.empty() ? 1 : dirs[0],
                null_dirs.empty() ? 1 : null_dirs[0]);
        }
        else
        {
            ColumnRawPtrs heap_cols(key_columns.begin(), key_columns.begin() + heap_key_count);
            init(heap_cols, query_k, dirs, null_dirs);
        }

        /// The window must cover at least one full fill of the heap plus as
        /// much again, so the skip rate is judged on a heap that had a chance
        /// to establish its boundary.
        profitability_window = observation_rows == 0 ? 0 : std::max<UInt64>(observation_rows, 2 * next_trim_size);
    }

    size_t size() const { return heap_indices.size(); }

    void recordRows(UInt64 observed, UInt64 skipped)
    {
        observed_rows += observed;
        skipped_rows += skipped;
    }

    bool everRejected() const { return skipped_rows > 0 || evicted_keys > 0; }

    bool shouldFreeze() const
    {
        if (frozen || !heap_column)
            return false;

        if (profitability_window && observed_rows >= profitability_window
            && static_cast<Float64>(skipped_rows) / static_cast<Float64>(observed_rows) < 0.1 && evicted_keys < k
            && heap_indices.size() >= k)
            return true;

        return tie_overflow;
    }

    void freeze()
    {
        frozen = true;
        heap_column = nullptr;
        heap_indices = {};
        hash_table_keys = {};
        key_arena.reset();
        skip_bitmap = {};
        trim_filter = {};
        trim_old_to_new = {};
    }

    bool shouldSkip(const ColumnRawPtrs & source_columns, size_t source_row) const
    {
        chassert(!frozen);
        chassert(!heap_indices.empty());
        const size_t boundary = heap_indices.front();
        if (is_composite)
            return sourceAboveHeapComposite(source_columns, source_row, boundary);
        return sourceAboveHeap(*source_columns[0], source_row, boundary);
    }

    bool shouldSkipTyped(const void * source_typed_data, const ColumnRawPtrs & source_columns, size_t source_row) const
    {
        if (should_skip_numeric_fn && source_typed_data)
        {
            chassert(!heap_indices.empty());
            return should_skip_numeric_fn(*this, source_typed_data, source_row);
        }
        return shouldSkip(source_columns, source_row);
    }

    const UInt8 * fillSkipBitmap(const void * source_typed_data, size_t begin, size_t end)
    {
        chassert(!frozen);
        if (!fill_skip_bitmap_fn || !source_typed_data)
            return nullptr;
        chassert(!heap_indices.empty());
        skip_bitmap.resize(end);
        fill_skip_bitmap_fn(*this, source_typed_data, begin, end, skip_bitmap.data());
        return skip_bitmap.data();
    }

    void push(const ColumnRawPtrs & source_columns, size_t source_row) { push(source_columns, source_row, Key{}); }

    void push(const ColumnRawPtrs & source_columns, size_t source_row, const Key & hash_table_key)
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
        if constexpr (keys_hold_pointers)
            hash_table_keys.push_back(persistHashTableKey(hash_table_key, keyArena()));
        else
            hash_table_keys.push_back(hash_table_key);
        heap_indices.push_back(new_idx);

        std::push_heap(heap_indices.begin(), heap_indices.end(), HeapComparator{this});
    }

    bool needsTrim() const
    {
        return heap_indices.size() > next_trim_size;
    }

    template <typename EvictCallback>
    size_t trimAndCompact(EvictCallback && on_evict)
    {
        size_t evicted_count = 0;
        const HeapComparator cmp{this};
        const auto tied = [&](size_t a, size_t b) { return !cmp(a, b) && !cmp(b, a); };
        while (heap_indices.size() > k)
        {
            std::pop_heap(heap_indices.begin(), heap_indices.end(), cmp);
            const size_t candidate = heap_indices.back();

            if (heap_indices.size() < 2 || !tied(candidate, heap_indices.front()))
            {
                heap_indices.pop_back();
                on_evict(candidate);
                ++evicted_count;
                tie_scan_size = 0;
                continue;
            }

            std::push_heap(heap_indices.begin(), heap_indices.end(), cmp);

            if (tie_scan_size != 0 && heap_indices.size() < 2 * tie_scan_size)
            {
                next_trim_size = std::max(next_trim_size, heap_indices.size() + trim_slack + 1);
                if (heap_indices.size() > tie_overflow_limit)
                    tie_overflow = true;
                break;
            }

            const size_t boundary = heap_indices.front();
            size_t plateau = 0;
            for (size_t idx : heap_indices)
                plateau += tied(idx, boundary);

            if (heap_indices.size() - plateau < k)
            {
                tie_scan_size = heap_indices.size();
                next_trim_size = std::max(next_trim_size, heap_indices.size() + trim_slack + 1);
                if (heap_indices.size() > tie_overflow_limit)
                    tie_overflow = true;
                break;
            }

            const auto plateau_begin = std::partition(
                heap_indices.begin(), heap_indices.end(), [&](size_t idx) { return !tied(idx, boundary); });
            for (auto it = plateau_begin; it != heap_indices.end(); ++it)
            {
                on_evict(*it);
                ++evicted_count;
            }
            heap_indices.erase(plateau_begin, heap_indices.end());
            std::make_heap(heap_indices.begin(), heap_indices.end(), cmp);
            tie_scan_size = 0;
        }
        evicted_keys += evicted_count;

        const size_t col_size = heap_column->size();
        if (col_size <= heap_indices.size())
            return evicted_count;

        trim_filter.clear();
        trim_filter.resize_fill(col_size, 0);

        for (size_t idx : heap_indices)
            trim_filter[idx] = 1;

        trim_old_to_new.resize(col_size);
        size_t new_idx = 0;

        for (size_t i = 0; i < col_size; ++i)
        {
            if (trim_filter[i])
                trim_old_to_new[i] = new_idx++;
        }

        heap_column->filter(trim_filter);
        compactDictionaries();

        std::vector<Key> compacted_hash_table_keys;
        compacted_hash_table_keys.reserve(heap_indices.size());
        for (size_t i = 0; i < col_size; ++i)
            if (trim_filter[i])
                compacted_hash_table_keys.push_back(std::move(hash_table_keys[i]));
        hash_table_keys = std::move(compacted_hash_table_keys);

        if constexpr (keys_hold_pointers)
        {
            if (key_arena)
            {
                auto compacted_arena = std::make_unique<Arena>();
                for (auto & key : hash_table_keys)
                    key = persistHashTableKey(key, *compacted_arena);
                key_arena = std::move(compacted_arena);
            }
        }

        for (auto & idx : heap_indices)
            idx = trim_old_to_new[idx];

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

    Arena & keyArena()
    {
        if (!key_arena)
            key_arena = std::make_unique<Arena>();
        return *key_arena;
    }

    /// The 8-byte padding on both sides keeps the hash table's word-sized reads in bounds.
    static const char * copyKeyBytes(std::string_view bytes, Arena & arena)
    {
        char * buf = arena.alloc(bytes.size() + 16);
        memcpy(buf + 8, bytes.data(), bytes.size());
        return buf + 8;
    }

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
            /// Small (and empty) keys are self-contained, nothing to copy.
            if (key.heapSize() == 0)
                return key;
            auto persistent_key = key;
            const char * copy = copyKeyBytes(static_cast<std::string_view>(key), arena);
            if (persistent_key.isMedium())
                persistent_key.setMediumPointer(copy);
            else
                persistent_key.setLargePointer(copy);
            return persistent_key;
        }
        else
            return key;
    }

    void compactDictionaries()
    {
        if (low_cardinality_columns.empty())
            return;

        if (is_composite)
        {
            auto & tuple = assert_cast<ColumnTuple &>(*heap_column);
            for (size_t i : low_cardinality_columns)
                assert_cast<ColumnLowCardinality &>(tuple.getColumn(i)).compactDictionaryInplace();
        }
        else
            assert_cast<ColumnLowCardinality &>(*heap_column).compactDictionaryInplace();
    }

    void findLowCardinalityColumns()
    {
        low_cardinality_columns.clear();

        if (is_composite)
        {
            const auto & tuple = assert_cast<const ColumnTuple &>(*heap_column);
            for (size_t i = 0; i < tuple.tupleSize(); ++i)
                if (typeid_cast<const ColumnLowCardinality *>(&tuple.getColumn(i)))
                    low_cardinality_columns.push_back(i);
        }
        else if (typeid_cast<const ColumnLowCardinality *>(heap_column.get()))
            low_cardinality_columns.push_back(0);
    }

    void setK(size_t query_k)
    {
        k = query_k;
        tie_scan_size = 0;
        trim_slack = std::max<size_t>(1, k / 2);
        next_trim_size = k + trim_slack;
        tie_overflow_limit = k + max_tie_rows;
    }

    void init(
        const IColumn & source_column,
        size_t query_k,
        int direction,
        int nulls_direction)
    {
        directions = {direction};
        nulls_directions = {nulls_direction};
        is_composite = false;
        setK(query_k);
        heap_column = source_column.cloneEmpty();
        const size_t reserve_hint = next_trim_size + 1;
        heap_column->reserve(reserve_hint);
        heap_indices.clear();
        heap_indices.reserve(reserve_hint);
        hash_table_keys.clear();
        hash_table_keys.reserve(reserve_hint);
        key_arena.reset();
        findLowCardinalityColumns();
        initNumericSkipFn();
    }

    void init(
        const ColumnRawPtrs & source_columns,
        size_t query_k,
        const std::vector<int> & dirs,
        const std::vector<int> & null_dirs)
    {
        const size_t n = source_columns.size();
        is_composite = true;
        setK(query_k);

        directions.assign(n, 1);
        for (size_t i = 0; i < dirs.size() && i < n; ++i)
            directions[i] = dirs[i];

        nulls_directions.assign(n, 1);
        for (size_t i = 0; i < null_dirs.size() && i < n; ++i)
            nulls_directions[i] = null_dirs[i];

        MutableColumns sub_columns;
        sub_columns.reserve(n);
        for (const auto * col : source_columns)
            sub_columns.emplace_back(col->cloneEmpty());
        heap_column = ColumnTuple::create(std::move(sub_columns));
        const size_t reserve_hint = next_trim_size + 1;
        heap_column->reserve(reserve_hint);

        heap_indices.clear();
        heap_indices.reserve(reserve_hint);
        hash_table_keys.clear();
        hash_table_keys.reserve(reserve_hint);
        key_arena.reset();
        findLowCardinalityColumns();
        should_skip_numeric_fn = nullptr;
        numeric_cmp_fn = nullptr;
        fill_skip_bitmap_fn = nullptr;
    }

    bool sourceAboveHeap(const IColumn & source_column, size_t source_row, size_t heap_row) const
    {
        const int cmp = compareColumns(source_column, source_row, *heap_column, heap_row, 0);
        return directions[0] * cmp > 0;
    }

    bool sourceAboveHeapComposite(const ColumnRawPtrs & source_columns, size_t source_row, size_t heap_row) const
    {
        const auto & tuple = assert_cast<const ColumnTuple &>(*heap_column);
        for (size_t i = 0; i < source_columns.size(); ++i)
        {
            const int cmp = compareColumns(*source_columns[i], source_row, tuple.getColumn(i), heap_row, i);
            if (cmp != 0)
                return directions[i] * cmp > 0;
        }
        return false;
    }

    int compareHeapRowsComposite(size_t a, size_t b) const
    {
        const auto & tuple = assert_cast<const ColumnTuple &>(*heap_column);
        for (size_t i = 0; i < directions.size(); ++i)
        {
            const auto & col = tuple.getColumn(i);
            const int cmp = compareColumns(col, a, col, b, i);
            if (cmp != 0)
                return directions[i] * cmp;
        }
        return 0;
    }

    int compareColumns(const IColumn & lhs, size_t lhs_row, const IColumn & rhs, size_t rhs_row, size_t column_index) const
    {
        return lhs.compareAt(lhs_row, rhs_row, rhs, nulls_directions[column_index]);
    }

    struct HeapComparator
    {
        const TopKAggregationHeap * owner;

        bool operator()(size_t a, size_t b) const
        {
            if (owner->numeric_cmp_fn)
                return owner->numeric_cmp_fn(*owner, a, b);

            if (owner->is_composite)
                return owner->compareHeapRowsComposite(a, b) < 0;

            const int cmp = owner->compareColumns(*owner->heap_column, a, *owner->heap_column, b, 0);
            return owner->directions[0] * cmp < 0;
        }
    };

    template <typename ActualKeyType>
    static bool shouldSkipNumericImpl(const TopKAggregationHeap & self, const void * source_data, size_t source_row)
    {
        const auto * src = reinterpret_cast<const ActualKeyType *>(source_data);
        const auto & heap_data = assert_cast<const ColumnVector<ActualKeyType> &>(*self.heap_column).getData();
        const size_t boundary_row = self.heap_indices.front();
        return self.directions[0] * CompareHelper<ActualKeyType>::compare(src[source_row], heap_data[boundary_row], self.nulls_directions[0]) > 0;
    }

    template <typename ActualKeyType>
    static bool heapCompareNumericImpl(const TopKAggregationHeap & self, size_t a, size_t b)
    {
        const auto & data = assert_cast<const ColumnVector<ActualKeyType> &>(*self.heap_column).getData();
        return self.directions[0] * CompareHelper<ActualKeyType>::compare(data[a], data[b], self.nulls_directions[0]) < 0;
    }

    template <typename ActualKeyType>
    static void fillSkipBitmapImpl(const TopKAggregationHeap & self, const void * source_data, size_t begin, size_t end, UInt8 * bitmap)
    {
        const auto * src = reinterpret_cast<const ActualKeyType *>(source_data);
        const auto & heap_data = assert_cast<const ColumnVector<ActualKeyType> &>(*self.heap_column).getData();
        const ActualKeyType boundary = heap_data[self.heap_indices.front()];
        const int direction = self.directions[0];
        const int nulls_direction = self.nulls_directions[0];
        for (size_t i = begin; i < end; ++i)
            bitmap[i] = direction * CompareHelper<ActualKeyType>::compare(src[i], boundary, nulls_direction) > 0;
    }

    template <typename ActualKeyType>
    void resolveNumericFastPath()
    {
        should_skip_numeric_fn = &shouldSkipNumericImpl<ActualKeyType>;
        numeric_cmp_fn = &heapCompareNumericImpl<ActualKeyType>;
        fill_skip_bitmap_fn = &fillSkipBitmapImpl<ActualKeyType>;
    }

    void initNumericSkipFn()
    {
        should_skip_numeric_fn = nullptr;
        numeric_cmp_fn = nullptr;
        fill_skip_bitmap_fn = nullptr;

        switch (heap_column->getDataType())
        {
            case TypeIndex::UInt8:     resolveNumericFastPath<UInt8>(); break;
            case TypeIndex::UInt16:    resolveNumericFastPath<UInt16>(); break;
            case TypeIndex::UInt32:    resolveNumericFastPath<UInt32>(); break;
            case TypeIndex::UInt64:    resolveNumericFastPath<UInt64>(); break;
            case TypeIndex::Int8:      resolveNumericFastPath<Int8>(); break;
            case TypeIndex::Int16:     resolveNumericFastPath<Int16>(); break;
            case TypeIndex::Int32:     resolveNumericFastPath<Int32>(); break;
            case TypeIndex::Int64:     resolveNumericFastPath<Int64>(); break;
            case TypeIndex::Float32:   resolveNumericFastPath<Float32>(); break;
            case TypeIndex::Float64:   resolveNumericFastPath<Float64>(); break;
            case TypeIndex::IPv4:      resolveNumericFastPath<IPv4>(); break;
            default: break;
        }
    }
};

}
