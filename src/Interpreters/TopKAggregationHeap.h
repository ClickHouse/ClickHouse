#pragma once
#include <algorithm>
#include <limits>
#include <vector>

#include <base/defines.h>
#include <AggregateFunctions/IAggregateFunction_fwd.h>
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
struct TopKAggregationHeap
{
    MutableColumnPtr heap_column;

    bool is_composite = false;
    bool is_prefix_mode = false;
    bool frozen = false;

    std::vector<AggregateDataPtr> free_states;

    TopKAggregationHeap() = default;
    TopKAggregationHeap(const TopKAggregationHeap &) = delete;
    TopKAggregationHeap & operator=(const TopKAggregationHeap &) = delete;
    TopKAggregationHeap(TopKAggregationHeap &&) noexcept = default;
    TopKAggregationHeap & operator=(TopKAggregationHeap &&) noexcept = default;

    void initIfNeeded(
        const ColumnRawPtrs & key_columns,
        size_t heap_key_count,
        size_t total_group_by_keys,
        size_t cap,
        const std::vector<int> & dirs,
        const std::vector<int> & null_dirs,
        Float64 load_factor,
        UInt64 observation_rows)
    {
        if (heap_column)
            return;

        is_prefix_mode = heap_key_count < total_group_by_keys;
        trim_load_factor = std::max(1.0, load_factor);

        if (heap_key_count == 1)
        {
            init(
                *key_columns[0],
                cap,
                dirs.empty() ? 1 : dirs[0],
                null_dirs.empty() ? 1 : null_dirs[0]);
        }
        else
        {
            ColumnRawPtrs heap_cols(key_columns.begin(), key_columns.begin() + heap_key_count);
            init(heap_cols, cap, dirs, null_dirs);
        }

        /// The window must cover at least one full fill of the heap plus as
        /// much again, so the skip rate is judged on a heap that had a chance
        /// to establish its boundary.
        if (observation_rows == 0)
            profitability_window = 0;
        else if (next_trim_size >= std::numeric_limits<UInt64>::max() / 2)
            profitability_window = std::numeric_limits<UInt64>::max();
        else
            profitability_window = std::max<UInt64>(observation_rows, 2 * next_trim_size);
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
            && skipped_rows == 0 && evicted_keys == 0
            && heap_indices.size() >= capacity)
            return true;

        return tie_overflow;
    }

    void freeze()
    {
        frozen = true;
        heap_column = nullptr;
        heap_indices = {};
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

    void push(const ColumnRawPtrs & source_columns, size_t source_row)
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
        while (heap_indices.size() > capacity)
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

            /// The worst value is a plateau of equal keys. Only the tie-set that straddles the
            /// capacity boundary must be kept: evicting a key tied with the boundary while its twin
            /// stays would resurface an incomplete aggregate. But a tie with the entry at the front
            /// does not prove the candidate sits at the boundary (the two coincide only at
            /// size == capacity + 1) - a plateau strictly outside the top-K, which is the normal case
            /// in prefix mode, is fully evictable. Treating it as protected would stall every trim,
            /// pin the skip boundary and grow the heap until it froze.
            std::push_heap(heap_indices.begin(), heap_indices.end(), cmp);

            /// Deciding needs a pass over the heap, so do not redo it for a plateau already found
            /// unevictable until enough keys have arrived to change the answer. Together with the
            /// `next_trim_size` ratchet below this keeps the cost amortized: a plateau that legitimately
            /// owns the boundary (every key ties, so eviction is never possible) is rescanned only once
            /// per doubling instead of on every trim.
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

            if (heap_indices.size() - plateau < capacity)
            {
                tie_scan_size = heap_indices.size();
                next_trim_size = std::max(next_trim_size, heap_indices.size() + trim_slack + 1);
                if (heap_indices.size() > tie_overflow_limit)
                    tie_overflow = true;
                break;
            }

            /// Enough strictly better keys remain, so the whole plateau is outside the top-K.
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

        for (auto & idx : heap_indices)
            idx = trim_old_to_new[idx];

        return evicted_count;
    }

private:
    static constexpr size_t max_preallocated_rows = 1ULL << 20;

    Float64 trim_load_factor = 1.5;
    size_t trim_slack = 0;
    UInt64 profitability_window = 0;

    std::vector<int> directions;
    std::vector<int> nulls_directions;

    UInt64 observed_rows = 0;
    UInt64 skipped_rows = 0;
    UInt64 evicted_keys = 0;

    bool tie_overflow = false;
    /// Heap size when a boundary plateau was last found unevictable; 0 means "decide again".
    size_t tie_scan_size = 0;

    std::vector<size_t> low_cardinality_columns;

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

    void setCapacity(size_t cap)
    {
        capacity = cap;
        tie_scan_size = 0;

        const auto slack_f = static_cast<Float64>(capacity) * (trim_load_factor - 1.0);
        trim_slack = slack_f >= static_cast<Float64>(std::numeric_limits<size_t>::max())
            ? std::numeric_limits<size_t>::max()
            : std::max<size_t>(1, static_cast<size_t>(slack_f));
        next_trim_size = capacity > std::numeric_limits<size_t>::max() - trim_slack
            ? std::numeric_limits<size_t>::max()
            : capacity + trim_slack;
        chassert(next_trim_size >= capacity);

        tie_overflow_limit = capacity > std::numeric_limits<size_t>::max() - max_preallocated_rows
            ? std::numeric_limits<size_t>::max()
            : capacity + max_preallocated_rows;
    }

    size_t reserveHint() const
    {
        const size_t hint = next_trim_size >= max_preallocated_rows ? max_preallocated_rows : next_trim_size + 1;
        chassert(hint <= max_preallocated_rows);
        return hint;
    }

    void init(
        const IColumn & source_column,
        size_t cap,
        int direction,
        int nulls_direction)
    {
        directions = {direction};
        nulls_directions = {nulls_direction};
        is_composite = false;
        setCapacity(cap);
        heap_column = source_column.cloneEmpty();
        const size_t reserve_hint = reserveHint();
        heap_column->reserve(reserve_hint);
        heap_indices.clear();
        heap_indices.reserve(reserve_hint);
        findLowCardinalityColumns();
        initNumericSkipFn();
    }

    void init(
        const ColumnRawPtrs & source_columns,
        size_t cap,
        const std::vector<int> & dirs,
        const std::vector<int> & null_dirs)
    {
        const size_t n = source_columns.size();
        is_composite = true;
        setCapacity(cap);

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
        const size_t reserve_hint = reserveHint();
        heap_column->reserve(reserve_hint);

        heap_indices.clear();
        heap_indices.reserve(reserve_hint);
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

    using ShouldSkipNumericFn = bool (*)(const TopKAggregationHeap &, const void *, size_t);
    ShouldSkipNumericFn should_skip_numeric_fn = nullptr;

    using NumericCmpFn = bool (*)(const TopKAggregationHeap &, size_t, size_t);
    NumericCmpFn numeric_cmp_fn = nullptr;

    using FillSkipBitmapFn = void (*)(const TopKAggregationHeap &, const void *, size_t, size_t, UInt8 *);
    FillSkipBitmapFn fill_skip_bitmap_fn = nullptr;

    std::vector<UInt8> skip_bitmap;

    IColumn::Filter trim_filter;
    std::vector<size_t> trim_old_to_new;

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
            case TypeIndex::Date:      resolveNumericFastPath<UInt16>(); break;
            case TypeIndex::Date32:    resolveNumericFastPath<Int32>(); break;
            case TypeIndex::DateTime:  resolveNumericFastPath<UInt32>(); break;
            case TypeIndex::Enum8:     resolveNumericFastPath<Int8>(); break;
            case TypeIndex::Enum16:    resolveNumericFastPath<Int16>(); break;
            case TypeIndex::IPv4:      resolveNumericFastPath<IPv4>(); break;
            default: break;
        }
    }

    std::vector<size_t> heap_indices;
    size_t capacity = 0;
    size_t next_trim_size = 0;
    size_t tie_overflow_limit = 0;
};

}
