#pragma once
#include <algorithm>
#include <limits>
#include <vector>

#include <base/defines.h>
#include <Common/assert_cast.h>
#include <Core/CompareHelper.h>
#include <Core/TypeId.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>

namespace DB
{

/** A bounded heap tracking the top-K best keys by the query's `ORDER BY`.
  * Supports single-column and composite (`ColumnTuple`) keys; the worst kept
  * key (evicted next) sits at the front of `heap_indices`.
  */
struct TopKAggregationHeap
{
    MutableColumnPtr heap_column;

    std::vector<int> directions;

    std::vector<int> nulls_directions;

    bool is_composite = false;

    bool is_prefix_mode = false;

    bool frozen = false;
    UInt64 observed_rows = 0;
    UInt64 skipped_rows = 0;
    UInt64 evicted_keys = 0;

    /// Set when the boundary tie-set (keys that compare equal to the boundary and
    /// therefore cannot be evicted) has grown the heap past `tie_overflow_limit`.
    /// Distinct keys can tie under the heap's order (NaN payloads, or in prefix
    /// mode every full key sharing the boundary prefix), so the tie guard can
    /// otherwise grow the heap without bound.  The caller freezes the heap when
    /// it is safe to do so (see `Aggregator::executeImpl`).
    bool tie_overflow = false;

    static constexpr UInt64 freeze_observation_threshold = 256 * 1024;

    bool shouldFreeze() const
    {
        return !frozen
            && heap_column
            && observed_rows >= freeze_observation_threshold
            && skipped_rows == 0
            && evicted_keys == 0
            && heap_indices.size() >= capacity;
    }

    void freeze()
    {
        frozen = true;
        heap_column = nullptr;
        heap_indices = {};
        skip_bitmap = {};
    }

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
        const std::vector<int> & null_dirs)
    {
        if (heap_column)
            return;

        is_prefix_mode = heap_key_count < total_group_by_keys;

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
    }

    size_t size() const { return heap_indices.size(); }

    bool shouldSkip(const ColumnRawPtrs & source_columns, size_t source_row) const
    {
        chassert(!heap_indices.empty());
        const size_t boundary = heap_indices.front();
        if (is_composite)
            return sourceAboveHeapComposite(source_columns, source_row, boundary);
        return sourceAboveHeap(*source_columns[0], source_row, boundary);
    }

    bool shouldSkip(const void * source_typed_data, const ColumnRawPtrs & source_columns, size_t source_row) const
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
        return heap_indices.size() > compaction_threshold;
    }

    template <typename EvictCallback>
    size_t trimAndCompact(EvictCallback && on_evict)
    {
        size_t evicted_count = 0;
        const HeapComparator cmp{this};
        while (heap_indices.size() > capacity)
        {
            std::pop_heap(heap_indices.begin(), heap_indices.end(), cmp);
            const size_t candidate = heap_indices.back();

            /// Never evict a key that ties with the boundary: it is a legitimate
            /// top-K member and evicting it would destroy a group whose later rows
            /// get re-admitted, surfacing an incomplete aggregate.
            const size_t new_front = heap_indices.front();
            if (!cmp(candidate, new_front) && !cmp(new_front, candidate))
            {
                std::push_heap(heap_indices.begin(), heap_indices.end(), cmp);
                compaction_threshold = std::max(compaction_threshold, heap_indices.size() + capacity / 2 + 1);
                if (heap_indices.size() > tie_overflow_limit)
                    tie_overflow = true;
                break;
            }

            heap_indices.pop_back();
            on_evict(candidate);
            ++evicted_count;
        }
        evicted_keys += evicted_count;

        const size_t col_size = heap_column->size();
        if (col_size <= heap_indices.size())
            return evicted_count;

        IColumn::Filter filter(col_size, 0);
        for (size_t idx : heap_indices)
            filter[idx] = 1;

        std::vector<size_t> old_to_new(col_size);
        size_t new_idx = 0;
        for (size_t i = 0; i < col_size; ++i)
        {
            if (filter[i])
                old_to_new[i] = new_idx++;
        }

        heap_column->filter(filter);

        for (auto & idx : heap_indices)
            idx = old_to_new[idx];

        return evicted_count;
    }

private:
    static constexpr size_t max_preallocated_rows = 1ULL << 20;

    void setCapacity(size_t cap)
    {
        capacity = cap;
        const size_t half = capacity / 2;
        compaction_threshold = capacity > std::numeric_limits<size_t>::max() - half
            ? std::numeric_limits<size_t>::max()
            : capacity + half;
        chassert(compaction_threshold >= capacity);

        /// The heap may exceed `capacity` by at most `max_preallocated_rows` of
        /// boundary-tied keys before `tie_overflow` is raised; this caps the
        /// extra storage the tie guard can accumulate.
        tie_overflow_limit = capacity > std::numeric_limits<size_t>::max() - max_preallocated_rows
            ? std::numeric_limits<size_t>::max()
            : capacity + max_preallocated_rows;
    }

    size_t reserveHint() const
    {
        const size_t hint = compaction_threshold >= max_preallocated_rows ? max_preallocated_rows : compaction_threshold + 1;
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

    /// `source_data` is reinterpreted from the hash key type (always unsigned) to
    /// the actual column type — safe because they share size and bit layout.
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
    size_t compaction_threshold = 0;
    size_t tie_overflow_limit = 0;  /// heap size past which `tie_overflow` is raised
};

}
