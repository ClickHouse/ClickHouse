#pragma once
#include <algorithm>
#include <vector>

#include <base/defines.h>
#include <Common/assert_cast.h>
#include <Core/CompareHelper.h>
#include <Core/TypeId.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>
#include <Columns/Collator.h>

namespace DB
{

/** A bounded heap that tracks the top-K best keys according to the query's
  * `ORDER BY` semantics.
  *
  * Supports both single-column and composite (multi-column) keys.
  * For composite keys, the heap stores a `ColumnTuple` of sub-columns
  * and performs lexicographic comparison with per-column direction,
  * NULLS direction, and collators.
  *
  * The heap itself is a plain `std::vector<size_t>` of row indices into
  * `heap_column`, manipulated through `std::push_heap` / `std::pop_heap` /
  * `std::make_heap` with a transient comparator.  The boundary element (the
  * worst kept key, which would be evicted next) sits at the front.
  *
  * Using a bare vector instead of `std::priority_queue` keeps move and
  * move-assignment defaultable: there is no comparator instance with a
  * back-pointer to the owning heap that would have to be rebuilt.
  */
struct TopKAggregationHeap
{
    /// Column holding the key values currently in the heap.
    /// For single-column keys this is a plain column; for composite keys it is a `ColumnTuple`.
    MutableColumnPtr heap_column;

    /// Per-column ORDER BY directions.
    std::vector<int> directions;

    /// Per-column NULLS/NaNs directions.
    std::vector<int> nulls_directions;

    /// Per-column collators for comparison (`nullptr` entries mean no collation for that column).
    /// For single-column keys this has exactly one element.
    std::vector<const Collator *> collators;

    /// True when the heap stores composite (multi-column) keys via `ColumnTuple`.
    bool is_composite = false;

    /// True when the heap tracks only a prefix of the GROUP BY keys (`ORDER BY`
    /// uses fewer columns than `GROUP BY`).  In this case the evicted heap value
    /// does not represent the full hash-table key, so hash-table pruning must
    /// be skipped — reading `KeyType` bytes from the prefix column would be an
    /// out-of-bounds read or match the wrong entry.
    bool is_prefix_mode = false;

    TopKAggregationHeap() = default;
    TopKAggregationHeap(const TopKAggregationHeap &) = delete;
    TopKAggregationHeap & operator=(const TopKAggregationHeap &) = delete;

    /// The heap container holds plain `size_t` indices and the comparator is
    /// constructed transiently per call, so move and move-assignment are trivial.
    TopKAggregationHeap(TopKAggregationHeap &&) noexcept = default;
    TopKAggregationHeap & operator=(TopKAggregationHeap &&) noexcept = default;

    /// Initialize the heap if not already initialized.
    /// Dispatches to the single-column or composite `init` based on `heap_key_count`.
    /// `total_group_by_keys` is the total number of `GROUP BY` key columns;
    /// when `heap_key_count < total_group_by_keys`, the heap is in prefix mode.
    void initIfNeeded(
        const ColumnRawPtrs & key_columns,
        size_t heap_key_count,
        size_t total_group_by_keys,
        size_t cap,
        const std::vector<int> & dirs,
        const std::vector<int> & null_dirs,
        const std::vector<const Collator *> & cols)
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
                null_dirs.empty() ? 1 : null_dirs[0],
                cols.empty() ? nullptr : cols[0]);
        }
        else
        {
            ColumnRawPtrs heap_cols(key_columns.begin(), key_columns.begin() + heap_key_count);
            init(heap_cols, cap, dirs, null_dirs, cols);
        }
    }

    size_t size() const { return heap_indices.size(); }
    bool empty() const { return heap_indices.empty(); }

    /// Returns true if the source key at `source_row` is worse than the current boundary
    /// (the heap root), meaning it should be skipped.
    /// Dispatches to single-column or composite comparison based on `is_composite`.
    /// Precondition: heap is non-empty. Callers guard with `size() >= top_k_keys`,
    /// and `top_k_keys >= 1` whenever the top-k path is active.
    bool shouldSkip(const ColumnRawPtrs & source_columns, size_t source_row) const
    {
        chassert(!heap_indices.empty());
        const size_t boundary = heap_indices.front();
        if (is_composite)
            return sourceAboveHeapComposite(source_columns, source_row, boundary);
        return sourceAboveHeap(*source_columns[0], source_row, boundary);
    }

    /// Whether the typed numeric fast path is available for `shouldSkipNumeric`.
    /// Resolved once at init time; false for composite keys, collated keys, or
    /// non-numeric column types.
    bool hasNumericSkipFn() const { return should_skip_numeric_fn != nullptr; }

    /// Typed fast path for single-column numeric keys (no collation).
    /// Avoids virtual `IColumn::compareAt` dispatch by reading the raw typed values directly.
    /// The actual column element type (which may differ in signedness from the hash key type)
    /// is resolved once at init time via the stored function pointer.
    /// The caller passes the raw key data as `const void *`; the function pointer
    /// reinterprets it to the correct actual type internally.
    bool shouldSkipNumeric(const void * source_data, size_t source_row) const
    {
        chassert(!heap_indices.empty());
        return should_skip_numeric_fn(*this, source_data, source_row);
    }

    /// Push a new key from `source_columns[source_row]` into the heap.
    /// Dispatches to single-column or composite insertion based on `is_composite`.
    void push(const ColumnRawPtrs & source_columns, size_t source_row)
    {
        size_t new_idx;
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

    /// Returns true when the heap has grown past the compaction threshold
    /// and needs to be trimmed back down to `capacity`.
    bool needsTrim() const
    {
        return heap_indices.size() > compaction_threshold;
    }

    /// Trim the heap back to `capacity` by popping excess elements, calling `on_evict`
    /// for each evicted element's index in `heap_column`, then compact the column to
    /// reclaim dead slots. This batches O(0.5 * capacity) pops and one column filter
    /// instead of doing a pop+erase per row.
    ///
    /// `on_evict` receives the `heap_column` index of each evicted key and is responsible
    /// for erasing it from the hash table and destroying aggregate states if needed.
    template <typename EvictCallback>
    void trimAndCompact(EvictCallback && on_evict)
    {
        /// Pop excess entries from the heap, calling the callback for each.
        /// Hash-table pruning is only safe when the heap stores the full
        /// `GROUP BY` key as a single column.  Skip when composite (multi-column
        /// heap — can't reconstruct the hash key) or when in prefix mode
        /// (heap column is narrower than the hash-table key — reading `KeyType`
        /// bytes would be an out-of-bounds read or match the wrong entry).
        const bool prune_hash_table = !is_composite && !is_prefix_mode;
        const HeapComparator cmp{this};
        while (heap_indices.size() > capacity)
        {
            std::pop_heap(heap_indices.begin(), heap_indices.end(), cmp);
            const size_t evicted = heap_indices.back();
            heap_indices.pop_back();
            if (prune_hash_table)
                on_evict(evicted);
        }

        /// Now compact the `heap_column`: filter out dead slots and remap indices.
        const size_t col_size = heap_column->size();
        const size_t heap_size = heap_indices.size();
        if (col_size <= heap_size)
            return;

        /// Build filter: mark live slots as 1, dead as 0.
        IColumn::Filter filter(col_size, 0);
        for (size_t idx : heap_indices)
            filter[idx] = 1;

        /// Compute prefix sum for index remapping: `old_to_new[old_idx] = new_idx`.
        /// Entries for dead slots are never read (only live indices are remapped below),
        /// so leaving them at the default zero is safe.
        std::vector<size_t> old_to_new(col_size);
        size_t new_idx = 0;
        for (size_t i = 0; i < col_size; ++i)
        {
            if (filter[i])
                old_to_new[i] = new_idx++;
        }

        /// Filter `heap_column` in-place, keeping only live rows.
        /// For `ColumnTuple` this filters all sub-columns and updates `column_length`.
        heap_column->filter(filter);

        /// Remap all indices in lockstep with the filter: the row formerly at
        /// `idx` now lives at `old_to_new[idx]`, so each heap entry resolves to
        /// the same physical data as before.  Because the comparator reads
        /// column data via the index, every parent/child pair compares the same
        /// values as before the compaction — the heap invariant is preserved by
        /// construction and no `std::make_heap` is needed.
        for (auto & idx : heap_indices)
            idx = old_to_new[idx];
    }

private:
    /// Initialize for a single-column key.
    void init(
        const IColumn & source_column,
        size_t cap,
        int direction,
        int nulls_direction,
        const Collator * col)
    {
        directions = {direction};
        nulls_directions = {nulls_direction};
        collators = {col};
        is_composite = false;
        capacity = cap;
        compaction_threshold = capacity + capacity / 2;  /// 1.5x capacity
        heap_column = source_column.cloneEmpty();
        /// The heap fills up to `compaction_threshold + 1` rows before each
        /// trim, so reserve once to avoid reallocation during fill.
        heap_column->reserve(compaction_threshold + 1);
        heap_indices.clear();
        heap_indices.reserve(compaction_threshold + 1);
        initNumericSkipFn();
    }

    /// Initialize for composite (multi-column) keys.
    /// The heap stores a `ColumnTuple` of cloned-empty sub-columns.
    void init(
        const ColumnRawPtrs & source_columns,
        size_t cap,
        const std::vector<int> & dirs,
        const std::vector<int> & null_dirs,
        const std::vector<const Collator *> & cols)
    {
        const size_t n = source_columns.size();
        is_composite = true;
        capacity = cap;
        compaction_threshold = capacity + capacity / 2;

        directions.assign(n, 1);
        for (size_t i = 0; i < dirs.size() && i < n; ++i)
            directions[i] = dirs[i];

        nulls_directions.assign(n, 1);
        for (size_t i = 0; i < null_dirs.size() && i < n; ++i)
            nulls_directions[i] = null_dirs[i];

        /// Pad or copy the collators vector to match the number of key columns.
        collators.assign(n, nullptr);
        for (size_t i = 0; i < cols.size() && i < n; ++i)
            collators[i] = cols[i];

        /// Build a `ColumnTuple` of cloned-empty sub-columns.
        MutableColumns sub_columns;
        sub_columns.reserve(n);
        for (const auto * col : source_columns)
            sub_columns.emplace_back(col->cloneEmpty());
        heap_column = ColumnTuple::create(std::move(sub_columns));
        /// The heap fills up to `compaction_threshold + 1` rows before each
        /// trim; `ColumnTuple::reserve` forwards to each sub-column.
        heap_column->reserve(compaction_threshold + 1);

        heap_indices.clear();
        heap_indices.reserve(compaction_threshold + 1);
        /// Composite keys never use the typed numeric fast path; reset the
        /// pointer defensively in case the heap is re-initialized after a
        /// previous single-column setup.
        should_skip_numeric_fn = nullptr;
    }

    /// Compare a row in `source_column` against a row in `heap_column` (single-column case).
    /// Returns true if source row is worse than the heap row in `ORDER BY` order.
    bool sourceAboveHeap(const IColumn & source_column, size_t source_row, size_t heap_row) const
    {
        const int cmp = compareColumns(source_column, source_row, *heap_column, heap_row, 0);
        return directions[0] * cmp > 0;
    }

    /// Compare a composite source key against a row in the heap's `ColumnTuple`.
    /// Performs lexicographic comparison with per-column `ORDER BY` semantics.
    /// Returns true if source row is worse than the heap row in `ORDER BY` order.
    bool sourceAboveHeapComposite(const ColumnRawPtrs & source_columns, size_t source_row, size_t heap_row) const
    {
        const auto & tuple = assert_cast<const ColumnTuple &>(*heap_column);
        for (size_t i = 0; i < source_columns.size(); ++i)
        {
            const int cmp = compareColumns(*source_columns[i], source_row, tuple.getColumn(i), heap_row, i);
            if (cmp != 0)
                return directions[i] * cmp > 0;
        }
        return false;  /// equal keys are not above the heap boundary
    }

    /// Lexicographic comparison of two rows within the heap's `ColumnTuple`.
    /// Returns negative if `a` should be ordered before `b`, positive if after, zero if equal.
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

    /// Compare a row in `lhs` against a row in `rhs` honoring the per-column collator
    /// and NULLS direction stored at position `column_index`.  Encapsulates the
    /// `compareAt` / `compareAtWithCollation` switch used in three places.
    int compareColumns(const IColumn & lhs, size_t lhs_row, const IColumn & rhs, size_t rhs_row, size_t column_index) const
    {
        if (collators[column_index])
            return lhs.compareAtWithCollation(lhs_row, rhs_row, rhs, nulls_directions[column_index], *collators[column_index]);
        return lhs.compareAt(lhs_row, rhs_row, rhs, nulls_directions[column_index]);
    }

    /// Comparator for the heap algorithms.  The worst kept element sits at the
    /// front, so we return true when `a` should be below `b` in `ORDER BY` order.
    /// The struct stores only a back-pointer; it is constructed transiently at
    /// each `std::*_heap` call and never persisted as a member of the heap.
    struct HeapComparator
    {
        const TopKAggregationHeap * owner;

        bool operator()(size_t a, size_t b) const
        {
            if (owner->is_composite)
                return owner->compareHeapRowsComposite(a, b) < 0;

            const int cmp = owner->compareColumns(*owner->heap_column, a, *owner->heap_column, b, 0);
            return owner->directions[0] * cmp < 0;
        }
    };

    /// Type-erased function pointer for the numeric skip fast path.
    /// Resolved once at init time based on `heap_column->getDataType()`.
    /// Takes `(self, raw_key_data, row_index)` and returns true if the row should be skipped.
    using ShouldSkipNumericFn = bool (*)(const TopKAggregationHeap &, const void *, size_t);
    ShouldSkipNumericFn should_skip_numeric_fn = nullptr;

    /// Typed implementation of the numeric skip comparison.
    /// `ActualKeyType` is the real column element type (e.g., `Int32` for `RegionID`).
    /// The source data pointer is reinterpreted from the hash key type (always unsigned)
    /// to the actual column type — safe because they have the same size and bit layout.
    template <typename ActualKeyType>
    static bool shouldSkipNumericImpl(const TopKAggregationHeap & self, const void * source_data, size_t source_row)
    {
        const auto * src = reinterpret_cast<const ActualKeyType *>(source_data);
        const auto & heap_data = assert_cast<const ColumnVector<ActualKeyType> &>(*self.heap_column).getData();
        const size_t boundary_row = self.heap_indices.front();
        return self.directions[0] * CompareHelper<ActualKeyType>::compare(src[source_row], heap_data[boundary_row], self.nulls_directions[0]) > 0;
    }

    /// Resolve the typed numeric skip function pointer based on the actual column type.
    /// Called once at init time from the single-column `init`; `collators` has
    /// exactly one entry at that point.
    void initNumericSkipFn()
    {
        should_skip_numeric_fn = nullptr;
        if (collators[0])
            return;

        switch (heap_column->getDataType())
        {
            case TypeIndex::UInt8:     should_skip_numeric_fn = &shouldSkipNumericImpl<UInt8>; break;
            case TypeIndex::UInt16:    should_skip_numeric_fn = &shouldSkipNumericImpl<UInt16>; break;
            case TypeIndex::UInt32:    should_skip_numeric_fn = &shouldSkipNumericImpl<UInt32>; break;
            case TypeIndex::UInt64:    should_skip_numeric_fn = &shouldSkipNumericImpl<UInt64>; break;
            case TypeIndex::Int8:      should_skip_numeric_fn = &shouldSkipNumericImpl<Int8>; break;
            case TypeIndex::Int16:     should_skip_numeric_fn = &shouldSkipNumericImpl<Int16>; break;
            case TypeIndex::Int32:     should_skip_numeric_fn = &shouldSkipNumericImpl<Int32>; break;
            case TypeIndex::Int64:     should_skip_numeric_fn = &shouldSkipNumericImpl<Int64>; break;
            case TypeIndex::Float32:   should_skip_numeric_fn = &shouldSkipNumericImpl<Float32>; break;
            case TypeIndex::Float64:   should_skip_numeric_fn = &shouldSkipNumericImpl<Float64>; break;
            case TypeIndex::Date:      should_skip_numeric_fn = &shouldSkipNumericImpl<UInt16>; break;
            case TypeIndex::Date32:    should_skip_numeric_fn = &shouldSkipNumericImpl<Int32>; break;
            case TypeIndex::DateTime:  should_skip_numeric_fn = &shouldSkipNumericImpl<UInt32>; break;
            case TypeIndex::Enum8:     should_skip_numeric_fn = &shouldSkipNumericImpl<Int8>; break;
            case TypeIndex::Enum16:    should_skip_numeric_fn = &shouldSkipNumericImpl<Int16>; break;
            case TypeIndex::IPv4:      should_skip_numeric_fn = &shouldSkipNumericImpl<UInt32>; break;
            default: break;
        }
    }

    /// Heap of row indices into `heap_column`, maintained via `std::*_heap`.
    /// `heap_indices.front()` is the worst (root) element under the comparator.
    std::vector<size_t> heap_indices;
    size_t capacity = 0;              /// target heap size (= `top_k_keys`)
    size_t compaction_threshold = 0;  /// heap size at which to trigger trim+compact (1.5x capacity)
};

}
