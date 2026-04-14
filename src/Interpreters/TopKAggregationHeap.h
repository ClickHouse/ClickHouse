#pragma once
#include <algorithm>
#include <queue>
#include <vector>

#include <base/defines.h>
#include <Common/assert_cast.h>
#include <Common/PODArray.h>
#include <Core/CompareHelper.h>
#include <Core/TypeId.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>
#include <Columns/Collator.h>

namespace DB
{

/** A bounded heap that tracks the top-N best keys according to the query's
  * `ORDER BY` semantics.
  *
  * Supports both single-column and composite (multi-column) keys.
  * For composite keys, the heap stores a ColumnTuple of sub-columns
  * and performs lexicographic comparison with per-column direction,
  * NULLS direction, and collators.
  *
  * Uses std::priority_queue over row indices into `heap_column`.
  * The boundary element (the worst kept key that would be evicted next) is at the top.
  */
struct TopKAggregationHeap
{
    /// Column holding the key values currently in the heap.
    /// For single-column keys this is a plain column; for composite keys it is a ColumnTuple.
    MutableColumnPtr heap_column;

    /// Per-column ORDER BY directions.
    std::vector<int> directions;

    /// Per-column NULLS/NaNs directions.
    std::vector<int> nulls_directions;

    /// Per-column collators for comparison (nullptr entries mean no collation for that column).
    /// For single-column keys this has exactly one element.
    std::vector<const Collator *> collators;

    /// True when the heap stores composite (multi-column) keys via ColumnTuple.
    bool is_composite = false;

    /// True when the heap tracks only a prefix of the GROUP BY keys (ORDER BY
    /// uses fewer columns than GROUP BY).  In this case the evicted heap value
    /// does not represent the full hash-table key, so hash-table pruning must
    /// be skipped — reading KeyType bytes from the prefix column would be an
    /// out-of-bounds read or match the wrong entry.
    bool is_prefix_mode = false;

    TopKAggregationHeap() = default;
    TopKAggregationHeap(const TopKAggregationHeap &) = delete;
    TopKAggregationHeap & operator=(const TopKAggregationHeap &) = delete;

    TopKAggregationHeap(TopKAggregationHeap && other) noexcept
        : heap_column(std::move(other.heap_column))
        , directions(std::move(other.directions))
        , nulls_directions(std::move(other.nulls_directions))
        , collators(std::move(other.collators))
        , is_composite(other.is_composite)
        , is_prefix_mode(other.is_prefix_mode)
        , should_skip_numeric_fn(other.should_skip_numeric_fn)
        , capacity(other.capacity)
        , compaction_threshold(other.compaction_threshold)
    {
        if (heap_column)
        {
            /// Reconstruct the priority queue with the correct 'this' pointer in the comparator.
            /// Extract the underlying container from the old priority_queue via a helper struct
            /// that inherits from it and exposes the protected member `c`.
            struct HeapAccess : std::priority_queue<size_t, std::vector<size_t>, HeapComparator>
            {
                using PQ = std::priority_queue<size_t, std::vector<size_t>, HeapComparator>;
                explicit HeapAccess(PQ && pq) : PQ(std::move(pq)) {}
                std::vector<size_t> && takeContainer() { return std::move(this->c); }
            };
            HeapAccess other_access(std::move(other.heap));
            heap = std::priority_queue<size_t, std::vector<size_t>, HeapComparator>(
                HeapComparator{this}, other_access.takeContainer());
        }
    }

    TopKAggregationHeap & operator=(TopKAggregationHeap && other) noexcept
    {
        if (this != &other)
        {
            heap_column = std::move(other.heap_column);
            directions = std::move(other.directions);
            nulls_directions = std::move(other.nulls_directions);
            collators = std::move(other.collators);
            is_composite = other.is_composite;
            is_prefix_mode = other.is_prefix_mode;
            should_skip_numeric_fn = other.should_skip_numeric_fn;
            capacity = other.capacity;
            compaction_threshold = other.compaction_threshold;
            if (heap_column)
            {
                struct HeapAccess : std::priority_queue<size_t, std::vector<size_t>, HeapComparator>
                {
                    using PQ = std::priority_queue<size_t, std::vector<size_t>, HeapComparator>;
                    explicit HeapAccess(PQ && pq) : PQ(std::move(pq)) {}
                    std::vector<size_t> && takeContainer() { return std::move(this->c); }
                };
                HeapAccess other_access(std::move(other.heap));
                heap = std::priority_queue<size_t, std::vector<size_t>, HeapComparator>(
                    HeapComparator{this}, std::move(other_access.takeContainer()));
            }
            else
            {
                heap = {};
            }
        }
        return *this;
    }

    /// Initialize for a single-column key.
    void init(
        const IColumn & source_column,
        size_t cap,
        int direction = 1,
        int nulls_direction = 1,
        const Collator * col = nullptr)
    {
        directions = {direction};
        nulls_directions = {nulls_direction};
        collators = {col};
        is_composite = false;
        capacity = cap;
        heap_column = source_column.cloneEmpty();
        heap = std::priority_queue<size_t, std::vector<size_t>, HeapComparator>(HeapComparator{this});
        compaction_threshold = capacity + capacity / 2;  /// 1.5x capacity
        initNumericSkipFn();
    }

    /// Initialize for composite (multi-column) keys.
    /// The heap stores a ColumnTuple of cloned-empty sub-columns.
    void init(
        const ColumnRawPtrs & source_columns,
        size_t cap,
        const std::vector<int> & dirs,
        const std::vector<int> & null_dirs,
        const std::vector<const Collator *> & cols)
    {
        is_composite = true;
        capacity = cap;

        directions.resize(source_columns.size(), 1);
        for (size_t i = 0; i < dirs.size() && i < source_columns.size(); ++i)
            directions[i] = dirs[i];

        nulls_directions.resize(source_columns.size(), 1);
        for (size_t i = 0; i < null_dirs.size() && i < source_columns.size(); ++i)
            nulls_directions[i] = null_dirs[i];

        /// Pad or copy the collators vector to match the number of key columns.
        collators.resize(source_columns.size(), nullptr);
        for (size_t i = 0; i < cols.size() && i < source_columns.size(); ++i)
            collators[i] = cols[i];

        /// Build a ColumnTuple of cloned-empty sub-columns.
        MutableColumns sub_columns;
        sub_columns.reserve(source_columns.size());
        for (const auto * col : source_columns)
            sub_columns.emplace_back(col->cloneEmpty());
        heap_column = ColumnTuple::create(std::move(sub_columns));

        heap = std::priority_queue<size_t, std::vector<size_t>, HeapComparator>(HeapComparator{this});
        compaction_threshold = capacity + capacity / 2;
    }

    /// Initialize the heap if not already initialized.
    /// Dispatches to the single-column or composite init based on `heap_key_count`.
    /// `total_group_by_keys` is the total number of GROUP BY key columns;
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

    size_t size() const { return heap.size(); }
    bool empty() const { return heap.empty(); }

    /// Returns true if the source key at source_row is worse than the current boundary
    /// (the heap root), meaning it should be skipped.
    /// Dispatches to single-column or composite comparison based on `is_composite`.
    bool shouldSkip(const ColumnRawPtrs & source_columns, size_t source_row) const
    {
        if (is_composite)
            return sourceAboveHeapComposite(source_columns, source_row, heap.top());
        return sourceAboveHeap(*source_columns[0], source_row, heap.top());
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
        return should_skip_numeric_fn(*this, source_data, source_row);
    }

    /// Push a new key from source_columns[source_row] into the heap.
    /// Dispatches to single-column or composite insertion based on `is_composite`.
    void push(const ColumnRawPtrs & source_columns, size_t source_row)
    {
        if (is_composite)
        {
            auto & tuple = assert_cast<ColumnTuple &>(*heap_column);
            size_t new_idx = tuple.size();
            for (size_t i = 0; i < source_columns.size(); ++i)
                tuple.getColumn(i).insertFrom(*source_columns[i], source_row);
            tuple.addSize(1);
            heap.push(new_idx);
        }
        else
        {
            size_t new_idx = heap_column->size();
            heap_column->insertFrom(*source_columns[0], source_row);
            heap.push(new_idx);
        }
    }

    /// Returns true when the heap has grown past the compaction threshold
    /// and needs to be trimmed back down to `capacity`.
    bool needsTrim() const
    {
        return compaction_threshold > 0 && heap.size() > compaction_threshold;
    }

    /// Trim the heap back to `capacity` by popping excess elements, calling `on_evict`
    /// for each evicted element's index in heap_column, then compact the column to
    /// reclaim dead slots. This batches O(0.5 * capacity) pops and one column filter
    /// instead of doing a pop+erase per row.
    ///
    /// `on_evict` receives the heap_column index of each evicted key and is responsible
    /// for erasing it from the hash table and destroying aggregate states if needed.
    template <typename EvictCallback>
    void trimAndCompact(EvictCallback && on_evict)
    {
        /// Pop excess entries from the heap, calling the callback for each.
        /// For composite (multi-column) keys, reconstructing the hash table key from the
        /// evicted ColumnTuple row is too complex and not worth the overhead.
        /// Just trim the heap without pruning — the hash table may keep some extra entries,
        /// but the heap still bounds the final result correctly.
        while (heap.size() > capacity)
        {
            size_t evicted = heap.top();
            heap.pop();
            /// Hash-table pruning is only safe when the heap stores the full
            /// GROUP BY key as a single column.  Skip when composite (multi-column
            /// heap — can't reconstruct the hash key) or when in prefix mode
            /// (heap column is narrower than the hash-table key — reading KeyType
            /// bytes would be an out-of-bounds read or match the wrong entry).
            if (!is_composite && !is_prefix_mode)
                on_evict(evicted);
        }

        /// Now compact the heap_column: filter out dead slots and remap indices.
        size_t col_size = heap_column->size();
        size_t heap_size = heap.size();
        if (col_size <= heap_size)
            return;

        /// Extract the underlying container from the priority queue.
        struct HeapAccess : std::priority_queue<size_t, std::vector<size_t>, HeapComparator>
        {
            using PQ = std::priority_queue<size_t, std::vector<size_t>, HeapComparator>;
            explicit HeapAccess(PQ && pq) : PQ(std::move(pq)) {}
            std::vector<size_t> & getContainer() { return this->c; }
        };

        HeapAccess access(std::move(heap));
        auto & indices = access.getContainer();

        /// Build filter: mark live slots as 1, dead as 0.
        IColumn::Filter filter(col_size, 0);
        /// old_to_new[old_idx] = new_idx after filtering.
        std::vector<size_t> old_to_new(col_size);
        for (size_t idx : indices)
            filter[idx] = 1;

        /// Compute prefix sum for index remapping.
        size_t new_idx = 0;
        for (size_t i = 0; i < col_size; ++i)
        {
            if (filter[i])
                old_to_new[i] = new_idx++;
        }

        /// Filter heap_column in-place, keeping only live rows.
        /// For ColumnTuple this filters all sub-columns and updates column_length.
        heap_column->filter(filter);

        /// Remap all indices in the priority queue container.
        for (auto & idx : indices)
            idx = old_to_new[idx];

        /// Rebuild the priority queue with the updated indices and correct comparator.
        /// The constructor calls std::make_heap on the container.
        heap = std::priority_queue<size_t, std::vector<size_t>, HeapComparator>(
            HeapComparator{this}, std::move(indices));
    }

private:
    /// Compare a row in source_column against a row in heap_column (single-column case).
    /// Returns true if source row is worse than the heap row in ORDER BY order.
    bool sourceAboveHeap(const IColumn & source_column, size_t source_row, size_t heap_row) const
    {
        int cmp;
        if (collators[0])
            cmp = source_column.compareAtWithCollation(source_row, heap_row, *heap_column, nulls_directions[0], *collators[0]);
        else
            cmp = source_column.compareAt(source_row, heap_row, *heap_column, nulls_directions[0]);
        return directions[0] * cmp > 0;
    }

    /// Compare a composite source key against a row in the heap's ColumnTuple.
    /// Performs lexicographic comparison with per-column ORDER BY semantics.
    /// Returns true if source row is worse than the heap row in ORDER BY order.
    bool sourceAboveHeapComposite(const ColumnRawPtrs & source_columns, size_t source_row, size_t heap_row) const
    {
        const auto & tuple = assert_cast<const ColumnTuple &>(*heap_column);
        for (size_t i = 0; i < source_columns.size(); ++i)
        {
            int cmp;
            if (collators[i])
                cmp = source_columns[i]->compareAtWithCollation(source_row, heap_row, tuple.getColumn(i), nulls_directions[i], *collators[i]);
            else
                cmp = source_columns[i]->compareAt(source_row, heap_row, tuple.getColumn(i), nulls_directions[i]);
            if (cmp != 0)
                return directions[i] * cmp > 0;
        }
        return false;  /// equal keys are not above the heap boundary
    }

    /// Lexicographic comparison of two rows within the heap's ColumnTuple.
    /// Returns negative if `a` should be ordered before `b`, positive if after, zero if equal.
    int compareHeapRowsComposite(size_t a, size_t b) const
    {
        const auto & tuple = assert_cast<const ColumnTuple &>(*heap_column);
        for (size_t i = 0; i < collators.size(); ++i)
        {
            const auto & col = tuple.getColumn(i);
            int cmp;
            if (collators[i])
                cmp = col.compareAtWithCollation(a, b, col, nulls_directions[i], *collators[i]);
            else
                cmp = col.compareAt(a, b, col, nulls_directions[i]);
            if (cmp != 0)
                return directions[i] * cmp;
        }
        return 0;
    }

    /// Comparator for the priority queue. The worst kept element sits at the top,
    /// so we return true when `a` should be below `b` in ORDER BY order.
    struct HeapComparator
    {
        const TopKAggregationHeap * owner;

        bool operator()(size_t a, size_t b) const
        {
            if (owner->is_composite)
                return owner->compareHeapRowsComposite(a, b) < 0;

            int cmp;
            if (owner->collators[0])
                cmp = owner->heap_column->compareAtWithCollation(a, b, *owner->heap_column, owner->nulls_directions[0], *owner->collators[0]);
            else
                cmp = owner->heap_column->compareAt(a, b, *owner->heap_column, owner->nulls_directions[0]);
            return owner->directions[0] * cmp < 0;
        }
    };

    /// Type-erased function pointer for the numeric skip fast path.
    /// Resolved once at init time based on `heap_column->getDataType()`.
    /// Takes (self, raw_key_data, row_index) and returns true if the row should be skipped.
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
        return self.directions[0] * CompareHelper<ActualKeyType>::compare(src[source_row], heap_data[self.heap.top()], self.nulls_directions[0]) > 0;
    }

    /// Resolve the typed numeric skip function pointer based on the actual column type.
    /// Called once at init time for single-column, non-collated keys.
    void initNumericSkipFn()
    {
        if (is_composite || (!collators.empty() && collators[0]))
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

    std::priority_queue<size_t, std::vector<size_t>, HeapComparator> heap;
    size_t capacity = 0;              /// target heap size (= top_k_keys)
    size_t compaction_threshold = 0;  /// heap size at which to trigger trim+compact (1.5x capacity)
};

}
