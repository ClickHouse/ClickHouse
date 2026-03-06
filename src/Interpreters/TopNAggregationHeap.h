#pragma once
#include <algorithm>
#include <queue>
#include <vector>

#include <base/defines.h>
#include <Common/assert_cast.h>
#include <Common/PODArray.h>

#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>
#include <Columns/Collator.h>

namespace DB
{

/** A bounded heap that tracks the top-N smallest (ascending) keys
  * seen during aggregation.
  *
  * Only ascending sort order is supported.
  *
  * Supports both single-column and composite (multi-column) keys.
  * For composite keys, the heap stores a ColumnTuple of sub-columns
  * and performs lexicographic comparison with per-column collators.
  *
  * Uses std::priority_queue over row indices into `heap_column`.
  * The boundary element (the largest kept key that would be evicted next) is at the top.
  */
struct TopNAggregationHeap
{
    /// Column holding the key values currently in the heap.
    /// For single-column keys this is a plain column; for composite keys it is a ColumnTuple.
    MutableColumnPtr heap_column;

    int nan_direction_hint = 1;  /// NULLs/NaNs go last by default

    /// Per-column collators for comparison (nullptr entries mean no collation for that column).
    /// For single-column keys this has exactly one element.
    std::vector<const Collator *> collators;

    /// True when the heap stores composite (multi-column) keys via ColumnTuple.
    bool is_composite = false;

    TopNAggregationHeap() = default;
    TopNAggregationHeap(const TopNAggregationHeap &) = delete;
    TopNAggregationHeap & operator=(const TopNAggregationHeap &) = delete;

    TopNAggregationHeap(TopNAggregationHeap && other) noexcept
        : heap_column(std::move(other.heap_column))
        , nan_direction_hint(other.nan_direction_hint)
        , collators(std::move(other.collators))
        , is_composite(other.is_composite)
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

    TopNAggregationHeap & operator=(TopNAggregationHeap && other) noexcept
    {
        if (this != &other)
        {
            heap_column = std::move(other.heap_column);
            nan_direction_hint = other.nan_direction_hint;
            collators = std::move(other.collators);
            is_composite = other.is_composite;
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
    void init(const IColumn & source_column, size_t cap, const Collator * col = nullptr)
    {
        collators = {col};
        is_composite = false;
        capacity = cap;
        /// NaN/NULL should be greater (direction_hint=1) so they get evicted first in ASC order.
        nan_direction_hint = 1;
        heap_column = source_column.cloneEmpty();
        heap = std::priority_queue<size_t, std::vector<size_t>, HeapComparator>(HeapComparator{this});
        compaction_threshold = capacity + capacity / 2;  /// 1.5x capacity
    }

    /// Initialize for composite (multi-column) keys.
    /// The heap stores a ColumnTuple of cloned-empty sub-columns.
    void init(const ColumnRawPtrs & source_columns, size_t cap, const std::vector<const Collator *> & cols)
    {
        is_composite = true;
        capacity = cap;
        nan_direction_hint = 1;

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
    void initIfNeeded(
        const ColumnRawPtrs & key_columns,
        size_t heap_key_count,
        size_t cap,
        const std::vector<const Collator *> & cols)
    {
        if (heap_column)
            return;

        if (heap_key_count == 1)
            init(*key_columns[0], cap, cols.empty() ? nullptr : cols[0]);
        else
        {
            ColumnRawPtrs heap_cols(key_columns.begin(), key_columns.begin() + heap_key_count);
            init(heap_cols, cap, cols);
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
            if (!is_composite)
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
    /// Returns true if source row is greater than the heap row (i.e. worse for ASC order).
    bool sourceAboveHeap(const IColumn & source_column, size_t source_row, size_t heap_row) const
    {
        int cmp;
        if (collators[0])
            cmp = source_column.compareAtWithCollation(source_row, heap_row, *heap_column, nan_direction_hint, *collators[0]);
        else
            cmp = source_column.compareAt(source_row, heap_row, *heap_column, nan_direction_hint);
        return cmp > 0;
    }

    /// Compare a composite source key against a row in the heap's ColumnTuple.
    /// Performs lexicographic comparison with per-column collators.
    /// Returns true if source row is greater than the heap row (i.e. worse for ASC order).
    bool sourceAboveHeapComposite(const ColumnRawPtrs & source_columns, size_t source_row, size_t heap_row) const
    {
        const auto & tuple = assert_cast<const ColumnTuple &>(*heap_column);
        for (size_t i = 0; i < source_columns.size(); ++i)
        {
            int cmp;
            if (collators[i])
                cmp = source_columns[i]->compareAtWithCollation(source_row, heap_row, tuple.getColumn(i), nan_direction_hint, *collators[i]);
            else
                cmp = source_columns[i]->compareAt(source_row, heap_row, tuple.getColumn(i), nan_direction_hint);
            if (cmp != 0)
                return cmp > 0;
        }
        return false;  /// equal keys are not above the heap boundary
    }

    /// Lexicographic comparison of two rows within the heap's ColumnTuple.
    /// Returns negative if a < b, positive if a > b, zero if equal.
    int compareHeapRowsComposite(size_t a, size_t b) const
    {
        const auto & tuple = assert_cast<const ColumnTuple &>(*heap_column);
        for (size_t i = 0; i < collators.size(); ++i)
        {
            const auto & col = tuple.getColumn(i);
            int cmp;
            if (collators[i])
                cmp = col.compareAtWithCollation(a, b, col, nan_direction_hint, *collators[i]);
            else
                cmp = col.compareAt(a, b, col, nan_direction_hint);
            if (cmp != 0)
                return cmp;
        }
        return 0;
    }

    /// Comparator for the priority queue. The "greatest" element sits at the top,
    /// so we return true when a should be below b.
    /// For ascending (max-heap): root = max boundary, so a is below b when a < b.
    struct HeapComparator
    {
        const TopNAggregationHeap * owner;

        bool operator()(size_t a, size_t b) const
        {
            if (owner->is_composite)
                return owner->compareHeapRowsComposite(a, b) < 0;

            int cmp;
            if (owner->collators[0])
                cmp = owner->heap_column->compareAtWithCollation(a, b, *owner->heap_column, owner->nan_direction_hint, *owner->collators[0]);
            else
                cmp = owner->heap_column->compareAt(a, b, *owner->heap_column, owner->nan_direction_hint);
            return cmp < 0;
        }
    };

    std::priority_queue<size_t, std::vector<size_t>, HeapComparator> heap;
    size_t capacity = 0;              /// target heap size (= top_n_keys)
    size_t compaction_threshold = 0;  /// heap size at which to trigger trim+compact (1.5x capacity)
};

}
