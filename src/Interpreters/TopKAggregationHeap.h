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
    /// uses fewer columns than `GROUP BY`).  An evicted prefix does not identify
    /// a single hash-table entry — every entry sharing the prefix matches — so
    /// hash-table pruning must be skipped in this mode.
    bool is_prefix_mode = false;

    /// A heap that has observed many rows without ever skipping one or
    /// evicting a key is pure overhead — e.g. when the number of distinct
    /// keys does not exceed the LIMIT, every row pays the boundary comparison
    /// and none is ever rejected.  In that state the hash table holds every
    /// group with a complete aggregate state, exactly as if the heap had been
    /// off, so it is safe to freeze it: the caller stops consulting and
    /// feeding the heap (routing to the `top_k = false` code path), which
    /// preserves the invariant for the rest of the aggregation.
    bool frozen = false;
    UInt64 observed_rows = 0;
    UInt64 skipped_rows = 0;
    UInt64 evicted_keys = 0;

    static constexpr UInt64 freeze_observation_threshold = 256 * 1024;

    bool shouldFreeze() const
    {
        return !frozen
            && heap_column
            && observed_rows >= freeze_observation_threshold
            && skipped_rows == 0
            && evicted_keys == 0
            /// Only freeze when full: a heap below capacity does not run
            /// boundary checks, and keeping it allows the optimization to
            /// engage if the key cardinality grows later.
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

    /// Returns true if the source key at `source_row` is worse than the current boundary
    /// (the heap root), meaning it should be skipped.
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

    /// Same as above, but for callers that have a raw typed pointer to the
    /// source column data (`AggregationMethodOneNumber`).  When the heap was
    /// initialised with a known numeric column type, this bypasses the virtual
    /// `IColumn::compareAt` dispatch.  Otherwise (collated, composite, or
    /// unknown numeric type) it transparently falls back to the virtual path;
    /// `source_typed_data` may be `nullptr` to opt out.
    bool shouldSkip(const void * source_typed_data, const ColumnRawPtrs & source_columns, size_t source_row) const
    {
        if (should_skip_numeric_fn && source_typed_data)
        {
            chassert(!heap_indices.empty());
            return should_skip_numeric_fn(*this, source_typed_data, source_row);
        }
        return shouldSkip(source_columns, source_row);
    }

    /// Batched variant of the typed-numeric skip check: fills
    /// `skip_bitmap[begin..end)` against the current boundary with a single
    /// indirect call instead of one per row.  Decisions stay valid under
    /// mid-block boundary movement because the boundary only improves (a push
    /// displaces it with a better key, a trim pops the worst entries): a row
    /// marked skippable remains skippable, and a row marked admissible is at
    /// worst admitted unnecessarily.  Returns nullptr when no typed fast path
    /// is available; the caller falls back to the per-row check.
    const UInt8 * fillSkipBitmap(const void * source_typed_data, size_t begin, size_t end)
    {
        if (!fill_skip_bitmap_fn || !source_typed_data)
            return nullptr;
        chassert(!heap_indices.empty());
        skip_bitmap.resize(end);
        fill_skip_bitmap_fn(*this, source_typed_data, begin, end, skip_bitmap.data());
        return skip_bitmap.data();
    }

    /// Push a new key from `source_columns[source_row]` into the heap.
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

    /// Trim the heap back to `capacity` by popping excess elements, calling `on_evict`
    /// for each evicted element's index in `heap_column`, then compact the column to
    /// reclaim dead slots. This batches O(0.5 * capacity) pops and one column filter
    /// instead of doing a pop+erase per row.
    ///
    /// `on_evict` receives the `heap_column` index of each evicted key and is responsible
    /// for erasing it from the hash table and destroying aggregate states if needed.
    /// The caller decides whether pruning is possible (see
    /// `Aggregator::trimHeapAndPruneHashTable`) and passes a no-op callback otherwise.
    /// Returns the number of evicted keys.
    template <typename EvictCallback>
    size_t trimAndCompact(EvictCallback && on_evict)
    {
        size_t evicted_count = 0;
        const HeapComparator cmp{this};
        while (heap_indices.size() > capacity)
        {
            std::pop_heap(heap_indices.begin(), heap_indices.end(), cmp);
            const size_t candidate = heap_indices.back();

            /// Never evict a key that ties with the boundary.  Distinct keys can
            /// compare equal (-0.0/+0.0, NaN payloads, collation), and an equal
            /// key is a legitimate member of the top-K result - the downstream
            /// LIMIT may break the tie either way.  Evicting it would destroy
            /// its aggregate state while later rows for it are re-admitted
            /// (they also tie with the boundary), surfacing an incomplete
            /// group.  Everything still in the heap is equal or better than the
            /// front, so stop trimming and retry once the heap grows further.
            const size_t new_front = heap_indices.front();
            if (!cmp(candidate, new_front) && !cmp(new_front, candidate))
            {
                std::push_heap(heap_indices.begin(), heap_indices.end(), cmp);
                compaction_threshold = std::max(compaction_threshold, heap_indices.size() + capacity / 2 + 1);
                break;
            }

            heap_indices.pop_back();
            on_evict(candidate);
            ++evicted_count;
        }
        evicted_keys += evicted_count;

        /// Compact the `heap_column`: filter out dead slots and remap indices.
        const size_t col_size = heap_column->size();
        if (col_size <= heap_indices.size())
            return evicted_count;

        IColumn::Filter filter(col_size, 0);
        for (size_t idx : heap_indices)
            filter[idx] = 1;

        /// `old_to_new[old_idx] = new_idx`; entries for dead slots are never read.
        std::vector<size_t> old_to_new(col_size);
        size_t new_idx = 0;
        for (size_t i = 0; i < col_size; ++i)
        {
            if (filter[i])
                old_to_new[i] = new_idx++;
        }

        heap_column->filter(filter);

        /// Each remapped index resolves to the same physical data as before, and
        /// the comparator reads column data through the index, so the heap
        /// invariant is preserved by construction — no `std::make_heap` needed.
        for (auto & idx : heap_indices)
            idx = old_to_new[idx];

        return evicted_count;
    }

private:
    /// `capacity` (= `top_k_keys`) is `limit + offset` and can approach the full
    /// `size_t` range, so cap the eager reservation; the column grows on demand.
    static constexpr size_t max_preallocated_rows = 1ULL << 20;  /// 1 Mi rows

    /// Set `capacity` and derive `compaction_threshold` as 1.5x it, without overflow.
    void setCapacity(size_t cap)
    {
        capacity = cap;
        const size_t half = capacity / 2;
        compaction_threshold = capacity > std::numeric_limits<size_t>::max() - half
            ? std::numeric_limits<size_t>::max()
            : capacity + half;
        chassert(compaction_threshold >= capacity);
    }

    size_t reserveHint() const
    {
        const size_t hint = compaction_threshold >= max_preallocated_rows ? max_preallocated_rows : compaction_threshold + 1;
        chassert(hint <= max_preallocated_rows);
        return hint;
    }

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
        setCapacity(cap);
        heap_column = source_column.cloneEmpty();
        const size_t reserve_hint = reserveHint();
        heap_column->reserve(reserve_hint);
        heap_indices.clear();
        heap_indices.reserve(reserve_hint);
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
        setCapacity(cap);

        directions.assign(n, 1);
        for (size_t i = 0; i < dirs.size() && i < n; ++i)
            directions[i] = dirs[i];

        nulls_directions.assign(n, 1);
        for (size_t i = 0; i < null_dirs.size() && i < n; ++i)
            nulls_directions[i] = null_dirs[i];

        collators.assign(n, nullptr);
        for (size_t i = 0; i < cols.size() && i < n; ++i)
            collators[i] = cols[i];

        MutableColumns sub_columns;
        sub_columns.reserve(n);
        for (const auto * col : source_columns)
            sub_columns.emplace_back(col->cloneEmpty());
        heap_column = ColumnTuple::create(std::move(sub_columns));
        const size_t reserve_hint = reserveHint();
        heap_column->reserve(reserve_hint);

        heap_indices.clear();
        heap_indices.reserve(reserve_hint);
        /// Composite keys never use the typed numeric fast paths.
        should_skip_numeric_fn = nullptr;
        numeric_cmp_fn = nullptr;
        fill_skip_bitmap_fn = nullptr;
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
    ///
    /// When a typed numeric fast path is installed at init time (single-column,
    /// non-collated, plain numeric), `numeric_cmp_fn` is dispatched directly,
    /// avoiding the virtual `IColumn::compareAt` call on every comparison.
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

    /// Type-erased function pointer for the numeric skip fast path.
    /// Resolved once at init time based on `heap_column->getDataType()`.
    /// Takes `(self, raw_key_data, row_index)` and returns true if the row should be skipped.
    using ShouldSkipNumericFn = bool (*)(const TopKAggregationHeap &, const void *, size_t);
    ShouldSkipNumericFn should_skip_numeric_fn = nullptr;

    /// Type-erased function pointer for the numeric heap-comparator fast path.
    /// Resolved alongside `should_skip_numeric_fn` for the same set of types.
    /// Returns true when row `a` is worse than row `b` in `ORDER BY` order —
    /// matching `HeapComparator::operator()` semantics.
    using NumericCmpFn = bool (*)(const TopKAggregationHeap &, size_t, size_t);
    NumericCmpFn numeric_cmp_fn = nullptr;

    /// Type-erased function pointer for the batched skip check; resolved
    /// alongside the other numeric fast paths.
    using FillSkipBitmapFn = void (*)(const TopKAggregationHeap &, const void *, size_t, size_t, UInt8 *);
    FillSkipBitmapFn fill_skip_bitmap_fn = nullptr;

    /// Reused across batches by `fillSkipBitmap`.
    std::vector<UInt8> skip_bitmap;

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

    /// Typed implementation of the heap comparator for single-column numeric keys.
    /// Reads the heap column's raw data directly, skipping the virtual `compareAt`.
    template <typename ActualKeyType>
    static bool heapCompareNumericImpl(const TopKAggregationHeap & self, size_t a, size_t b)
    {
        const auto & data = assert_cast<const ColumnVector<ActualKeyType> &>(*self.heap_column).getData();
        return self.directions[0] * CompareHelper<ActualKeyType>::compare(data[a], data[b], self.nulls_directions[0]) < 0;
    }

    /// Typed implementation of the batched skip check.  The boundary is loaded
    /// once and the loop is free of indirect calls, so the compiler can keep
    /// it tight (and vectorize it for types with branchless comparison).
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

    /// Install the numeric fast paths for the given column element type.
    template <typename ActualKeyType>
    void resolveNumericFastPath()
    {
        should_skip_numeric_fn = &shouldSkipNumericImpl<ActualKeyType>;
        numeric_cmp_fn = &heapCompareNumericImpl<ActualKeyType>;
        fill_skip_bitmap_fn = &fillSkipBitmapImpl<ActualKeyType>;
    }

    /// Resolve the typed numeric fast paths for `shouldSkip` and the heap comparator.
    /// Called once at init time from the single-column `init`; `collators` has
    /// exactly one entry at that point.
    void initNumericSkipFn()
    {
        should_skip_numeric_fn = nullptr;
        numeric_cmp_fn = nullptr;
        fill_skip_bitmap_fn = nullptr;
        if (collators[0])
            return;

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
            case TypeIndex::IPv4:      resolveNumericFastPath<UInt32>(); break;
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
