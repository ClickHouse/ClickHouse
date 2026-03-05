#pragma once
#include <algorithm>
#include <queue>
#include <vector>
#include <Common/ColumnsHashing.h>

#include <Columns/ColumnString.h>
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
struct ColumnBoundedHeap
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

    ColumnBoundedHeap() = default;
    ColumnBoundedHeap(const ColumnBoundedHeap &) = delete;
    ColumnBoundedHeap & operator=(const ColumnBoundedHeap &) = delete;

    ColumnBoundedHeap(ColumnBoundedHeap && other) noexcept
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

    ColumnBoundedHeap & operator=(ColumnBoundedHeap && other) noexcept
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

    size_t size() const { return heap.size(); }
    bool empty() const { return heap.empty(); }

    /// Returns true if the source key at source_row is worse than the current boundary
    /// (the heap root), meaning it should be skipped.
    /// Single-column overload.
    bool shouldSkip(const IColumn & source_column, size_t source_row) const
    {
        return sourceAboveHeap(source_column, source_row, heap.top());
    }

    /// Returns true if the composite source key at source_row is worse than the current
    /// boundary (the heap root), meaning it should be skipped.
    bool shouldSkip(const ColumnRawPtrs & source_columns, size_t source_row) const
    {
        return sourceAboveHeapComposite(source_columns, source_row, heap.top());
    }

    /// Push a new key value from source_column[source_row] into the heap.
    /// Single-column overload.
    void push(const IColumn & source_column, size_t source_row)
    {
        size_t new_idx = heap_column->size();
        heap_column->insertFrom(source_column, source_row);
        heap.push(new_idx);
    }

    /// Push a new composite key from source_columns[source_row] into the heap.
    void push(const ColumnRawPtrs & source_columns, size_t source_row)
    {
        auto & tuple = assert_cast<ColumnTuple &>(*heap_column);
        size_t new_idx = tuple.size();
        for (size_t i = 0; i < source_columns.size(); ++i)
            tuple.getColumn(i).insertFrom(*source_columns[i], source_row);
        tuple.addSize(1);
        heap.push(new_idx);
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
        while (heap.size() > capacity)
        {
            size_t evicted = heap.top();
            heap.pop();
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
        const ColumnBoundedHeap * owner;

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

/// For the case where there is one numeric key.
/// FieldType is UInt8/16/32/64 for any type with corresponding bit width.
template <typename FieldType, typename TData,
        bool consecutive_keys_optimization = true, bool nullable = false>
struct AggregationMethodOneNumber
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;
    ColumnBoundedHeap top_n_heap;

    AggregationMethodOneNumber() = default;

    explicit AggregationMethodOneNumber(size_t size_hint) : data(size_hint) { }

    template <typename Other>
    explicit AggregationMethodOneNumber(const Other & other) : data(other.data)
    {
    }

    /// To use one `Method` in different threads, use different `State`.
    template <bool use_cache>
    using StateImpl = ColumnsHashing::HashMethodOneNumber<
        typename Data::value_type,
        Mapped,
        FieldType,
        use_cache && consecutive_keys_optimization,
        /*need_offset=*/ false,
        nullable>;

    using State = StateImpl<true>;
    using StateNoCache = StateImpl<false>;

    /// Use optimization for low cardinality.
    static const bool low_cardinality_optimization = false;
    static const bool one_key_nullable_optimization = nullable;

    /// Shuffle key columns before `insertKeyIntoColumns` call if needed.
    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    // Insert the key from the hash table into columns.
    static void insertKeyIntoColumns(const Key & key, std::vector<IColumn *> & key_columns, const Sizes & /*key_sizes*/, const IColumn::SerializationSettings * settings);
};

/// For the case where there is one string key.
template <typename TData>
struct AggregationMethodString
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;
    ColumnBoundedHeap top_n_heap;

    AggregationMethodString() = default;

    template <typename Other>
    explicit AggregationMethodString(const Other & other) : data(other.data)
    {
    }

    explicit AggregationMethodString(size_t size_hint) : data(size_hint) { }

    template <bool use_cache>
    using StateImpl = ColumnsHashing::HashMethodString<typename Data::value_type, Mapped, /*place_string_to_arena=*/ true, use_cache>;

    using State = StateImpl<true>;
    using StateNoCache = StateImpl<false>;

    static const bool low_cardinality_optimization = false;
    static const bool one_key_nullable_optimization = false;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    static void insertKeyIntoColumns(std::string_view key, std::vector<IColumn *> & key_columns, const Sizes &, const IColumn::SerializationSettings *)
    {
        static_cast<ColumnString *>(key_columns[0])->insertData(key.data(), key.size());
    }
};

/// Same as above but without cache
template <typename TData, bool nullable = false>
struct AggregationMethodStringNoCache
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;
    ColumnBoundedHeap top_n_heap;

    AggregationMethodStringNoCache() = default;

    explicit AggregationMethodStringNoCache(size_t size_hint) : data(size_hint) { }

    template <typename Other>
    explicit AggregationMethodStringNoCache(const Other & other) : data(other.data)
    {
    }

    template <bool use_cache>
    using StateImpl = ColumnsHashing::HashMethodString<typename Data::value_type, Mapped, true, false, false, nullable>;

    using State = StateImpl<true>;
    using StateNoCache = StateImpl<false>;

    static const bool low_cardinality_optimization = false;
    static const bool one_key_nullable_optimization = nullable;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    static void insertKeyIntoColumns(std::string_view key, std::vector<IColumn *> & key_columns, const Sizes &, const IColumn::SerializationSettings * settings);
};

/// For the case where there is one fixed-length string key.
template <typename TData>
struct AggregationMethodFixedString
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;
    ColumnBoundedHeap top_n_heap;

    AggregationMethodFixedString() = default;

    explicit AggregationMethodFixedString(size_t size_hint) : data(size_hint) { }

    template <typename Other>
    explicit AggregationMethodFixedString(const Other & other) : data(other.data)
    {
    }

    template <bool use_cache>
    using StateImpl = ColumnsHashing::HashMethodFixedString<typename Data::value_type, Mapped, /*place_string_to_arena=*/ true, use_cache>;

    using State = StateImpl<true>;
    using StateNoCache = StateImpl<false>;

    static const bool low_cardinality_optimization = false;
    static const bool one_key_nullable_optimization = false;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    static void insertKeyIntoColumns(std::string_view key, std::vector<IColumn *> & key_columns, const Sizes &, const IColumn::SerializationSettings * settings);
};

/// Same as above but without cache
template <typename TData, bool nullable = false>
struct AggregationMethodFixedStringNoCache
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;
    ColumnBoundedHeap top_n_heap;

    AggregationMethodFixedStringNoCache() = default;

    explicit AggregationMethodFixedStringNoCache(size_t size_hint) : data(size_hint) { }

    template <typename Other>
    explicit AggregationMethodFixedStringNoCache(const Other & other) : data(other.data)
    {
    }

    template <bool use_cache>
    using StateImpl = ColumnsHashing::HashMethodFixedString<typename Data::value_type, Mapped, true, false, false, nullable>;

    using State = StateImpl<true>;
    using StateNoCache = StateImpl<false>;

    static const bool low_cardinality_optimization = false;
    static const bool one_key_nullable_optimization = nullable;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    static void insertKeyIntoColumns(
        std::string_view key, std::vector<IColumn *> & key_columns, const Sizes &, const IColumn::SerializationSettings * settings);
};

/// Single low cardinality column.
template <typename SingleColumnMethod>
struct AggregationMethodSingleLowCardinalityColumn : public SingleColumnMethod
{
    using Base = SingleColumnMethod;
    using Data = typename Base::Data;
    using Key = typename Base::Key;
    using Mapped = typename Base::Mapped;
    using Base::data;
    ColumnBoundedHeap top_n_heap;

    template <bool use_cache>
    using BaseStateImpl = typename Base::template StateImpl<use_cache>;

    AggregationMethodSingleLowCardinalityColumn() = default;

    template <typename Other>
    explicit AggregationMethodSingleLowCardinalityColumn(const Other & other) : Base(other) {}

    template <bool use_cache>
    using StateImpl = ColumnsHashing::HashMethodSingleLowCardinalityColumn<BaseStateImpl<use_cache>, Mapped, use_cache>;

    using State = StateImpl<true>;
    using StateNoCache = StateImpl<false>;

    static const bool low_cardinality_optimization = true;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    static void insertKeyIntoColumns(
        const Key & key,
        std::vector<IColumn *> & key_columns_low_cardinality,
        const Sizes & /*key_sizes*/,
        const IColumn::SerializationSettings * settings);
};

/// For the case where all keys are of fixed length, and they fit in N (for example, 128) bits.
template <typename TData, bool has_nullable_keys_ = false, bool has_low_cardinality_ = false, bool consecutive_keys_optimization = false>
struct AggregationMethodKeysFixed
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;
    static constexpr bool has_nullable_keys = has_nullable_keys_;
    static constexpr bool has_low_cardinality = has_low_cardinality_;

    Data data;
    ColumnBoundedHeap top_n_heap;

    AggregationMethodKeysFixed() = default;

    explicit AggregationMethodKeysFixed(size_t size_hint) : data(size_hint) { }

    template <typename Other>
    explicit AggregationMethodKeysFixed(const Other & other) : data(other.data)
    {
    }

    template <bool use_cache>
    using StateImpl = ColumnsHashing::HashMethodKeysFixed<
        typename Data::value_type,
        Key,
        Mapped,
        has_nullable_keys,
        has_low_cardinality,
        use_cache && consecutive_keys_optimization>;

    using State = StateImpl<true>;
    using StateNoCache = StateImpl<false>;

    static const bool low_cardinality_optimization = false;
    static const bool one_key_nullable_optimization = false;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> & key_columns, const Sizes & key_sizes)
    {
        return State::shuffleKeyColumns(key_columns, key_sizes);
    }

    static void insertKeyIntoColumns(
        const Key & key, std::vector<IColumn *> & key_columns, const Sizes & key_sizes, const IColumn::SerializationSettings * settings);
};

/** Aggregates by concatenating serialized key values.
  * The serialized value differs in that it uniquely allows to deserialize it, having only the position with which it starts.
  * That is, for example, for strings, it contains first the serialized length of the string, and then the bytes.
  * Therefore, when aggregating by several strings, there is no ambiguity.
  */
template <typename TData, bool nullable = false, bool prealloc = false>
struct AggregationMethodSerialized
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;
    ColumnBoundedHeap top_n_heap;

    AggregationMethodSerialized() = default;

    explicit AggregationMethodSerialized(size_t size_hint) : data(size_hint)
    {
    }

    template <typename Other>
    explicit AggregationMethodSerialized(const Other & other) : data(other.data)
    {
    }

    template <bool use_cache>
    using StateImpl = ColumnsHashing::HashMethodSerialized<typename Data::value_type, Mapped, nullable, prealloc>;

    using State = StateImpl<true>;
    using StateNoCache = StateImpl<false>;

    static const bool low_cardinality_optimization = false;
    static const bool one_key_nullable_optimization = false;

    std::optional<Sizes> shuffleKeyColumns(std::vector<IColumn *> &, const Sizes &) { return {}; }

    static void insertKeyIntoColumns(std::string_view key, std::vector<IColumn *> & key_columns, const Sizes &, const IColumn::SerializationSettings * settings);
};

template <typename TData>
using AggregationMethodNullableSerialized = AggregationMethodSerialized<TData, true>;

template <typename TData>
using AggregationMethodPreallocSerialized = AggregationMethodSerialized<TData, false, true>;

template <typename TData>
using AggregationMethodNullablePreallocSerialized = AggregationMethodSerialized<TData, true, true>;


}
