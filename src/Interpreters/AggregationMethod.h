#pragma once
#include <algorithm>
#include <vector>
#include <Common/ColumnsHashing.h>

#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Columns/Collator.h>

namespace DB
{

/** A bounded heap that tracks the top-N smallest (ascending) or largest (descending) keys
  * seen during aggregation. Keys are stored in a small IColumn and compared using
  * IColumn::compareAt / compareAtWithCollation, which correctly handles all types
  * (signed integers, floats, strings, Nullable, LowCardinality, Enum, collations, etc.)
  * without extracting Field values.
  *
  * The heap is a standard binary max-heap (for ascending) or min-heap (for descending)
  * over row indices into `heap_column`. The boundary element is always at index 0
  * (the root of the heap = the "worst" key that would be evicted next).
  */
struct ColumnBoundedHeap
{
    /// Column holding the key values currently in the heap.
    MutableColumnPtr heap_column;
    /// Binary heap of row indices into heap_column.
    std::vector<size_t> heap_indices;

    int direction = 0;      /// 1 = ASC (max-heap to evict largest), -1 = DESC (min-heap to evict smallest)
    int nan_direction_hint = 1;  /// NULLs/NaNs go last by default
    const Collator * collator = nullptr;

    void init(const IColumn & source_column, int dir, const Collator * col = nullptr)
    {
        direction = dir;
        collator = col;
        /// For ascending sort, NaN/NULL should be greater (direction_hint=1) so they get evicted first.
        /// For descending sort, NaN/NULL should be less (direction_hint=-1) so they get evicted first.
        nan_direction_hint = dir;
        heap_column = source_column.cloneEmpty();
    }

    size_t size() const { return heap_indices.size(); }
    bool empty() const { return heap_indices.empty(); }

    /// Compare two rows within heap_column. Returns true if a should be "above" b in the heap.
    /// For ascending (max-heap): a is above b if a > b (so root = max = boundary).
    /// For descending (min-heap): a is above b if a < b (so root = min = boundary).
    bool heapOrderBefore(size_t a, size_t b) const
    {
        int cmp;
        if (collator)
            cmp = heap_column->compareAtWithCollation(a, b, *heap_column, nan_direction_hint, *collator);
        else
            cmp = heap_column->compareAt(a, b, *heap_column, nan_direction_hint);
        return direction == 1 ? (cmp > 0) : (cmp < 0);
    }

    /// Compare a row in source_column against a row in heap_column.
    /// Returns true if source row should be "above" the heap row (i.e. is worse).
    bool sourceAboveHeap(const IColumn & source_column, size_t source_row, size_t heap_row) const
    {
        int cmp;
        if (collator)
            cmp = source_column.compareAtWithCollation(source_row, heap_row, *heap_column, nan_direction_hint, *collator);
        else
            cmp = source_column.compareAt(source_row, heap_row, *heap_column, nan_direction_hint);
        return direction == 1 ? (cmp > 0) : (cmp < 0);
    }

    /// Returns true if the source key at source_row is worse than the current boundary
    /// (the heap root), meaning it should be skipped.
    bool shouldSkip(const IColumn & source_column, size_t source_row) const
    {
        return sourceAboveHeap(source_column, source_row, heap_indices[0]);
    }

    /// Push a new key value from source_column[source_row] into the heap.
    void push(const IColumn & source_column, size_t source_row)
    {
        size_t new_idx = heap_column->size();
        heap_column->insertFrom(source_column, source_row);
        heap_indices.push_back(new_idx);
        siftUp(heap_indices.size() - 1);
    }

    /// Remove the boundary element (heap root).
    void pop()
    {
        /// Move last element to root and sift down.
        heap_indices[0] = heap_indices.back();
        heap_indices.pop_back();
        if (!heap_indices.empty())
            siftDown(0);
    }

private:
    void siftUp(size_t idx)
    {
        while (idx > 0)
        {
            size_t parent = (idx - 1) / 2;
            if (heapOrderBefore(heap_indices[idx], heap_indices[parent]))
            {
                std::swap(heap_indices[idx], heap_indices[parent]);
                idx = parent;
            }
            else
                break;
        }
    }

    void siftDown(size_t idx)
    {
        size_t n = heap_indices.size();
        while (true)
        {
            size_t best = idx;
            size_t left = 2 * idx + 1;
            size_t right = 2 * idx + 2;
            if (left < n && heapOrderBefore(heap_indices[left], heap_indices[best]))
                best = left;
            if (right < n && heapOrderBefore(heap_indices[right], heap_indices[best]))
                best = right;
            if (best != idx)
            {
                std::swap(heap_indices[idx], heap_indices[best]);
                idx = best;
            }
            else
                break;
        }
    }
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
    ColumnBoundedHeap pqueue;

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
    ColumnBoundedHeap pqueue;

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
    ColumnBoundedHeap pqueue;

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
    ColumnBoundedHeap pqueue;

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
    ColumnBoundedHeap pqueue;

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
    ColumnBoundedHeap pqueue;

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
    ColumnBoundedHeap pqueue;

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
    ColumnBoundedHeap pqueue;

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
