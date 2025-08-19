#pragma once
#include <vector>
#include <Common/ColumnsHashing.h>
#include <Interpreters/AggregationCommon.h>
#include <Interpreters/AggregatedData.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>

namespace DB
{
class IColumn;
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
    static void insertKeyIntoColumns(const Key & key, std::vector<IColumn *> & key_columns, const Sizes & /*key_sizes*/);
};

/// For the case where there is one string key.
template <typename TData>
struct AggregationMethodString
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

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

    static void insertKeyIntoColumns(StringRef key, std::vector<IColumn *> & key_columns, const Sizes &)
    {
        static_cast<ColumnString *>(key_columns[0])->insertData(key.data, key.size);
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

    static void insertKeyIntoColumns(StringRef key, std::vector<IColumn *> & key_columns, const Sizes &);
};

/// For the case where there is one fixed-length string key.
template <typename TData>
struct AggregationMethodFixedString
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

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

    static void insertKeyIntoColumns(StringRef key, std::vector<IColumn *> & key_columns, const Sizes &);
};

/// Same as above but without cache
template <typename TData, bool nullable = false>
struct AggregationMethodFixedStringNoCache
{
    using Data = TData;
    using Key = typename Data::key_type;
    using Mapped = typename Data::mapped_type;

    Data data;

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

    static void insertKeyIntoColumns(StringRef key, std::vector<IColumn *> & key_columns, const Sizes &);
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

    static void insertKeyIntoColumns(const Key & key,
         std::vector<IColumn *> & key_columns_low_cardinality, const Sizes & /*key_sizes*/);
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

    static void insertKeyIntoColumns(const Key & key, std::vector<IColumn *> & key_columns, const Sizes & key_sizes);
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

    AggregationMethodSerialized() = default;

    explicit AggregationMethodSerialized(size_t size_hint) : data(size_hint) { }

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

    static void insertKeyIntoColumns(StringRef key, std::vector<IColumn *> & key_columns, const Sizes &);
};

template <typename TData>
using AggregationMethodNullableSerialized = AggregationMethodSerialized<TData, true>;

template <typename TData>
using AggregationMethodPreallocSerialized = AggregationMethodSerialized<TData, false, true>;

template <typename TData>
using AggregationMethodNullablePreallocSerialized = AggregationMethodSerialized<TData, true, true>;


}
