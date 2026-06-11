#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>
#include <Core/callOnTypeIndex.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/FieldVisitorSum.h>
#include <Common/NaNUtils.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <cstring>
#include <type_traits>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
}

/// ---- Aggregation Operation ----

enum class SumMapOp { Sum, Min, Max };


/// Compute the result DataType for the sumMap/minMap/maxMap family.
/// Shared between the generic (Field-based) and typed fast paths.
inline DataTypePtr createSumMapResultType(
    const DataTypePtr & keys_type,
    const DataTypes & values_types,
    bool overflow)
{
    DataTypes types;
    types.emplace_back(std::make_shared<DataTypeArray>(keys_type));

    for (const auto & value_type : values_types)
    {
        DataTypePtr result_type;

        if (overflow)
        {
            if (value_type->onlyNull())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Cannot calculate -Map of type {}", value_type->getName());
            result_type = removeNullable(value_type);
        }
        else
        {
            auto value_type_without_nullable = removeNullable(value_type);
            if (!value_type_without_nullable->canBePromoted())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Values for -Map are expected to be Numeric, Float or Decimal, passed type {}",
                    value_type->getName());

            WhichDataType which(value_type_without_nullable);

            /// Do not promote decimal because of implementation issues of this function design.
            /// Currently we cannot get result column type in case of decimal we cannot get decimal scale
            /// in method insertResultInto.
            if (which.isDecimal())
                result_type = value_type_without_nullable;
            else
                result_type = value_type_without_nullable->promoteNumericType();
        }

        types.emplace_back(std::make_shared<DataTypeArray>(result_type));
    }

    return std::make_shared<DataTypeTuple>(types);
}


/// ---- Key Traits ----

/// Concept: key types that HashMap can store by value (trivially copyable, non-string).
/// Covers integers, floats (including BFloat16), decimals, and enum underlying types.
template <typename T>
concept SumMapDirectKey = std::is_trivially_copyable_v<T> && !std::is_same_v<T, std::string_view>;

/// Primary template (unspecialized)
template <typename K>
struct SumMapKeyTraits;

/// Specialization for numeric / trivially-copyable keys
template <SumMapDirectKey K>
struct SumMapKeyTraits<K>
{
    static constexpr bool needs_arena = false;
    using MapKey = K;
    using MapType = HashMap<K, char *>;
    using FilterSet = HashSet<K>;

    static K extractKey(const IColumn & key_col, size_t offset)
    {
        if constexpr (is_decimal<K>)
            return assert_cast<const ColumnDecimal<K> &>(key_col).getData()[offset];
        else
            return assert_cast<const ColumnVector<K> &>(key_col).getData()[offset];
    }

    static auto emplace(MapType & map, K key, Arena * /* arena */)
    {
        typename MapType::LookupResult it = nullptr;
        bool inserted = false;
        map.emplace(key, it, inserted);
        return std::pair{it, inserted};
    }

    static auto emplaceForMerge(MapType & map, K key, Arena * /* arena */)
    {
        return emplace(map, key, nullptr);
    }

    static void serializeKey(K key, WriteBuffer & buf)
    {
        if constexpr (is_decimal<K>)
            writeBinaryLittleEndian(key.value, buf);
        else
            writeBinaryLittleEndian(key, buf);
    }

    static K deserializeKey(ReadBuffer & buf, Arena & /* arena */)
    {
        if constexpr (is_decimal<K>)
        {
            typename K::NativeType native;
            readBinaryLittleEndian(native, buf);
            return K(native);
        }
        else
        {
            K key;
            readBinaryLittleEndian(key, buf);
            return key;
        }
    }

    static void insertKeyIntoColumn(IColumn & col, K key)
    {
        if constexpr (is_decimal<K>)
            assert_cast<ColumnDecimal<K> &>(col).getData().push_back(key);
        else
            assert_cast<ColumnVector<K> &>(col).getData().push_back(key);
    }

    /// For sorting keys in insertResultInto.
    /// Must match Field::operator< semantics: NaN sorts last (greater than all non-NaN).
    static bool less(K a, K b)
    {
        if constexpr (is_floating_point<K>)
        {
            bool a_nan = isNaN(a);
            bool b_nan = isNaN(b);
            if (a_nan != b_nan)
                return b_nan; /// non-NaN < NaN
            if (a_nan)
                return false; /// NaN == NaN
            return a < b;
        }
        else if constexpr (is_decimal<K>)
            return a.value < b.value;
        else
            return a < b;
    }
};

/// Specialization for string keys (stored as std::string_view with Arena backing)
template <>
struct SumMapKeyTraits<std::string_view>
{
    static constexpr bool needs_arena = true;
    using MapKey = std::string_view;
    using MapType = HashMapWithSavedHash<std::string_view, char *>;
    using FilterSet = HashSetWithSavedHash<std::string_view>;

    static std::string_view extractKey(const IColumn & key_col, size_t offset)
    {
        return key_col.getDataAt(offset);
    }

    static auto emplace(MapType & map, std::string_view key, Arena * arena)
    {
        typename MapType::LookupResult it = nullptr;
        bool inserted = false;
        ArenaKeyHolder key_holder{key, *arena};
        map.emplace(key_holder, it, inserted);
        return std::pair{it, inserted};
    }

    static auto emplaceForMerge(MapType & map, std::string_view key, Arena * arena)
    {
        return emplace(map, key, arena);
    }

    static void serializeKey(std::string_view key, WriteBuffer & buf)
    {
        writeVarUInt(key.size(), buf);
        buf.write(key.data(), key.size());
    }

    static std::string_view deserializeKey(ReadBuffer & buf, Arena & arena)
    {
        size_t size = 0;
        readVarUInt(size, buf);
        char * data = arena.alloc(size);
        buf.readStrict(data, size);
        return std::string_view(data, size);
    }

    static void insertKeyIntoColumn(IColumn & col, std::string_view key)
    {
        col.insertData(key.data(), key.size());
    }

    static bool less(std::string_view a, std::string_view b)
    {
        return a < b;
    }
};


/// ---- Accumulator block layout ----
///
/// Each unique key in the HashMap points to an arena-allocated block:
///
///   +-------------+---------+-------------+-----+-------------+--------------+
///   |  value[0]   | padding |  value[1]   | ... |  value[N-1] | null bitmap  |
///   +-------------+---------+-------------+-----+-------------+--------------+
///   ^                       ^                    ^              ^
///   desc[0].offset          desc[1].offset       desc[N-1]     null_bitmap_offset
///                                                .offset
///
///   |<------------ naturally-aligned accumulators ------------>|<- ceil(N/8) >|
///   |<------------------------- block_size ---------------------------------------->|
///
/// - Each value[i] is stored at its natural alignment (block_alignment = max).
/// - Accumulator type may differ from input (e.g. UInt8 -> UInt64 when !overflow).
/// - Null bitmap: 1 bit per value column, packed LSB-first per byte.
///     bit = 1: null (no value accumulated yet, initial state)
///     bit = 0: value present (cleared on first non-null input)

struct SumMapValueColumnDesc
{
    size_t offset;          /// byte offset within accumulator block
    size_t native_size;     /// sizeof the native accumulator type
    TypeIndex input_type_index;  /// TypeIndex of the input column (to extract values)
    TypeIndex acc_type_index;    /// TypeIndex of the accumulator (may be promoted)
    bool is_nullable;       /// whether the source column is Nullable
};

/// ---- State ----

template <typename KeyType>
struct SumMapTypedData
{
    using KT = SumMapKeyTraits<KeyType>;
    typename KT::MapType map;
};


/// ---- Accumulator block helpers ----
inline size_t nullBitmapBytes(size_t num_columns)
{
    return (num_columns + 7) / 8;
}

inline bool isNullInBitmap(const char * null_bitmap, size_t col)
{
    return (null_bitmap[col / 8] >> (col % 8)) & 1;
}

inline void setNullBit(char * null_bitmap, size_t col)
{
    null_bitmap[col / 8] |= (1 << (col % 8));
}

inline void clearNullBit(char * null_bitmap, size_t col)
{
    null_bitmap[col / 8] &= ~(1 << (col % 8));
}


/// ---- The typed aggregate function ----

template <typename KeyType>
class AggregateFunctionSumMapTyped final
    : public IAggregateFunctionDataHelper<SumMapTypedData<KeyType>,
        AggregateFunctionSumMapTyped<KeyType>>
{
    using KT = SumMapKeyTraits<KeyType>;
    using Data = SumMapTypedData<KeyType>;
    using Base = IAggregateFunctionDataHelper<Data,
        AggregateFunctionSumMapTyped<KeyType>>;

    static constexpr auto STATE_VERSION_1_MIN_REVISION = 54452;

    DataTypePtr keys_type;
    SerializationPtr keys_serialization;
    DataTypes values_types;
    Serializations values_serializations;
    Serializations promoted_values_serializations;

    std::vector<SumMapValueColumnDesc> value_descs; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    size_t null_bitmap_offset = 0;  /// offset of null bitmap within block
    size_t block_size = 0;          /// total accumulator block size
    size_t block_alignment = 1;     /// max alignment required across all accumulators

    SumMapOp op;
    bool overflow;
    bool compact;
    bool tuple_argument;

    /// Key filter for sumMapFiltered / sumMapFilteredWithOverflow.
    /// When non-empty, only keys present in this set are accumulated.
    typename KT::FilterSet keys_to_keep;

    /// For string keys, the filter set stores string_views, so we need
    /// persistent owned storage for the filter key strings.
    std::vector<String> owned_filter_strings; // STYLE_CHECK_ALLOW_STD_CONTAINERS

public:
    AggregateFunctionSumMapTyped(
        const DataTypePtr & keys_type_,
        const DataTypes & values_types_,
        const DataTypes & argument_types_,
        bool overflow_,
        bool tuple_argument_,
        SumMapOp op_,
        const Array & keys_filter = {})
        : Base(argument_types_, keys_filter.empty() ? Array{} : Array{keys_filter}, createSumMapResultType(keys_type_, values_types_, overflow_))
        , keys_type(keys_type_)
        , keys_serialization(keys_type->getDefaultSerialization())
        , values_types(values_types_)
        , op(op_)
        , overflow(overflow_)
        , compact(op_ == SumMapOp::Sum)
        , tuple_argument(tuple_argument_)
    {
        /// Build key filter set if provided
        if (!keys_filter.empty())
        {
            if constexpr (std::is_same_v<KeyType, std::string_view>)
            {
                /// For string keys, collect all owned strings first (to avoid
                /// vector reallocation invalidating string_views in the set),
                /// then insert views into the filter set.
                owned_filter_strings.reserve(keys_filter.size());
                for (const auto & f : keys_filter)
                    owned_filter_strings.emplace_back(keyFromField(f, nullptr));
                for (const auto & s : owned_filter_strings)
                    keys_to_keep.insert(std::string_view(s.data(), s.size()));
            }
            else
            {
                for (const auto & f : keys_filter)
                    keys_to_keep.insert(keyFromField(f, nullptr));
            }
        }

        /// Build value column descriptors
        size_t offset = 0;
        value_descs.reserve(values_types.size());
        for (const auto & type : values_types)
        {
            SumMapValueColumnDesc desc{};
            desc.is_nullable = type->isNullable();
            auto inner_type = removeNullable(type);
            desc.input_type_index = inner_type->getTypeId();

            /// Determine accumulator type: for overflow=false, promote numeric types
            /// (but not decimals, matching the generic path behavior)
            if (!overflow)
            {
                WhichDataType which(inner_type);
                if (which.isDecimal())
                    desc.acc_type_index = desc.input_type_index;
                else if (inner_type->canBePromoted())
                    desc.acc_type_index = inner_type->promoteNumericType()->getTypeId();
                else
                    desc.acc_type_index = desc.input_type_index;
            }
            else
            {
                desc.acc_type_index = desc.input_type_index;
            }

            auto [sz, al] = nativeSizeAlignForTypeIndex(desc.acc_type_index);
            desc.native_size = sz;
            /// Align offset to the natural alignment of the accumulator type
            offset = (offset + al - 1) / al * al;
            desc.offset = offset;
            offset += desc.native_size;
            block_alignment = std::max(block_alignment, al);
            value_descs.push_back(desc);
        }
        null_bitmap_offset = offset;
        block_size = offset + nullBitmapBytes(values_types.size());

        /// Build serializations (same as the generic path)
        values_serializations.reserve(values_types.size());
        promoted_values_serializations.reserve(values_types.size());
        for (const auto & type : values_types)
        {
            values_serializations.emplace_back(type->getDefaultSerialization());
            if (type->canBePromoted())
            {
                if (type->isNullable())
                    promoted_values_serializations.emplace_back(
                        makeNullable(removeNullable(type)->promoteNumericType())->getDefaultSerialization());
                else
                    promoted_values_serializations.emplace_back(type->promoteNumericType()->getDefaultSerialization());
            }
            else
            {
                promoted_values_serializations.emplace_back(type->getDefaultSerialization());
            }
        }
    }

    String getName() const override
    {
        bool is_filtered = !keys_to_keep.empty();
        switch (op)
        {
            case SumMapOp::Sum:
                if (is_filtered)
                    return overflow ? "sumMapFilteredWithOverflow" : "sumMapFiltered";
                return overflow ? "sumMapWithOverflow" : "sumMap";
            case SumMapOp::Min: return "minMap";
            case SumMapOp::Max: return "maxMap";
        }
        UNREACHABLE();
    }

    bool isVersioned() const override { return true; }

    size_t getDefaultVersion() const override { return 1; }

    size_t getVersionFromRevision(size_t revision) const override
    {
        if (revision >= STATE_VERSION_1_MIN_REVISION)
            return 1;
        return 0;
    }

    bool allocatesMemoryInArena() const override { return true; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns_, const size_t row_num, Arena * arena) const override
    {
        /// Unpack tuple argument into raw column pointers if needed
        const IColumn * col_ptrs_buf[16];
        std::unique_ptr<const IColumn *[]> col_ptrs_heap;
        const IColumn ** columns = nullptr;
        if (tuple_argument)
        {
            const auto & tuple_cols = assert_cast<const ColumnTuple *>(columns_[0])->getColumns();
            const size_t n_cols = tuple_cols.size();
            const IColumn ** col_ptrs = n_cols <= 16
                ? col_ptrs_buf
                : (col_ptrs_heap = std::make_unique<const IColumn *[]>(n_cols)).get();
            for (size_t c = 0; c < n_cols; ++c)
                col_ptrs[c] = tuple_cols[c].get();
            columns = col_ptrs;
        }
        else
        {
            columns = columns_;
        }

        const ColumnArray & keys_array = assert_cast<const ColumnArray &>(*columns[0]);
        const IColumn::Offsets & keys_offsets = keys_array.getOffsets();
        const IColumn & key_col = keys_array.getData();
        const size_t keys_vec_offset = keys_offsets[row_num - 1];
        const size_t keys_vec_size = keys_offsets[row_num] - keys_vec_offset;

        auto & data = this->data(place);
        const size_t num_columns = value_descs.size();

        /// Pre-extract value column data
        struct ValColInfo
        {
            const IColumn * data_col;
            const UInt8 * null_map;
            size_t vec_offset;
        };

        /// Use stack storage for typical cases (up to 8 value columns)
        ValColInfo val_info_buf[8];
        std::unique_ptr<ValColInfo[]> val_info_heap;
        ValColInfo * val_info = num_columns <= 8 ? val_info_buf : (val_info_heap = std::make_unique<ValColInfo[]>(num_columns)).get();

        for (size_t col = 0; col < num_columns; ++col)
        {
            const auto & arr_col = assert_cast<const ColumnArray &>(*columns[col + 1]);
            const IColumn::Offsets & offsets = arr_col.getOffsets();
            const size_t v_offset = offsets[row_num - 1];
            const size_t v_size = offsets[row_num] - v_offset;

            if (keys_vec_size != v_size)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sizes of keys and values arrays do not match");

            const auto & desc = value_descs[col];
            if (desc.is_nullable)
            {
                const auto & nullable_col = assert_cast<const ColumnNullable &>(arr_col.getData());
                val_info[col] = {&nullable_col.getNestedColumn(), nullable_col.getNullMapData().data(), v_offset};
            }
            else
            {
                val_info[col] = {&arr_col.getData(), nullptr, v_offset};
            }
        }

        /// Outer loop over keys, inner loop over value columns
        for (size_t i = 0; i < keys_vec_size; ++i)
        {
            auto key = KT::extractKey(key_col, keys_vec_offset + i);

            if (!keys_to_keep.empty() && !keys_to_keep.has(key))
                continue;

            auto [it, inserted] = KT::emplace(data.map, key, arena);

            char * block = nullptr;
            if (inserted)
            {
                block = arena->alignedAlloc(block_size, block_alignment);
                memset(block, 0, block_size);
                /// Mark all values as null initially
                char * null_bitmap = block + null_bitmap_offset;
                memset(null_bitmap, 0xFF, nullBitmapBytes(num_columns));
                it->getMapped() = block;
            }
            else
            {
                block = it->getMapped();
            }

            char * null_bitmap = block + null_bitmap_offset;
            for (size_t col = 0; col < num_columns; ++col)
            {
                const auto & desc = value_descs[col];
                const size_t elem_idx = val_info[col].vec_offset + i;

                /// Check if source value is null
                if (val_info[col].null_map && val_info[col].null_map[elem_idx])
                    continue;

                bool acc_is_null = isNullInBitmap(null_bitmap, col);
                accumulateValue(block, null_bitmap, col, desc, *val_info[col].data_col, elem_idx, acc_is_null);
            }
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & lhs_data = this->data(place);
        const auto & rhs_data = this->data(rhs);
        const size_t num_columns = value_descs.size();

        for (const auto & [rhs_key, rhs_block] : rhs_data.map)
        {
            auto [it, inserted] = KT::emplaceForMerge(lhs_data.map, rhs_key, arena);

            if (inserted)
            {
                char * new_block = arena->alignedAlloc(block_size, block_alignment);
                memcpy(new_block, rhs_block, block_size);
                it->getMapped() = new_block;
            }
            else
            {
                char * lhs_block = it->getMapped();
                const char * rhs_null_bitmap = rhs_block + null_bitmap_offset;
                char * lhs_null_bitmap = lhs_block + null_bitmap_offset;

                for (size_t col = 0; col < num_columns; ++col)
                {
                    if (isNullInBitmap(rhs_null_bitmap, col))
                        continue;

                    const auto & desc = value_descs[col];
                    bool lhs_is_null = isNullInBitmap(lhs_null_bitmap, col);

                    mergeAccumulator(lhs_block, lhs_null_bitmap, rhs_block, col, desc, lhs_is_null);
                }
            }
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        if (!version)
            version = getDefaultVersion();

        const auto & data = this->data(place);
        const size_t num_columns = value_descs.size();

        writeVarUInt(data.map.size(), buf);

        /// Sort entries by key before serializing to produce a canonical byte
        /// representation, independent of hash-map iteration order.
        constexpr bool sort_serialize = true;

        using Entry = std::pair<typename KT::MapKey, char *>;
        std::vector<Entry> sorted; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        if constexpr (sort_serialize)
        {
            sorted.reserve(data.map.size());
            for (const auto & [key, block] : data.map)
                sorted.emplace_back(key, block);
            std::sort(sorted.begin(), sorted.end(),
                [](const auto & a, const auto & b) { return KT::less(a.first, b.first); });
        }

        const auto serialize_entry = [&](typename KT::MapKey key, const char * block)
        {
            keys_serialization->serializeBinary(fieldFromKey(key), buf, {});

            const char * null_bitmap = block + null_bitmap_offset;

            for (size_t col = 0; col < num_columns; ++col)
            {
                const auto & desc = value_descs[col];

                if (isNullInBitmap(null_bitmap, col))
                {
                    Field null_field;
                    if (*version == 0)
                        values_serializations[col]->serializeBinary(null_field, buf, {});
                    else
                        promoted_values_serializations[col]->serializeBinary(null_field, buf, {});
                    continue;
                }

                Field value = fieldFromAccumulator(block, col, desc);

                if (*version == 1 && !value.isNull())
                {
                    WhichDataType value_type(removeNullable(values_types[col]));
                    if (value_type.isDecimal32())
                    {
                        auto source = value.safeGet<DecimalField<Decimal32>>();
                        value = DecimalField<Decimal128>(source.getValue(), source.getScale());
                    }
                    else if (value_type.isDecimal64())
                    {
                        auto source = value.safeGet<DecimalField<Decimal64>>();
                        value = DecimalField<Decimal128>(source.getValue(), source.getScale());
                    }
                }

                if (*version == 0)
                    values_serializations[col]->serializeBinary(value, buf, {});
                else
                    promoted_values_serializations[col]->serializeBinary(value, buf, {});
            }
        };

        if constexpr (sort_serialize)
        {
            for (const auto & [key, block] : sorted)
                serialize_entry(key, block);
        }
        else
        {
            for (const auto & [key, block] : data.map)
                serialize_entry(key, block);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        if (!version)
            version = getDefaultVersion();

        auto & data = this->data(place);
        const size_t num_columns = value_descs.size();

        size_t size = 0;
        readVarUInt(size, buf);

        FormatSettings format_settings;

        for (size_t i = 0; i < size; ++i)
        {
            /// Deserialize key using generic serialization for wire compatibility
            Field key_field;
            keys_serialization->deserializeBinary(key_field, buf, format_settings);

            auto key = keyFromField(key_field, arena);
            auto [it, inserted] = KT::emplace(data.map, key, arena);

            char * block = nullptr;
            if (inserted)
            {
                block = arena->alignedAlloc(block_size, block_alignment);
                memset(block, 0, block_size);
                char * null_bitmap = block + null_bitmap_offset;
                memset(null_bitmap, 0xFF, nullBitmapBytes(num_columns));
                it->getMapped() = block;
            }
            else
            {
                block = it->getMapped();
            }

            char * null_bitmap = block + null_bitmap_offset;

            for (size_t col = 0; col < num_columns; ++col)
            {
                Field value;
                if (*version == 0)
                    values_serializations[col]->deserializeBinary(value, buf, format_settings);
                else
                {
                    promoted_values_serializations[col]->deserializeBinary(value, buf, format_settings);

                    /// Narrow Decimal128 back to Decimal32/Decimal64
                    if (value.getType() == Field::Types::Decimal128)
                    {
                        auto source = value.safeGet<DecimalField<Decimal128>>();
                        WhichDataType value_type(removeNullable(values_types[col]));
                        if (value_type.isDecimal32())
                            value = DecimalField<Decimal32>(source.getValue(), source.getScale());
                        else if (value_type.isDecimal64())
                            value = DecimalField<Decimal64>(source.getValue(), source.getScale());
                    }
                }

                if (value.isNull())
                {
                    /// If the existing accumulator is also null, nothing to do.
                    /// If it has a value, keep it (null is a no-op for merge).
                    continue;
                }

                const auto & desc = value_descs[col];
                bool acc_is_null = isNullInBitmap(null_bitmap, col);

                if (acc_is_null)
                {
                    /// Just set the value
                    setAccumulatorFromField(block, null_bitmap, col, desc, value);
                }
                else
                {
                    /// Merge: extract current, apply op, store back
                    Field current = fieldFromAccumulator(block, col, desc);
                    applyFieldOp(current, value);
                    setAccumulatorFromField(block, null_bitmap, col, desc, current);
                }
            }
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & data = this->data(place);
        const size_t num_columns = value_descs.size();

        /// Collect keys and sort them
        using KeyVec = std::vector<std::pair<typename KT::MapKey, char *>>; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        KeyVec sorted_entries;
        sorted_entries.reserve(data.map.size());

        for (const auto & [key, block] : data.map)
        {
            /// If compact, skip keys where all values are null or zero
            if (compact)
            {
                bool all_identity = true;
                const char * null_bitmap = block + null_bitmap_offset;
                for (size_t col = 0; col < num_columns; ++col)
                {
                    if (isNullInBitmap(null_bitmap, col))
                        continue;

                    /// For nullable columns, a non-null value always prevents
                    /// compaction because the type's default is Null, and any
                    /// non-null value (even zero) differs from Null.
                    if (value_descs[col].is_nullable)
                    {
                        all_identity = false;
                        break;
                    }

                    if (!checkIsIdentity(block, col, value_descs[col]))
                    {
                        all_identity = false;
                        break;
                    }
                }
                if (all_identity)
                    continue;
            }
            sorted_entries.emplace_back(key, block);
        }

        std::sort(sorted_entries.begin(), sorted_entries.end(),
            [](const auto & a, const auto & b) { return KT::less(a.first, b.first); });

        const size_t result_size = sorted_entries.size();

        auto & to_tuple = assert_cast<ColumnTuple &>(to);
        auto & to_keys_arr = assert_cast<ColumnArray &>(to_tuple.getColumn(0));
        auto & to_keys_col = to_keys_arr.getData();

        auto & to_keys_offsets = to_keys_arr.getOffsets();
        to_keys_offsets.push_back(to_keys_offsets.back() + result_size);
        to_keys_col.reserve(result_size);

        for (size_t col = 0; col < num_columns; ++col)
        {
            auto & to_values_arr = assert_cast<ColumnArray &>(to_tuple.getColumn(col + 1));
            auto & to_values_offsets = to_values_arr.getOffsets();
            to_values_offsets.push_back(to_values_offsets.back() + result_size);
            to_values_arr.getData().reserve(result_size);
        }

        for (const auto & [key, block] : sorted_entries)
        {
            KT::insertKeyIntoColumn(to_keys_col, key);

            const char * null_bitmap = block + null_bitmap_offset;
            for (size_t col = 0; col < num_columns; ++col)
            {
                auto & to_values_col = assert_cast<ColumnArray &>(to_tuple.getColumn(col + 1)).getData();
                if (isNullInBitmap(null_bitmap, col))
                {
                    to_values_col.insertDefault();
                }
                else
                {
                    insertAccumulatorIntoColumn(to_values_col, block, value_descs[col]);
                }
            }
        }
    }

private:
    /// Get the native size and alignment for a TypeIndex used in accumulator blocks
    struct SizeAlign { size_t size; size_t align; };

    static SizeAlign nativeSizeAlignForTypeIndex(TypeIndex idx)
    {
        SizeAlign result{};
        bool found = callOnBasicType<void, true, true, true, true>(idx, [&](auto tag)
        {
            using V = typename decltype(tag)::RightType;
            result = {sizeof(V), alignof(V)};
            return true;
        });
        if (!found)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Unsupported TypeIndex {} in SumMapTyped accumulator", magic_enum::enum_name(idx));
        return result;
    }

    template <typename F>
    void dispatchOnTypeIndex(TypeIndex idx, F && func) const
    {
        bool found = callOnBasicType<void, true, true, true, true>(idx, [&](auto tag)
        {
            using V = typename decltype(tag)::RightType;
            func(V{});
            return true;
        });
        if (!found)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Unsupported TypeIndex {} in SumMapTyped dispatch", magic_enum::enum_name(idx));
    }

    /// Extract a value from a typed column at the given offset
    template <typename V>
    static V extractValueFromColumn(const IColumn & col, size_t offset)
    {
        if constexpr (is_decimal<V>)
            return assert_cast<const ColumnDecimal<V> &>(col).getData()[offset];
        else
            return assert_cast<const ColumnVector<V> &>(col).getData()[offset];
    }

    /// Extract value from column at input_type_index and widen to AccType.
    /// Only valid conversions are: same type, or integer/float widening.
    template <typename AccType>
    AccType extractAndWiden(const IColumn & col, size_t offset, TypeIndex input_idx) const
    {
        AccType result{};
        dispatchOnTypeIndex(input_idx, [&](auto input_tag)
        {
            using InputType = decltype(input_tag);
            InputType raw = extractValueFromColumn<InputType>(col, offset);
            if constexpr (std::is_same_v<InputType, AccType>)
            {
                result = raw;
            }
            else if constexpr (is_decimal<InputType> || is_decimal<AccType>)
            {
                /// Decimal types are not promoted, so input == acc type for decimals.
                /// The is_same_v branch above handles that case; reaching here means
                /// an inconsistent type setup.  Cannot use static_assert because
                /// dispatchOnTypeIndex instantiates all type combinations.
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Unexpected decimal type mismatch in SumMapTyped extractAndWiden");
            }
            else if constexpr (std::is_same_v<InputType, BFloat16> && std::is_floating_point_v<AccType>)
            {
                result = AccType(Float32(raw));
            }
            else if constexpr (std::is_convertible_v<InputType, AccType>)
            {
                result = static_cast<AccType>(raw);
            }
            else
            {
                /// Instantiated at compile time for nonsensical type combinations
                /// (e.g., UInt128 -> Float32) that type-promotion logic guarantees
                /// will never be reached at runtime.  Cannot use static_assert
                /// because dispatchOnTypeIndex instantiates all type combinations.
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Unexpected type combination in SumMapTyped extractAndWiden");
            }
        });
        return result;
    }

    template <typename V>
    void applyOp(V & acc, V val) const
    {
        switch (op)
        {
            case SumMapOp::Sum:
                acc += val;
                break;
            case SumMapOp::Min:
                if constexpr (is_floating_point<V>)
                {
                    if (isNaN(val))
                        return;
                    if (isNaN(acc))
                    {
                        acc = val;
                        return;
                    }
                }
                if (val < acc)
                    acc = val;
                break;
            case SumMapOp::Max:
                if constexpr (is_floating_point<V>)
                {
                    if (isNaN(val))
                        return;
                    if (isNaN(acc))
                    {
                        acc = val;
                        return;
                    }
                }
                if (val > acc)
                    acc = val;
                break;
        }
    }

    /// Accumulate a single value into the block
    void accumulateValue(
        char * block, char * null_bitmap, size_t col,
        const SumMapValueColumnDesc & desc,
        const IColumn & data_col, size_t elem_idx,
        bool acc_is_null) const
    {
        dispatchOnTypeIndex(desc.acc_type_index, [&](auto acc_tag)
        {
            using AccType = decltype(acc_tag);
            /// Fast path: when input type matches accumulator type (overflow mode,
            /// decimals, or same-type inputs), skip the inner type dispatch entirely.
            AccType val = (desc.input_type_index == desc.acc_type_index)
                ? extractValueFromColumn<AccType>(data_col, elem_idx)
                : extractAndWiden<AccType>(data_col, elem_idx, desc.input_type_index);
            AccType & acc = *reinterpret_cast<AccType *>(block + desc.offset);

            if (acc_is_null)
            {
                acc = val;
                clearNullBit(null_bitmap, col);
            }
            else
            {
                applyOp(acc, val);
            }
        });
    }

    /// Merge accumulators from rhs into lhs
    void mergeAccumulator(
        char * lhs_block, char * lhs_null_bitmap,
        const char * rhs_block,
        size_t col, const SumMapValueColumnDesc & desc,
        bool lhs_is_null) const
    {
        dispatchOnTypeIndex(desc.acc_type_index, [&](auto type_tag)
        {
            using V = decltype(type_tag);
            V rhs_val = *reinterpret_cast<const V *>(rhs_block + desc.offset);
            V & lhs_acc = *reinterpret_cast<V *>(lhs_block + desc.offset);

            if (lhs_is_null)
            {
                lhs_acc = rhs_val;
                clearNullBit(lhs_null_bitmap, col);
            }
            else
            {
                applyOp(lhs_acc, rhs_val);
            }
        });
    }

    /// Convert a native key to a Field for serialization
    static Field fieldFromKey(typename KT::MapKey key)
    {
        if constexpr (std::is_same_v<typename KT::MapKey, std::string_view>)
            return Field(String(key));
        else /// Handles integers, floats, and decimals uniformly
            return Field(key);
    }

    /// Convert a Field back to a native key
    static typename KT::MapKey keyFromField(const Field & field, Arena * /* arena */)
    {
        if constexpr (std::is_same_v<typename KT::MapKey, std::string_view>)
        {
            const auto & str = field.safeGet<String>();
            return std::string_view(str.data(), str.size());
        }
        else if constexpr (is_decimal<typename KT::MapKey>)
        {
            using DecType = typename KT::MapKey;
            return field.safeGet<DecimalField<DecType>>().getValue();
        }
        else
        {
            return static_cast<typename KT::MapKey>(field.safeGet<NearestFieldType<typename KT::MapKey>>());
        }
    }

    /// Extract a Field from an accumulator slot for serialization
    Field fieldFromAccumulator(const char * block, size_t col, const SumMapValueColumnDesc & desc) const
    {
        Field result;
        dispatchOnTypeIndex(desc.acc_type_index, [&](auto type_tag)
        {
            using V = decltype(type_tag);
            V val = *reinterpret_cast<const V *>(block + desc.offset);
            if constexpr (is_decimal<V>)
            {
                /// Get scale from the value type
                UInt32 scale = 0;
                auto inner_type = removeNullable(values_types[col]);
                if (const auto * decimal_type = checkAndGetDataType<DataTypeDecimal<V>>(inner_type.get()))
                    scale = decimal_type->getScale();
                result = DecimalField<V>(val, scale);
            }
            else
            {
                result = val;
            }
        });
        return result;
    }

    /// Set an accumulator slot from a Field (used during deserialization)
    void setAccumulatorFromField(char * block, char * null_bitmap, size_t col,
        const SumMapValueColumnDesc & desc, const Field & value) const
    {
        dispatchOnTypeIndex(desc.acc_type_index, [&](auto type_tag)
        {
            using V = decltype(type_tag);
            V & acc = *reinterpret_cast<V *>(block + desc.offset);
            if constexpr (is_decimal<V>)
                acc = value.safeGet<DecimalField<V>>().getValue();
            else
                acc = static_cast<V>(value.safeGet<NearestFieldType<V>>());
            clearNullBit(null_bitmap, col);
        });
    }

    /// Apply the aggregation operation on Fields (for deserialization merge).
    /// This is a cold path (only used when deserializing into a state that
    /// already has data for the same key).
    void applyFieldOp(Field & acc, const Field & val) const
    {
        switch (op)
        {
            case SumMapOp::Sum:
                applyVisitor(FieldVisitorSum(val), acc);
                break;
            case SumMapOp::Min:
                if (isFieldNaN(val))
                    return;
                if (isFieldNaN(acc) || val < acc)
                    acc = val;
                break;
            case SumMapOp::Max:
                if (isFieldNaN(val))
                    return;
                if (isFieldNaN(acc) || val > acc)
                    acc = val;
                break;
        }
    }

    static bool isFieldNaN(const Field & f)
    {
        /// BFloat16 is stored as Float64 in Field (via NearestFieldType),
        /// so the Float64 case covers both.
        if (f.getType() == Field::Types::Float64)
            return isNaN(f.safeGet<Float64>());
        return false;
    }

    /// Check if accumulator value equals the identity element (for compaction)
    bool checkIsIdentity(const char * block, size_t /* col */, const SumMapValueColumnDesc & desc) const
    {
        bool result = false;
        dispatchOnTypeIndex(desc.acc_type_index, [&](auto type_tag)
        {
            using V = decltype(type_tag);
            V val = *reinterpret_cast<const V *>(block + desc.offset);
            /// Only called for Sum (compact mode), where identity is zero
            result = (val == V(0));
        });
        return result;
    }

    /// Insert an accumulator value into the result column
    void insertAccumulatorIntoColumn(IColumn & col, const char * block,
        const SumMapValueColumnDesc & desc) const
    {
        dispatchOnTypeIndex(desc.acc_type_index, [&](auto type_tag)
        {
            using V = decltype(type_tag);
            V val = *reinterpret_cast<const V *>(block + desc.offset);
            if constexpr (is_decimal<V>)
                assert_cast<ColumnDecimal<V> &>(col).getData().push_back(val);
            else
                assert_cast<ColumnVector<V> &>(col).getData().push_back(val);
        });
    }
};

/// Helper to check if a key type is supported by the typed path
inline bool isSumMapTypedKeySupported(TypeIndex idx)
{
    switch (idx)
    {
        case TypeIndex::UInt8:
        case TypeIndex::UInt16:
        case TypeIndex::UInt32:
        case TypeIndex::UInt64:
        case TypeIndex::UInt128:
        case TypeIndex::UInt256:
        case TypeIndex::Int8:
        case TypeIndex::Int16:
        case TypeIndex::Int32:
        case TypeIndex::Int64:
        case TypeIndex::Int128:
        case TypeIndex::Int256:
        case TypeIndex::BFloat16:
        case TypeIndex::Float32:
        case TypeIndex::Float64:
        case TypeIndex::Decimal32:
        case TypeIndex::Decimal64:
        case TypeIndex::Decimal128:
        case TypeIndex::Decimal256:
        case TypeIndex::Date:
        case TypeIndex::DateTime:
        case TypeIndex::Enum8:
        case TypeIndex::Enum16:
        case TypeIndex::String:
        case TypeIndex::FixedString:
            return true;
        default:
            return false;
    }
}

/// Helper to check if all value types are supported by the typed path
inline bool areSumMapTypedValuesSupported(const DataTypes & values_types)
{
    for (const auto & type : values_types)
    {
        auto inner = removeNullable(type);
        TypeIndex idx = inner->getTypeId();
        switch (idx)
        {
            case TypeIndex::UInt8:
            case TypeIndex::UInt16:
            case TypeIndex::UInt32:
            case TypeIndex::UInt64:
            case TypeIndex::UInt128:
            case TypeIndex::UInt256:
            case TypeIndex::Int8:
            case TypeIndex::Int16:
            case TypeIndex::Int32:
            case TypeIndex::Int64:
            case TypeIndex::Int128:
            case TypeIndex::Int256:
            case TypeIndex::BFloat16:
            case TypeIndex::Float32:
            case TypeIndex::Float64:
            case TypeIndex::Decimal32:
            case TypeIndex::Decimal64:
            case TypeIndex::Decimal128:
            case TypeIndex::Decimal256:
                break;
            default:
                return false;
        }
    }
    return true;
}

/// Create a typed sumMap aggregate function, dispatching on key type.
/// Returns nullptr if the key type is not supported.
/// When keys_filter is non-empty, the function only accumulates keys present in the filter.
inline AggregateFunctionPtr createSumMapTyped(
    const DataTypePtr & keys_type,
    const DataTypes & values_types,
    const DataTypes & argument_types,
    bool overflow,
    bool tuple_argument,
    SumMapOp op,
    const Array & keys_filter = {})
{
    TypeIndex key_idx = keys_type->getTypeId();

    switch (key_idx)
    {
// NOLINTBEGIN(bugprone-macro-parentheses)
#define DISPATCH_KEY(TYPE) \
    case TypeIndex::TYPE: \
        return std::make_shared<AggregateFunctionSumMapTyped<TYPE>>( \
            keys_type, values_types, argument_types, overflow, tuple_argument, op, keys_filter);
// NOLINTEND(bugprone-macro-parentheses)

        DISPATCH_KEY(UInt8)
        DISPATCH_KEY(UInt16)
        DISPATCH_KEY(UInt32)
        DISPATCH_KEY(UInt64)
        DISPATCH_KEY(UInt128)
        DISPATCH_KEY(UInt256)
        DISPATCH_KEY(Int8)
        DISPATCH_KEY(Int16)
        DISPATCH_KEY(Int32)
        DISPATCH_KEY(Int64)
        DISPATCH_KEY(Int128)
        DISPATCH_KEY(Int256)
        DISPATCH_KEY(BFloat16)
        DISPATCH_KEY(Float32)
        DISPATCH_KEY(Float64)
        DISPATCH_KEY(Decimal32)
        DISPATCH_KEY(Decimal64)
        DISPATCH_KEY(Decimal128)
        DISPATCH_KEY(Decimal256)

#undef DISPATCH_KEY

        case TypeIndex::Date:
            return std::make_shared<AggregateFunctionSumMapTyped<UInt16>>(
                keys_type, values_types, argument_types, overflow, tuple_argument, op, keys_filter);
        case TypeIndex::DateTime:
            return std::make_shared<AggregateFunctionSumMapTyped<UInt32>>(
                keys_type, values_types, argument_types, overflow, tuple_argument, op, keys_filter);
        case TypeIndex::Enum8:
            return std::make_shared<AggregateFunctionSumMapTyped<Int8>>(
                keys_type, values_types, argument_types, overflow, tuple_argument, op, keys_filter);
        case TypeIndex::Enum16:
            return std::make_shared<AggregateFunctionSumMapTyped<Int16>>(
                keys_type, values_types, argument_types, overflow, tuple_argument, op, keys_filter);

        case TypeIndex::String:
        case TypeIndex::FixedString:
            return std::make_shared<AggregateFunctionSumMapTyped<std::string_view>>(
                keys_type, values_types, argument_types, overflow, tuple_argument, op, keys_filter);

        default:
            return nullptr;
    }
}

}
