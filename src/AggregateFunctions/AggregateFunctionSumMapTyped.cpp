#include <AggregateFunctions/AggregateFunctionSumMapTyped.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>
#include <Common/FieldVisitorSum.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/HashTableAllocator.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/NaNUtils.h>
#include <Common/assert_cast.h>
#include <Core/callOnTypeIndex.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/VarInt.h>
#include <base/EnumReflection.h>

#include <algorithm>
#include <bit>
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

DataTypePtr createSumMapResultType(
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

namespace
{

/// ---- Key Traits ----

/// Concept: key types that HashMap can store by value (trivially copyable, non-string).
/// Covers integers, floats (including BFloat16), decimals, and enum underlying types.
template <typename T>
concept SumMapDirectKey = std::is_trivially_copyable_v<T> && !std::is_same_v<T, std::string_view>;

/// Each group of `GROUP BY` has its own map, and most maps have few keys.
/// Keep the first few cells inside the state, so a small map needs no heap allocation.
constexpr size_t SUM_MAP_INITIAL_SIZE_DEGREE = 2;

/// `HashTableGrower` takes one byte, and `HashTableGrowerWithPrecalculation` takes a cache line.
/// Smaller states are faster when there are many groups.
using SumMapGrower = HashTableGrower<SUM_MAP_INITIAL_SIZE_DEGREE>;

template <typename Cell>
using SumMapAllocator = HashTableAllocatorWithStackMemory<(1ULL << SUM_MAP_INITIAL_SIZE_DEGREE) * sizeof(Cell)>;

template <typename K>
struct SumMapKeyHash
{
    size_t operator()(K key) const
    {
        if constexpr (is_decimal<K>)
            return SumMapKeyHash<typename K::NativeType>{}(key.value);
        else if constexpr (is_big_int_v<K> && sizeof(K) == sizeof(UInt128))
            return UInt128Hash{}(std::bit_cast<UInt128>(key));
        else if constexpr (is_big_int_v<K> && sizeof(K) == sizeof(UInt256))
            return UInt256Hash{}(std::bit_cast<UInt256>(key));
        else
            return DefaultHash<K>{}(key);
    }
};

/// Primary template (unspecialized)
template <typename K>
struct SumMapKeyTraits;

/// Specialization for numeric / trivially-copyable keys
template <SumMapDirectKey K>
struct SumMapKeyTraits<K>
{
    using MapKey = K;
    using MapType = HashMap<K, char *, SumMapKeyHash<K>, SumMapGrower, SumMapAllocator<HashMapCell<K, char *, SumMapKeyHash<K>>>>;
    using FilterSet = HashSet<K, SumMapKeyHash<K>>;

    /// Canonicalize floats: collapse -0.0 → +0.0 and all NaN payloads → quiet_NaN (HashMap uses bitEquals).
    static K normalizeKey(K key)
    {
        if constexpr (is_floating_point<K>)
        {
            if (isNaN(key))
                return NaNOrZero<K>();
            if (key == K(0))
                return K(0);
        }
        return key;
    }

    static K extractKey(const IColumn & key_col, size_t offset)
    {
        if constexpr (is_decimal<K>)
            return normalizeKey(assert_cast<const ColumnDecimal<K> &>(key_col).getData()[offset]);
        else
            return normalizeKey(assert_cast<const ColumnVector<K> &>(key_col).getData()[offset]);
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
    using MapKey = std::string_view;
    using MapType = HashMapWithSavedHash<std::string_view, char *, DefaultHash<std::string_view>, SumMapGrower,
        SumMapAllocator<HashMapCellWithSavedHash<std::string_view, char *, DefaultHash<std::string_view>>>>;
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
/// - Accumulator type may differ from input: sums are accumulated in `NearestFieldType`
///   (e.g. UInt8 -> UInt64), and -WithOverflow narrows them to the input type only on output.
/// - Null bitmap: 1 bit per value column, packed LSB-first per byte.
///     bit = 1: null (no value accumulated yet, initial state)
///     bit = 0: value present (cleared on first non-null input)

/// The value array of one value column in one input row.
struct SumMapRowColumn
{
    const IColumn * data_col;  /// the nested column, without Nullable
    const UInt8 * null_map;    /// the null map of a Nullable column, or nullptr
    size_t vec_offset;         /// the offset of the row's array in `data_col`
};

/// Accumulate elements `[first, first + count)` of one value column of a row into their blocks.
/// `blocks[i]` is the block of the key of element `first + i`, or nullptr to skip that element.
using SumMapAccumulateColumnFn = void (*)(
    char * const * blocks, size_t count, const SumMapRowColumn & column, size_t first,
    size_t value_offset, size_t null_bitmap_offset, size_t col);

/// Merge the accumulators of one value column of `rhs_blocks[i]` into `lhs_blocks[i]`, for each `i < count`.
using SumMapMergeColumnFn = void (*)(
    char * const * lhs_blocks, const char * const * rhs_blocks, size_t count,
    size_t value_offset, size_t null_bitmap_offset, size_t col);

struct SumMapValueColumnDesc
{
    size_t offset;               /// byte offset within accumulator block
    TypeIndex input_type_index;  /// TypeIndex of the input column (to extract values)
    TypeIndex acc_type_index;    /// TypeIndex of the accumulator (may be promoted)
    UInt32 scale;                /// scale of a decimal input column
    bool is_nullable;            /// whether the source column is Nullable

    /// The loops over a batch of blocks for this column's types and the operation. They are
    /// resolved once in the constructor of `SumMapTypedValues`, so that `add` and `merge`
    /// do not dispatch on the type or the operation per element.
    SumMapAccumulateColumnFn accumulate_column;
    SumMapMergeColumnFn merge_column;
};

/// The accumulator type for values of type `In`. It matches the generic path, which accumulates
/// `Field`s of `NearestFieldType`:
/// - Sums (with or without overflow) are kept wide, so that -WithOverflow wraps around only once,
///   on output (`insertNarrowed`), and version-1 states keep the wide sum.
/// - Min/max never leave the input range, so they keep the input type, except that floats are
///   compared as `Float64`.
/// - Decimals are never promoted.
/// Both the block layout and the accumulation derive the accumulator type from this trait, so they
/// cannot disagree, and only these input-to-accumulator pairs are instantiated.
template <typename In, bool is_sum>
using SumMapAccumulator = std::conditional_t<is_decimal<In>, In,
    std::conditional_t<is_sum, NearestFieldType<In>,
        std::conditional_t<is_floating_point<In>, Float64, In>>>;

template <typename F>
void dispatchOnTypeIndex(TypeIndex idx, F && func)
{
    bool found = callOnBasicType<void, true, true, true, false>(idx, [&](auto tag)
    {
        using V = typename decltype(tag)::RightType;
        func(V{});
        return true;
    });
    if (!found)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Unsupported TypeIndex {} in SumMapTyped dispatch", magic_enum::enum_name(idx));
}

/// Null bitmap helpers, see the block layout above.
size_t nullBitmapBytes(size_t num_columns)
{
    return (num_columns + 7) / 8;
}

bool isNullInBitmap(const char * null_bitmap, size_t col)
{
    return (null_bitmap[col / 8] >> (col % 8)) & 1;
}

void clearNullBit(char * null_bitmap, size_t col)
{
    null_bitmap[col / 8] &= ~(1 << (col % 8));
}

template <typename V>
const V * columnData(const IColumn & col)
{
    if constexpr (is_decimal<V>)
        return assert_cast<const ColumnDecimal<V> &>(col).getData().data();
    else
        return assert_cast<const ColumnVector<V> &>(col).getData().data();
}

/// Widen an input value to its accumulator type (see `SumMapAccumulator`).
template <typename Acc, typename In>
Acc widen(In raw)
{
    if constexpr (std::is_same_v<In, Acc>)
        return raw;
    else if constexpr (std::is_same_v<In, BFloat16>)
        return Acc(Float32(raw));
    else
        return static_cast<Acc>(raw);
}

template <SumMapOp op, typename V>
void applyOp(V & acc, V val)
{
    if constexpr (op == SumMapOp::Sum)
    {
        if constexpr (is_signed_v<V> && is_integer<V>)
        {
            using U = make_unsigned_t<V>;
            acc = static_cast<V>(static_cast<U>(acc) + static_cast<U>(val));
        }
        else
        {
            acc += val;
        }
    }
    else
    {
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

        if constexpr (op == SumMapOp::Min)
        {
            if (val < acc)
                acc = val;
        }
        else
        {
            if (val > acc)
                acc = val;
        }
    }
}

/// Apply the aggregation operation on Fields (for deserialization merge).
/// This is a cold path (only used when deserializing into a state that
/// already has data for the same key).
void applyFieldOp(SumMapOp op, Field & acc, const Field & val)
{
    switch (op)
    {
        case SumMapOp::Sum:
            applyVisitor(FieldVisitorSum(val), acc);
            break;
        case SumMapOp::Min:
            if (isNaNField(val))
                return;
            if (isNaNField(acc) || val < acc)
                acc = val;
            break;
        case SumMapOp::Max:
            if (isNaNField(val))
                return;
            if (isNaNField(acc) || val > acc)
                acc = val;
            break;
    }
}

/// A `SumMapAccumulateColumnFn` for input type `In` and operation `op`.
template <typename In, SumMapOp op>
void accumulateColumn(
    char * const * blocks, size_t count, const SumMapRowColumn & column, size_t first,
    size_t value_offset, size_t null_bitmap_offset, size_t col)
{
    using Acc = SumMapAccumulator<In, op == SumMapOp::Sum>;

    const size_t begin = column.vec_offset + first;
    const In * data = columnData<In>(*column.data_col) + begin;
    const UInt8 * null_map = column.null_map ? column.null_map + begin : nullptr;

    for (size_t i = 0; i < count; ++i)
    {
        char * block = blocks[i];
        if (!block || (null_map && null_map[i]))
            continue;

        Acc & acc = *reinterpret_cast<Acc *>(block + value_offset);
        char * null_bitmap = block + null_bitmap_offset;
        if (isNullInBitmap(null_bitmap, col))
        {
            acc = widen<Acc>(data[i]);
            clearNullBit(null_bitmap, col);
        }
        else
        {
            applyOp<op>(acc, widen<Acc>(data[i]));
        }
    }
}

/// A `SumMapMergeColumnFn` for accumulator type `Acc` and operation `op`.
template <typename Acc, SumMapOp op>
void mergeColumn(
    char * const * lhs_blocks, const char * const * rhs_blocks, size_t count,
    size_t value_offset, size_t null_bitmap_offset, size_t col)
{
    for (size_t i = 0; i < count; ++i)
    {
        const char * rhs_block = rhs_blocks[i];
        if (isNullInBitmap(rhs_block + null_bitmap_offset, col))
            continue;

        const Acc rhs_val = *reinterpret_cast<const Acc *>(rhs_block + value_offset);
        char * lhs_block = lhs_blocks[i];
        Acc & lhs_acc = *reinterpret_cast<Acc *>(lhs_block + value_offset);
        char * lhs_null_bitmap = lhs_block + null_bitmap_offset;
        if (isNullInBitmap(lhs_null_bitmap, col))
        {
            lhs_acc = rhs_val;
            clearNullBit(lhs_null_bitmap, col);
        }
        else
        {
            applyOp<op>(lhs_acc, rhs_val);
        }
    }
}

/// Extract a Field from an accumulator slot for serialization
Field fieldFromAccumulator(const SumMapValueColumnDesc & desc, const char * block)
{
    Field result;
    dispatchOnTypeIndex(desc.acc_type_index, [&](auto type_tag)
    {
        using V = decltype(type_tag);
        V val = *reinterpret_cast<const V *>(block + desc.offset);
        if constexpr (is_decimal<V>)
            result = DecimalField<V>(val, desc.scale);
        else
            result = val;
    });
    return result;
}

/// Set an accumulator slot from a Field (used during deserialization)
void setAccumulatorFromField(const SumMapValueColumnDesc & desc, char * block, char * null_bitmap, size_t col, const Field & value)
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

/// Check if accumulator value equals the identity element of the sum (for compaction)
bool isZeroAccumulator(const SumMapValueColumnDesc & desc, const char * block)
{
    bool result = false;
    dispatchOnTypeIndex(desc.acc_type_index, [&](auto type_tag)
    {
        using V = decltype(type_tag);
        V val = *reinterpret_cast<const V *>(block + desc.offset);
        result = (val == V(0));
    });
    return result;
}

template <typename T, typename V>
void pushBackNarrowed(IColumn & col, V val)
{
    assert_cast<ColumnVector<T> &>(col).getData().push_back(static_cast<T>(val));
}

/// -WithOverflow returns the input type, but accumulates in a wider type (see `SumMapAccumulator`).
/// Narrow on output: integers wrap around, as `ColumnVector::insert(Field)` does in the generic path.
/// The cases are listed explicitly to avoid instantiating another type matrix.
template <typename V>
void insertNarrowed(IColumn & col, V val, TypeIndex result_type_index)
{
    if constexpr (std::is_same_v<V, UInt64>)
    {
        switch (result_type_index)
        {
            case TypeIndex::UInt8: pushBackNarrowed<UInt8>(col, val); return;
            case TypeIndex::UInt16: pushBackNarrowed<UInt16>(col, val); return;
            case TypeIndex::UInt32: pushBackNarrowed<UInt32>(col, val); return;
            default: break;
        }
    }
    else if constexpr (std::is_same_v<V, Int64>)
    {
        switch (result_type_index)
        {
            case TypeIndex::Int8: pushBackNarrowed<Int8>(col, val); return;
            case TypeIndex::Int16: pushBackNarrowed<Int16>(col, val); return;
            case TypeIndex::Int32: pushBackNarrowed<Int32>(col, val); return;
            default: break;
        }
    }
    else if constexpr (std::is_same_v<V, Float64>)
    {
        switch (result_type_index)
        {
            case TypeIndex::Float32: pushBackNarrowed<Float32>(col, val); return;
            case TypeIndex::BFloat16: pushBackNarrowed<BFloat16>(col, val); return;
            default: break;
        }
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR,
        "Unexpected narrowing of {} accumulator to {} in SumMapTyped", TypeName<V>, magic_enum::enum_name(result_type_index));
}

/// Insert an accumulator value into the result column
void insertAccumulatorIntoColumn(const SumMapValueColumnDesc & desc, bool overflow, IColumn & col, const char * block)
{
    dispatchOnTypeIndex(desc.acc_type_index, [&](auto type_tag)
    {
        using V = decltype(type_tag);
        V val = *reinterpret_cast<const V *>(block + desc.offset);
        if constexpr (is_decimal<V>)
            assert_cast<ColumnDecimal<V> &>(col).getData().push_back(val);
        else if (overflow && desc.acc_type_index != desc.input_type_index)
            insertNarrowed(col, val, desc.input_type_index);
        else
            assert_cast<ColumnVector<V> &>(col).getData().push_back(val);
    });
}


/// The value side of the typed path: the accumulator block layout above, and all operations on
/// blocks. It does not depend on the key type, so it is not a template: it is compiled once,
/// rather than once per key type like `AggregateFunctionSumMapTyped`.
class SumMapTypedValues
{
public:
    /// The value arrays of one input row, see `prepareRow`.
    using RowColumn = SumMapRowColumn;

    /// The maximum number of blocks that callers pass to `accumulate` and `merge` at once.
    /// Small enough for buffers on the stack, large enough to amortise the per-column call.
    static constexpr size_t batch_size = 256;

    SumMapTypedValues(const DataTypes & values_types, SumMapOp op_, bool overflow_);

    size_t size() const { return value_descs.size(); }
    SumMapOp getOp() const { return op; }
    bool isOverflow() const { return overflow; }

    /// Allocate a block with all values null.
    char * allocateBlock(Arena * arena) const
    {
        char * block = arena->alignedAlloc(block_size, block_alignment);
        memset(block, 0, null_bitmap_offset);
        memset(block + null_bitmap_offset, 0xFF, block_size - null_bitmap_offset);
        return block;
    }

    char * cloneBlock(const char * block, Arena * arena) const
    {
        char * new_block = arena->alignedAlloc(block_size, block_alignment);
        memcpy(new_block, block, block_size);
        return new_block;
    }

    /// Check that each of the `value_columns` arrays has `keys_size` elements in row `row_num`,
    /// and fill `row_columns`, one per value column.
    void prepareRow(const IColumn ** value_columns, size_t row_num, size_t keys_size, RowColumn * row_columns) const;

    /// Accumulate elements `[first, first + count)` of each value array of a row prepared by `prepareRow`.
    /// `blocks[i]` is the block of the key of element `first + i`, or nullptr to skip that element.
    /// Elements are accumulated in order, so repeated keys in a batch are fine.
    void accumulate(char * const * blocks, size_t count, const RowColumn * row_columns, size_t first) const;

    /// Merge `rhs_blocks[i]` into `lhs_blocks[i]` for each `i < count`.
    void merge(char * const * lhs_blocks, const char * const * rhs_blocks, size_t count) const;

    void serialize(const char * block, WriteBuffer & buf, size_t version) const;

    /// Deserialize the values of one key and merge them into `block`.
    void deserialize(char * block, ReadBuffer & buf, size_t version, const FormatSettings & format_settings) const;

    /// sumMap omits keys whose values are all zero or null. Only for `SumMapOp::Sum`.
    bool isOmittedFromResult(const char * block) const;

    /// Add the offsets of a result with `result_size` keys to the value columns of `to`.
    void prepareResult(ColumnTuple & to, size_t result_size) const;

    void insertResult(ColumnTuple & to, const char * block) const;

private:
    std::vector<SumMapValueColumnDesc> value_descs; // STYLE_CHECK_ALLOW_STD_CONTAINERS
    Serializations values_serializations;
    Serializations promoted_values_serializations;
    size_t null_bitmap_offset = 0;  /// offset of null bitmap within block
    size_t block_size = 0;          /// total accumulator block size
    size_t block_alignment = 1;     /// max alignment required across all accumulators
    SumMapOp op;
    bool overflow;
};

SumMapTypedValues::SumMapTypedValues(const DataTypes & values_types, SumMapOp op_, bool overflow_)
    : op(op_)
    , overflow(overflow_)
{
    /// Build value column descriptors
    size_t offset = 0;
    value_descs.reserve(values_types.size());
    for (const auto & type : values_types)
    {
        SumMapValueColumnDesc desc{};
        desc.is_nullable = type->isNullable();
        auto inner_type = removeNullable(type);
        desc.input_type_index = inner_type->getTypeId();
        if (isDecimal(inner_type))
            desc.scale = getDecimalScale(*inner_type);

        size_t size = 0;
        size_t alignment = 1;
        /// The only dispatch on the value type and the operation: everything that depends on them
        /// is resolved here, once per column.
        dispatchOnTypeIndex(desc.input_type_index, [&](auto input_tag)
        {
            using In = decltype(input_tag);
            const auto resolve = [&]<SumMapOp static_op>()
            {
                using Acc = SumMapAccumulator<In, static_op == SumMapOp::Sum>;
                desc.acc_type_index = TypeToTypeIndex<Acc>;
                size = sizeof(Acc);
                alignment = alignof(Acc);
                desc.accumulate_column = &accumulateColumn<In, static_op>;
                desc.merge_column = &mergeColumn<Acc, static_op>;
            };
            switch (op)
            {
                case SumMapOp::Sum: resolve.template operator()<SumMapOp::Sum>(); break;
                case SumMapOp::Min: resolve.template operator()<SumMapOp::Min>(); break;
                case SumMapOp::Max: resolve.template operator()<SumMapOp::Max>(); break;
            }
        });

        /// Align offset to the natural alignment of the accumulator type
        offset = (offset + alignment - 1) / alignment * alignment;
        desc.offset = offset;
        offset += size;
        block_alignment = std::max(block_alignment, alignment);
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

void SumMapTypedValues::prepareRow(const IColumn ** value_columns, size_t row_num, size_t keys_size, RowColumn * row_columns) const
{
    for (size_t col = 0; col < value_descs.size(); ++col)
    {
        const auto & arr_col = assert_cast<const ColumnArray &>(*value_columns[col]);
        const IColumn::Offsets & offsets = arr_col.getOffsets();
        const size_t v_offset = offsets[row_num - 1];
        const size_t v_size = offsets[row_num] - v_offset;

        if (keys_size != v_size)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sizes of keys and values arrays do not match");

        if (value_descs[col].is_nullable)
        {
            const auto & nullable_col = assert_cast<const ColumnNullable &>(arr_col.getData());
            row_columns[col] = {&nullable_col.getNestedColumn(), nullable_col.getNullMapData().data(), v_offset};
        }
        else
        {
            row_columns[col] = {&arr_col.getData(), nullptr, v_offset};
        }
    }
}

void SumMapTypedValues::accumulate(char * const * blocks, size_t count, const RowColumn * row_columns, size_t first) const
{
    for (size_t col = 0; col < value_descs.size(); ++col)
    {
        const auto & desc = value_descs[col];
        desc.accumulate_column(blocks, count, row_columns[col], first, desc.offset, null_bitmap_offset, col);
    }
}

void SumMapTypedValues::merge(char * const * lhs_blocks, const char * const * rhs_blocks, size_t count) const
{
    for (size_t col = 0; col < value_descs.size(); ++col)
    {
        const auto & desc = value_descs[col];
        desc.merge_column(lhs_blocks, rhs_blocks, count, desc.offset, null_bitmap_offset, col);
    }
}

void SumMapTypedValues::serialize(const char * block, WriteBuffer & buf, size_t version) const
{
    const Serializations & serializations = version == 0 ? values_serializations : promoted_values_serializations;
    const char * null_bitmap = block + null_bitmap_offset;

    for (size_t col = 0; col < value_descs.size(); ++col)
    {
        const auto & desc = value_descs[col];

        if (isNullInBitmap(null_bitmap, col))
        {
            serializations[col]->serializeBinary(Field(), buf, {});
            continue;
        }

        Field value = fieldFromAccumulator(desc, block);

        /// Version 1 widens Decimal32 and Decimal64 to Decimal128.
        if (version == 1)
        {
            if (desc.input_type_index == TypeIndex::Decimal32)
            {
                auto source = value.safeGet<DecimalField<Decimal32>>();
                value = DecimalField<Decimal128>(source.getValue(), source.getScale());
            }
            else if (desc.input_type_index == TypeIndex::Decimal64)
            {
                auto source = value.safeGet<DecimalField<Decimal64>>();
                value = DecimalField<Decimal128>(source.getValue(), source.getScale());
            }
        }

        serializations[col]->serializeBinary(value, buf, {});
    }
}

void SumMapTypedValues::deserialize(char * block, ReadBuffer & buf, size_t version, const FormatSettings & format_settings) const
{
    char * null_bitmap = block + null_bitmap_offset;

    for (size_t col = 0; col < value_descs.size(); ++col)
    {
        const auto & desc = value_descs[col];

        Field value;
        if (version == 0)
        {
            values_serializations[col]->deserializeBinary(value, buf, format_settings);
        }
        else
        {
            promoted_values_serializations[col]->deserializeBinary(value, buf, format_settings);

            /// Narrow Decimal128 back to Decimal32/Decimal64
            if (value.getType() == Field::Types::Decimal128)
            {
                auto source = value.safeGet<DecimalField<Decimal128>>();
                if (desc.input_type_index == TypeIndex::Decimal32)
                    value = DecimalField<Decimal32>(source.getValue(), source.getScale());
                else if (desc.input_type_index == TypeIndex::Decimal64)
                    value = DecimalField<Decimal64>(source.getValue(), source.getScale());
            }
        }

        /// Null is a no-op for merge: keep the existing accumulator, null or not.
        if (value.isNull())
            continue;

        if (isNullInBitmap(null_bitmap, col))
        {
            setAccumulatorFromField(desc, block, null_bitmap, col, value);
        }
        else
        {
            /// Merge: extract current, apply op, store back
            Field current = fieldFromAccumulator(desc, block);
            applyFieldOp(op, current, value);
            setAccumulatorFromField(desc, block, null_bitmap, col, current);
        }
    }
}

bool SumMapTypedValues::isOmittedFromResult(const char * block) const
{
    chassert(op == SumMapOp::Sum);

    const char * null_bitmap = block + null_bitmap_offset;
    for (size_t col = 0; col < value_descs.size(); ++col)
    {
        if (isNullInBitmap(null_bitmap, col))
            continue;

        /// For nullable columns, a non-null value always prevents
        /// compaction because the type's default is Null, and any
        /// non-null value (even zero) differs from Null.
        if (value_descs[col].is_nullable)
            return false;

        if (!isZeroAccumulator(value_descs[col], block))
            return false;
    }
    return true;
}

void SumMapTypedValues::prepareResult(ColumnTuple & to, size_t result_size) const
{
    for (size_t col = 0; col < value_descs.size(); ++col)
    {
        auto & to_values_arr = assert_cast<ColumnArray &>(to.getColumn(col + 1));
        auto & to_values_offsets = to_values_arr.getOffsets();
        to_values_offsets.push_back(to_values_offsets.back() + result_size);
        to_values_arr.getData().reserve(result_size);
    }
}

void SumMapTypedValues::insertResult(ColumnTuple & to, const char * block) const
{
    const char * null_bitmap = block + null_bitmap_offset;
    for (size_t col = 0; col < value_descs.size(); ++col)
    {
        auto & to_values_col = assert_cast<ColumnArray &>(to.getColumn(col + 1)).getData();
        if (isNullInBitmap(null_bitmap, col))
            to_values_col.insertDefault();
        else
            insertAccumulatorIntoColumn(value_descs[col], overflow, to_values_col, block);
    }
}


/// ---- State ----

template <typename KeyType>
struct SumMapTypedData
{
    using KT = SumMapKeyTraits<KeyType>;
    typename KT::MapType map;
};


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

    static constexpr auto STATE_VERSION_1_MIN_REVISION = SUMMAP_STATE_VERSION_1_MIN_REVISION;

    DataTypePtr keys_type;
    SerializationPtr keys_serialization;
    SumMapTypedValues values;
    bool tuple_argument;

    /// Key filter for sumMapFiltered / sumMapFilteredWithOverflow.
    bool is_filtered = false;
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
        , values(values_types_, op_, overflow_)
        , tuple_argument(tuple_argument_)
    {
        /// Build key filter set if provided
        if (!keys_filter.empty())
        {
            is_filtered = true;
            if constexpr (std::is_same_v<KeyType, std::string_view>)
            {
                /// For string keys, collect all owned strings first (to avoid
                /// vector reallocation invalidating string_views in the set),
                /// then insert views into the filter set.
                owned_filter_strings.reserve(keys_filter.size());
                for (const auto & f : keys_filter)
                {
                    if (f.getType() != Field::Types::String)
                        continue;
                    owned_filter_strings.emplace_back(keyFromField(f, nullptr));
                }
                for (const auto & s : owned_filter_strings)
                    keys_to_keep.insert(std::string_view(s.data(), s.size()));
            }
            else
            {
                for (const auto & f : keys_filter)
                {
                    auto maybe_key = tryKeyFromField(f);
                    if (maybe_key)
                        keys_to_keep.insert(*maybe_key);
                }
            }
        }
    }

    String getName() const override
    {
        switch (values.getOp())
        {
            case SumMapOp::Sum:
                if (is_filtered)
                    return values.isOverflow() ? "sumMapFilteredWithOverflow" : "sumMapFiltered";
                return values.isOverflow() ? "sumMapWithOverflow" : "sumMap";
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
        const size_t num_columns = values.size();

        /// Use stack storage for typical cases (up to 8 value columns)
        SumMapTypedValues::RowColumn row_columns_buf[8];
        std::unique_ptr<SumMapTypedValues::RowColumn[]> row_columns_heap;
        SumMapTypedValues::RowColumn * row_columns = num_columns <= 8
            ? row_columns_buf
            : (row_columns_heap = std::make_unique<SumMapTypedValues::RowColumn[]>(num_columns)).get();
        values.prepareRow(columns + 1, row_num, keys_vec_size, row_columns);

        /// First find the blocks of a batch of keys, then accumulate the batch one value column at
        /// a time, so that the value type is dispatched once per batch and column, not per element.
        char * blocks[SumMapTypedValues::batch_size];
        for (size_t first = 0; first < keys_vec_size; first += SumMapTypedValues::batch_size)
        {
            const size_t count = std::min(SumMapTypedValues::batch_size, keys_vec_size - first);
            for (size_t i = 0; i < count; ++i)
            {
                auto key = KT::extractKey(key_col, keys_vec_offset + first + i);

                if (is_filtered && !keys_to_keep.has(key))
                {
                    blocks[i] = nullptr;
                    continue;
                }

                auto [it, inserted] = KT::emplace(data.map, key, arena);
                if (inserted)
                {
                    /// `emplace` leaves the mapped value uninitialized. Clear it before the allocation,
                    /// so that the cell holds a null pointer rather than garbage if the allocation throws.
                    it->getMapped() = nullptr;
                    it->getMapped() = values.allocateBlock(arena);
                }

                /// The block lives in the arena, so the pointer stays valid when the map grows.
                blocks[i] = it->getMapped();
            }

            values.accumulate(blocks, count, row_columns, first);
        }
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & lhs_data = this->data(place);
        const auto & rhs_data = this->data(rhs);

        /// Merge the keys present on both sides in batches, see `add`. The keys of `rhs` are unique,
        /// so a batch never has the same block twice.
        char * lhs_blocks[SumMapTypedValues::batch_size];
        const char * rhs_blocks[SumMapTypedValues::batch_size];
        size_t pending = 0;

        for (const auto & [rhs_key, rhs_block] : rhs_data.map)
        {
            auto [it, inserted] = KT::emplaceForMerge(lhs_data.map, rhs_key, arena);

            if (inserted)
            {
                /// See `add`.
                it->getMapped() = nullptr;
                it->getMapped() = values.cloneBlock(rhs_block, arena);
                continue;
            }

            lhs_blocks[pending] = it->getMapped();
            rhs_blocks[pending] = rhs_block;
            if (++pending == SumMapTypedValues::batch_size)
            {
                values.merge(lhs_blocks, rhs_blocks, pending);
                pending = 0;
            }
        }

        values.merge(lhs_blocks, rhs_blocks, pending);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        if (!version)
            version = getDefaultVersion();

        /// The version comes from the AggregateFunction data type parameter, which is
        /// user/data-controlled and not validated at type creation.
        if (*version > 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected version {} of -Map aggregate function serialization state", *version);

        const auto & data = this->data(place);

        writeVarUInt(data.map.size(), buf);

        /// Sort entries by key to produce a canonical byte representation,
        /// independent of hash-map iteration order.
        for (const auto & [key, block] : collectSorted(data, /* compact= */ false))
        {
            keys_serialization->serializeBinary(fieldFromKey(key), buf, {});
            values.serialize(block, buf, *version);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        if (!version)
            version = getDefaultVersion();

        /// The version comes from the AggregateFunction data type parameter, which is
        /// user/data-controlled and not validated at type creation.
        if (*version > 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected version {} of -Map aggregate function serialization state", *version);

        auto & data = this->data(place);

        size_t size = 0;
        readVarUInt(size, buf);

        FormatSettings format_settings;
        /// `Bool` keys and values must be read as integers: the underlying `UInt8` can exceed 1
        /// (e.g. a version 0 state of summed `Bool` values, or keys taken from `sumMapWithOverflow`
        /// over `Bool`), and `SerializationBool` would otherwise collapse every nonzero value to 1.
        /// Same as the generic path.
        format_settings.binary.read_bool_field_as_int = true;

        for (size_t i = 0; i < size; ++i)
        {
            /// Deserialize key using generic serialization for wire compatibility
            Field key_field;
            keys_serialization->deserializeBinary(key_field, buf, format_settings);

            auto key = keyFromField(key_field, arena);
            auto [it, inserted] = KT::emplace(data.map, key, arena);

            if (inserted)
            {
                /// See `add`.
                it->getMapped() = nullptr;
                it->getMapped() = values.allocateBlock(arena);
            }

            values.deserialize(it->getMapped(), buf, *version, format_settings);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const Entries sorted_entries = collectSorted(this->data(place), /* compact= */ values.getOp() == SumMapOp::Sum);
        const size_t result_size = sorted_entries.size();

        auto & to_tuple = assert_cast<ColumnTuple &>(to);
        auto & to_keys_arr = assert_cast<ColumnArray &>(to_tuple.getColumn(0));
        auto & to_keys_col = to_keys_arr.getData();

        auto & to_keys_offsets = to_keys_arr.getOffsets();
        to_keys_offsets.push_back(to_keys_offsets.back() + result_size);
        to_keys_col.reserve(result_size);

        values.prepareResult(to_tuple, result_size);

        for (const auto & [key, block] : sorted_entries)
        {
            KT::insertKeyIntoColumn(to_keys_col, key);
            values.insertResult(to_tuple, block);
        }
    }

private:
    using Entry = std::pair<typename KT::MapKey, char *>;
    using Entries = std::vector<Entry>; // STYLE_CHECK_ALLOW_STD_CONTAINERS

    /// The entries of the map sorted by key. If `compact`, without the keys omitted from the result.
    /// `serialize` and `insertResultInto` share it, so that `std::sort` is instantiated once per key type.
    Entries collectSorted(const Data & data, bool compact) const
    {
        Entries entries;
        entries.reserve(data.map.size());
        for (const auto & [key, block] : data.map)
        {
            if (compact && values.isOmittedFromResult(block))
                continue;
            entries.emplace_back(key, block);
        }
        std::sort(entries.begin(), entries.end(), [](const Entry & a, const Entry & b) { return KT::less(a.first, b.first); });
        return entries;
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
            return KT::normalizeKey(static_cast<typename KT::MapKey>(field.safeGet<NearestFieldType<typename KT::MapKey>>()));
        }
    }

    /// Like keyFromField but returns std::nullopt when the field value is not
    /// representable in MapKey (e.g. a negative Int64 filter for UInt64 keys).
    /// Only called for integral key types (createSumMapTyped routes other types
    /// to the generic path when a filter is present).
    static std::optional<typename KT::MapKey> tryKeyFromField(const Field & field)
    {
        using MapKey = typename KT::MapKey;

        if constexpr (!std::is_integral_v<MapKey>)
        {
            /// Non-integral keys use the generic (non-typed) path when a filter
            /// is present, so this function is never called at runtime for them.
            (void)field;
            return std::nullopt;
        }
        else
        {
            if (field.getType() == Field::Types::UInt64)
            {
                auto v = field.safeGet<UInt64>();
                if constexpr (sizeof(MapKey) < sizeof(UInt64) || std::is_signed_v<MapKey>)
                {
                    if (v > static_cast<UInt64>(std::numeric_limits<MapKey>::max()))
                        return std::nullopt;
                }
                return static_cast<MapKey>(v);
            }
            if (field.getType() == Field::Types::Int64)
            {
                auto v = field.safeGet<Int64>();
                if constexpr (std::is_unsigned_v<MapKey>)
                {
                    if (v < 0 || static_cast<UInt64>(v) > static_cast<UInt64>(std::numeric_limits<MapKey>::max()))
                        return std::nullopt;
                }
                else if constexpr (sizeof(MapKey) < sizeof(Int64))
                {
                    if (v < static_cast<Int64>(std::numeric_limits<MapKey>::min())
                        || v > static_cast<Int64>(std::numeric_limits<MapKey>::max()))
                        return std::nullopt;
                }
                return static_cast<MapKey>(v);
            }
            return std::nullopt;
        }
    }
};

}

bool isSumMapTypedKeySupported(TypeIndex idx)
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

bool areSumMapTypedValuesSupported(const DataTypes & values_types)
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

AggregateFunctionPtr createSumMapTyped(
    const DataTypePtr & keys_type,
    const DataTypes & values_types,
    const DataTypes & argument_types,
    bool overflow,
    bool tuple_argument,
    SumMapOp op,
    const Array & keys_filter)
{
    TypeIndex key_idx = keys_type->getTypeId();

    /// Fall back to the generic (Field-based) path for filtered keys
    /// to avoid problematic mismatches.
    if (!keys_filter.empty())
    {
        switch (key_idx)
        {
            case TypeIndex::Decimal32:
            case TypeIndex::Decimal64:
            case TypeIndex::Decimal128:
            case TypeIndex::Decimal256:
            case TypeIndex::Int128:
            case TypeIndex::UInt128:
            case TypeIndex::Int256:
            case TypeIndex::UInt256:
            case TypeIndex::Float32:
            case TypeIndex::Float64:
            case TypeIndex::BFloat16:
                return nullptr;
            default:
                break;
        }
    }

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
