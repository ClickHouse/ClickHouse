#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Interpreters/SetVariants.h>
#include <base/arithmeticOverflow.h>

#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template <typename Variant>
void SetVariantsTemplate<Variant>::init(Type type_)
{
    /// Allocate before changing `type`: if make_unique throws, the object stays EMPTY
    /// instead of having `type != EMPTY` with a null variant pointer (matches AggregatedDataVariants::init).
    switch (type_)
    {
        case Type::EMPTY: break;

    #define M(NAME) \
        case Type::NAME: (NAME) = std::make_unique<typename decltype(NAME)::element_type>(); break;
        APPLY_FOR_SET_VARIANTS(M)
    #undef M
    }

    type = type_;
}

template <typename Variant>
size_t SetVariantsTemplate<Variant>::estimateGrowthMemory(const ColumnRawPtrs & key_columns, size_t start_row, size_t num_rows) const
    requires std::is_same_v<Variant, NonClearableSet>
{
    chassert(type != Type::EMPTY);

    size_t arena_growth_memory = 0;
    if (type == Type::key_string || type == Type::key_fixed_string)
    {
        chassert(key_columns.size() == 1);
        size_t key_bytes = 0;
        if (type == Type::key_string)
        {
            const auto & offsets = assert_cast<const ColumnString &>(*key_columns.front()).getOffsets();
            key_bytes = num_rows == 0 ? 0 : offsets[start_row + num_rows - 1] - offsets[static_cast<ssize_t>(start_row) - 1];
        }
        else
            key_bytes = num_rows * assert_cast<const ColumnFixedString &>(*key_columns.front()).getN();
        arena_growth_memory = string_pool.estimateGrowthMemory(num_rows, key_bytes);
    }

    auto estimate = [num_rows, arena_growth_memory]<typename Method>(const Method & method) -> size_t
    {
        using Table = typename Method::Data;
        if constexpr (std::is_same_v<Table, FixedHashSet<UInt8>> || std::is_same_v<Table, FixedHashSet<UInt16>>)
            return 0;
        else
        {
            size_t growth_memory = 0;
            if (common::addOverflow(method.data.estimateGrowthMemory(num_rows), arena_growth_memory, growth_memory))
                return std::numeric_limits<size_t>::max();
            return growth_memory;
        }
    };

    switch (type)
    {
        case Type::EMPTY: UNREACHABLE();

    #define M(NAME) case Type::NAME: return estimate(*(NAME));
        APPLY_FOR_SET_VARIANTS(M)
    #undef M
    }
    UNREACHABLE();
}

template <typename Variant>
size_t SetVariantsTemplate<Variant>::estimatePreparedKeysMemory(size_t num_rows, const Sizes & key_sizes) const
{
    chassert(type != Type::EMPTY);

    auto estimate = [num_rows, &key_sizes]<typename Method>(const Method &) -> size_t
    {
        if constexpr (requires { Method::State::estimatePreparedKeysMemory(num_rows, key_sizes); })
            return Method::State::estimatePreparedKeysMemory(num_rows, key_sizes);
        else
            return 0;
    };

    switch (type)
    {
        case Type::EMPTY: UNREACHABLE();

    #define M(NAME) case Type::NAME: return estimate(*(NAME));
        APPLY_FOR_SET_VARIANTS(M)
    #undef M
    }
    UNREACHABLE();
}

template <typename Variant>
size_t SetVariantsTemplate<Variant>::getTotalRowCount() const
{
    switch (type)
    {
        case Type::EMPTY: return 0;

    #define M(NAME) \
        case Type::NAME: return (NAME)->data.size();
        APPLY_FOR_SET_VARIANTS(M)
    #undef M
    }
}

template <typename Variant>
size_t SetVariantsTemplate<Variant>::getTotalByteCount() const
{
    /// String keys are stored in the string_pool arena, not in the hash table buffer.
    size_t bytes = string_pool.allocatedBytes();
    switch (type)
    {
        case Type::EMPTY: break;

    #define M(NAME) \
        case Type::NAME: bytes += (NAME)->data.getBufferSizeInBytes(); break;
        APPLY_FOR_SET_VARIANTS(M)
    #undef M
    }
    return bytes;
}

template <typename Variant>
typename SetVariantsTemplate<Variant>::Type SetVariantsTemplate<Variant>::chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes)
{
    /// Check if at least one of the specified keys is nullable.
    /// Create a set of nested key columns from the corresponding key columns.
    /// Here "nested" means that, if a key column is nullable, we take its nested
    /// column; otherwise we take the key column as is.
    ColumnRawPtrs nested_key_columns;
    nested_key_columns.reserve(key_columns.size());
    bool has_nullable_key = false;

    for (const auto & col : key_columns)
    {
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(&*col))
        {
            nested_key_columns.push_back(&nullable->getNestedColumn());
            has_nullable_key = true;
        }
        else
            nested_key_columns.push_back(col);
    }

    size_t keys_size = nested_key_columns.size();

    bool all_fixed = true;
    size_t keys_bytes = 0;
    key_sizes.resize(keys_size);
    for (size_t j = 0; j < keys_size; ++j)
    {
        if (!nested_key_columns[j]->isFixedAndContiguous())
        {
            all_fixed = false;
            break;
        }
        key_sizes[j] = nested_key_columns[j]->sizeOfValueIfFixed();
        keys_bytes += key_sizes[j];
    }

    if (has_nullable_key)
    {
        /// At least one key is nullable. Therefore we choose a method
        /// that takes into account this fact.
        if ((keys_size == 1) && (nested_key_columns[0]->isNumeric()))
        {
            /// We have exactly one key and it is nullable. We shall add it a tag
            /// which specifies whether its value is null or not.
            size_t size_of_field = nested_key_columns[0]->sizeOfValueIfFixed();
            if ((size_of_field == 1) || (size_of_field == 2) || (size_of_field == 4) || (size_of_field == 8))
                return Type::nullable_keys128;

            /// Pass to more generic method
        }

        if (all_fixed)
        {
            /// Pack if possible all the keys along with information about which key values are nulls
            /// into a fixed 16- or 32-byte blob.
            if (keys_bytes > (std::numeric_limits<size_t>::max() - std::tuple_size_v<KeysNullMap<UInt128>>))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Aggregator: keys sizes overflow");
            if ((std::tuple_size_v<KeysNullMap<UInt128>> + keys_bytes) <= 16)
                return Type::nullable_keys128;
            if ((std::tuple_size_v<KeysNullMap<UInt256>> + keys_bytes) <= 32)
                return Type::nullable_keys256;
        }

        /// Fallback case.
        return Type::hashed;
    }

    /// If there is one numeric key that fits into 64 bits
    if (keys_size == 1 && nested_key_columns[0]->isNumeric() && !nested_key_columns[0]->lowCardinality())
    {
        size_t size_of_field = nested_key_columns[0]->sizeOfValueIfFixed();
        if (size_of_field == 1)
            return Type::key8;
        if (size_of_field == 2)
            return Type::key16;
        if (size_of_field == 4)
            return Type::key32;
        if (size_of_field == 8)
            return Type::key64;
        if (size_of_field == 16)
            return Type::keys128;
        if (size_of_field == 32)
            return Type::keys256;
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Numeric column has sizeOfField not in 1, 2, 4, 8, 16, 32.");
    }

    /// If the keys fit in N bits, we will use a hash table for N-bit-packed keys
    if (all_fixed && keys_bytes <= 4)
        return Type::keys32;
    if (all_fixed && keys_bytes <= 8)
        return Type::keys64;
    if (all_fixed && keys_bytes <= 16)
        return Type::keys128;
    if (all_fixed && keys_bytes <= 32)
        return Type::keys256;

    /// If there is single string key, use hash table of it's values.
    if (keys_size == 1
        && (typeid_cast<const ColumnString *>(nested_key_columns[0])
            || (isColumnConst(*nested_key_columns[0]) && typeid_cast<const ColumnString *>(&assert_cast<const ColumnConst *>(nested_key_columns[0])->getDataColumn()))))
        return Type::key_string;

    if (keys_size == 1 && typeid_cast<const ColumnFixedString *>(nested_key_columns[0]))
        return Type::key_fixed_string;

    /// Otherwise, will use set of cryptographic hashes of unambiguously serialized values.
    return Type::hashed;
}

template struct SetVariantsTemplate<NonClearableSet>;
template struct SetVariantsTemplate<ClearableSet>;
template struct SetVariantsTemplate<CountingSet>;

}
