#include <Interpreters/AggregatedDataVariants.h>
#include <Interpreters/Aggregator.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Poco/Logger.h>
#include <Common/HashTable/HashTableTraits.h>
#include <Common/MemoryTracker.h>
#include <Common/logger_useful.h>

#include <bit>
#include <optional>

namespace ProfileEvents
{
    extern const Event AggregationPreallocatedElementsInHashTables;
    extern const Event AggregationConvertedToTwoLevel;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
    extern const int LOGICAL_ERROR;

}
using ColumnsHashing::HashMethodContext;
using ColumnsHashing::HashMethodContextPtr;

AggregatedDataVariants::AggregatedDataVariants() : aggregates_pools(1, std::make_shared<Arena>()), aggregates_pool(aggregates_pools.back().get()) {}

AggregatedDataVariants::~AggregatedDataVariants()
{
    if (aggregator && !aggregator->all_aggregates_has_trivial_destructor)
    {
        try
        {
            aggregator->destroyAllAggregateStates(*this);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

template <typename Method>
auto constructWithReserveIfPossible(AggregatedDataVariants::SizeHint size_hint)
{
    if constexpr (HasConstructorOfNumberOfElements<typename Method::Data>::value)
    {
        ProfileEvents::increment(ProfileEvents::AggregationPreallocatedElementsInHashTables, size_hint.keys);
        return std::make_unique<Method>(size_hint.keys);
    }
    else if constexpr (is_keys_and_packed_string<typename Method::Data::key_type>)
    {
        if (!size_hint.keys_with_empty_string)
            return std::make_unique<Method>();
        ProfileEvents::increment(ProfileEvents::AggregationPreallocatedElementsInHashTables, size_hint.keys);
        return std::make_unique<Method>(size_hint.keys, *size_hint.keys_with_empty_string);
    }
    else
        return std::make_unique<Method>();
}

void AggregatedDataVariants::init(Type type_, std::optional<SizeHint> size_hint)
{
    /// `init` also reinitializes a table after an external-aggregation flush, so preserve
    /// whether the heap rejected anything before the old method is replaced.
    top_k_heap_ever_rejected = topKHeapEverRejected();

    switch (type_)
    {
        case Type::EMPTY:
        case Type::without_key:
            break;

    #define M(NAME, IS_TWO_LEVEL) \
        case Type::NAME: \
            if (size_hint) \
                (NAME) = constructWithReserveIfPossible<decltype(NAME)::element_type>(*size_hint); \
            else \
                (NAME) = std::make_unique<decltype(NAME)::element_type>(); \
            break;
        APPLY_FOR_AGGREGATED_VARIANTS(M)
#undef M
    }

    type = type_;
}

size_t AggregatedDataVariants::size() const
{
    switch (type)
    {
        case Type::EMPTY:
            return 0;
        case Type::without_key:
            return 1;

    #define M(NAME, IS_TWO_LEVEL) \
        case Type::NAME: \
            return (NAME)->data.size() + (without_key != nullptr);
        APPLY_FOR_AGGREGATED_VARIANTS(M)
    #undef M
    }
}

template <typename Method>
size_t sizeWithEmptyStringOf(const Method & method)
{
    if constexpr (is_keys_and_packed_string<typename Method::Data::key_type>)
        return method.data.sizeWithEmptyString();
    else
        return 0;
}

size_t AggregatedDataVariants::sizeWithEmptyString() const
{
    switch (type)
    {
        case Type::EMPTY:
        case Type::without_key:
            return 0;

    #define M(NAME, IS_TWO_LEVEL) \
        case Type::NAME: \
            return sizeWithEmptyStringOf(*(NAME));
        APPLY_FOR_AGGREGATED_VARIANTS(M)
    #undef M
    }
}

bool AggregatedDataVariants::topKHeapEverRejected() const
{
    if (top_k_heap_ever_rejected)
        return true;

    switch (type)
    {
        case Type::EMPTY:
        case Type::without_key:
            return false;

    #define M(NAME, IS_TWO_LEVEL) \
        case Type::NAME: \
            return (NAME)->top_k_heap.everRejected();
        APPLY_FOR_AGGREGATED_VARIANTS(M)
    #undef M
    }
}

bool AggregatedDataVariants::topKHeapInactive() const
{
    switch (type)
    {
        case Type::EMPTY:
        case Type::without_key:
            return false;

    #define M(NAME, IS_TWO_LEVEL) \
        case Type::NAME: \
            return (NAME)->top_k_heap.willBeInactive();
        APPLY_FOR_AGGREGATED_VARIANTS(M)
    #undef M
    }
}

void AggregatedDataVariants::resetAfterStateOwnershipTransfer()
{
    chassert(!aggregator);
    switch (type)
    {
        case Type::EMPTY:
        case Type::without_key:
            break;

    #define M(NAME, IS_TWO_LEVEL) \
        case Type::NAME: \
            (NAME).reset(); \
            break;
        APPLY_FOR_AGGREGATED_VARIANTS(M)
    #undef M
    }
    without_key = nullptr;
    aggregates_pools.clear();
    aggregates_pool = nullptr;
    aggregator = nullptr;
    type = Type::EMPTY;
    restricted_to_kept_keys = false;
    kept_keys_seed = nullptr;
    kept_keys_rebuild_in_progress = false;
}

size_t AggregatedDataVariants::allocatedBytes() const
{
    size_t res = 0;
    for (const auto & pool : aggregates_pools)
        res += pool->allocatedBytes();

    switch (type)
    {
        case Type::EMPTY:
        case Type::without_key:
            break;

    #define M(NAME, IS_TWO_LEVEL) \
        case Type::NAME: \
            res += (NAME)->data.getBufferSizeInBytes(); \
            break;
        APPLY_FOR_AGGREGATED_VARIANTS(M)
    #undef M
    }
    return res;
}

size_t AggregatedDataVariants::usedArenaBytes() const
{
    size_t res = 0;
    for (const auto & pool : aggregates_pools)
        res += pool->usedBytes();
    return res;
}

size_t AggregatedDataVariants::memoryUsage() const
{
    if (memory_tracker)
        return std::max<Int64>(memory_tracker->get(), 0);
    return allocatedBytes();
}

size_t AggregatedDataVariants::sizeWithoutOverflowRow() const
{
    switch (type)
    {
        case Type::EMPTY:
        return 0;
        case Type::without_key:
        return 1;

    #define M(NAME, IS_TWO_LEVEL) \
        case Type::NAME: \
            return (NAME)->data.size();
        APPLY_FOR_AGGREGATED_VARIANTS(M)
    #undef M
    }
}

const char * AggregatedDataVariants::getMethodName() const
{
    switch (type)
    {
        case Type::EMPTY:
        return "EMPTY";
        case Type::without_key:
        return "without_key";

    #define M(NAME, IS_TWO_LEVEL) \
        case Type::NAME: \
            return #NAME;
        APPLY_FOR_AGGREGATED_VARIANTS(M)
    #undef M
    }
}

bool AggregatedDataVariants::isTwoLevel() const
{
    switch (type)
    {
        case Type::EMPTY:
        return false;
        case Type::without_key:
        return false;

    #define M(NAME, IS_TWO_LEVEL) \
        case Type::NAME: \
            return IS_TWO_LEVEL;
        APPLY_FOR_AGGREGATED_VARIANTS(M)
    #undef M
    }
}

bool AggregatedDataVariants::isConvertibleToTwoLevel() const
{
    return isConvertibleToTwoLevel(type);
}

bool AggregatedDataVariants::isConvertibleToTwoLevel(Type type_)
{
    switch (type_)
    {
    #define M(NAME) \
        case Type::NAME: \
            return true;

        APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)

    #undef M
        default:
        return false;
    }
}

bool AggregatedDataVariants::isKeysAndPackedString(Type type_)
{
    switch (type_)
    {
        case Type::keys32_packed_string:
        case Type::keys64_packed_string:
        case Type::keys128_packed_string:
        case Type::keys32_packed_string_two_level:
        case Type::keys64_packed_string_two_level:
        case Type::keys128_packed_string_two_level:
            return true;
        default:
            return false;
    }
}

void AggregatedDataVariants::resetToSingleLevel()
{
    chassert(!hasData());
    switch (type)
    {
#define M(NAME) \
        case Type::NAME ## _two_level: \
            init(Type::NAME); \
            (NAME ## _two_level).reset(); \
            break;

        APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)

    #undef M

        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong data variant passed.");
    }
}

void AggregatedDataVariants::convertToTwoLevel()
{
    ProfileEvents::increment(ProfileEvents::AggregationConvertedToTwoLevel);

    if (aggregator)
        LOG_TRACE(aggregator->log, "Converting aggregation data to two-level.");

    switch (type)
    {
#define M(NAME) \
        case Type::NAME: \
            NAME ## _two_level = std::make_unique<decltype(NAME ## _two_level)::element_type>(*(NAME)); \
            (NAME ## _two_level)->top_k_heap = std::move((NAME)->top_k_heap); \
            (NAME).reset(); \
            type = Type::NAME ## _two_level; \
            break;

        APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)

    #undef M

        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong data variant passed.");
    }
}

bool AggregatedDataVariants::isLowCardinality() const
{
    switch (type)
    {
    #define M(NAME) \
        case Type::NAME: \
            return true;

        APPLY_FOR_LOW_CARDINALITY_VARIANTS(M)
    #undef M
        default:
            return false;
    }
}

bool AggregatedDataVariants::isSerialized() const
{
    switch (type)
    {
        case Type::serialized:
        case Type::nullable_serialized:
        case Type::prealloc_serialized:
        case Type::nullable_prealloc_serialized:
        case Type::serialized_two_level:
        case Type::nullable_serialized_two_level:
        case Type::prealloc_serialized_two_level:
        case Type::nullable_prealloc_serialized_two_level:
        case Type::serialized_hash64:
        case Type::nullable_serialized_hash64:
        case Type::prealloc_serialized_hash64:
        case Type::nullable_prealloc_serialized_hash64:
        case Type::serialized_void:
        case Type::nullable_serialized_void:
        case Type::prealloc_serialized_void:
        case Type::nullable_prealloc_serialized_void:
        case Type::serialized_void_two_level:
        case Type::nullable_serialized_void_two_level:
        case Type::prealloc_serialized_void_two_level:
        case Type::nullable_prealloc_serialized_void_two_level:
        case Type::serialized_void_hash64:
        case Type::nullable_serialized_void_hash64:
        case Type::prealloc_serialized_void_hash64:
        case Type::nullable_prealloc_serialized_void_hash64:
            return true;
        default:
            return false;
    }
}

HashMethodContextPtr AggregatedDataVariants::createCache(Type type, const HashMethodContext::Settings & settings)
{
    switch (type)
    {
        case Type::without_key:
            return nullptr;

    #define M(NAME, IS_TWO_LEVEL) \
        case Type::NAME: { \
            using TPtr##NAME = decltype(AggregatedDataVariants::NAME); \
            using T##NAME = typename TPtr##NAME ::element_type; \
            return T##NAME ::State::createContext(settings); \
        }

            APPLY_FOR_AGGREGATED_VARIANTS(M)
    #undef M

        default:
            throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant.");
    }
}

AggregatedDataVariants::Type AggregatedDataVariants::chooseMethod(
    const Block & header, const Names & keys, Sizes & out_key_sizes)
{
    const size_t keys_size = keys.size();

    /// If no keys. All aggregating to single row.
    if (keys_size == 0)
        return Type::without_key;

    /// Check if at least one of the specified keys is nullable.
    DataTypes types_removed_nullable;
    types_removed_nullable.reserve(keys_size);
    bool has_nullable_key = false;
    bool has_low_cardinality = false;

    for (const auto & key : keys)
    {
        DataTypePtr type = header.getByName(key).type;

        if (type->lowCardinality())
        {
            has_low_cardinality = true;
            type = removeLowCardinality(type);
        }

        if (type->isNullable())
        {
            has_nullable_key = true;
            type = removeNullable(type);
        }

        types_removed_nullable.push_back(type);
    }

    /** Returns ordinary (not two-level) methods, because we start from them.
      * Later, during aggregation process, data may be converted (partitioned) to two-level structure, if cardinality is high.
      */

    size_t keys_bytes = 0;
    size_t num_fixed_contiguous_keys = 0;

    out_key_sizes.resize(keys_size);
    for (size_t j = 0; j < keys_size; ++j)
    {
        if (types_removed_nullable[j]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
        {
            if (types_removed_nullable[j]->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
            {
                ++num_fixed_contiguous_keys;
                out_key_sizes[j] = types_removed_nullable[j]->getSizeOfValueInMemory();
                keys_bytes += out_key_sizes[j];
            }
        }
    }

    bool all_keys_are_numbers_or_strings = true;
    for (size_t j = 0; j < keys_size; ++j)
    {
        if (!types_removed_nullable[j]->isValueRepresentedByNumber() && !isString(types_removed_nullable[j])
            && !isFixedString(types_removed_nullable[j]))
        {
            all_keys_are_numbers_or_strings = false;
            break;
        }
    }

    if (has_nullable_key)
    {
        /// Optimization for one key
        if (keys_size == 1 && !has_low_cardinality)
        {
            if (types_removed_nullable[0]->isValueRepresentedByNumber())
            {
                size_t size_of_field = types_removed_nullable[0]->getSizeOfValueInMemory();
                if (size_of_field == 1)
                    return Type::nullable_key8;
                if (size_of_field == 2)
                    return Type::nullable_key16;
                if (size_of_field == 4)
                    return Type::nullable_key32;
                if (size_of_field == 8)
                    return Type::nullable_key64;
            }
            if (isFixedString(types_removed_nullable[0]))
            {
                return Type::nullable_key_fixed_string;
            }
            if (isString(types_removed_nullable[0]))
            {
                return Type::nullable_key_string;
            }
        }

        if (keys_size == num_fixed_contiguous_keys && !has_low_cardinality)
        {
            /// Pack if possible all the keys along with information about which key values are nulls
            /// into a fixed 16- or 32-byte blob.
            if (std::tuple_size_v<KeysNullMap<UInt128>> + keys_bytes <= 16)
                return Type::nullable_keys128;
            if (std::tuple_size_v<KeysNullMap<UInt256>> + keys_bytes <= 32)
                return Type::nullable_keys256;
        }

        if (has_low_cardinality && keys_size == 1)
        {
            if (types_removed_nullable[0]->isValueRepresentedByNumber())
            {
                size_t size_of_field = types_removed_nullable[0]->getSizeOfValueInMemory();

                if (size_of_field == 1)
                    return Type::low_cardinality_key8;
                if (size_of_field == 2)
                    return Type::low_cardinality_key16;
                if (size_of_field == 4)
                    return Type::low_cardinality_key32;
                if (size_of_field == 8)
                    return Type::low_cardinality_key64;
            }
            else if (isString(types_removed_nullable[0]))
                return Type::low_cardinality_key_string;
            else if (isFixedString(types_removed_nullable[0]))
                return Type::low_cardinality_key_fixed_string;
        }

        if (keys_size > 1 && all_keys_are_numbers_or_strings)
            return Type::nullable_prealloc_serialized;

        /// Fallback case.
        return Type::nullable_serialized;
    }

    /// No key has been found to be nullable.

    /// Single numeric key.
    if (keys_size == 1 && types_removed_nullable[0]->isValueRepresentedByNumber())
    {
        size_t size_of_field = types_removed_nullable[0]->getSizeOfValueInMemory();

        if (has_low_cardinality)
        {
            if (size_of_field == 1)
                return Type::low_cardinality_key8;
            if (size_of_field == 2)
                return Type::low_cardinality_key16;
            if (size_of_field == 4)
                return Type::low_cardinality_key32;
            if (size_of_field == 8)
                return Type::low_cardinality_key64;
            if (size_of_field == 16)
                return Type::low_cardinality_keys128;
            if (size_of_field == 32)
                return Type::low_cardinality_keys256;
            throw Exception(ErrorCodes::LOGICAL_ERROR, "LowCardinality numeric column has sizeOfField not in 1, 2, 4, 8, 16, 32.");
        }

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

    if (keys_size == 1 && isFixedString(types_removed_nullable[0]))
    {
        if (has_low_cardinality)
            return Type::low_cardinality_key_fixed_string;
        return Type::key_fixed_string;
    }

    /// If all keys fits in N bits, will use hash table with all keys packed (placed contiguously) to single N-bit key.
    if (keys_size == num_fixed_contiguous_keys)
    {
        if (has_low_cardinality)
        {
            if (keys_bytes <= 16)
                return Type::low_cardinality_keys128;
            if (keys_bytes <= 32)
                return Type::low_cardinality_keys256;
        }

        if (keys_bytes <= 2)
            return Type::keys16;
        if (keys_bytes <= 4)
            return Type::keys32;
        if (keys_bytes <= 8)
            return Type::keys64;
        if (keys_bytes <= 16)
            return Type::keys128;
        if (keys_bytes <= 32)
            return Type::keys256;
    }

    /// If single string key - will use hash table with 16-byte packed string references. Strings that do not fit
    /// inline are stored separately in Arena. The Aggregator may remap this to the legacy `key_string` method
    /// (see `Params::enable_packed_string_keys`).
    if (keys_size == 1 && isString(types_removed_nullable[0]))
    {
        if (has_low_cardinality)
            return Type::low_cardinality_key_string;
        return Type::key_packed_string;
    }

    /// If fixed-width keys of 1, 2, 4, 8 or 16 bytes and one string key - will use a hash table keyed by the
    /// fixed-width values packed into 32, 64 or 128 bits and a packed reference to the string, which keeps
    /// strings of up to 15 bytes inline after 32 bits and of up to 11 bytes otherwise (see
    /// `KeysAndPackedString`). The string key is the one with value size 0. The Aggregator may remap this to
    /// `prealloc_serialized` (see `Params::enable_fixed_and_packed_string_keys`). `LowCardinality` keys take
    /// these methods as well (see `HashMethodKeysAndPackedString`): with another method, the shards of a
    /// distributed query that store a column as `String` and as `LowCardinality(String)` would number the
    /// two-level buckets differently and return a group once per numbering. A `LowCardinality(Nullable)` key
    /// is nullable, so it took a nullable method above.
    if (keys_size > 1 && num_fixed_contiguous_keys + 1 == keys_size && keys_bytes <= 16)
    {
        std::optional<size_t> string_key;
        bool fixed_sizes_are_powers_of_two = true;
        for (size_t j = 0; j < keys_size; ++j)
        {
            if (isString(types_removed_nullable[j]))
                string_key = j;
            else
                fixed_sizes_are_powers_of_two &= std::has_single_bit(out_key_sizes[j]);
        }

        if (string_key && fixed_sizes_are_powers_of_two)
        {
            out_key_sizes[*string_key] = 0;
            if (keys_bytes <= 4)
                return Type::keys32_packed_string;
            if (keys_bytes <= 8)
                return Type::keys64_packed_string;
            return Type::keys128_packed_string;
        }
    }

    if (keys_size > 1 && all_keys_are_numbers_or_strings)
        return Type::prealloc_serialized;

    return Type::serialized;
}

}
