#include <Interpreters/AggregatedDataVariants.h>
#include <Interpreters/Aggregator.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Poco/Logger.h>
#include <Common/HashTable/HashTableTraits.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
    extern const Event AggregationPreallocatedElementsInHashTables;
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
auto constructWithReserveIfPossible(size_t size_hint)
{
    if constexpr (HasConstructorOfNumberOfElements<typename Method::Data>::value)
    {
        ProfileEvents::increment(ProfileEvents::AggregationPreallocatedElementsInHashTables, size_hint);
        return std::make_unique<Method>(size_hint);
    }
    else
        return std::make_unique<Method>();
}

void AggregatedDataVariants::init(Type type_, std::optional<size_t> size_hint)
{
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
    switch (type)
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

void AggregatedDataVariants::convertToTwoLevel()
{
    if (aggregator)
        LOG_TRACE(aggregator->log, "Converting aggregation data to two-level.");

    switch (type)
    {
#define M(NAME) \
        case Type::NAME: \
            NAME ## _two_level = std::make_unique<decltype(NAME ## _two_level)::element_type>(*(NAME)); \
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

    /// If single string key - will use hash table with references to it. Strings itself are stored separately in Arena.
    if (keys_size == 1 && isString(types_removed_nullable[0]))
    {
        if (has_low_cardinality)
            return Type::low_cardinality_key_string;
        return Type::key_string;
    }

    if (keys_size > 1 && all_keys_are_numbers_or_strings)
        return Type::prealloc_serialized;

    return Type::serialized;
}

}
