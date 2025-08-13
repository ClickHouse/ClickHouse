#include <Interpreters/AggregatedDataVariants.h>
#include <Interpreters/Aggregator.h>
#include <Poco/Logger.h>
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

// The std::is_constructible trait isn't suitable here because some classes have template constructors with semantics different from providing size hints.
// Also string hash table variants are not supported due to the fact that both local perf tests and tests in CI showed slowdowns for them.
template <typename...>
struct HasConstructorOfNumberOfElements : std::false_type
{
};

template <typename... Ts>
struct HasConstructorOfNumberOfElements<HashMapTable<Ts...>> : std::true_type
{
};

template <typename Key, typename Cell, typename Hash, typename Grower, typename Allocator, template <typename...> typename ImplTable>
struct HasConstructorOfNumberOfElements<TwoLevelHashMapTable<Key, Cell, Hash, Grower, Allocator, ImplTable>> : std::true_type
{
};

template <typename... Ts>
struct HasConstructorOfNumberOfElements<HashTable<Ts...>> : std::true_type
{
};

template <typename... Ts>
struct HasConstructorOfNumberOfElements<TwoLevelHashTable<Ts...>> : std::true_type
{
};

template <template <typename> typename Method, typename Base>
struct HasConstructorOfNumberOfElements<Method<Base>> : HasConstructorOfNumberOfElements<Base>
{
};

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
}
