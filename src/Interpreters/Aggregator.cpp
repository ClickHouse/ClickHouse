#include <algorithm>
#include <future>
#include <numeric>
#include <Poco/Util/Application.h>

#include <base/sort.h>
#include <Common/Stopwatch.h>
#include <Common/setThreadName.h>
#include <Common/formatReadable.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnSparse.h>
#include <Formats/NativeWriter.h>
#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Interpreters/Aggregator.h>
#include <Common/LRUCache.h>
#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Common/JSONBuilder.h>
#include <AggregateFunctions/AggregateFunctionArray.h>
#include <AggregateFunctions/AggregateFunctionState.h>
#include <IO/Operators.h>
#include <Interpreters/JIT/compileFunction.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Core/ProtocolDefines.h>

#include <Parsers/ASTSelectQuery.h>

namespace ProfileEvents
{
extern const Event ExternalAggregationWritePart;
extern const Event ExternalAggregationCompressedBytes;
extern const Event ExternalAggregationUncompressedBytes;
extern const Event AggregationPreallocatedElementsInHashTables;
extern const Event AggregationHashTablesInitializedAsTwoLevel;
}

namespace
{
/** Collects observed HashMap-s sizes to avoid redundant intermediate resizes.
  */
class HashTablesStatistics
{
public:
    struct Entry
    {
        size_t sum_of_sizes; // used to determine if it's better to convert aggregation to two-level from the beginning
        size_t median_size; // roughly the size we're going to preallocate on each thread
    };

    using Cache = DB::LRUCache<UInt64, Entry>;
    using CachePtr = std::shared_ptr<Cache>;
    using Params = DB::Aggregator::Params::StatsCollectingParams;

    /// Collection and use of the statistics should be enabled.
    std::optional<Entry> getSizeHint(const Params & params)
    {
        if (!params.isCollectionAndUseEnabled())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Collection and use of the statistics should be enabled.");

        std::lock_guard lock(mutex);
        const auto cache = getHashTableStatsCache(params, lock);
        if (const auto hint = cache->get(params.key))
        {
            LOG_TRACE(
                &Poco::Logger::get("Aggregator"),
                "An entry for key={} found in cache: sum_of_sizes={}, median_size={}",
                params.key,
                hint->sum_of_sizes,
                hint->median_size);
            return *hint;
        }
        return std::nullopt;
    }

    /// Collection and use of the statistics should be enabled.
    void update(size_t sum_of_sizes, size_t median_size, const Params & params)
    {
        if (!params.isCollectionAndUseEnabled())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Collection and use of the statistics should be enabled.");

        std::lock_guard lock(mutex);
        const auto cache = getHashTableStatsCache(params, lock);
        const auto hint = cache->get(params.key);
        // We'll maintain the maximum among all the observed values until the next prediction turns out to be too wrong.
        if (!hint || sum_of_sizes < hint->sum_of_sizes / 2 || hint->sum_of_sizes < sum_of_sizes || median_size < hint->median_size / 2
            || hint->median_size < median_size)
        {
            LOG_TRACE(
                &Poco::Logger::get("Aggregator"),
                "Statistics updated for key={}: new sum_of_sizes={}, median_size={}",
                params.key,
                sum_of_sizes,
                median_size);
            cache->set(params.key, std::make_shared<Entry>(Entry{.sum_of_sizes = sum_of_sizes, .median_size = median_size}));
        }
    }

    std::optional<DB::HashTablesCacheStatistics> getCacheStats() const
    {
        std::lock_guard lock(mutex);
        if (hash_table_stats)
        {
            size_t hits = 0, misses = 0;
            hash_table_stats->getStats(hits, misses);
            return DB::HashTablesCacheStatistics{.entries = hash_table_stats->count(), .hits = hits, .misses = misses};
        }
        return std::nullopt;
    }

    static size_t calculateCacheKey(const DB::ASTPtr & select_query)
    {
        if (!select_query)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Query ptr cannot be null");

        const auto & select = select_query->as<DB::ASTSelectQuery &>();

        // It may happen in some corner cases like `select 1 as num group by num`.
        if (!select.tables())
            return 0;

        SipHash hash;
        hash.update(select.tables()->getTreeHash());
        if (const auto where = select.where())
            hash.update(where->getTreeHash());
        if (const auto group_by = select.groupBy())
            hash.update(group_by->getTreeHash());
        return hash.get64();
    }

private:
    CachePtr getHashTableStatsCache(const Params & params, const std::lock_guard<std::mutex> &)
    {
        if (!hash_table_stats || hash_table_stats->maxSize() != params.max_entries_for_hash_table_stats)
            hash_table_stats = std::make_shared<Cache>(params.max_entries_for_hash_table_stats);
        return hash_table_stats;
    }

    mutable std::mutex mutex;
    CachePtr hash_table_stats;
};

HashTablesStatistics & getHashTablesStatistics()
{
    static HashTablesStatistics hash_tables_stats;
    return hash_tables_stats;
}

bool worthConvertToTwoLevel(
    size_t group_by_two_level_threshold, size_t result_size, size_t group_by_two_level_threshold_bytes, auto result_size_bytes)
{
    // params.group_by_two_level_threshold will be equal to 0 if we have only one thread to execute aggregation (refer to AggregatingStep::transformPipeline).
    return (group_by_two_level_threshold && result_size >= group_by_two_level_threshold)
        || (group_by_two_level_threshold_bytes && result_size_bytes >= static_cast<Int64>(group_by_two_level_threshold_bytes));
}

DB::AggregatedDataVariants::Type convertToTwoLevelTypeIfPossible(DB::AggregatedDataVariants::Type type)
{
    using Type = DB::AggregatedDataVariants::Type;
    switch (type)
    {
#define M(NAME) \
    case Type::NAME: \
        return Type::NAME##_two_level;
        APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)
#undef M
        default:
            return type;
    }
    __builtin_unreachable();
}

void initDataVariantsWithSizeHint(
    DB::AggregatedDataVariants & result, DB::AggregatedDataVariants::Type method_chosen, const DB::Aggregator::Params & params)
{
    const auto & stats_collecting_params = params.stats_collecting_params;
    if (stats_collecting_params.isCollectionAndUseEnabled())
    {
        if (auto hint = getHashTablesStatistics().getSizeHint(stats_collecting_params))
        {
            const auto max_threads = params.group_by_two_level_threshold != 0 ? std::max(params.max_threads, 1ul) : 1;
            const auto lower_limit = hint->sum_of_sizes / max_threads;
            const auto upper_limit = stats_collecting_params.max_size_to_preallocate_for_aggregation / max_threads;
            const auto adjusted = std::min(std::max(lower_limit, hint->median_size), upper_limit);
            if (worthConvertToTwoLevel(
                    params.group_by_two_level_threshold,
                    hint->sum_of_sizes,
                    /*group_by_two_level_threshold_bytes*/ 0,
                    /*result_size_bytes*/ 0))
                method_chosen = convertToTwoLevelTypeIfPossible(method_chosen);
            result.init(method_chosen, adjusted);
            ProfileEvents::increment(ProfileEvents::AggregationHashTablesInitializedAsTwoLevel, result.isTwoLevel());
            return;
        }
    }
    result.init(method_chosen);
}

/// Collection and use of the statistics should be enabled.
void updateStatistics(const DB::ManyAggregatedDataVariants & data_variants, const DB::Aggregator::Params::StatsCollectingParams & params)
{
    if (!params.isCollectionAndUseEnabled())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Collection and use of the statistics should be enabled.");

    std::vector<size_t> sizes(data_variants.size());
    for (size_t i = 0; i < data_variants.size(); ++i)
        sizes[i] = data_variants[i]->size();
    const auto median_size = sizes.begin() + sizes.size() / 2; // not precisely though...
    std::nth_element(sizes.begin(), median_size, sizes.end());
    const auto sum_of_sizes = std::accumulate(sizes.begin(), sizes.end(), 0ull);
    getHashTablesStatistics().update(sum_of_sizes, *median_size, params);
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

DB::ColumnNumbers calculateKeysPositions(const DB::Block & header, const DB::Aggregator::Params & params)
{
    DB::ColumnNumbers keys_positions(params.keys_size);
    for (size_t i = 0; i < params.keys_size; ++i)
        keys_positions[i] = header.getPositionByName(params.keys[i]);
    return keys_positions;
}
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
    extern const int NOT_ENOUGH_SPACE;
    extern const int TOO_MANY_ROWS;
    extern const int EMPTY_DATA_PASSED;
    extern const int CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS;
    extern const int LOGICAL_ERROR;
}


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

std::optional<HashTablesCacheStatistics> getHashTablesCacheStatistics()
{
    return getHashTablesStatistics().getCacheStats();
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
            throw Exception("Wrong data variant passed.", ErrorCodes::LOGICAL_ERROR);
    }
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

Aggregator::Params::StatsCollectingParams::StatsCollectingParams() = default;

Aggregator::Params::StatsCollectingParams::StatsCollectingParams(
    const ASTPtr & select_query_,
    bool collect_hash_table_stats_during_aggregation_,
    size_t max_entries_for_hash_table_stats_,
    size_t max_size_to_preallocate_for_aggregation_)
    : key(collect_hash_table_stats_during_aggregation_ ? HashTablesStatistics::calculateCacheKey(select_query_) : 0)
    , max_entries_for_hash_table_stats(max_entries_for_hash_table_stats_)
    , max_size_to_preallocate_for_aggregation(max_size_to_preallocate_for_aggregation_)
{
}

Block Aggregator::getHeader(bool final) const
{
    return params.getHeader(header, final);
}

Block Aggregator::Params::getHeader(
    const Block & header, bool only_merge, const Names & keys, const AggregateDescriptions & aggregates, bool final)
{
    Block res;

    if (only_merge)
    {
        NameSet needed_columns(keys.begin(), keys.end());
        for (const auto & aggregate : aggregates)
            needed_columns.emplace(aggregate.column_name);

        for (const auto & column : header)
        {
            if (needed_columns.contains(column.name))
                res.insert(column.cloneEmpty());
        }

        if (final)
        {
            for (const auto & aggregate : aggregates)
            {
                auto & elem = res.getByName(aggregate.column_name);

                elem.type = aggregate.function->getReturnType();
                elem.column = elem.type->createColumn();
            }
        }
    }
    else
    {
        for (const auto & key : keys)
            res.insert(header.getByName(key).cloneEmpty());

        for (const auto & aggregate : aggregates)
        {
            size_t arguments_size = aggregate.argument_names.size();
            DataTypes argument_types(arguments_size);
            for (size_t j = 0; j < arguments_size; ++j)
                argument_types[j] = header.getByName(aggregate.argument_names[j]).type;

            DataTypePtr type;
            if (final)
                type = aggregate.function->getReturnType();
            else
                type = std::make_shared<DataTypeAggregateFunction>(aggregate.function, argument_types, aggregate.parameters);

            res.insert({ type, aggregate.column_name });
        }
    }

    return materializeBlock(res);
}

ColumnRawPtrs Aggregator::Params::makeRawKeyColumns(const Block & block) const
{
    ColumnRawPtrs key_columns(keys_size);

    for (size_t i = 0; i < keys_size; ++i)
        key_columns[i] = block.safeGetByPosition(i).column.get();

    return key_columns;
}

Aggregator::AggregateColumnsConstData Aggregator::Params::makeAggregateColumnsData(const Block & block) const
{
    AggregateColumnsConstData aggregate_columns(aggregates_size);

    for (size_t i = 0; i < aggregates_size; ++i)
    {
        const auto & aggregate_column_name = aggregates[i].column_name;
        aggregate_columns[i] = &typeid_cast<const ColumnAggregateFunction &>(*block.getByName(aggregate_column_name).column).getData();
    }

    return aggregate_columns;
}

void Aggregator::Params::explain(WriteBuffer & out, size_t indent) const
{
    Strings res;
    String prefix(indent, ' ');

    {
        /// Dump keys.
        out << prefix << "Keys: ";

        bool first = true;
        for (const auto & key : keys)
        {
            if (!first)
                out << ", ";
            first = false;

            out << key;
        }

        out << '\n';
    }

    if (!aggregates.empty())
    {
        out << prefix << "Aggregates:\n";

        for (const auto & aggregate : aggregates)
            aggregate.explain(out, indent + 4);
    }
}

void Aggregator::Params::explain(JSONBuilder::JSONMap & map) const
{
    auto keys_array = std::make_unique<JSONBuilder::JSONArray>();

    for (const auto & key : keys)
        keys_array->add(key);

    map.add("Keys", std::move(keys_array));

    if (!aggregates.empty())
    {
        auto aggregates_array = std::make_unique<JSONBuilder::JSONArray>();

        for (const auto & aggregate : aggregates)
        {
            auto aggregate_map = std::make_unique<JSONBuilder::JSONMap>();
            aggregate.explain(*aggregate_map);
            aggregates_array->add(std::move(aggregate_map));
        }

        map.add("Aggregates", std::move(aggregates_array));
    }
}

#if USE_EMBEDDED_COMPILER

static CHJIT & getJITInstance()
{
    static CHJIT jit;
    return jit;
}

class CompiledAggregateFunctionsHolder final : public CompiledExpressionCacheEntry
{
public:
    explicit CompiledAggregateFunctionsHolder(CompiledAggregateFunctions compiled_function_)
        : CompiledExpressionCacheEntry(compiled_function_.compiled_module.size)
        , compiled_aggregate_functions(compiled_function_)
    {}

    ~CompiledAggregateFunctionsHolder() override
    {
        getJITInstance().deleteCompiledModule(compiled_aggregate_functions.compiled_module);
    }

    CompiledAggregateFunctions compiled_aggregate_functions;
};

#endif

Aggregator::Aggregator(const Block & header_, const Params & params_)
    : header(header_), keys_positions(calculateKeysPositions(header, params_)), params(params_)
{
    /// Use query-level memory tracker
    if (auto * memory_tracker_child = CurrentThread::getMemoryTracker())
        if (auto * memory_tracker = memory_tracker_child->getParent())
            memory_usage_before_aggregation = memory_tracker->get();

    aggregate_functions.resize(params.aggregates_size);
    for (size_t i = 0; i < params.aggregates_size; ++i)
        aggregate_functions[i] = params.aggregates[i].function.get();

    /// Initialize sizes of aggregation states and its offsets.
    offsets_of_aggregate_states.resize(params.aggregates_size);
    total_size_of_aggregate_states = 0;
    all_aggregates_has_trivial_destructor = true;

    // aggregate_states will be aligned as below:
    // |<-- state_1 -->|<-- pad_1 -->|<-- state_2 -->|<-- pad_2 -->| .....
    //
    // pad_N will be used to match alignment requirement for each next state.
    // The address of state_1 is aligned based on maximum alignment requirements in states
    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        offsets_of_aggregate_states[i] = total_size_of_aggregate_states;

        total_size_of_aggregate_states += params.aggregates[i].function->sizeOfData();

        // aggregate states are aligned based on maximum requirement
        align_aggregate_states = std::max(align_aggregate_states, params.aggregates[i].function->alignOfData());

        // If not the last aggregate_state, we need pad it so that next aggregate_state will be aligned.
        if (i + 1 < params.aggregates_size)
        {
            size_t alignment_of_next_state = params.aggregates[i + 1].function->alignOfData();
            if ((alignment_of_next_state & (alignment_of_next_state - 1)) != 0)
                throw Exception("Logical error: alignOfData is not 2^N", ErrorCodes::LOGICAL_ERROR);

            /// Extend total_size to next alignment requirement
            /// Add padding by rounding up 'total_size_of_aggregate_states' to be a multiplier of alignment_of_next_state.
            total_size_of_aggregate_states = (total_size_of_aggregate_states + alignment_of_next_state - 1) / alignment_of_next_state * alignment_of_next_state;
        }

        if (!params.aggregates[i].function->hasTrivialDestructor())
            all_aggregates_has_trivial_destructor = false;
    }

    method_chosen = chooseAggregationMethod();
    HashMethodContext::Settings cache_settings;
    cache_settings.max_threads = params.max_threads;
    aggregation_state_cache = AggregatedDataVariants::createCache(method_chosen, cache_settings);

#if USE_EMBEDDED_COMPILER
    compileAggregateFunctionsIfNeeded();
#endif
}

#if USE_EMBEDDED_COMPILER

void Aggregator::compileAggregateFunctionsIfNeeded()
{
    static std::unordered_map<UInt128, UInt64, UInt128Hash> aggregate_functions_description_to_count;
    static std::mutex mutex;

    if (!params.compile_aggregate_expressions)
        return;

    std::vector<AggregateFunctionWithOffset> functions_to_compile;
    String functions_description;

    is_aggregate_function_compiled.resize(aggregate_functions.size());

    /// Add values to the aggregate functions.
    for (size_t i = 0; i < aggregate_functions.size(); ++i)
    {
        const auto * function = aggregate_functions[i];
        size_t offset_of_aggregate_function = offsets_of_aggregate_states[i];

        if (function->isCompilable())
        {
            AggregateFunctionWithOffset function_to_compile
            {
                .function = function,
                .aggregate_data_offset = offset_of_aggregate_function
            };

            functions_to_compile.emplace_back(std::move(function_to_compile));

            functions_description += function->getDescription();
            functions_description += ' ';

            functions_description += std::to_string(offset_of_aggregate_function);
            functions_description += ' ';
        }

        is_aggregate_function_compiled[i] = function->isCompilable();
    }

    if (functions_to_compile.empty())
        return;

    SipHash aggregate_functions_description_hash;
    aggregate_functions_description_hash.update(functions_description);

    UInt128 aggregate_functions_description_hash_key;
    aggregate_functions_description_hash.get128(aggregate_functions_description_hash_key);

    {
        std::lock_guard<std::mutex> lock(mutex);

        if (aggregate_functions_description_to_count[aggregate_functions_description_hash_key]++ < params.min_count_to_compile_aggregate_expression)
            return;
    }

    if (auto * compilation_cache = CompiledExpressionCacheFactory::instance().tryGetCache())
    {
        auto [compiled_function_cache_entry, _] = compilation_cache->getOrSet(aggregate_functions_description_hash_key, [&] ()
        {
            LOG_TRACE(log, "Compile expression {}", functions_description);

            auto compiled_aggregate_functions = compileAggregateFunctions(getJITInstance(), functions_to_compile, functions_description);
            return std::make_shared<CompiledAggregateFunctionsHolder>(std::move(compiled_aggregate_functions));
        });
        compiled_aggregate_functions_holder = std::static_pointer_cast<CompiledAggregateFunctionsHolder>(compiled_function_cache_entry);
    }
    else
    {
        LOG_TRACE(log, "Compile expression {}", functions_description);
        auto compiled_aggregate_functions = compileAggregateFunctions(getJITInstance(), functions_to_compile, functions_description);
        compiled_aggregate_functions_holder = std::make_shared<CompiledAggregateFunctionsHolder>(std::move(compiled_aggregate_functions));
    }
}

#endif

AggregatedDataVariants::Type Aggregator::chooseAggregationMethod()
{
    /// If no keys. All aggregating to single row.
    if (params.keys_size == 0)
        return AggregatedDataVariants::Type::without_key;

    /// Check if at least one of the specified keys is nullable.
    DataTypes types_removed_nullable;
    types_removed_nullable.reserve(params.keys.size());
    bool has_nullable_key = false;
    bool has_low_cardinality = false;

    for (const auto & key : params.keys)
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

    key_sizes.resize(params.keys_size);
    for (size_t j = 0; j < params.keys_size; ++j)
    {
        if (types_removed_nullable[j]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
        {
            if (types_removed_nullable[j]->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
            {
                ++num_fixed_contiguous_keys;
                key_sizes[j] = types_removed_nullable[j]->getSizeOfValueInMemory();
                keys_bytes += key_sizes[j];
            }
        }
    }

    if (has_nullable_key)
    {
        if (params.keys_size == num_fixed_contiguous_keys && !has_low_cardinality)
        {
            /// Pack if possible all the keys along with information about which key values are nulls
            /// into a fixed 16- or 32-byte blob.
            if (std::tuple_size<KeysNullMap<UInt128>>::value + keys_bytes <= 16)
                return AggregatedDataVariants::Type::nullable_keys128;
            if (std::tuple_size<KeysNullMap<UInt256>>::value + keys_bytes <= 32)
                return AggregatedDataVariants::Type::nullable_keys256;
        }

        if (has_low_cardinality && params.keys_size == 1)
        {
            if (types_removed_nullable[0]->isValueRepresentedByNumber())
            {
                size_t size_of_field = types_removed_nullable[0]->getSizeOfValueInMemory();

                if (size_of_field == 1)
                    return AggregatedDataVariants::Type::low_cardinality_key8;
                if (size_of_field == 2)
                    return AggregatedDataVariants::Type::low_cardinality_key16;
                if (size_of_field == 4)
                    return AggregatedDataVariants::Type::low_cardinality_key32;
                if (size_of_field == 8)
                    return AggregatedDataVariants::Type::low_cardinality_key64;
            }
            else if (isString(types_removed_nullable[0]))
                return AggregatedDataVariants::Type::low_cardinality_key_string;
            else if (isFixedString(types_removed_nullable[0]))
                return AggregatedDataVariants::Type::low_cardinality_key_fixed_string;
        }

        /// Fallback case.
        return AggregatedDataVariants::Type::serialized;
    }

    /// No key has been found to be nullable.

    /// Single numeric key.
    if (params.keys_size == 1 && types_removed_nullable[0]->isValueRepresentedByNumber())
    {
        size_t size_of_field = types_removed_nullable[0]->getSizeOfValueInMemory();

        if (has_low_cardinality)
        {
            if (size_of_field == 1)
                return AggregatedDataVariants::Type::low_cardinality_key8;
            if (size_of_field == 2)
                return AggregatedDataVariants::Type::low_cardinality_key16;
            if (size_of_field == 4)
                return AggregatedDataVariants::Type::low_cardinality_key32;
            if (size_of_field == 8)
                return AggregatedDataVariants::Type::low_cardinality_key64;
        }

        if (size_of_field == 1)
            return AggregatedDataVariants::Type::key8;
        if (size_of_field == 2)
            return AggregatedDataVariants::Type::key16;
        if (size_of_field == 4)
            return AggregatedDataVariants::Type::key32;
        if (size_of_field == 8)
            return AggregatedDataVariants::Type::key64;
        if (size_of_field == 16)
            return AggregatedDataVariants::Type::keys128;
        if (size_of_field == 32)
            return AggregatedDataVariants::Type::keys256;
        throw Exception("Logical error: numeric column has sizeOfField not in 1, 2, 4, 8, 16, 32.", ErrorCodes::LOGICAL_ERROR);
    }

    if (params.keys_size == 1 && isFixedString(types_removed_nullable[0]))
    {
        if (has_low_cardinality)
            return AggregatedDataVariants::Type::low_cardinality_key_fixed_string;
        else
            return AggregatedDataVariants::Type::key_fixed_string;
    }

    /// If all keys fits in N bits, will use hash table with all keys packed (placed contiguously) to single N-bit key.
    if (params.keys_size == num_fixed_contiguous_keys)
    {
        if (has_low_cardinality)
        {
            if (keys_bytes <= 16)
                return AggregatedDataVariants::Type::low_cardinality_keys128;
            if (keys_bytes <= 32)
                return AggregatedDataVariants::Type::low_cardinality_keys256;
        }

        if (keys_bytes <= 2)
            return AggregatedDataVariants::Type::keys16;
        if (keys_bytes <= 4)
            return AggregatedDataVariants::Type::keys32;
        if (keys_bytes <= 8)
            return AggregatedDataVariants::Type::keys64;
        if (keys_bytes <= 16)
            return AggregatedDataVariants::Type::keys128;
        if (keys_bytes <= 32)
            return AggregatedDataVariants::Type::keys256;
    }

    /// If single string key - will use hash table with references to it. Strings itself are stored separately in Arena.
    if (params.keys_size == 1 && isString(types_removed_nullable[0]))
    {
        if (has_low_cardinality)
            return AggregatedDataVariants::Type::low_cardinality_key_string;
        else
            return AggregatedDataVariants::Type::key_string;
    }

    return AggregatedDataVariants::Type::serialized;
}

template <bool skip_compiled_aggregate_functions>
void Aggregator::createAggregateStates(AggregateDataPtr & aggregate_data) const
{
    for (size_t j = 0; j < params.aggregates_size; ++j)
    {
        if constexpr (skip_compiled_aggregate_functions)
            if (is_aggregate_function_compiled[j])
                continue;

        try
        {
            /** An exception may occur if there is a shortage of memory.
              * In order that then everything is properly destroyed, we "roll back" some of the created states.
              * The code is not very convenient.
              */
            aggregate_functions[j]->create(aggregate_data + offsets_of_aggregate_states[j]);
        }
        catch (...)
        {
            for (size_t rollback_j = 0; rollback_j < j; ++rollback_j)
            {
                if constexpr (skip_compiled_aggregate_functions)
                    if (is_aggregate_function_compiled[j])
                        continue;

                aggregate_functions[rollback_j]->destroy(aggregate_data + offsets_of_aggregate_states[rollback_j]);
            }

            throw;
        }
    }
}

bool Aggregator::hasSparseArguments(AggregateFunctionInstruction * aggregate_instructions)
{
    for (auto * inst = aggregate_instructions; inst->that; ++inst)
        if (inst->has_sparse_arguments)
            return true;
    return false;
}

void Aggregator::executeOnBlockSmall(
    AggregatedDataVariants & result,
    size_t row_begin,
    size_t row_end,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction * aggregate_instructions) const
{
    /// `result` will destroy the states of aggregate functions in the destructor
    result.aggregator = this;

    /// How to perform the aggregation?
    if (result.empty())
    {
        initDataVariantsWithSizeHint(result, method_chosen, params);
        result.keys_size = params.keys_size;
        result.key_sizes = key_sizes;
    }

    executeImpl(result, row_begin, row_end, key_columns, aggregate_instructions);
}

void Aggregator::mergeOnBlockSmall(
    AggregatedDataVariants & result,
    size_t row_begin,
    size_t row_end,
    const AggregateColumnsConstData & aggregate_columns_data,
    const ColumnRawPtrs & key_columns) const
{
    /// `result` will destroy the states of aggregate functions in the destructor
    result.aggregator = this;

    /// How to perform the aggregation?
    if (result.empty())
    {
        initDataVariantsWithSizeHint(result, method_chosen, params);
        result.keys_size = params.keys_size;
        result.key_sizes = key_sizes;
    }

    if (false) {} // NOLINT
#define M(NAME, IS_TWO_LEVEL) \
    else if (result.type == AggregatedDataVariants::Type::NAME) \
        mergeStreamsImpl(result.aggregates_pool, *result.NAME, result.NAME->data, \
                         result.without_key, /* no_more_keys= */ false, \
                         row_begin, row_end, \
                         aggregate_columns_data, key_columns);

    APPLY_FOR_AGGREGATED_VARIANTS(M)
#undef M
    else
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
}

void Aggregator::executeImpl(
    AggregatedDataVariants & result,
    size_t row_begin,
    size_t row_end,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction * aggregate_instructions,
    bool no_more_keys,
    AggregateDataPtr overflow_row) const
{
    #define M(NAME, IS_TWO_LEVEL) \
        else if (result.type == AggregatedDataVariants::Type::NAME) \
            executeImpl(*result.NAME, result.aggregates_pool, row_begin, row_end, key_columns, aggregate_instructions, no_more_keys, overflow_row);

    if (false) {} // NOLINT
    APPLY_FOR_AGGREGATED_VARIANTS(M)
    #undef M
}

/** It's interesting - if you remove `noinline`, then gcc for some reason will inline this function, and the performance decreases (~ 10%).
  * (Probably because after the inline of this function, more internal functions no longer be inlined.)
  * Inline does not make sense, since the inner loop is entirely inside this function.
  */
template <typename Method>
void NO_INLINE Aggregator::executeImpl(
    Method & method,
    Arena * aggregates_pool,
    size_t row_begin,
    size_t row_end,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction * aggregate_instructions,
    bool no_more_keys,
    AggregateDataPtr overflow_row) const
{
    typename Method::State state(key_columns, key_sizes, aggregation_state_cache);

    if (!no_more_keys)
    {
#if USE_EMBEDDED_COMPILER
        if (compiled_aggregate_functions_holder && !hasSparseArguments(aggregate_instructions))
        {
            executeImplBatch<false, true>(method, state, aggregates_pool, row_begin, row_end, aggregate_instructions, overflow_row);
        }
        else
#endif
        {
            executeImplBatch<false, false>(method, state, aggregates_pool, row_begin, row_end, aggregate_instructions, overflow_row);
        }
    }
    else
    {
        executeImplBatch<true, false>(method, state, aggregates_pool, row_begin, row_end, aggregate_instructions, overflow_row);
    }
}

template <bool no_more_keys, bool use_compiled_functions, typename Method>
void NO_INLINE Aggregator::executeImplBatch(
    Method & method,
    typename Method::State & state,
    Arena * aggregates_pool,
    size_t row_begin,
    size_t row_end,
    AggregateFunctionInstruction * aggregate_instructions,
    AggregateDataPtr overflow_row) const
{
    /// Optimization for special case when there are no aggregate functions.
    if (params.aggregates_size == 0)
    {
        if constexpr (no_more_keys)
            return;

        /// For all rows.
        AggregateDataPtr place = aggregates_pool->alloc(0);
        for (size_t i = row_begin; i < row_end; ++i)
            state.emplaceKey(method.data, i, *aggregates_pool).setMapped(place);
        return;
    }

    /// Optimization for special case when aggregating by 8bit key.
    if constexpr (!no_more_keys && std::is_same_v<Method, typename decltype(AggregatedDataVariants::key8)::element_type>)
    {
        /// We use another method if there are aggregate functions with -Array combinator.
        bool has_arrays = false;
        for (AggregateFunctionInstruction * inst = aggregate_instructions; inst->that; ++inst)
        {
            if (inst->offsets)
            {
                has_arrays = true;
                break;
            }
        }

        if (!has_arrays && !hasSparseArguments(aggregate_instructions))
        {
            for (AggregateFunctionInstruction * inst = aggregate_instructions; inst->that; ++inst)
            {
                inst->batch_that->addBatchLookupTable8(
                    row_begin,
                    row_end,
                    reinterpret_cast<AggregateDataPtr *>(method.data.data()),
                    inst->state_offset,
                    [&](AggregateDataPtr & aggregate_data)
                    {
                        aggregate_data = aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
                        createAggregateStates(aggregate_data);
                    },
                    state.getKeyData(),
                    inst->batch_arguments,
                    aggregates_pool);
            }
            return;
        }
    }

    /// NOTE: only row_end-row_start is required, but:
    /// - this affects only optimize_aggregation_in_order,
    /// - this is just a pointer, so it should not be significant,
    /// - and plus this will require other changes in the interface.
    std::unique_ptr<AggregateDataPtr[]> places(new AggregateDataPtr[row_end]);

    /// For all rows.
    for (size_t i = row_begin; i < row_end; ++i)
    {
        AggregateDataPtr aggregate_data = nullptr;

        if constexpr (!no_more_keys)
        {
            auto emplace_result = state.emplaceKey(method.data, i, *aggregates_pool);

            /// If a new key is inserted, initialize the states of the aggregate functions, and possibly something related to the key.
            if (emplace_result.isInserted())
            {
                /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
                emplace_result.setMapped(nullptr);

                aggregate_data = aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);

#if USE_EMBEDDED_COMPILER
                if constexpr (use_compiled_functions)
                {
                    const auto & compiled_aggregate_functions = compiled_aggregate_functions_holder->compiled_aggregate_functions;
                    compiled_aggregate_functions.create_aggregate_states_function(aggregate_data);
                    if (compiled_aggregate_functions.functions_count != aggregate_functions.size())
                    {
                        static constexpr bool skip_compiled_aggregate_functions = true;
                        createAggregateStates<skip_compiled_aggregate_functions>(aggregate_data);
                    }

#if defined(MEMORY_SANITIZER)

                    /// We compile only functions that do not allocate some data in Arena. Only store necessary state in AggregateData place.
                    for (size_t aggregate_function_index = 0; aggregate_function_index < aggregate_functions.size(); ++aggregate_function_index)
                    {
                        if (!is_aggregate_function_compiled[aggregate_function_index])
                            continue;

                        auto aggregate_data_with_offset = aggregate_data + offsets_of_aggregate_states[aggregate_function_index];
                        auto data_size = params.aggregates[aggregate_function_index].function->sizeOfData();
                        __msan_unpoison(aggregate_data_with_offset, data_size);
                    }
#endif
                }
                else
#endif
                {
                    createAggregateStates(aggregate_data);
                }

                emplace_result.setMapped(aggregate_data);
            }
            else
                aggregate_data = emplace_result.getMapped();

            assert(aggregate_data != nullptr);
        }
        else
        {
            /// Add only if the key already exists.
            auto find_result = state.findKey(method.data, i, *aggregates_pool);
            if (find_result.isFound())
                aggregate_data = find_result.getMapped();
            else
                aggregate_data = overflow_row;
        }

        places[i] = aggregate_data;
    }

#if USE_EMBEDDED_COMPILER
    if constexpr (use_compiled_functions)
    {
        std::vector<ColumnData> columns_data;

        for (size_t i = 0; i < aggregate_functions.size(); ++i)
        {
            if (!is_aggregate_function_compiled[i])
                continue;

            AggregateFunctionInstruction * inst = aggregate_instructions + i;
            size_t arguments_size = inst->that->getArgumentTypes().size();

            for (size_t argument_index = 0; argument_index < arguments_size; ++argument_index)
                columns_data.emplace_back(getColumnData(inst->batch_arguments[argument_index]));
        }

        auto add_into_aggregate_states_function = compiled_aggregate_functions_holder->compiled_aggregate_functions.add_into_aggregate_states_function;
        add_into_aggregate_states_function(row_begin, row_end, columns_data.data(), places.get());
    }
#endif

    /// Add values to the aggregate functions.
    for (size_t i = 0; i < aggregate_functions.size(); ++i)
    {
#if USE_EMBEDDED_COMPILER
        if constexpr (use_compiled_functions)
            if (is_aggregate_function_compiled[i])
                continue;
#endif

        AggregateFunctionInstruction * inst = aggregate_instructions + i;

        if (inst->offsets)
            inst->batch_that->addBatchArray(row_begin, row_end, places.get(), inst->state_offset, inst->batch_arguments, inst->offsets, aggregates_pool);
        else if (inst->has_sparse_arguments)
            inst->batch_that->addBatchSparse(row_begin, row_end, places.get(), inst->state_offset, inst->batch_arguments, aggregates_pool);
        else
            inst->batch_that->addBatch(row_begin, row_end, places.get(), inst->state_offset, inst->batch_arguments, aggregates_pool);
    }
}


template <bool use_compiled_functions>
void NO_INLINE Aggregator::executeWithoutKeyImpl(
    AggregatedDataWithoutKey & res,
    size_t row_begin, size_t row_end,
    AggregateFunctionInstruction * aggregate_instructions,
    Arena * arena) const
{
    if (row_begin == row_end)
        return;

#if USE_EMBEDDED_COMPILER
    if constexpr (use_compiled_functions)
    {
        std::vector<ColumnData> columns_data;

        for (size_t i = 0; i < aggregate_functions.size(); ++i)
        {
            if (!is_aggregate_function_compiled[i])
                continue;

            AggregateFunctionInstruction * inst = aggregate_instructions + i;
            size_t arguments_size = inst->that->getArgumentTypes().size();

            for (size_t argument_index = 0; argument_index < arguments_size; ++argument_index)
            {
                columns_data.emplace_back(getColumnData(inst->batch_arguments[argument_index]));
            }
        }

        auto add_into_aggregate_states_function_single_place = compiled_aggregate_functions_holder->compiled_aggregate_functions.add_into_aggregate_states_function_single_place;
        add_into_aggregate_states_function_single_place(row_begin, row_end, columns_data.data(), res);

#if defined(MEMORY_SANITIZER)

        /// We compile only functions that do not allocate some data in Arena. Only store necessary state in AggregateData place.
        for (size_t aggregate_function_index = 0; aggregate_function_index < aggregate_functions.size(); ++aggregate_function_index)
        {
            if (!is_aggregate_function_compiled[aggregate_function_index])
                continue;

            auto aggregate_data_with_offset = res + offsets_of_aggregate_states[aggregate_function_index];
            auto data_size = params.aggregates[aggregate_function_index].function->sizeOfData();
            __msan_unpoison(aggregate_data_with_offset, data_size);
        }
#endif
    }
#endif

    /// Adding values
    for (size_t i = 0; i < aggregate_functions.size(); ++i)
    {
        AggregateFunctionInstruction * inst = aggregate_instructions + i;

#if USE_EMBEDDED_COMPILER
        if constexpr (use_compiled_functions)
            if (is_aggregate_function_compiled[i])
                continue;
#endif

        if (inst->offsets)
            inst->batch_that->addBatchSinglePlace(
                inst->offsets[static_cast<ssize_t>(row_begin) - 1],
                inst->offsets[row_end - 1],
                res + inst->state_offset,
                inst->batch_arguments,
                arena);
        else if (inst->has_sparse_arguments)
            inst->batch_that->addBatchSparseSinglePlace(
                row_begin, row_end,
                res + inst->state_offset,
                inst->batch_arguments,
                arena);
        else
            inst->batch_that->addBatchSinglePlace(
                row_begin, row_end,
                res + inst->state_offset,
                inst->batch_arguments,
                arena);
    }
}


void NO_INLINE Aggregator::executeOnIntervalWithoutKeyImpl(
    AggregatedDataVariants & data_variants,
    size_t row_begin,
    size_t row_end,
    AggregateFunctionInstruction * aggregate_instructions) const
{
    /// `data_variants` will destroy the states of aggregate functions in the destructor
    data_variants.aggregator = this;
    data_variants.init(AggregatedDataVariants::Type::without_key);

    AggregatedDataWithoutKey & res = data_variants.without_key;

    /// Adding values
    for (AggregateFunctionInstruction * inst = aggregate_instructions; inst->that; ++inst)
    {
        if (inst->offsets)
            inst->batch_that->addBatchSinglePlaceFromInterval(
                inst->offsets[static_cast<ssize_t>(row_begin) - 1],
                inst->offsets[row_end - 1],
                res + inst->state_offset,
                inst->batch_arguments, data_variants.aggregates_pool);
        else
            inst->batch_that->addBatchSinglePlaceFromInterval(
                row_begin,
                row_end,
                res + inst->state_offset,
                inst->batch_arguments,
                data_variants.aggregates_pool);
    }
}

void NO_INLINE Aggregator::mergeOnIntervalWithoutKeyImpl(
    AggregatedDataVariants & data_variants,
    size_t row_begin,
    size_t row_end,
    const AggregateColumnsConstData & aggregate_columns_data) const
{
    /// `data_variants` will destroy the states of aggregate functions in the destructor
    data_variants.aggregator = this;
    data_variants.init(AggregatedDataVariants::Type::without_key);

    mergeWithoutKeyStreamsImpl(data_variants, row_begin, row_end, aggregate_columns_data);
}


void Aggregator::prepareAggregateInstructions(
    Columns columns,
    AggregateColumns & aggregate_columns,
    Columns & materialized_columns,
    AggregateFunctionInstructions & aggregate_functions_instructions,
    NestedColumnsHolder & nested_columns_holder) const
{
    for (size_t i = 0; i < params.aggregates_size; ++i)
        aggregate_columns[i].resize(params.aggregates[i].argument_names.size());

    aggregate_functions_instructions.resize(params.aggregates_size + 1);
    aggregate_functions_instructions[params.aggregates_size].that = nullptr;

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        bool allow_sparse_arguments = aggregate_columns[i].size() == 1;
        bool has_sparse_arguments = false;

        for (size_t j = 0; j < aggregate_columns[i].size(); ++j)
        {
            const auto pos = header.getPositionByName(params.aggregates[i].argument_names[j]);
            materialized_columns.push_back(columns.at(pos)->convertToFullColumnIfConst());
            aggregate_columns[i][j] = materialized_columns.back().get();

            auto full_column = allow_sparse_arguments
                ? aggregate_columns[i][j]->getPtr()
                : recursiveRemoveSparse(aggregate_columns[i][j]->getPtr());

            full_column = recursiveRemoveLowCardinality(full_column);
            if (full_column.get() != aggregate_columns[i][j])
            {
                materialized_columns.emplace_back(std::move(full_column));
                aggregate_columns[i][j] = materialized_columns.back().get();
            }

            if (aggregate_columns[i][j]->isSparse())
                has_sparse_arguments = true;
        }

        aggregate_functions_instructions[i].has_sparse_arguments = has_sparse_arguments;
        aggregate_functions_instructions[i].arguments = aggregate_columns[i].data();
        aggregate_functions_instructions[i].state_offset = offsets_of_aggregate_states[i];

        const auto * that = aggregate_functions[i];
        /// Unnest consecutive trailing -State combinators
        while (const auto * func = typeid_cast<const AggregateFunctionState *>(that))
            that = func->getNestedFunction().get();
        aggregate_functions_instructions[i].that = that;

        if (const auto * func = typeid_cast<const AggregateFunctionArray *>(that))
        {
            /// Unnest consecutive -State combinators before -Array
            that = func->getNestedFunction().get();
            while (const auto * nested_func = typeid_cast<const AggregateFunctionState *>(that))
                that = nested_func->getNestedFunction().get();
            auto [nested_columns, offsets] = checkAndGetNestedArrayOffset(aggregate_columns[i].data(), that->getArgumentTypes().size());
            nested_columns_holder.push_back(std::move(nested_columns));
            aggregate_functions_instructions[i].batch_arguments = nested_columns_holder.back().data();
            aggregate_functions_instructions[i].offsets = offsets;
        }
        else
            aggregate_functions_instructions[i].batch_arguments = aggregate_columns[i].data();

        aggregate_functions_instructions[i].batch_that = that;
    }
}


bool Aggregator::executeOnBlock(const Block & block,
    AggregatedDataVariants & result,
    ColumnRawPtrs & key_columns,
    AggregateColumns & aggregate_columns,
    bool & no_more_keys) const
{
    return executeOnBlock(block.getColumns(),
        /* row_begin= */ 0, block.rows(),
        result,
        key_columns,
        aggregate_columns,
        no_more_keys);
}


bool Aggregator::executeOnBlock(Columns columns,
    size_t row_begin, size_t row_end,
    AggregatedDataVariants & result,
    ColumnRawPtrs & key_columns,
    AggregateColumns & aggregate_columns,
    bool & no_more_keys) const
{
    /// `result` will destroy the states of aggregate functions in the destructor
    result.aggregator = this;

    /// How to perform the aggregation?
    if (result.empty())
    {
        initDataVariantsWithSizeHint(result, method_chosen, params);
        result.keys_size = params.keys_size;
        result.key_sizes = key_sizes;
        LOG_TRACE(log, "Aggregation method: {}", result.getMethodName());
    }

    /** Constant columns are not supported directly during aggregation.
      * To make them work anyway, we materialize them.
      */
    Columns materialized_columns;

    /// Remember the columns we will work with
    for (size_t i = 0; i < params.keys_size; ++i)
    {
        materialized_columns.push_back(recursiveRemoveSparse(columns.at(keys_positions[i]))->convertToFullColumnIfConst());
        key_columns[i] = materialized_columns.back().get();

        if (!result.isLowCardinality())
        {
            auto column_no_lc = recursiveRemoveLowCardinality(key_columns[i]->getPtr());
            if (column_no_lc.get() != key_columns[i])
            {
                materialized_columns.emplace_back(std::move(column_no_lc));
                key_columns[i] = materialized_columns.back().get();
            }
        }
    }

    NestedColumnsHolder nested_columns_holder;
    AggregateFunctionInstructions aggregate_functions_instructions;
    prepareAggregateInstructions(columns, aggregate_columns, materialized_columns, aggregate_functions_instructions, nested_columns_holder);

    if ((params.overflow_row || result.type == AggregatedDataVariants::Type::without_key) && !result.without_key)
    {
        AggregateDataPtr place = result.aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
        createAggregateStates(place);
        result.without_key = place;
    }

    /// We select one of the aggregation methods and call it.

    /// For the case when there are no keys (all aggregate into one row).
    if (result.type == AggregatedDataVariants::Type::without_key)
    {
        /// TODO: Enable compilation after investigation
// #if USE_EMBEDDED_COMPILER
//         if (compiled_aggregate_functions_holder)
//         {
//             executeWithoutKeyImpl<true>(result.without_key, row_begin, row_end, aggregate_functions_instructions.data(), result.aggregates_pool);
//         }
//         else
// #endif
        {
            executeWithoutKeyImpl<false>(result.without_key, row_begin, row_end, aggregate_functions_instructions.data(), result.aggregates_pool);
        }
    }
    else
    {
        /// This is where data is written that does not fit in `max_rows_to_group_by` with `group_by_overflow_mode = any`.
        AggregateDataPtr overflow_row_ptr = params.overflow_row ? result.without_key : nullptr;
        executeImpl(result, row_begin, row_end, key_columns, aggregate_functions_instructions.data(), no_more_keys, overflow_row_ptr);
    }

    size_t result_size = result.sizeWithoutOverflowRow();
    Int64 current_memory_usage = 0;
    if (auto * memory_tracker_child = CurrentThread::getMemoryTracker())
        if (auto * memory_tracker = memory_tracker_child->getParent())
            current_memory_usage = memory_tracker->get();

    /// Here all the results in the sum are taken into account, from different threads.
    auto result_size_bytes = current_memory_usage - memory_usage_before_aggregation;

    bool worth_convert_to_two_level = worthConvertToTwoLevel(
        params.group_by_two_level_threshold, result_size, params.group_by_two_level_threshold_bytes, result_size_bytes);

    /** Converting to a two-level data structure.
      * It allows you to make, in the subsequent, an effective merge - either economical from memory or parallel.
      */
    if (result.isConvertibleToTwoLevel() && worth_convert_to_two_level)
        result.convertToTwoLevel();

    /// Checking the constraints.
    if (!checkLimits(result_size, no_more_keys))
        return false;

    /** Flush data to disk if too much RAM is consumed.
      * Data can only be flushed to disk if a two-level aggregation structure is used.
      */
    if (params.max_bytes_before_external_group_by
        && result.isTwoLevel()
        && current_memory_usage > static_cast<Int64>(params.max_bytes_before_external_group_by)
        && worth_convert_to_two_level)
    {
        size_t size = current_memory_usage + params.min_free_disk_space;

        std::string tmp_path = params.tmp_volume->getDisk()->getPath();

        // enoughSpaceInDirectory() is not enough to make it right, since
        // another process (or another thread of aggregator) can consume all
        // space.
        //
        // But true reservation (IVolume::reserve()) cannot be used here since
        // current_memory_usage does not takes compression into account and
        // will reserve way more that actually will be used.
        //
        // Hence let's do a simple check.
        if (!enoughSpaceInDirectory(tmp_path, size))
            throw Exception("Not enough space for external aggregation in " + tmp_path, ErrorCodes::NOT_ENOUGH_SPACE);

        writeToTemporaryFile(result, tmp_path);
    }

    return true;
}


void Aggregator::writeToTemporaryFile(AggregatedDataVariants & data_variants, const String & tmp_path) const
{
    Stopwatch watch;
    size_t rows = data_variants.size();

    auto file = createTemporaryFile(tmp_path);
    const std::string & path = file->path();
    WriteBufferFromFile file_buf(path);
    CompressedWriteBuffer compressed_buf(file_buf);
    NativeWriter block_out(compressed_buf, DBMS_TCP_PROTOCOL_VERSION, getHeader(false));

    LOG_DEBUG(log, "Writing part of aggregation data into temporary file {}.", path);
    ProfileEvents::increment(ProfileEvents::ExternalAggregationWritePart);

    /// Flush only two-level data and possibly overflow data.

#define M(NAME) \
    else if (data_variants.type == AggregatedDataVariants::Type::NAME) \
        writeToTemporaryFileImpl(data_variants, *data_variants.NAME, block_out);

    if (false) {} // NOLINT
    APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M
    else
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    /// NOTE Instead of freeing up memory and creating new hash tables and arenas, you can re-use the old ones.
    data_variants.init(data_variants.type);
    data_variants.aggregates_pools = Arenas(1, std::make_shared<Arena>());
    data_variants.aggregates_pool = data_variants.aggregates_pools.back().get();
    if (params.overflow_row || data_variants.type == AggregatedDataVariants::Type::without_key)
    {
        AggregateDataPtr place = data_variants.aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
        createAggregateStates(place);
        data_variants.without_key = place;
    }

    block_out.flush();
    compressed_buf.next();
    file_buf.next();

    double elapsed_seconds = watch.elapsedSeconds();
    double compressed_bytes = file_buf.count();
    double uncompressed_bytes = compressed_buf.count();

    {
        std::lock_guard lock(temporary_files.mutex);
        temporary_files.files.emplace_back(std::move(file));
        temporary_files.sum_size_uncompressed += uncompressed_bytes;
        temporary_files.sum_size_compressed += compressed_bytes;
    }

    ProfileEvents::increment(ProfileEvents::ExternalAggregationCompressedBytes, compressed_bytes);
    ProfileEvents::increment(ProfileEvents::ExternalAggregationUncompressedBytes, uncompressed_bytes);

    LOG_DEBUG(log,
        "Written part in {:.3f} sec., {} rows, {} uncompressed, {} compressed,"
        " {:.3f} uncompressed bytes per row, {:.3f} compressed bytes per row, compression rate: {:.3f}"
        " ({:.3f} rows/sec., {}/sec. uncompressed, {}/sec. compressed)",
        elapsed_seconds,
        rows,
        ReadableSize(uncompressed_bytes),
        ReadableSize(compressed_bytes),
        uncompressed_bytes / rows,
        compressed_bytes / rows,
        uncompressed_bytes / compressed_bytes,
        rows / elapsed_seconds,
        ReadableSize(uncompressed_bytes / elapsed_seconds),
        ReadableSize(compressed_bytes / elapsed_seconds));
}


void Aggregator::writeToTemporaryFile(AggregatedDataVariants & data_variants) const
{
    String tmp_path = params.tmp_volume->getDisk()->getPath();
    return writeToTemporaryFile(data_variants, tmp_path);
}


template <typename Method>
Block Aggregator::convertOneBucketToBlock(
    AggregatedDataVariants & data_variants,
    Method & method,
    Arena * arena,
    bool final,
    size_t bucket) const
{
    Block block = prepareBlockAndFill(data_variants, final, method.data.impls[bucket].size(),
        [bucket, &method, arena, this] (
            MutableColumns & key_columns,
            AggregateColumnsData & aggregate_columns,
            MutableColumns & final_aggregate_columns,
            bool final_)
        {
            convertToBlockImpl(method, method.data.impls[bucket],
                key_columns, aggregate_columns, final_aggregate_columns, arena, final_);
        });

    block.info.bucket_num = bucket;
    return block;
}

Block Aggregator::mergeAndConvertOneBucketToBlock(
    ManyAggregatedDataVariants & variants,
    Arena * arena,
    bool final,
    size_t bucket,
    std::atomic<bool> * is_cancelled) const
{
    auto & merged_data = *variants[0];
    auto method = merged_data.type;
    Block block;

    if (false) {} // NOLINT
#define M(NAME) \
    else if (method == AggregatedDataVariants::Type::NAME) \
    { \
        mergeBucketImpl<decltype(merged_data.NAME)::element_type>(variants, bucket, arena); \
        if (is_cancelled && is_cancelled->load(std::memory_order_seq_cst)) \
            return {}; \
        block = convertOneBucketToBlock(merged_data, *merged_data.NAME, arena, final, bucket); \
    }

    APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M

    return block;
}


template <typename Method>
void Aggregator::writeToTemporaryFileImpl(
    AggregatedDataVariants & data_variants,
    Method & method,
    NativeWriter & out) const
{
    size_t max_temporary_block_size_rows = 0;
    size_t max_temporary_block_size_bytes = 0;

    auto update_max_sizes = [&](const Block & block)
    {
        size_t block_size_rows = block.rows();
        size_t block_size_bytes = block.bytes();

        if (block_size_rows > max_temporary_block_size_rows)
            max_temporary_block_size_rows = block_size_rows;
        if (block_size_bytes > max_temporary_block_size_bytes)
            max_temporary_block_size_bytes = block_size_bytes;
    };

    for (size_t bucket = 0; bucket < Method::Data::NUM_BUCKETS; ++bucket)
    {
        Block block = convertOneBucketToBlock(data_variants, method, data_variants.aggregates_pool, false, bucket);
        out.write(block);
        update_max_sizes(block);
    }

    if (params.overflow_row)
    {
        Block block = prepareBlockAndFillWithoutKey(data_variants, false, true);
        out.write(block);
        update_max_sizes(block);
    }

    /// Pass ownership of the aggregate functions states:
    /// `data_variants` will not destroy them in the destructor, they are now owned by ColumnAggregateFunction objects.
    data_variants.aggregator = nullptr;

    LOG_DEBUG(log, "Max size of temporary block: {} rows, {}.", max_temporary_block_size_rows, ReadableSize(max_temporary_block_size_bytes));
}


bool Aggregator::checkLimits(size_t result_size, bool & no_more_keys) const
{
    if (!no_more_keys && params.max_rows_to_group_by && result_size > params.max_rows_to_group_by)
    {
        switch (params.group_by_overflow_mode)
        {
            case OverflowMode::THROW:
                throw Exception("Limit for rows to GROUP BY exceeded: has " + toString(result_size)
                    + " rows, maximum: " + toString(params.max_rows_to_group_by),
                    ErrorCodes::TOO_MANY_ROWS);

            case OverflowMode::BREAK:
                return false;

            case OverflowMode::ANY:
                no_more_keys = true;
                break;
        }
    }

    /// Some aggregate functions cannot throw exceptions on allocations (e.g. from C malloc)
    /// but still tracks memory. Check it here.
    CurrentMemoryTracker::check();
    return true;
}


template <typename Method, typename Table>
void Aggregator::convertToBlockImpl(
    Method & method,
    Table & data,
    MutableColumns & key_columns,
    AggregateColumnsData & aggregate_columns,
    MutableColumns & final_aggregate_columns,
    Arena * arena,
    bool final) const
{
    if (data.empty())
        return;

    if (key_columns.size() != params.keys_size)
        throw Exception{"Aggregate. Unexpected key columns size.", ErrorCodes::LOGICAL_ERROR};

    std::vector<IColumn *> raw_key_columns;
    raw_key_columns.reserve(key_columns.size());
    for (auto & column : key_columns)
        raw_key_columns.push_back(column.get());

    if (final)
    {
#if USE_EMBEDDED_COMPILER
        if (compiled_aggregate_functions_holder)
        {
            static constexpr bool use_compiled_functions = !Method::low_cardinality_optimization;
            convertToBlockImplFinal<Method, use_compiled_functions>(method, data, std::move(raw_key_columns), final_aggregate_columns, arena);
        }
        else
#endif
        {
            convertToBlockImplFinal<Method, false>(method, data, std::move(raw_key_columns), final_aggregate_columns, arena);
        }
    }
    else
    {
        convertToBlockImplNotFinal(method, data, std::move(raw_key_columns), aggregate_columns);
    }
    /// In order to release memory early.
    data.clearAndShrink();
}


template <typename Mapped>
inline void Aggregator::insertAggregatesIntoColumns(Mapped & mapped, MutableColumns & final_aggregate_columns, Arena * arena) const
{
    /** Final values of aggregate functions are inserted to columns.
      * Then states of aggregate functions, that are not longer needed, are destroyed.
      *
      * We mark already destroyed states with "nullptr" in data,
      *  so they will not be destroyed in destructor of Aggregator
      * (other values will be destroyed in destructor in case of exception).
      *
      * But it becomes tricky, because we have multiple aggregate states pointed by a single pointer in data.
      * So, if exception is thrown in the middle of moving states for different aggregate functions,
      *  we have to catch exceptions and destroy all the states that are no longer needed,
      *  to keep the data in consistent state.
      *
      * It is also tricky, because there are aggregate functions with "-State" modifier.
      * When we call "insertResultInto" for them, they insert a pointer to the state to ColumnAggregateFunction
      *  and ColumnAggregateFunction will take ownership of this state.
      * So, for aggregate functions with "-State" modifier, the state must not be destroyed
      *  after it has been transferred to ColumnAggregateFunction.
      * But we should mark that the data no longer owns these states.
      */

    size_t insert_i = 0;
    std::exception_ptr exception;

    try
    {
        /// Insert final values of aggregate functions into columns.
        for (; insert_i < params.aggregates_size; ++insert_i)
            aggregate_functions[insert_i]->insertResultInto(
                mapped + offsets_of_aggregate_states[insert_i],
                *final_aggregate_columns[insert_i],
                arena);
    }
    catch (...)
    {
        exception = std::current_exception();
    }

    /** Destroy states that are no longer needed. This loop does not throw.
        *
        * Don't destroy states for "-State" aggregate functions,
        *  because the ownership of this state is transferred to ColumnAggregateFunction
        *  and ColumnAggregateFunction will take care.
        *
        * But it's only for states that has been transferred to ColumnAggregateFunction
        *  before exception has been thrown;
        */
    for (size_t destroy_i = 0; destroy_i < params.aggregates_size; ++destroy_i)
    {
        /// If ownership was not transferred to ColumnAggregateFunction.
        if (!(destroy_i < insert_i && aggregate_functions[destroy_i]->isState()))
            aggregate_functions[destroy_i]->destroy(
                mapped + offsets_of_aggregate_states[destroy_i]);
    }

    /// Mark the cell as destroyed so it will not be destroyed in destructor.
    mapped = nullptr;

    if (exception)
        std::rethrow_exception(exception);
}


template <typename Method, bool use_compiled_functions, typename Table>
void NO_INLINE Aggregator::convertToBlockImplFinal(
    Method & method,
    Table & data,
    std::vector<IColumn *>  key_columns,
    MutableColumns & final_aggregate_columns,
    Arena * arena) const
{
    if constexpr (Method::low_cardinality_optimization)
    {
        if (data.hasNullKeyData())
        {
            key_columns[0]->insertDefault();
            insertAggregatesIntoColumns(data.getNullKeyData(), final_aggregate_columns, arena);
        }
    }

    auto shuffled_key_sizes = method.shuffleKeyColumns(key_columns, key_sizes);
    const auto & key_sizes_ref = shuffled_key_sizes ? *shuffled_key_sizes :  key_sizes;

    PaddedPODArray<AggregateDataPtr> places;
    places.reserve(data.size());

    data.forEachValue([&](const auto & key, auto & mapped)
    {
        method.insertKeyIntoColumns(key, key_columns, key_sizes_ref);
        places.emplace_back(mapped);

        /// Mark the cell as destroyed so it will not be destroyed in destructor.
        mapped = nullptr;
    });

    std::exception_ptr exception;
    size_t aggregate_functions_destroy_index = 0;

    try
    {
#if USE_EMBEDDED_COMPILER
        if constexpr (use_compiled_functions)
        {
            /** For JIT compiled functions we need to resize columns before pass them into compiled code.
              * insert_aggregates_into_columns_function function does not throw exception.
              */
            std::vector<ColumnData> columns_data;

            auto compiled_functions = compiled_aggregate_functions_holder->compiled_aggregate_functions;

            for (size_t i = 0; i < params.aggregates_size; ++i)
            {
                if (!is_aggregate_function_compiled[i])
                    continue;

                auto & final_aggregate_column = final_aggregate_columns[i];
                final_aggregate_column = final_aggregate_column->cloneResized(places.size());
                columns_data.emplace_back(getColumnData(final_aggregate_column.get()));
            }

            auto insert_aggregates_into_columns_function = compiled_functions.insert_aggregates_into_columns_function;
            insert_aggregates_into_columns_function(0, places.size(), columns_data.data(), places.data());
        }
#endif

        for (; aggregate_functions_destroy_index < params.aggregates_size;)
        {
            if constexpr (use_compiled_functions)
            {
                if (is_aggregate_function_compiled[aggregate_functions_destroy_index])
                {
                    ++aggregate_functions_destroy_index;
                    continue;
                }
            }

            auto & final_aggregate_column = final_aggregate_columns[aggregate_functions_destroy_index];
            size_t offset = offsets_of_aggregate_states[aggregate_functions_destroy_index];

            /** We increase aggregate_functions_destroy_index because by function contract if insertResultIntoBatch
              * throws exception, it also must destroy all necessary states.
              * Then code need to continue to destroy other aggregate function states with next function index.
              */
            size_t destroy_index = aggregate_functions_destroy_index;
            ++aggregate_functions_destroy_index;

            /// For State AggregateFunction ownership of aggregate place is passed to result column after insert
            bool is_state = aggregate_functions[destroy_index]->isState();
            bool destroy_place_after_insert = !is_state;

            aggregate_functions[destroy_index]->insertResultIntoBatch(0, places.size(), places.data(), offset, *final_aggregate_column, arena, destroy_place_after_insert);
        }
    }
    catch (...)
    {
        exception = std::current_exception();
    }

    for (; aggregate_functions_destroy_index < params.aggregates_size; ++aggregate_functions_destroy_index)
    {
        if constexpr (use_compiled_functions)
        {
            if (is_aggregate_function_compiled[aggregate_functions_destroy_index])
            {
                ++aggregate_functions_destroy_index;
                continue;
            }
        }

        size_t offset = offsets_of_aggregate_states[aggregate_functions_destroy_index];
        aggregate_functions[aggregate_functions_destroy_index]->destroyBatch(0, places.size(), places.data(), offset);
    }

    if (exception)
        std::rethrow_exception(exception);
}

template <typename Method, typename Table>
void NO_INLINE Aggregator::convertToBlockImplNotFinal(
    Method & method,
    Table & data,
    std::vector<IColumn *>  key_columns,
    AggregateColumnsData & aggregate_columns) const
{
    if constexpr (Method::low_cardinality_optimization)
    {
        if (data.hasNullKeyData())
        {
            key_columns[0]->insertDefault();

            for (size_t i = 0; i < params.aggregates_size; ++i)
                aggregate_columns[i]->push_back(data.getNullKeyData() + offsets_of_aggregate_states[i]);

            data.getNullKeyData() = nullptr;
        }
    }

    auto shuffled_key_sizes = method.shuffleKeyColumns(key_columns, key_sizes);
    const auto & key_sizes_ref = shuffled_key_sizes ? *shuffled_key_sizes :  key_sizes;

    data.forEachValue([&](const auto & key, auto & mapped)
    {
        method.insertKeyIntoColumns(key, key_columns, key_sizes_ref);

        /// reserved, so push_back does not throw exceptions
        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_columns[i]->push_back(mapped + offsets_of_aggregate_states[i]);

        mapped = nullptr;
    });
}


template <typename Filler>
Block Aggregator::prepareBlockAndFill(
    AggregatedDataVariants & data_variants,
    bool final,
    size_t rows,
    Filler && filler) const
{
    MutableColumns key_columns(params.keys_size);
    MutableColumns aggregate_columns(params.aggregates_size);
    MutableColumns final_aggregate_columns(params.aggregates_size);
    AggregateColumnsData aggregate_columns_data(params.aggregates_size);

    Block res_header = getHeader(final);

    for (size_t i = 0; i < params.keys_size; ++i)
    {
        key_columns[i] = res_header.safeGetByPosition(i).type->createColumn();
        key_columns[i]->reserve(rows);
    }

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        if (!final)
        {
            const auto & aggregate_column_name = params.aggregates[i].column_name;
            aggregate_columns[i] = res_header.getByName(aggregate_column_name).type->createColumn();

            /// The ColumnAggregateFunction column captures the shared ownership of the arena with the aggregate function states.
            ColumnAggregateFunction & column_aggregate_func = assert_cast<ColumnAggregateFunction &>(*aggregate_columns[i]);

            for (auto & pool : data_variants.aggregates_pools)
                column_aggregate_func.addArena(pool);

            aggregate_columns_data[i] = &column_aggregate_func.getData();
            aggregate_columns_data[i]->reserve(rows);
        }
        else
        {
            final_aggregate_columns[i] = aggregate_functions[i]->getReturnType()->createColumn();
            final_aggregate_columns[i]->reserve(rows);

            if (aggregate_functions[i]->isState())
            {
                /// The ColumnAggregateFunction column captures the shared ownership of the arena with aggregate function states.
                if (auto * column_aggregate_func = typeid_cast<ColumnAggregateFunction *>(final_aggregate_columns[i].get()))
                    for (auto & pool : data_variants.aggregates_pools)
                        column_aggregate_func->addArena(pool);

                /// Aggregate state can be wrapped into array if aggregate function ends with -Resample combinator.
                final_aggregate_columns[i]->forEachSubcolumn([&data_variants](auto & subcolumn)
                {
                    if (auto * column_aggregate_func = typeid_cast<ColumnAggregateFunction *>(subcolumn.get()))
                        for (auto & pool : data_variants.aggregates_pools)
                            column_aggregate_func->addArena(pool);
                });
            }
        }
    }

    filler(key_columns, aggregate_columns_data, final_aggregate_columns, final);

    Block res = res_header.cloneEmpty();

    for (size_t i = 0; i < params.keys_size; ++i)
        res.getByPosition(i).column = std::move(key_columns[i]);

    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        const auto & aggregate_column_name = params.aggregates[i].column_name;
        if (final)
            res.getByName(aggregate_column_name).column = std::move(final_aggregate_columns[i]);
        else
            res.getByName(aggregate_column_name).column = std::move(aggregate_columns[i]);
    }

    /// Change the size of the columns-constants in the block.
    size_t columns = res_header.columns();
    for (size_t i = 0; i < columns; ++i)
        if (isColumnConst(*res.getByPosition(i).column))
            res.getByPosition(i).column = res.getByPosition(i).column->cut(0, rows);

    return res;
}

void Aggregator::addSingleKeyToAggregateColumns(
    AggregatedDataVariants & data_variants,
    MutableColumns & aggregate_columns) const
{
    auto & data = data_variants.without_key;

    size_t i = 0;
    try
    {
        for (i = 0; i < params.aggregates_size; ++i)
        {
            auto & column_aggregate_func = assert_cast<ColumnAggregateFunction &>(*aggregate_columns[i]);
            column_aggregate_func.getData().push_back(data + offsets_of_aggregate_states[i]);
        }
    }
    catch (...)
    {
        /// Rollback
        for (size_t rollback_i = 0; rollback_i < i; ++rollback_i)
        {
            auto & column_aggregate_func = assert_cast<ColumnAggregateFunction &>(*aggregate_columns[rollback_i]);
            column_aggregate_func.getData().pop_back();
        }
        throw;
    }

    data = nullptr;
}

void Aggregator::addArenasToAggregateColumns(
    const AggregatedDataVariants & data_variants,
    MutableColumns & aggregate_columns) const
{
    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        auto & column_aggregate_func = assert_cast<ColumnAggregateFunction &>(*aggregate_columns[i]);
        for (const auto & pool : data_variants.aggregates_pools)
            column_aggregate_func.addArena(pool);
    }
}

void Aggregator::createStatesAndFillKeyColumnsWithSingleKey(
    AggregatedDataVariants & data_variants,
    Columns & key_columns,
    size_t key_row,
    MutableColumns & final_key_columns) const
{
    AggregateDataPtr place = data_variants.aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
    createAggregateStates(place);
    data_variants.without_key = place;

    for (size_t i = 0; i < params.keys_size; ++i)
    {
        final_key_columns[i]->insertFrom(*key_columns[i].get(), key_row);
    }
}

Block Aggregator::prepareBlockAndFillWithoutKey(AggregatedDataVariants & data_variants, bool final, bool is_overflows) const
{
    size_t rows = 1;

    auto filler = [&data_variants, this](
        MutableColumns & key_columns,
        AggregateColumnsData & aggregate_columns,
        MutableColumns & final_aggregate_columns,
        bool final_)
    {
        if (data_variants.type == AggregatedDataVariants::Type::without_key || params.overflow_row)
        {
            AggregatedDataWithoutKey & data = data_variants.without_key;

            if (!data)
                throw Exception("Wrong data variant passed.", ErrorCodes::LOGICAL_ERROR);

            if (!final_)
            {
                for (size_t i = 0; i < params.aggregates_size; ++i)
                    aggregate_columns[i]->push_back(data + offsets_of_aggregate_states[i]);
                data = nullptr;
            }
            else
            {
                /// Always single-thread. It's safe to pass current arena from 'aggregates_pool'.
                insertAggregatesIntoColumns(data, final_aggregate_columns, data_variants.aggregates_pool);
            }

            if (params.overflow_row)
                for (size_t i = 0; i < params.keys_size; ++i)
                    key_columns[i]->insertDefault();
        }
    };

    Block block = prepareBlockAndFill(data_variants, final, rows, filler);

    if (is_overflows)
        block.info.is_overflows = true;

    if (final)
        destroyWithoutKey(data_variants);

    return block;
}

Block Aggregator::prepareBlockAndFillSingleLevel(AggregatedDataVariants & data_variants, bool final) const
{
    size_t rows = data_variants.sizeWithoutOverflowRow();

    auto filler = [&data_variants, this](
        MutableColumns & key_columns,
        AggregateColumnsData & aggregate_columns,
        MutableColumns & final_aggregate_columns,
        bool final_)
    {
    #define M(NAME) \
        else if (data_variants.type == AggregatedDataVariants::Type::NAME) \
            convertToBlockImpl(*data_variants.NAME, data_variants.NAME->data, \
                key_columns, aggregate_columns, final_aggregate_columns, data_variants.aggregates_pool, final_);

        if (false) {} // NOLINT
        APPLY_FOR_VARIANTS_SINGLE_LEVEL(M)
    #undef M
        else
            throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
    };

    return prepareBlockAndFill(data_variants, final, rows, filler);
}


BlocksList Aggregator::prepareBlocksAndFillTwoLevel(AggregatedDataVariants & data_variants, bool final, ThreadPool * thread_pool) const
{
#define M(NAME) \
    else if (data_variants.type == AggregatedDataVariants::Type::NAME) \
        return prepareBlocksAndFillTwoLevelImpl(data_variants, *data_variants.NAME, final, thread_pool);

    if (false) {} // NOLINT
    APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M
    else
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
}


template <typename Method>
BlocksList Aggregator::prepareBlocksAndFillTwoLevelImpl(
    AggregatedDataVariants & data_variants,
    Method & method,
    bool final,
    ThreadPool * thread_pool) const
{
    size_t max_threads = thread_pool ? thread_pool->getMaxThreads() : 1;
    if (max_threads > data_variants.aggregates_pools.size())
        for (size_t i = data_variants.aggregates_pools.size(); i < max_threads; ++i)
            data_variants.aggregates_pools.push_back(std::make_shared<Arena>());

    std::atomic<UInt32> next_bucket_to_merge = 0;

    auto converter = [&](size_t thread_id, ThreadGroupStatusPtr thread_group)
    {
        if (thread_group)
            CurrentThread::attachToIfDetached(thread_group);

        BlocksList blocks;
        while (true)
        {
            UInt32 bucket = next_bucket_to_merge.fetch_add(1);

            if (bucket >= Method::Data::NUM_BUCKETS)
                break;

            if (method.data.impls[bucket].empty())
                continue;

            /// Select Arena to avoid race conditions
            Arena * arena = data_variants.aggregates_pools.at(thread_id).get();
            blocks.emplace_back(convertOneBucketToBlock(data_variants, method, arena, final, bucket));
        }
        return blocks;
    };

    /// packaged_task is used to ensure that exceptions are automatically thrown into the main stream.

    std::vector<std::packaged_task<BlocksList()>> tasks(max_threads);

    try
    {
        for (size_t thread_id = 0; thread_id < max_threads; ++thread_id)
        {
            tasks[thread_id] = std::packaged_task<BlocksList()>(
                [group = CurrentThread::getGroup(), thread_id, &converter] { return converter(thread_id, group); });

            if (thread_pool)
                thread_pool->scheduleOrThrowOnError([thread_id, &tasks] { tasks[thread_id](); });
            else
                tasks[thread_id]();
        }
    }
    catch (...)
    {
        /// If this is not done, then in case of an exception, tasks will be destroyed before the threads are completed, and it will be bad.
        if (thread_pool)
            thread_pool->wait();

        throw;
    }

    if (thread_pool)
        thread_pool->wait();

    BlocksList blocks;

    for (auto & task : tasks)
    {
        if (!task.valid())
            continue;

        blocks.splice(blocks.end(), task.get_future().get());
    }

    return blocks;
}


BlocksList Aggregator::convertToBlocks(AggregatedDataVariants & data_variants, bool final, size_t max_threads) const
{
    LOG_TRACE(log, "Converting aggregated data to blocks");

    Stopwatch watch;

    BlocksList blocks;

    /// In what data structure is the data aggregated?
    if (data_variants.empty())
        return blocks;

    std::unique_ptr<ThreadPool> thread_pool;
    if (max_threads > 1 && data_variants.sizeWithoutOverflowRow() > 100000  /// TODO Make a custom threshold.
        && data_variants.isTwoLevel())                      /// TODO Use the shared thread pool with the `merge` function.
        thread_pool = std::make_unique<ThreadPool>(max_threads);

    if (data_variants.without_key)
        blocks.emplace_back(prepareBlockAndFillWithoutKey(
            data_variants, final, data_variants.type != AggregatedDataVariants::Type::without_key));

    if (data_variants.type != AggregatedDataVariants::Type::without_key)
    {
        if (!data_variants.isTwoLevel())
            blocks.emplace_back(prepareBlockAndFillSingleLevel(data_variants, final));
        else
            blocks.splice(blocks.end(), prepareBlocksAndFillTwoLevel(data_variants, final, thread_pool.get()));
    }

    if (!final)
    {
        /// data_variants will not destroy the states of aggregate functions in the destructor.
        /// Now ColumnAggregateFunction owns the states.
        data_variants.aggregator = nullptr;
    }

    size_t rows = 0;
    size_t bytes = 0;

    for (const auto & block : blocks)
    {
        rows += block.rows();
        bytes += block.bytes();
    }

    double elapsed_seconds = watch.elapsedSeconds();
    LOG_DEBUG(log,
        "Converted aggregated data to blocks. {} rows, {} in {} sec. ({:.3f} rows/sec., {}/sec.)",
        rows, ReadableSize(bytes),
        elapsed_seconds, rows / elapsed_seconds,
        ReadableSize(bytes / elapsed_seconds));

    return blocks;
}


template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeDataNullKey(
    Table & table_dst,
    Table & table_src,
    Arena * arena) const
{
    if constexpr (Method::low_cardinality_optimization)
    {
        if (table_src.hasNullKeyData())
        {
            if (!table_dst.hasNullKeyData())
            {
                table_dst.hasNullKeyData() = true;
                table_dst.getNullKeyData() = table_src.getNullKeyData();
            }
            else
            {
                for (size_t i = 0; i < params.aggregates_size; ++i)
                    aggregate_functions[i]->merge(
                            table_dst.getNullKeyData() + offsets_of_aggregate_states[i],
                            table_src.getNullKeyData() + offsets_of_aggregate_states[i],
                            arena);

                for (size_t i = 0; i < params.aggregates_size; ++i)
                    aggregate_functions[i]->destroy(
                            table_src.getNullKeyData() + offsets_of_aggregate_states[i]);
            }

            table_src.hasNullKeyData() = false;
            table_src.getNullKeyData() = nullptr;
        }
    }
}


template <typename Method, bool use_compiled_functions, typename Table>
void NO_INLINE Aggregator::mergeDataImpl(
    Table & table_dst,
    Table & table_src,
    Arena * arena) const
{
    if constexpr (Method::low_cardinality_optimization)
        mergeDataNullKey<Method, Table>(table_dst, table_src, arena);

    table_src.mergeToViaEmplace(table_dst, [&](AggregateDataPtr & __restrict dst, AggregateDataPtr & __restrict src, bool inserted)
    {
        if (!inserted)
        {
#if USE_EMBEDDED_COMPILER
            if constexpr (use_compiled_functions)
            {
                const auto & compiled_functions = compiled_aggregate_functions_holder->compiled_aggregate_functions;
                compiled_functions.merge_aggregate_states_function(dst, src);

                if (compiled_aggregate_functions_holder->compiled_aggregate_functions.functions_count != params.aggregates_size)
                {
                    for (size_t i = 0; i < params.aggregates_size; ++i)
                    {
                        if (!is_aggregate_function_compiled[i])
                            aggregate_functions[i]->merge(dst + offsets_of_aggregate_states[i], src + offsets_of_aggregate_states[i], arena);
                    }

                    for (size_t i = 0; i < params.aggregates_size; ++i)
                    {
                        if (!is_aggregate_function_compiled[i])
                            aggregate_functions[i]->destroy(src + offsets_of_aggregate_states[i]);
                    }
                }
            }
            else
#endif
            {
                for (size_t i = 0; i < params.aggregates_size; ++i)
                    aggregate_functions[i]->merge(dst + offsets_of_aggregate_states[i], src + offsets_of_aggregate_states[i], arena);

                for (size_t i = 0; i < params.aggregates_size; ++i)
                    aggregate_functions[i]->destroy(src + offsets_of_aggregate_states[i]);
            }
        }
        else
        {
            dst = src;
        }

        src = nullptr;
    });

    table_src.clearAndShrink();
}


template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeDataNoMoreKeysImpl(
    Table & table_dst,
    AggregatedDataWithoutKey & overflows,
    Table & table_src,
    Arena * arena) const
{
    /// Note : will create data for NULL key if not exist
    if constexpr (Method::low_cardinality_optimization)
        mergeDataNullKey<Method, Table>(table_dst, table_src, arena);

    table_src.mergeToViaFind(table_dst, [&](AggregateDataPtr dst, AggregateDataPtr & src, bool found)
    {
        AggregateDataPtr res_data = found ? dst : overflows;

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->merge(
                res_data + offsets_of_aggregate_states[i],
                src + offsets_of_aggregate_states[i],
                arena);

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->destroy(src + offsets_of_aggregate_states[i]);

        src = nullptr;
    });
    table_src.clearAndShrink();
}

template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeDataOnlyExistingKeysImpl(
    Table & table_dst,
    Table & table_src,
    Arena * arena) const
{
    /// Note : will create data for NULL key if not exist
    if constexpr (Method::low_cardinality_optimization)
        mergeDataNullKey<Method, Table>(table_dst, table_src, arena);

    table_src.mergeToViaFind(table_dst,
        [&](AggregateDataPtr dst, AggregateDataPtr & src, bool found)
    {
        if (!found)
            return;

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->merge(
                dst + offsets_of_aggregate_states[i],
                src + offsets_of_aggregate_states[i],
                arena);

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->destroy(src + offsets_of_aggregate_states[i]);

        src = nullptr;
    });
    table_src.clearAndShrink();
}


void NO_INLINE Aggregator::mergeWithoutKeyDataImpl(
    ManyAggregatedDataVariants & non_empty_data) const
{
    AggregatedDataVariantsPtr & res = non_empty_data[0];

    /// We merge all aggregation results to the first.
    for (size_t result_num = 1, size = non_empty_data.size(); result_num < size; ++result_num)
    {
        AggregatedDataWithoutKey & res_data = res->without_key;
        AggregatedDataWithoutKey & current_data = non_empty_data[result_num]->without_key;

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->merge(res_data + offsets_of_aggregate_states[i], current_data + offsets_of_aggregate_states[i], res->aggregates_pool);

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->destroy(current_data + offsets_of_aggregate_states[i]);

        current_data = nullptr;
    }
}


template <typename Method>
void NO_INLINE Aggregator::mergeSingleLevelDataImpl(
    ManyAggregatedDataVariants & non_empty_data) const
{
    AggregatedDataVariantsPtr & res = non_empty_data[0];
    bool no_more_keys = false;

    /// We merge all aggregation results to the first.
    for (size_t result_num = 1, size = non_empty_data.size(); result_num < size; ++result_num)
    {
        if (!checkLimits(res->sizeWithoutOverflowRow(), no_more_keys))
            break;

        AggregatedDataVariants & current = *non_empty_data[result_num];

        if (!no_more_keys)
        {
#if USE_EMBEDDED_COMPILER
            if (compiled_aggregate_functions_holder)
            {
                mergeDataImpl<Method, true>(
                    getDataVariant<Method>(*res).data,
                    getDataVariant<Method>(current).data,
                    res->aggregates_pool);
            }
            else
#endif
            {
                mergeDataImpl<Method, false>(
                    getDataVariant<Method>(*res).data,
                    getDataVariant<Method>(current).data,
                    res->aggregates_pool);
            }
        }
        else if (res->without_key)
        {
            mergeDataNoMoreKeysImpl<Method>(
                getDataVariant<Method>(*res).data,
                res->without_key,
                getDataVariant<Method>(current).data,
                res->aggregates_pool);
        }
        else
        {
            mergeDataOnlyExistingKeysImpl<Method>(
                getDataVariant<Method>(*res).data,
                getDataVariant<Method>(current).data,
                res->aggregates_pool);
        }

        /// `current` will not destroy the states of aggregate functions in the destructor
        current.aggregator = nullptr;
    }
}

#define M(NAME) \
    template void NO_INLINE Aggregator::mergeSingleLevelDataImpl<decltype(AggregatedDataVariants::NAME)::element_type>( \
        ManyAggregatedDataVariants & non_empty_data) const;
    APPLY_FOR_VARIANTS_SINGLE_LEVEL(M)
#undef M

template <typename Method>
void NO_INLINE Aggregator::mergeBucketImpl(
    ManyAggregatedDataVariants & data, Int32 bucket, Arena * arena, std::atomic<bool> * is_cancelled) const
{
    /// We merge all aggregation results to the first.
    AggregatedDataVariantsPtr & res = data[0];
    for (size_t result_num = 1, size = data.size(); result_num < size; ++result_num)
    {
        if (is_cancelled && is_cancelled->load(std::memory_order_seq_cst))
            return;

        AggregatedDataVariants & current = *data[result_num];
#if USE_EMBEDDED_COMPILER
        if (compiled_aggregate_functions_holder)
        {
            mergeDataImpl<Method, true>(
                getDataVariant<Method>(*res).data.impls[bucket],
                getDataVariant<Method>(current).data.impls[bucket],
                arena);
        }
        else
#endif
        {
            mergeDataImpl<Method, false>(
                getDataVariant<Method>(*res).data.impls[bucket],
                getDataVariant<Method>(current).data.impls[bucket],
                arena);
        }
    }
}

ManyAggregatedDataVariants Aggregator::prepareVariantsToMerge(ManyAggregatedDataVariants & data_variants) const
{
    if (data_variants.empty())
        throw Exception("Empty data passed to Aggregator::mergeAndConvertToBlocks.", ErrorCodes::EMPTY_DATA_PASSED);

    LOG_TRACE(log, "Merging aggregated data");

    if (params.stats_collecting_params.isCollectionAndUseEnabled())
        updateStatistics(data_variants, params.stats_collecting_params);

    ManyAggregatedDataVariants non_empty_data;
    non_empty_data.reserve(data_variants.size());
    for (auto & data : data_variants)
        if (!data->empty())
            non_empty_data.push_back(data);

    if (non_empty_data.empty())
        return {};

    if (non_empty_data.size() > 1)
    {
        /// Sort the states in descending order so that the merge is more efficient (since all states are merged into the first).
        ::sort(non_empty_data.begin(), non_empty_data.end(),
            [](const AggregatedDataVariantsPtr & lhs, const AggregatedDataVariantsPtr & rhs)
            {
                return lhs->sizeWithoutOverflowRow() > rhs->sizeWithoutOverflowRow();
            });
    }

    /// If at least one of the options is two-level, then convert all the options into two-level ones, if there are not such.
    /// Note - perhaps it would be more optimal not to convert single-level versions before the merge, but merge them separately, at the end.

    bool has_at_least_one_two_level = false;
    for (const auto & variant : non_empty_data)
    {
        if (variant->isTwoLevel())
        {
            has_at_least_one_two_level = true;
            break;
        }
    }

    if (has_at_least_one_two_level)
        for (auto & variant : non_empty_data)
            if (!variant->isTwoLevel())
                variant->convertToTwoLevel();

    AggregatedDataVariantsPtr & first = non_empty_data[0];

    for (size_t i = 1, size = non_empty_data.size(); i < size; ++i)
    {
        if (first->type != non_empty_data[i]->type)
            throw Exception("Cannot merge different aggregated data variants.", ErrorCodes::CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS);

        /** Elements from the remaining sets can be moved to the first data set.
          * Therefore, it must own all the arenas of all other sets.
          */
        first->aggregates_pools.insert(first->aggregates_pools.end(),
            non_empty_data[i]->aggregates_pools.begin(), non_empty_data[i]->aggregates_pools.end());
    }

    return non_empty_data;
}

template <bool no_more_keys, typename Method, typename Table>
void NO_INLINE Aggregator::mergeStreamsImplCase(
    Arena * aggregates_pool,
    Method & method [[maybe_unused]],
    Table & data,
    AggregateDataPtr overflow_row,
    size_t row_begin,
    size_t row_end,
    const AggregateColumnsConstData & aggregate_columns_data,
    const ColumnRawPtrs & key_columns) const
{
    typename Method::State state(key_columns, key_sizes, aggregation_state_cache);

    std::unique_ptr<AggregateDataPtr[]> places(new AggregateDataPtr[row_end]);

    for (size_t i = row_begin; i < row_end; ++i)
    {
        AggregateDataPtr aggregate_data = nullptr;

        if (!no_more_keys)
        {
            auto emplace_result = state.emplaceKey(data, i, *aggregates_pool);
            if (emplace_result.isInserted())
            {
                emplace_result.setMapped(nullptr);

                aggregate_data = aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
                createAggregateStates(aggregate_data);

                emplace_result.setMapped(aggregate_data);
            }
            else
                aggregate_data = emplace_result.getMapped();
        }
        else
        {
            auto find_result = state.findKey(data, i, *aggregates_pool);
            if (find_result.isFound())
                aggregate_data = find_result.getMapped();
        }

        /// aggregate_date == nullptr means that the new key did not fit in the hash table because of no_more_keys.

        AggregateDataPtr value = aggregate_data ? aggregate_data : overflow_row;
        places[i] = value;
    }

    for (size_t j = 0; j < params.aggregates_size; ++j)
    {
        /// Merge state of aggregate functions.
        aggregate_functions[j]->mergeBatch(
            row_begin, row_end,
            places.get(), offsets_of_aggregate_states[j],
            aggregate_columns_data[j]->data(),
            aggregates_pool);
    }
}

template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeStreamsImpl(
    Block block,
    Arena * aggregates_pool,
    Method & method,
    Table & data,
    AggregateDataPtr overflow_row,
    bool no_more_keys) const
{
    const AggregateColumnsConstData & aggregate_columns_data = params.makeAggregateColumnsData(block);
    const ColumnRawPtrs & key_columns = params.makeRawKeyColumns(block);

    mergeStreamsImpl<Method, Table>(
        aggregates_pool,
        method,
        data,
        overflow_row,
        no_more_keys,
        0,
        block.rows(),
        aggregate_columns_data,
        key_columns);
}

template <typename Method, typename Table>
void NO_INLINE Aggregator::mergeStreamsImpl(
    Arena * aggregates_pool,
    Method & method,
    Table & data,
    AggregateDataPtr overflow_row,
    bool no_more_keys,
    size_t row_begin,
    size_t row_end,
    const AggregateColumnsConstData & aggregate_columns_data,
    const ColumnRawPtrs & key_columns) const
{
    if (!no_more_keys)
        mergeStreamsImplCase<false>(aggregates_pool, method, data, overflow_row, row_begin, row_end, aggregate_columns_data, key_columns);
    else
        mergeStreamsImplCase<true>(aggregates_pool, method, data, overflow_row, row_begin, row_end, aggregate_columns_data, key_columns);
}


void NO_INLINE Aggregator::mergeBlockWithoutKeyStreamsImpl(
    Block block,
    AggregatedDataVariants & result) const
{
    AggregateColumnsConstData aggregate_columns = params.makeAggregateColumnsData(block);
    mergeWithoutKeyStreamsImpl(result, 0, block.rows(), aggregate_columns);
}
void NO_INLINE Aggregator::mergeWithoutKeyStreamsImpl(
    AggregatedDataVariants & result,
    size_t row_begin,
    size_t row_end,
    const AggregateColumnsConstData & aggregate_columns_data) const
{
    AggregatedDataWithoutKey & res = result.without_key;
    if (!res)
    {
        AggregateDataPtr place = result.aggregates_pool->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
        createAggregateStates(place);
        res = place;
    }

    for (size_t row = row_begin; row < row_end; ++row)
    {
        /// Adding Values
        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->merge(res + offsets_of_aggregate_states[i], (*aggregate_columns_data[i])[row], result.aggregates_pool);
    }
}


bool Aggregator::mergeOnBlock(Block block, AggregatedDataVariants & result, bool & no_more_keys) const
{
    /// `result` will destroy the states of aggregate functions in the destructor
    result.aggregator = this;

    /// How to perform the aggregation?
    if (result.empty())
    {
        result.init(method_chosen);
        result.keys_size = params.keys_size;
        result.key_sizes = key_sizes;
        LOG_TRACE(log, "Aggregation method: {}", result.getMethodName());
    }

    if (result.type == AggregatedDataVariants::Type::without_key || block.info.is_overflows)
        mergeBlockWithoutKeyStreamsImpl(std::move(block), result);
#define M(NAME, IS_TWO_LEVEL) \
    else if (result.type == AggregatedDataVariants::Type::NAME) \
        mergeStreamsImpl(std::move(block), result.aggregates_pool, *result.NAME, result.NAME->data, result.without_key, no_more_keys);

    APPLY_FOR_AGGREGATED_VARIANTS(M)
#undef M
    else if (result.type != AggregatedDataVariants::Type::without_key)
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    size_t result_size = result.sizeWithoutOverflowRow();
    Int64 current_memory_usage = 0;
    if (auto * memory_tracker_child = CurrentThread::getMemoryTracker())
        if (auto * memory_tracker = memory_tracker_child->getParent())
            current_memory_usage = memory_tracker->get();

    /// Here all the results in the sum are taken into account, from different threads.
    auto result_size_bytes = current_memory_usage - memory_usage_before_aggregation;

    bool worth_convert_to_two_level = worthConvertToTwoLevel(
        params.group_by_two_level_threshold, result_size, params.group_by_two_level_threshold_bytes, result_size_bytes);

    /** Converting to a two-level data structure.
      * It allows you to make, in the subsequent, an effective merge - either economical from memory or parallel.
      */
    if (result.isConvertibleToTwoLevel() && worth_convert_to_two_level)
        result.convertToTwoLevel();

    /// Checking the constraints.
    if (!checkLimits(result_size, no_more_keys))
        return false;

    /** Flush data to disk if too much RAM is consumed.
      * Data can only be flushed to disk if a two-level aggregation structure is used.
      */
    if (params.max_bytes_before_external_group_by
        && result.isTwoLevel()
        && current_memory_usage > static_cast<Int64>(params.max_bytes_before_external_group_by)
        && worth_convert_to_two_level)
    {
        size_t size = current_memory_usage + params.min_free_disk_space;

        std::string tmp_path = params.tmp_volume->getDisk()->getPath();

        // enoughSpaceInDirectory() is not enough to make it right, since
        // another process (or another thread of aggregator) can consume all
        // space.
        //
        // But true reservation (IVolume::reserve()) cannot be used here since
        // current_memory_usage does not takes compression into account and
        // will reserve way more that actually will be used.
        //
        // Hence let's do a simple check.
        if (!enoughSpaceInDirectory(tmp_path, size))
            throw Exception("Not enough space for external aggregation in " + tmp_path, ErrorCodes::NOT_ENOUGH_SPACE);

        writeToTemporaryFile(result, tmp_path);
    }

    return true;
}


void Aggregator::mergeBlocks(BucketToBlocks bucket_to_blocks, AggregatedDataVariants & result, size_t max_threads)
{
    if (bucket_to_blocks.empty())
        return;

    UInt64 total_input_rows = 0;
    for (auto & bucket : bucket_to_blocks)
        for (auto & block : bucket.second)
            total_input_rows += block.rows();

    /** `minus one` means the absence of information about the bucket
      * - in the case of single-level aggregation, as well as for blocks with "overflowing" values.
      * If there is at least one block with a bucket number greater or equal than zero, then there was a two-level aggregation.
      */
    auto max_bucket = bucket_to_blocks.rbegin()->first;
    bool has_two_level = max_bucket >= 0;

    if (has_two_level)
    {
    #define M(NAME) \
        if (method_chosen == AggregatedDataVariants::Type::NAME) \
            method_chosen = AggregatedDataVariants::Type::NAME ## _two_level;

        APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)

    #undef M
    }

    /// result will destroy the states of aggregate functions in the destructor
    result.aggregator = this;

    result.init(method_chosen);
    result.keys_size = params.keys_size;
    result.key_sizes = key_sizes;

    bool has_blocks_with_unknown_bucket = bucket_to_blocks.contains(-1);

    /// First, parallel the merge for the individual buckets. Then we continue merge the data not allocated to the buckets.
    if (has_two_level)
    {
        /** In this case, no_more_keys is not supported due to the fact that
          *  from different threads it is difficult to update the general state for "other" keys (overflows).
          * That is, the keys in the end can be significantly larger than max_rows_to_group_by.
          */

        LOG_TRACE(log, "Merging partially aggregated two-level data.");

        auto merge_bucket = [&bucket_to_blocks, &result, this](Int32 bucket, Arena * aggregates_pool, ThreadGroupStatusPtr thread_group)
        {
            if (thread_group)
                CurrentThread::attachToIfDetached(thread_group);

            for (Block & block : bucket_to_blocks[bucket])
            {
            #define M(NAME) \
                else if (result.type == AggregatedDataVariants::Type::NAME) \
                    mergeStreamsImpl(std::move(block), aggregates_pool, *result.NAME, result.NAME->data.impls[bucket], nullptr, false);

                if (false) {} // NOLINT
                    APPLY_FOR_VARIANTS_TWO_LEVEL(M)
            #undef M
                else
                    throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
            }
        };

        std::unique_ptr<ThreadPool> thread_pool;
        if (max_threads > 1 && total_input_rows > 100000)    /// TODO Make a custom threshold.
            thread_pool = std::make_unique<ThreadPool>(max_threads);

        for (const auto & bucket_blocks : bucket_to_blocks)
        {
            const auto bucket = bucket_blocks.first;

            if (bucket == -1)
                continue;

            result.aggregates_pools.push_back(std::make_shared<Arena>());
            Arena * aggregates_pool = result.aggregates_pools.back().get();

            auto task = [group = CurrentThread::getGroup(), bucket, &merge_bucket, aggregates_pool]{ return merge_bucket(bucket, aggregates_pool, group); };

            if (thread_pool)
                thread_pool->scheduleOrThrowOnError(task);
            else
                task();
        }

        if (thread_pool)
            thread_pool->wait();

        LOG_TRACE(log, "Merged partially aggregated two-level data.");
    }

    if (has_blocks_with_unknown_bucket)
    {
        LOG_TRACE(log, "Merging partially aggregated single-level data.");

        bool no_more_keys = false;

        BlocksList & blocks = bucket_to_blocks[-1];
        for (Block & block : blocks)
        {
            if (!checkLimits(result.sizeWithoutOverflowRow(), no_more_keys))
                break;

            if (result.type == AggregatedDataVariants::Type::without_key || block.info.is_overflows)
                mergeBlockWithoutKeyStreamsImpl(std::move(block), result);

        #define M(NAME, IS_TWO_LEVEL) \
            else if (result.type == AggregatedDataVariants::Type::NAME) \
                mergeStreamsImpl(std::move(block), result.aggregates_pool, *result.NAME, result.NAME->data, result.without_key, no_more_keys);

            APPLY_FOR_AGGREGATED_VARIANTS(M)
        #undef M
            else if (result.type != AggregatedDataVariants::Type::without_key)
                throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
        }

        LOG_TRACE(log, "Merged partially aggregated single-level data.");
    }
}


Block Aggregator::mergeBlocks(BlocksList & blocks, bool final)
{
    if (blocks.empty())
        return {};

    auto bucket_num = blocks.front().info.bucket_num;
    bool is_overflows = blocks.front().info.is_overflows;

    LOG_TRACE(log, "Merging partially aggregated blocks (bucket = {}).", bucket_num);
    Stopwatch watch;

    /** If possible, change 'method' to some_hash64. Otherwise, leave as is.
      * Better hash function is needed because during external aggregation,
      *  we may merge partitions of data with total number of keys far greater than 4 billion.
      */
    auto merge_method = method_chosen;

#define APPLY_FOR_VARIANTS_THAT_MAY_USE_BETTER_HASH_FUNCTION(M) \
        M(key64)            \
        M(key_string)       \
        M(key_fixed_string) \
        M(keys128)          \
        M(keys256)          \
        M(serialized)       \

#define M(NAME) \
    if (merge_method == AggregatedDataVariants::Type::NAME) \
        merge_method = AggregatedDataVariants::Type::NAME ## _hash64; \

    APPLY_FOR_VARIANTS_THAT_MAY_USE_BETTER_HASH_FUNCTION(M)
#undef M

#undef APPLY_FOR_VARIANTS_THAT_MAY_USE_BETTER_HASH_FUNCTION

    /// Temporary data for aggregation.
    AggregatedDataVariants result;

    /// result will destroy the states of aggregate functions in the destructor
    result.aggregator = this;

    result.init(merge_method);
    result.keys_size = params.keys_size;
    result.key_sizes = key_sizes;

    size_t source_rows = 0;

    for (Block & block : blocks)
    {
        source_rows += block.rows();

        if (bucket_num >= 0 && block.info.bucket_num != bucket_num)
            bucket_num = -1;

        if (result.type == AggregatedDataVariants::Type::without_key || is_overflows)
            mergeBlockWithoutKeyStreamsImpl(std::move(block), result);

    #define M(NAME, IS_TWO_LEVEL) \
        else if (result.type == AggregatedDataVariants::Type::NAME) \
            mergeStreamsImpl(std::move(block), result.aggregates_pool, *result.NAME, result.NAME->data, nullptr, false);

        APPLY_FOR_AGGREGATED_VARIANTS(M)
    #undef M
        else if (result.type != AggregatedDataVariants::Type::without_key)
            throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
    }

    Block block;
    if (result.type == AggregatedDataVariants::Type::without_key || is_overflows)
        block = prepareBlockAndFillWithoutKey(result, final, is_overflows);
    else
        block = prepareBlockAndFillSingleLevel(result, final);
    /// NOTE: two-level data is not possible here - chooseAggregationMethod chooses only among single-level methods.

    if (!final)
    {
        /// Pass ownership of aggregate function states from result to ColumnAggregateFunction objects in the resulting block.
        result.aggregator = nullptr;
    }

    size_t rows = block.rows();
    size_t bytes = block.bytes();
    double elapsed_seconds = watch.elapsedSeconds();
    LOG_DEBUG(
        log,
        "Merged partially aggregated blocks for bucket #{}. Got {} rows, {} from {} source rows in {} sec. ({:.3f} rows/sec., {}/sec.)",
        bucket_num,
        rows,
        ReadableSize(bytes),
        source_rows,
        elapsed_seconds,
        rows / elapsed_seconds,
        ReadableSize(bytes / elapsed_seconds));

    block.info.bucket_num = bucket_num;
    return block;
}

template <typename Method>
void NO_INLINE Aggregator::convertBlockToTwoLevelImpl(
    Method & method,
    Arena * pool,
    ColumnRawPtrs & key_columns,
    const Block & source,
    std::vector<Block> & destinations) const
{
    typename Method::State state(key_columns, key_sizes, aggregation_state_cache);

    size_t rows = source.rows();
    size_t columns = source.columns();

    /// Create a 'selector' that will contain bucket index for every row. It will be used to scatter rows to buckets.
    IColumn::Selector selector(rows);

    /// For every row.
    for (size_t i = 0; i < rows; ++i)
    {
        if constexpr (Method::low_cardinality_optimization)
        {
            if (state.isNullAt(i))
            {
                selector[i] = 0;
                continue;
            }
        }

        /// Calculate bucket number from row hash.
        auto hash = state.getHash(method.data, i, *pool);
        auto bucket = method.data.getBucketFromHash(hash);

        selector[i] = bucket;
    }

    size_t num_buckets = destinations.size();

    for (size_t column_idx = 0; column_idx < columns; ++column_idx)
    {
        const ColumnWithTypeAndName & src_col = source.getByPosition(column_idx);
        MutableColumns scattered_columns = src_col.column->scatter(num_buckets, selector);

        for (size_t bucket = 0, size = num_buckets; bucket < size; ++bucket)
        {
            if (!scattered_columns[bucket]->empty())
            {
                Block & dst = destinations[bucket];
                dst.info.bucket_num = bucket;
                dst.insert({std::move(scattered_columns[bucket]), src_col.type, src_col.name});
            }

            /** Inserted columns of type ColumnAggregateFunction will own states of aggregate functions
              *  by holding shared_ptr to source column. See ColumnAggregateFunction.h
              */
        }
    }
}


std::vector<Block> Aggregator::convertBlockToTwoLevel(const Block & block) const
{
    if (!block)
        return {};

    AggregatedDataVariants data;

    ColumnRawPtrs key_columns(params.keys_size);

    /// Remember the columns we will work with
    for (size_t i = 0; i < params.keys_size; ++i)
        key_columns[i] = block.safeGetByPosition(i).column.get();

    AggregatedDataVariants::Type type = method_chosen;
    data.keys_size = params.keys_size;
    data.key_sizes = key_sizes;

#define M(NAME) \
    else if (type == AggregatedDataVariants::Type::NAME) \
        type = AggregatedDataVariants::Type::NAME ## _two_level;

    if (false) {} // NOLINT
    APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)
#undef M
    else
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    data.init(type);

    size_t num_buckets = 0;

#define M(NAME) \
    else if (data.type == AggregatedDataVariants::Type::NAME) \
        num_buckets = data.NAME->data.NUM_BUCKETS;

    if (false) {} // NOLINT
    APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M
    else
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    std::vector<Block> splitted_blocks(num_buckets);

#define M(NAME) \
    else if (data.type == AggregatedDataVariants::Type::NAME) \
        convertBlockToTwoLevelImpl(*data.NAME, data.aggregates_pool, \
            key_columns, block, splitted_blocks);

    if (false) {} // NOLINT
    APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M
    else
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);

    return splitted_blocks;
}


template <typename Method, typename Table>
void NO_INLINE Aggregator::destroyImpl(Table & table) const
{
    table.forEachMapped([&](AggregateDataPtr & data)
    {
        /** If an exception (usually a lack of memory, the MemoryTracker throws) arose
          *  after inserting the key into a hash table, but before creating all states of aggregate functions,
          *  then data will be equal nullptr.
          */
        if (nullptr == data)
            return;

        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->destroy(data + offsets_of_aggregate_states[i]);

        data = nullptr;
    });
}


void Aggregator::destroyWithoutKey(AggregatedDataVariants & result) const
{
    AggregatedDataWithoutKey & res_data = result.without_key;

    if (nullptr != res_data)
    {
        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->destroy(res_data + offsets_of_aggregate_states[i]);

        res_data = nullptr;
    }
}


void Aggregator::destroyAllAggregateStates(AggregatedDataVariants & result) const
{
    if (result.empty())
        return;

    LOG_TRACE(log, "Destroying aggregate states");

    /// In what data structure is the data aggregated?
    if (result.type == AggregatedDataVariants::Type::without_key || params.overflow_row)
        destroyWithoutKey(result);

#define M(NAME, IS_TWO_LEVEL) \
    else if (result.type == AggregatedDataVariants::Type::NAME) \
        destroyImpl<decltype(result.NAME)::element_type>(result.NAME->data);

    if (false) {} // NOLINT
    APPLY_FOR_AGGREGATED_VARIANTS(M)
#undef M
    else if (result.type != AggregatedDataVariants::Type::without_key)
        throw Exception("Unknown aggregated data variant.", ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT);
}


}
