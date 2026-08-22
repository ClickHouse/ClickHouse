#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>
#include <Common/ColumnsHashing.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashMap.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/AggregatedData.h>
#include <base/types.h>

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <type_traits>
#include <utility>

using namespace DB;

namespace
{

template <typename Key>
using KeyHash = std::conditional_t<std::is_same_v<Key, UInt128>, UInt128HashCRC32, UInt256HashCRC32>;

template <typename Key>
using BenchmarkData = AggregationDataWithNullKey<HashMap<Key, AggregateDataPtr, KeyHash<Key>>>;

template <typename Key>
using OneNumberMethod = ColumnsHashing::HashMethodOneNumber<
    typename BenchmarkData<Key>::value_type,
    AggregateDataPtr,
    Key,
    false>;

template <typename Key>
using SingleLowCardinalityMethod = ColumnsHashing::HashMethodSingleLowCardinalityColumn<
    OneNumberMethod<Key>,
    AggregateDataPtr,
    true>;

template <typename Key>
using PackedLowCardinalityMethod = ColumnsHashing::HashMethodKeysFixed<
    typename BenchmarkData<Key>::value_type,
    Key,
    AggregateDataPtr,
    false,
    true,
    false>;

template <typename Index>
size_t getAddressableDictionarySize(size_t dictionary_size)
{
    if constexpr (sizeof(Index) < sizeof(UInt64))
    {
        return std::min(
            dictionary_size,
            static_cast<size_t>(std::numeric_limits<Index>::max()) + 1);
    }
    else
        return dictionary_size;
}

template <typename Key, typename Index = UInt32>
ColumnPtr makeSharedKeyColumn(size_t dictionary_size, size_t rows, size_t active_dictionary_size, bool randomize_indexes = true)
{
    auto dictionary_keys = ColumnVector<Key>::create(dictionary_size);
    auto & dictionary_data = dictionary_keys->getData();
    for (size_t i = 0; i < dictionary_size; ++i)
        dictionary_data[i] = static_cast<Key>(i);

    auto nested_type = std::make_shared<DataTypeNumber<Key>>();
    MutableColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(*nested_type, std::move(dictionary_keys));

    auto indexes = ColumnVector<Index>::create(rows);
    auto & index_data = indexes->getData();
    const size_t addressable_dictionary_size = getAddressableDictionarySize<Index>(dictionary_size);
    for (size_t row = 0; row < rows; ++row)
    {
        /// The randomized case spreads the active entries over the range
        /// addressable by the index type. For the power-of-two sizes used
        /// below, the odd multiplier makes this a permutation rather than
        /// collapsing the active set.
        const UInt64 active_index = row % active_dictionary_size;
        const UInt64 dictionary_index = randomize_indexes
            ? (active_index * 0x9E3779B1ULL) % addressable_dictionary_size
            : active_index;
        index_data[row] = static_cast<Index>(dictionary_index);
    }

    return ColumnLowCardinality::create(std::move(dictionary), std::move(indexes), /*is_shared=*/true);
}

template <typename Key, bool use_single_low_cardinality_method>
void aggregateSharedDictionaryBlocks(benchmark::State & state, size_t aggregation_states)
{
    const size_t dictionary_size = 1ULL << state.range(0);
    const size_t rows = 1ULL << state.range(1);
    const size_t active_dictionary_size = 1ULL << state.range(2);

    ColumnPtr column = makeSharedKeyColumn<Key>(dictionary_size, rows, active_dictionary_size);
    ColumnRawPtrs key_columns{column.get()};
    Sizes key_sizes{sizeof(Key)};

    ColumnsHashing::HashMethodContextSettings context_settings;
    context_settings.max_threads = 1;

    using Method = std::conditional_t<
        use_single_low_cardinality_method,
        SingleLowCardinalityMethod<Key>,
        PackedLowCardinalityMethod<Key>>;

    ColumnsHashing::HashMethodContextPtr context;
    if constexpr (use_single_low_cardinality_method)
        context = Method::createContext(context_settings);

    /// Aggregate-state addresses must stay stable because the single-LC
    /// method caches them by dictionary position.
    PaddedPODArray<UInt64> aggregate_states;
    aggregate_states.assign(active_dictionary_size * aggregation_states, UInt64{0});
    bool used_dictionary_cache = false;

    for (auto _ : state)
    {
        for (size_t aggregation_state = 0; aggregation_state < aggregation_states; ++aggregation_state)
        {
            BenchmarkData<Key> data;
            Arena pool;
            Method method(key_columns, key_sizes, context);
            if constexpr (use_single_low_cardinality_method)
                used_dictionary_cache = method.isUsingDictionaryCache();

            size_t next_aggregate_state = aggregation_state * active_dictionary_size;

            auto aggregate = [&](auto & dispatched_method)
            {
                for (size_t row = 0; row < rows; ++row)
                {
                    auto result = dispatched_method.emplaceKey(data, row, pool);
                    if (result.isInserted())
                        result.setMapped(reinterpret_cast<AggregateDataPtr>(&aggregate_states[next_aggregate_state++]));

                    ++*reinterpret_cast<UInt64 *>(result.getMapped());
                }
            };
            ColumnsHashing::dispatchLowCardinalityDictionaryCache(method, aggregate);

            benchmark::DoNotOptimize(data.size());
        }
        benchmark::ClobberMemory();
    }

    const auto iterations = static_cast<int64_t>(state.iterations());
    const auto processed_rows = static_cast<int64_t>(rows * aggregation_states);
    state.SetItemsProcessed(iterations * processed_rows);
    state.SetBytesProcessed(iterations * processed_rows * static_cast<int64_t>(sizeof(UInt32)));
    state.counters["active_entries"] = static_cast<double>(active_dictionary_size);
    state.counters["aggregation_states"] = static_cast<double>(aggregation_states);
    state.counters["dictionary_cache"] = used_dictionary_cache ? 1.0 : 0.0;
    state.counters["dictionary_entries"] = static_cast<double>(dictionary_size);
    if (used_dictionary_cache)
    {
        state.counters["single_lc_cache_bytes"]
            = static_cast<double>(dictionary_size * aggregation_states * (sizeof(UInt64 *) + sizeof(UInt8)));
    }
}

template <typename Key, bool use_single_low_cardinality_method>
void AggregateSharedDictionaryBlock(benchmark::State & state)
{
    aggregateSharedDictionaryBlocks<Key, use_single_low_cardinality_method>(state, 1);
}

template <typename Key>
void AggregateSharedDictionaryStates(benchmark::State & state)
{
    aggregateSharedDictionaryBlocks<Key, true>(state, static_cast<size_t>(state.range(3)));
}

template <typename Index, bool randomize_indexes>
void ReadSharedDictionaryIndexes(benchmark::State & state)
{
    const size_t dictionary_size = 1ULL << state.range(0);
    const size_t rows = 1ULL << state.range(1);

    const size_t addressable_dictionary_size = getAddressableDictionarySize<Index>(dictionary_size);

    ColumnPtr column = makeSharedKeyColumn<UInt128, Index>(
        dictionary_size, rows, addressable_dictionary_size, randomize_indexes);
    ColumnRawPtrs key_columns{column.get()};
    Sizes key_sizes{sizeof(UInt128)};

    ColumnsHashing::HashMethodContextSettings context_settings;
    context_settings.max_threads = 1;
    auto context = SingleLowCardinalityMethod<UInt128>::createContext(context_settings);
    SingleLowCardinalityMethod<UInt128> method(key_columns, key_sizes, context);
    const bool used_dictionary_cache = method.isUsingDictionaryCache();

    for (auto _ : state)
    {
        UInt64 checksum = 0;
        for (size_t row = 0; row < rows; ++row)
            checksum += method.getIndexAt(row);
        benchmark::DoNotOptimize(checksum);
    }

    const auto iterations = static_cast<int64_t>(state.iterations());
    const auto processed_rows = static_cast<int64_t>(rows);
    state.SetItemsProcessed(iterations * processed_rows);
    state.SetBytesProcessed(iterations * processed_rows * static_cast<int64_t>(sizeof(Index)));
    state.counters["dictionary_cache"] = used_dictionary_cache ? 1.0 : 0.0;
    state.counters["dictionary_entries"] = static_cast<double>(dictionary_size);
    state.counters["index_bytes"] = static_cast<double>(sizeof(Index));
    state.counters["randomized"] = randomize_indexes ? 1.0 : 0.0;
}

void dictionarySizeSweep(benchmark::internal::Benchmark * benchmark)
{
    /// Small and dense: the optimized single-LC method should win.
    benchmark->Args({8, 20, 8});

    /// Keep one 65,536-row block with 4,096 active dictionary entries while
    /// increasing the size of the shared dictionary around it.
    benchmark->Args({12, 16, 12});
    benchmark->Args({16, 16, 12});
    benchmark->Args({20, 16, 12});
    benchmark->Args({22, 16, 12});

    /// At the production-sized dictionary, vary how much of it this block
    /// actually touches.
    benchmark->Args({22, 16, 8});
    benchmark->Args({22, 16, 16});
}

void aggregationStateSweep(benchmark::internal::Benchmark * benchmark)
{
    /// The 4,194,304-entry dictionary takes the uncached path. Process the
    /// same block through one or 64 independent aggregation states to model
    /// single-threaded and highly parallel aggregation.
    benchmark->Args({22, 16, 12, 1});
    benchmark->Args({22, 16, 12, 64});
    benchmark->Args({22, 16, 16, 1});
    benchmark->Args({22, 16, 16, 64});
}

BENCHMARK_TEMPLATE(AggregateSharedDictionaryBlock, UInt128, true)->Apply(dictionarySizeSweep);
BENCHMARK_TEMPLATE(AggregateSharedDictionaryBlock, UInt128, false)->Apply(dictionarySizeSweep);
BENCHMARK_TEMPLATE(AggregateSharedDictionaryBlock, UInt256, true)->Apply(dictionarySizeSweep);
BENCHMARK_TEMPLATE(AggregateSharedDictionaryBlock, UInt256, false)->Apply(dictionarySizeSweep);

BENCHMARK_TEMPLATE(AggregateSharedDictionaryStates, UInt128)->Apply(aggregationStateSweep);
BENCHMARK_TEMPLATE(AggregateSharedDictionaryStates, UInt256)->Apply(aggregationStateSweep);

BENCHMARK_TEMPLATE(ReadSharedDictionaryIndexes, UInt8, false)->Args({22, 20});
BENCHMARK_TEMPLATE(ReadSharedDictionaryIndexes, UInt8, true)->Args({22, 20});
BENCHMARK_TEMPLATE(ReadSharedDictionaryIndexes, UInt16, false)->Args({22, 20});
BENCHMARK_TEMPLATE(ReadSharedDictionaryIndexes, UInt16, true)->Args({22, 20});
BENCHMARK_TEMPLATE(ReadSharedDictionaryIndexes, UInt32, false)->Args({22, 20});
BENCHMARK_TEMPLATE(ReadSharedDictionaryIndexes, UInt32, true)->Args({22, 20});
BENCHMARK_TEMPLATE(ReadSharedDictionaryIndexes, UInt64, false)->Args({22, 20});
BENCHMARK_TEMPLATE(ReadSharedDictionaryIndexes, UInt64, true)->Args({22, 20});

}
