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

#include <cstdint>
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

template <typename Key>
ColumnPtr makeSharedKeyColumn(size_t dictionary_size, size_t rows, size_t active_dictionary_size)
{
    auto dictionary_keys = ColumnVector<Key>::create(dictionary_size);
    auto & dictionary_data = dictionary_keys->getData();
    for (size_t i = 0; i < dictionary_size; ++i)
        dictionary_data[i] = static_cast<Key>(i);

    auto nested_type = std::make_shared<DataTypeNumber<Key>>();
    MutableColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(*nested_type, std::move(dictionary_keys));

    auto indexes = ColumnUInt32::create(rows);
    auto & index_data = indexes->getData();
    for (size_t row = 0; row < rows; ++row)
    {
        /// Spread the active entries over the entire dictionary. For the
        /// power-of-two sizes used below, the odd multiplier makes this a
        /// permutation rather than collapsing the active set.
        const UInt64 active_index = row % active_dictionary_size;
        index_data[row] = static_cast<UInt32>((active_index * 0x9E3779B1ULL) % dictionary_size);
    }

    return ColumnLowCardinality::create(std::move(dictionary), std::move(indexes), /*is_shared=*/true);
}

template <typename Key, bool use_single_low_cardinality_method>
void AggregateSharedDictionaryBlock(benchmark::State & state)
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
    aggregate_states.assign(active_dictionary_size, UInt64{0});
    bool used_dictionary_cache = false;

    for (auto _ : state)
    {
        BenchmarkData<Key> data;
        Arena pool;
        Method method(key_columns, key_sizes, context);
        if constexpr (use_single_low_cardinality_method)
            used_dictionary_cache = method.isUsingDictionaryCache();

        size_t next_aggregate_state = 0;

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
        benchmark::ClobberMemory();
    }

    const auto iterations = static_cast<int64_t>(state.iterations());
    const auto processed_rows = static_cast<int64_t>(rows);
    state.SetItemsProcessed(iterations * processed_rows);
    state.SetBytesProcessed(iterations * processed_rows * static_cast<int64_t>(sizeof(UInt32)));
    state.counters["active_entries"] = static_cast<double>(active_dictionary_size);
    state.counters["dictionary_cache"] = used_dictionary_cache ? 1.0 : 0.0;
    state.counters["dictionary_entries"] = static_cast<double>(dictionary_size);
    if (used_dictionary_cache)
    {
        state.counters["single_lc_cache_bytes"]
            = static_cast<double>(dictionary_size * (sizeof(UInt64 *) + sizeof(UInt8)));
    }
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

BENCHMARK_TEMPLATE(AggregateSharedDictionaryBlock, UInt128, true)->Apply(dictionarySizeSweep);
BENCHMARK_TEMPLATE(AggregateSharedDictionaryBlock, UInt128, false)->Apply(dictionarySizeSweep);
BENCHMARK_TEMPLATE(AggregateSharedDictionaryBlock, UInt256, true)->Apply(dictionarySizeSweep);
BENCHMARK_TEMPLATE(AggregateSharedDictionaryBlock, UInt256, false)->Apply(dictionarySizeSweep);

}
