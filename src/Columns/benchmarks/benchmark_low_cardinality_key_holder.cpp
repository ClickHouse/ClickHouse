#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>
#include <Common/ColumnsHashing.h>
#include <Common/HashTable/HashMap.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <base/types.h>

#include <benchmark/benchmark.h>

#include <cstdint>
#include <memory>
#include <type_traits>
#include <utility>

using namespace DB;

namespace
{

constexpr size_t rows_per_operation = 1ULL << 20;
constexpr UInt32 dictionary_size = 1ULL << 8;

/// Keep the logical values and the UInt32 LC index width identical for every
/// key type so that each LC/plain pair primarily measures key construction.
UInt32 getDictionaryIndex(size_t row)
{
    return static_cast<UInt32>((row * 167 + 13) % dictionary_size);
}

template <typename Key>
using BenchmarkData = HashMap<Key, UInt64>;

template <typename Key>
using OneNumberMethod = ColumnsHashing::HashMethodOneNumber<
    typename BenchmarkData<Key>::value_type,
    UInt64,
    Key,
    false>;

template <typename Key>
using KeysFixedMethod = ColumnsHashing::HashMethodKeysFixed<
    typename BenchmarkData<Key>::value_type,
    Key,
    UInt64,
    false,
    false,
    false>;

template <typename Key>
using LowCardinalityOneNumberMethod = ColumnsHashing::HashMethodSingleLowCardinalityColumn<
    OneNumberMethod<Key>,
    UInt64,
    false>;

template <typename Key>
using LowCardinalityKeysFixedMethod = ColumnsHashing::HashMethodKeysFixed<
    typename BenchmarkData<Key>::value_type,
    Key,
    UInt64,
    false,
    true,
    false>;

template <typename Key>
using PlainKeyMethod = std::conditional_t<
    (sizeof(Key) <= sizeof(UInt64)),
    OneNumberMethod<Key>,
    KeysFixedMethod<Key>>;

template <typename Key>
using LowCardinalityKeyMethod = std::conditional_t<
    (sizeof(Key) <= sizeof(UInt256)),
    LowCardinalityOneNumberMethod<Key>,
    LowCardinalityKeysFixedMethod<Key>>;

template <typename Key, bool low_cardinality>
using KeyMethod = std::conditional_t<low_cardinality, LowCardinalityKeyMethod<Key>, PlainKeyMethod<Key>>;

template <typename Key, bool low_cardinality>
ColumnPtr makeKeyColumn()
{
    if constexpr (low_cardinality)
    {
        auto dictionary_keys = ColumnVector<Key>::create(dictionary_size);
        auto & dictionary_data = dictionary_keys->getData();
        for (size_t i = 0; i < dictionary_size; ++i)
            dictionary_data[i] = static_cast<Key>(i);

        auto nested_type = std::make_shared<DataTypeNumber<Key>>();
        MutableColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(*nested_type, std::move(dictionary_keys));

        auto indexes = ColumnUInt32::create(rows_per_operation);
        auto & index_data = indexes->getData();
        for (size_t row = 0; row < rows_per_operation; ++row)
            index_data[row] = getDictionaryIndex(row);

        return ColumnLowCardinality::create(std::move(dictionary), std::move(indexes), /*is_shared=*/false);
    }
    else
    {
        auto column = ColumnVector<Key>::create(rows_per_operation);
        auto & data = column->getData();
        for (size_t row = 0; row < rows_per_operation; ++row)
            data[row] = static_cast<Key>(getDictionaryIndex(row));
        return column;
    }
}

template <typename Key, bool low_cardinality>
void KeyHolder(benchmark::State & state)
{
    ColumnPtr column = makeKeyColumn<Key, low_cardinality>();
    ColumnRawPtrs key_columns{column.get()};
    Sizes key_sizes{sizeof(Key)};

    using Method = KeyMethod<Key, low_cardinality>;
    ColumnsHashing::HashMethodContextSettings context_settings;
    context_settings.max_threads = 1;
    auto context = Method::createContext(context_settings);
    Method method(key_columns, key_sizes, context);
    Arena pool;

    for (auto _ : state)
    {
        for (size_t row = 0; row < rows_per_operation; ++row)
            benchmark::DoNotOptimize(method.getKeyHolder(row, pool));
    }

    const auto iterations = static_cast<int64_t>(state.iterations());
    const auto rows = static_cast<int64_t>(rows_per_operation);
    state.SetItemsProcessed(iterations * rows);
    state.SetBytesProcessed(iterations * rows * static_cast<int64_t>(sizeof(Key)));
}

BENCHMARK_TEMPLATE(KeyHolder, UInt8, false);
BENCHMARK_TEMPLATE(KeyHolder, UInt8, true);
BENCHMARK_TEMPLATE(KeyHolder, UInt16, false);
BENCHMARK_TEMPLATE(KeyHolder, UInt16, true);
BENCHMARK_TEMPLATE(KeyHolder, UInt32, false);
BENCHMARK_TEMPLATE(KeyHolder, UInt32, true);
BENCHMARK_TEMPLATE(KeyHolder, UInt64, false);
BENCHMARK_TEMPLATE(KeyHolder, UInt64, true);
BENCHMARK_TEMPLATE(KeyHolder, UInt128, false);
BENCHMARK_TEMPLATE(KeyHolder, UInt128, true);
BENCHMARK_TEMPLATE(KeyHolder, UInt256, false);
BENCHMARK_TEMPLATE(KeyHolder, UInt256, true);

}
