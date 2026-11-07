/// Microbenchmark of `HashedJoinKey`, the key of the `hashed` and `two_level_hashed` join maps. For each key type:
///   buildRange/<keys>     the keys of a block in one call, as `PreparedJoinKeys::buildAllKeys` builds them;
///   buildRange256/<keys>  the keys from row 77 on, 256 rows per call into one reused buffer, as
///                         `PreparedJoinKeys::buildNextKeys` builds them for a join loop;
///   hash128/<keys>        `ColumnsHashing::hash128` per row, as `HashedJoinKey::build` builds them.
/// `items_per_second` is the rows per second.
///
/// Run with:
///   ./benchmark_hashed_join_key --benchmark_filter=String

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/HashJoin/KeyGetter.h>

#include <benchmark/benchmark.h>

#include <functional>
#include <random>
#include <string>
#include <string_view>
#include <vector>

using namespace DB;

namespace
{

/// A block has at most `max_block_size` rows.
constexpr size_t num_rows = 65536;
/// A join loop builds the keys of its range 256 rows per call, from any row on.
constexpr size_t join_loop_rows = 256;
constexpr size_t join_loop_first_row = 77;

using Rng = std::mt19937_64;

template <typename T>
MutableColumnPtr numbers(Rng & rng)
{
    auto column = ColumnVector<T>::create(num_rows);
    auto * bytes = reinterpret_cast<UInt8 *>(column->getData().data());
    for (size_t i = 0; i < num_rows * sizeof(T); ++i)
        bytes[i] = static_cast<UInt8>(rng());
    return column;
}

MutableColumnPtr fixedStrings(Rng & rng, size_t n)
{
    auto column = ColumnFixedString::create(n);
    auto & chars = column->getChars();
    chars.resize(num_rows * n);
    for (auto & c : chars)
        c = static_cast<UInt8>(rng());
    return column;
}

MutableColumnPtr strings(Rng & rng, size_t min_length, size_t max_length)
{
    auto column = ColumnString::create();
    std::string value;
    for (size_t row = 0; row < num_rows; ++row)
    {
        value.resize(std::uniform_int_distribution<size_t>(min_length, max_length)(rng));
        for (auto & c : value)
            c = static_cast<char>('a' + rng() % 26);
        column->insertData(value.data(), value.size());
    }
    return column;
}

template <typename... Columns>
MutableColumns columns(Columns &&... elements)
{
    MutableColumns result;
    (result.push_back(std::move(elements)), ...);
    return result;
}

template <typename... Columns>
MutableColumnPtr tuple(Columns &&... elements)
{
    return ColumnTuple::create(columns(std::move(elements)...));
}

MutableColumnPtr nullable(MutableColumnPtr nested, Rng & rng)
{
    auto null_map = ColumnUInt8::create(num_rows);
    for (auto & value : null_map->getData())
        value = rng() % 4 == 0;
    return ColumnNullable::create(std::move(nested), std::move(null_map));
}

struct Keys
{
    const char * name;
    std::function<MutableColumns(Rng &)> columns;
};

const std::vector<Keys> & allKeys()
{
    static const std::vector<Keys> keys{
        {"Tuple(UInt64)", [](Rng & rng) { return columns(tuple(numbers<UInt64>(rng))); }},
        {"Tuple(UInt8,UInt8)", [](Rng & rng) { return columns(tuple(numbers<UInt8>(rng), numbers<UInt8>(rng))); }},
        {"UInt64x5",
         [](Rng & rng)
         { return columns(numbers<UInt64>(rng), numbers<UInt64>(rng), numbers<UInt64>(rng), numbers<UInt64>(rng), numbers<UInt64>(rng)); }},
        {"UUIDx3", [](Rng & rng) { return columns(numbers<UUID>(rng), numbers<UUID>(rng), numbers<UUID>(rng)); }},
        {"UInt256,UInt64", [](Rng & rng) { return columns(numbers<UInt256>(rng), numbers<UInt64>(rng)); }},
        {"Tuple(FixedString(40))", [](Rng & rng) { return columns(tuple(fixedStrings(rng, 40))); }},
        {"String0-15,UInt64", [](Rng & rng) { return columns(strings(rng, 0, 15), numbers<UInt64>(rng)); }},
        {"String16-40,UInt64", [](Rng & rng) { return columns(strings(rng, 16, 40), numbers<UInt64>(rng)); }},
        {"String30-120,UInt64", [](Rng & rng) { return columns(strings(rng, 30, 120), numbers<UInt64>(rng)); }},
        {"String0-30,String0-30", [](Rng & rng) { return columns(strings(rng, 0, 30), strings(rng, 0, 30)); }},
        {"Tuple(Nullable(String0-30))", [](Rng & rng) { return columns(tuple(nullable(strings(rng, 0, 30), rng))); }},
    };
    return keys;
}

ColumnRawPtrs rawPtrs(const MutableColumns & columns)
{
    ColumnRawPtrs ptrs;
    for (const auto & column : columns)
        ptrs.push_back(column.get());
    return ptrs;
}

void buildRange(benchmark::State & state, const Keys & keys)
{
    Rng rng(std::hash<std::string_view>{}(keys.name));
    const auto key_columns = keys.columns(rng);
    HashedJoinKey key(rawPtrs(key_columns));
    PaddedPODArray<UInt128> result(num_rows);
    const PaddedPODArray<UInt8> no_skip(num_rows, 0);
    for (auto _ [[maybe_unused]] : state)
    {
        key.buildRange(0, num_rows, result.data(), no_skip.data());
        benchmark::DoNotOptimize(result.data());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * num_rows);
}

void buildRange256(benchmark::State & state, const Keys & keys)
{
    Rng rng(std::hash<std::string_view>{}(keys.name));
    const auto key_columns = keys.columns(rng);
    HashedJoinKey key(rawPtrs(key_columns));
    PaddedPODArray<UInt128> result(join_loop_rows);
    const PaddedPODArray<UInt8> no_skip(num_rows, 0);
    for (auto _ [[maybe_unused]] : state)
    {
        for (size_t begin = join_loop_first_row; begin < num_rows; begin += join_loop_rows)
        {
            key.buildRange(begin, std::min(num_rows, begin + join_loop_rows), result.data(), no_skip.data());
            benchmark::DoNotOptimize(result.data());
            benchmark::ClobberMemory();
        }
    }
    state.SetItemsProcessed(state.iterations() * (num_rows - join_loop_first_row));
}

void hash128(benchmark::State & state, const Keys & keys)
{
    Rng rng(std::hash<std::string_view>{}(keys.name));
    const auto key_columns = keys.columns(rng);
    HashedJoinKey key(rawPtrs(key_columns));
    PaddedPODArray<UInt128> result(num_rows);
    for (auto _ [[maybe_unused]] : state)
    {
        for (size_t row = 0; row < num_rows; ++row)
            result[row] = key.build(row);
        benchmark::DoNotOptimize(result.data());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * num_rows);
}

[[maybe_unused]] const bool registered = []
{
    for (const auto & keys : allKeys())
    {
        benchmark::RegisterBenchmark(std::string("buildRange/") + keys.name, buildRange, keys);
        benchmark::RegisterBenchmark(std::string("buildRange256/") + keys.name, buildRange256, keys);
        benchmark::RegisterBenchmark(std::string("hash128/") + keys.name, hash128, keys);
    }
    return true;
}();

}
