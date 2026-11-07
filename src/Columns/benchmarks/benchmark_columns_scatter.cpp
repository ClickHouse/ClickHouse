/// Microbenchmark of `IColumn::scatter`.
///
/// One source serves every tree that is compared. The CMake entry of the tree sets `SCATTER_BENCH_LEVEL`, which picks
/// the arms it can compile:
///   0  `scatter` with `IColumn::Selector`; the block arm is the per-column loop of the callers.
///   1  also `scatter` with `IColumn::SmallSelector`; the block arm calls `scatterBlock`, with either id type.
///   2  also the batched entry `ColumnsScatter::scatter` over several sources.
///
/// A name is "Scatter/arm:<column|block|batched>/id:<u64|u16>/type:<type>/pieces:<n>/sel:<uniform|skewed>/rows:<n>",
/// then optional keys, then the suffixes of Google Benchmark ("min_time:0.500", "real_time", and "threads:<n>" on cells
/// with several threads). The keys are "avg:<n>", the average bytes of a `String` and elements of an `Array` or a
/// `Map`; "keys:<n>", the size of the `LowCardinality` dictionary; and "sources:<n>", in the batched arm. A cell has the
/// same name in every tree.
///
/// Inputs and selectors are built before the timed loop, with fixed seeds. Each thread scatters the same input into
/// its own pieces. Counters:
///   peak_bytes    the most memory any scatter call held at its peak, beyond what its thread held before the call;
///   pieces_bytes  the sum of `allocatedBytes` over the pieces of one call.
///
/// Run with:
///   ./benchmark_columns_scatter --benchmark_list_tests
///   ./benchmark_columns_scatter --benchmark_filter='^Scatter/arm:column/id:u64/type:UInt64/' --benchmark_format=json

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/MemoryTracker.h>
#include <Common/assert_cast.h>
#include <Common/ThreadStatus.h>
#include <base/defines.h>

#ifndef SCATTER_BENCH_LEVEL
#error "SCATTER_BENCH_LEVEL is set by the CMake entry of benchmark_columns_scatter"
#endif

#if SCATTER_BENCH_LEVEL >= 2
#include <Columns/ColumnsScatter.h>
#endif

#include <benchmark/benchmark.h>
#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

using namespace DB;

namespace
{

using Rng = std::mt19937_64;

constexpr UInt64 data_seed = 42;
constexpr UInt64 selector_seed = 4242;

/// Average bytes of a `String`, and elements of an `Array` or a `Map`, where a type says nothing else.
constexpr size_t default_avg = 8;
constexpr size_t low_cardinality_keys = 1000;

constexpr std::array<size_t, 3> matrix_rows{1000, 65536, 1048576};
constexpr std::array<size_t, 4> matrix_pieces{2, 16, 256, 1024};
constexpr std::array<int, 4> sweep_threads{16, 32, 64, 96};
constexpr std::array<size_t, 3> sweep_pieces{2, 256, 1024};
constexpr size_t sweep_rows = 65536;
constexpr std::array<size_t, 2> block_rows{1000, 65536};

size_t randomLength(Rng & rng, size_t avg)
{
    return rng() % (2 * avg + 1);
}

template <typename T>
ColumnPtr makeNumbers(size_t rows, Rng & rng)
{
    auto column = ColumnVector<T>::create(rows);
    for (auto & value : column->getData())
    {
        if constexpr (std::is_same_v<T, UInt256>)
        {
            for (auto & item : value.items)
                item = rng();
        }
        else if constexpr (std::is_floating_point_v<T>)
            value = static_cast<T>(rng() >> 11) * 0x1.0p-53;
        else
            value = static_cast<T>(rng());
    }
    return column;
}

ColumnPtr makeDecimal128(size_t rows, Rng & rng)
{
    auto column = ColumnDecimal<Decimal128>::create(rows, 6);
    for (auto & value : column->getData())
        value = Decimal128(static_cast<Int128>(static_cast<Int64>(rng())));
    return column;
}

ColumnPtr makeFixedStrings(size_t rows, size_t n, Rng & rng)
{
    auto column = ColumnFixedString::create(n);
    auto & chars = column->getChars();
    chars.resize(rows * n);
    for (auto & c : chars)
        c = static_cast<UInt8>(rng());
    return column;
}

ColumnPtr makeStrings(size_t rows, size_t avg, Rng & rng)
{
    auto column = ColumnString::create();
    std::string value;
    for (size_t i = 0; i < rows; ++i)
    {
        value.resize(randomLength(rng, avg));
        for (auto & c : value)
            c = static_cast<char>('a' + rng() % 26);
        column->insertData(value.data(), value.size());
    }
    return column;
}

/// About one row in ten is NULL, with the default value under it.
ColumnPtr withNulls(const ColumnPtr & values, Rng & rng)
{
    const size_t rows = values->size();
    auto null_map = ColumnUInt8::create(rows);
    auto nested = values->cloneEmpty();
    nested->reserve(rows);
    for (size_t i = 0; i < rows; ++i)
    {
        const bool is_null = rng() % 10 == 0;
        null_map->getData()[i] = is_null;
        if (is_null)
            nested->insertDefault();
        else
            nested->insertFrom(*values, i);
    }
    return ColumnNullable::create(std::move(nested), std::move(null_map));
}

ColumnPtr makeOffsets(size_t rows, size_t avg, Rng & rng)
{
    auto offsets = ColumnArray::ColumnOffsets::create(rows);
    size_t total = 0;
    for (auto & offset : offsets->getData())
    {
        total += randomLength(rng, avg);
        offset = total;
    }
    return offsets;
}

template <typename MakeElements>
ColumnPtr makeArray(size_t rows, size_t avg, Rng & rng, MakeElements make_elements)
{
    ColumnPtr offsets = makeOffsets(rows, avg, rng);
    const size_t elements = assert_cast<const ColumnArray::ColumnOffsets &>(*offsets).getData().back();
    return ColumnArray::create(make_elements(elements), offsets);
}

template <typename MakeKeys, typename MakeValues>
ColumnPtr makeMap(size_t rows, Rng & rng, MakeKeys make_keys, MakeValues make_values)
{
    ColumnPtr offsets = makeOffsets(rows, default_avg, rng);
    const size_t entries = assert_cast<const ColumnArray::ColumnOffsets &>(*offsets).getData().back();
    ColumnPtr keys = make_keys(entries);
    ColumnPtr values = make_values(entries);
    return ColumnMap::create(keys, values, offsets);
}

ColumnPtr makeTupleUInt64String(size_t rows, Rng & rng)
{
    ColumnPtr numbers = makeNumbers<UInt64>(rows, rng);
    ColumnPtr strings = makeStrings(rows, default_avg, rng);
    return ColumnTuple::create(Columns{numbers, strings});
}

ColumnPtr makeLowCardinalityString(size_t rows, Rng & rng)
{
    const auto keys = makeStrings(low_cardinality_keys, default_avg, rng);
    auto column = DataTypeLowCardinality(std::make_shared<DataTypeString>()).createColumn();
    for (size_t i = 0; i < rows; ++i)
    {
        const auto key = keys->getDataAt(rng() % low_cardinality_keys);
        column->insertData(key.data(), key.size());
    }
    return column;
}

/// 95% of the rows are the default, above the 0.9375 past which a `MergeTree` part stores a column as sparse.
ColumnPtr makeSparseUInt64(size_t rows, Rng & rng)
{
    auto values = ColumnUInt64::create();
    auto offsets = ColumnUInt64::create();
    values->insertDefault();
    for (size_t i = 0; i < rows; ++i)
    {
        if (rng() % 20 == 0)
        {
            values->insertValue(rng() | 1);
            offsets->insertValue(i);
        }
    }
    return ColumnSparse::create(std::move(values), std::move(offsets), rows);
}

/// Half of the rows in each variant, no NULLs.
ColumnPtr makeVariantUInt64String(size_t rows, Rng & rng)
{
    auto discriminators = ColumnVariant::ColumnDiscriminators::create(rows);
    auto numbers = ColumnUInt64::create();
    auto strings = ColumnString::create();
    std::string value;
    for (auto & discriminator : discriminators->getData())
    {
        discriminator = static_cast<ColumnVariant::Discriminator>(rng() & 1);
        if (discriminator == 0)
        {
            numbers->insertValue(rng());
            continue;
        }
        value.resize(randomLength(rng, default_avg));
        for (auto & c : value)
            c = static_cast<char>('a' + rng() % 26);
        strings->insertData(value.data(), value.size());
    }
    MutableColumns variants;
    variants.push_back(std::move(numbers));
    variants.push_back(std::move(strings));
    return ColumnVariant::create(std::move(discriminators), std::move(variants));
}

struct TypeSpec
{
    /// The ClickHouse spelling, without spaces.
    std::string name;
    /// Optional keys of the name, each with its leading "/".
    std::string keys;
    std::function<ColumnPtr(size_t rows, Rng & rng)> make;
};

const std::vector<TypeSpec> & matrixTypes()
{
    static const std::vector<TypeSpec> types{
        {"UInt8", "", makeNumbers<UInt8>},
        {"UInt32", "", makeNumbers<UInt32>},
        {"UInt64", "", makeNumbers<UInt64>},
        {"UInt256", "", makeNumbers<UInt256>},
        {"Float64", "", makeNumbers<Float64>},
        {"Decimal128", "", makeDecimal128},
        {"FixedString(16)", "", [](size_t rows, Rng & rng) { return makeFixedStrings(rows, 16, rng); }},
        {"FixedString(7)", "", [](size_t rows, Rng & rng) { return makeFixedStrings(rows, 7, rng); }},
        {"String", "/avg:8", [](size_t rows, Rng & rng) { return makeStrings(rows, 8, rng); }},
        {"String", "/avg:64", [](size_t rows, Rng & rng) { return makeStrings(rows, 64, rng); }},
        {"Nullable(UInt64)", "", [](size_t rows, Rng & rng) { return withNulls(makeNumbers<UInt64>(rows, rng), rng); }},
        {"Nullable(String)", "/avg:8", [](size_t rows, Rng & rng) { return withNulls(makeStrings(rows, default_avg, rng), rng); }},
        {"Nullable(Tuple(UInt64,String))", "/avg:8",
         [](size_t rows, Rng & rng) { return withNulls(makeTupleUInt64String(rows, rng), rng); }},
        {"Array(UInt64)", "/avg:8",
         [](size_t rows, Rng & rng)
         { return makeArray(rows, default_avg, rng, [&](size_t elements) { return makeNumbers<UInt64>(elements, rng); }); }},
        {"Array(String)", "/avg:8",
         [](size_t rows, Rng & rng)
         { return makeArray(rows, default_avg, rng, [&](size_t elements) { return makeStrings(elements, default_avg, rng); }); }},
        {"Map(String,UInt64)", "/avg:8",
         [](size_t rows, Rng & rng)
         {
             return makeMap(
                 rows,
                 rng,
                 [&](size_t entries) { return makeStrings(entries, default_avg, rng); },
                 [&](size_t entries) { return makeNumbers<UInt64>(entries, rng); });
         }},
        {"Tuple(UInt64,String)", "/avg:8", makeTupleUInt64String},
        {"LowCardinality(String)", "/keys:1000", makeLowCardinalityString},
        /// `Const` and `Sparse` skip the kernels, as controls. `Variant` scatters its discriminators and variants through them.
        {"Const(UInt64)", "", [](size_t rows, Rng &) { return ColumnConst::create(ColumnUInt64::create(1, UInt64{42}), rows); }},
        {"Sparse(UInt64)", "", makeSparseUInt64},
        {"Variant(UInt64,String)", "/avg:8", makeVariantUInt64String},
    };
    return types;
}

const std::vector<TypeSpec> & extraTypes()
{
    static const std::vector<TypeSpec> types{
        {"Array(UInt8)", "/avg:100",
         [](size_t rows, Rng & rng)
         { return makeArray(rows, 100, rng, [&](size_t elements) { return makeNumbers<UInt8>(elements, rng); }); }},
        {"Array(UInt16)", "/avg:8",
         [](size_t rows, Rng & rng)
         { return makeArray(rows, default_avg, rng, [&](size_t elements) { return makeNumbers<UInt16>(elements, rng); }); }},
        {"Map(UInt8,UInt8)", "/avg:8",
         [](size_t rows, Rng & rng)
         {
             return makeMap(
                 rows,
                 rng,
                 [&](size_t entries) { return makeNumbers<UInt8>(entries, rng); },
                 [&](size_t entries) { return makeNumbers<UInt8>(entries, rng); });
         }},
    };
    return types;
}

const TypeSpec & findType(const std::string & name_and_keys)
{
    for (const auto * types : {&matrixTypes(), &extraTypes()})
        for (const auto & type : *types)
            if (type.name + type.keys == name_and_keys)
                return type;
    throw std::logic_error("No type " + name_and_keys);
}

const TypeSpec block_type{"Block(UInt64,UInt64,UInt64,UInt64,String,String,Nullable(UInt32),LowCardinality(String))", "", {}};

Block makeBlock(size_t rows)
{
    ColumnsWithTypeAndName columns;
    auto add = [&](ColumnPtr column, DataTypePtr type)
    {
        const std::string name = fmt::format("c{}", columns.size());
        columns.emplace_back(std::move(column), std::move(type), name);
    };

    /// Each column has its own seed, so the columns differ.
    std::vector<Rng> rngs;
    for (UInt64 i = 0; i < 8; ++i)
        rngs.emplace_back(data_seed + i); // NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp): fixed seed
    for (size_t i = 0; i < 4; ++i)
        add(makeNumbers<UInt64>(rows, rngs[i]), std::make_shared<DataTypeUInt64>());
    for (size_t i = 4; i < 6; ++i)
        add(makeStrings(rows, default_avg, rngs[i]), std::make_shared<DataTypeString>());
    add(withNulls(makeNumbers<UInt32>(rows, rngs[6]), rngs[6]), std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt32>()));
    add(makeLowCardinalityString(rows, rngs[7]), std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()));
    return Block(std::move(columns));
}

enum class Distribution : uint8_t
{
    Uniform,
    /// 90% of the rows in piece 0, the others spread over the other pieces.
    Skewed,
};

const char * distributionName(Distribution distribution)
{
    return distribution == Distribution::Uniform ? "uniform" : "skewed";
}

/// Gives the same piece ids for either id type.
template <typename Id>
PaddedPODArray<Id> makeSelector(size_t rows, size_t pieces, Distribution distribution, UInt64 seed)
{
    Rng rng(seed); // NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp): fixed seed
    PaddedPODArray<Id> selector(rows);
    for (auto & id : selector)
    {
        UInt64 piece = 0;
        if (distribution == Distribution::Uniform)
            piece = rng() % pieces;
        else if (rng() % 10 == 0)
            piece = 1 + rng() % (pieces - 1);
        id = static_cast<Id>(piece);
    }
    return selector;
}

/// Keeps the input of the last key asked for. The cells run in registration order, so consecutive cells mostly share it.
template <typename Handle>
class LastBuilt
{
public:
    template <typename Build>
    Handle get(const std::string & key, Build build)
    {
        std::lock_guard lock(mutex);
        if (key != built_key)
        {
            built = Handle{}; /// Only one input of a kind is alive at a time.
            built = build();
            built_key = key;
        }
        return built;
    }

private:
    std::mutex mutex;
    std::string built_key;
    Handle built{};
};

LastBuilt<ColumnPtr> column_inputs;
LastBuilt<std::shared_ptr<const Block>> block_inputs;

template <typename Id>
LastBuilt<std::shared_ptr<const PaddedPODArray<Id>>> selector_inputs;

ColumnPtr columnInput(const TypeSpec & type, size_t rows, UInt64 seed)
{
    return column_inputs.get(
        fmt::format("{}{}/{}/{}", type.name, type.keys, rows, seed),
        [&]
        {
            Rng rng(seed); // NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp): fixed seed
            return type.make(rows, rng);
        });
}

template <typename Id>
std::shared_ptr<const PaddedPODArray<Id>> selectorInput(size_t rows, size_t pieces, Distribution distribution)
{
    return selector_inputs<Id>.get(
        fmt::format("{}/{}/{}", rows, pieces, distributionName(distribution)),
        [&] { return std::make_shared<const PaddedPODArray<Id>>(makeSelector<Id>(rows, pieces, distribution, selector_seed)); });
}

/// The memory tracker of a benchmark thread. It sees each allocation as it happens (`untracked_memory_limit` = 0).
class ThreadMemory
{
public:
    ThreadMemory()
    {
        thread_status.untracked_memory_limit = 0;
#ifndef DEBUG_OR_SANITIZER_BUILD
        /// Without the global tracker as parent, the threads of a cell do not contend on it for every allocation.
        /// In debug and sanitizer builds `setParent` throws unless the chain reaches it.
        thread_status.memory_tracker.setParent(nullptr);
#endif
    }

    /// Calls `scatter` and returns its pieces. Keeps the most memory a call held at its peak beyond what the thread
    /// held before the call, the returned pieces included.
    template <typename Scatter>
    auto measure(Scatter & scatter)
    {
        auto & tracker = thread_status.memory_tracker;
        const Int64 before = tracker.get();
        tracker.resetPeak();
        auto pieces = scatter();
        max_peak = std::max(max_peak, tracker.getPeak() - before);
        return pieces;
    }

    Int64 maxPeak() const { return max_peak; }

private:
    ThreadStatus thread_status;
    Int64 max_peak = 0;
};

/// The peak of one run of a cell over all its threads.
struct RunMemory
{
    std::atomic<Int64> peak{0};
    std::atomic<int> published{0};
};

size_t piecesBytes(const VectorWithMemoryTracking<MutableColumnPtr> & pieces)
{
    size_t bytes = 0;
    for (const auto & piece : pieces)
        bytes += piece->allocatedBytes();
    return bytes;
}

size_t piecesBytes(const Blocks & blocks)
{
    size_t bytes = 0;
    for (const auto & block : blocks)
        bytes += block.allocatedBytes();
    return bytes;
}

/// Times `scatter`, then reports the memory counters from thread 0. One untimed call before the loop gives
/// `pieces_bytes`.
template <typename Scatter>
void timeScatter(benchmark::State & state, RunMemory & run_memory, size_t rows_per_call, Scatter scatter)
{
    ThreadMemory memory;
    const size_t pieces_bytes = piecesBytes(memory.measure(scatter));

    /// No thread publishes before thread 0 resets: all of them enter the loop together.
    if (state.thread_index() == 0)
    {
        run_memory.peak = 0;
        run_memory.published = 0;
    }

    for (auto _ [[maybe_unused]] : state)
    {
        auto pieces = memory.measure(scatter);
        benchmark::DoNotOptimize(pieces);
    }

    Int64 peak = run_memory.peak.load();
    while (peak < memory.maxPeak() && !run_memory.peak.compare_exchange_weak(peak, memory.maxPeak()))
    {
    }
    ++run_memory.published;

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(rows_per_call));
    if (state.thread_index() != 0)
        return;

    while (run_memory.published.load() < state.threads())
        std::this_thread::yield();
    state.counters["peak_bytes"] = static_cast<double>(run_memory.peak.load());
    state.counters["pieces_bytes"] = static_cast<double>(pieces_bytes);
}

struct Cell
{
    const char * arm;
    const TypeSpec * type;
    size_t pieces;
    Distribution distribution;
    size_t rows;
    int threads;
    /// The optional keys of the name: the type's keys, and "/sources:<n>" in the batched arm.
    std::string keys;
};

template <typename Id>
void runColumn(benchmark::State & state, const Cell & cell, RunMemory & run_memory)
{
    const ColumnPtr column = columnInput(*cell.type, cell.rows, data_seed);
    const auto selector = selectorInput<Id>(cell.rows, cell.pieces, cell.distribution);
    timeScatter(state, run_memory, cell.rows, [&] { return column->scatter(cell.pieces, *selector); });
}

#if SCATTER_BENCH_LEVEL == 0
/// The loop of the callers, e.g. `JoinCommon::scatterBlockByHash` and `DistributedSink::splitBlock`.
Blocks scatterBlockByColumns(const Block & block, size_t num_pieces, const IColumn::Selector & selector)
{
    Blocks blocks;
    blocks.reserve(num_pieces);
    for (size_t i = 0; i < num_pieces; ++i)
        blocks.emplace_back(block.cloneEmpty());

    for (size_t position = 0; position < block.columns(); ++position)
    {
        auto pieces = block.getByPosition(position).column->scatter(num_pieces, selector);
        for (size_t i = 0; i < num_pieces; ++i)
            blocks[i].getByPosition(position).column = std::move(pieces[i]);
    }
    return blocks;
}
#endif

template <typename Id>
void runBlock(benchmark::State & state, const Cell & cell, RunMemory & run_memory)
{
    const auto block = block_inputs.get(
        fmt::format("{}", cell.rows), [&] { return std::make_shared<const Block>(makeBlock(cell.rows)); });
    const auto selector = selectorInput<Id>(cell.rows, cell.pieces, cell.distribution);
#if SCATTER_BENCH_LEVEL == 0
    timeScatter(state, run_memory, cell.rows, [&] { return scatterBlockByColumns(*block, cell.pieces, *selector); });
#else
    timeScatter(state, run_memory, cell.rows, [&] { return scatterBlock(*block, cell.pieces, *selector); });
#endif
}

#if SCATTER_BENCH_LEVEL >= 2
/// The batched entry `ColumnsScatter::scatter` over several sources of one column position.
constexpr size_t batched_sources = 8;

template <typename Id>
struct BatchedInput
{
    Columns sources;
    std::vector<PaddedPODArray<Id>> ids;
};

template <typename Id>
LastBuilt<std::shared_ptr<const BatchedInput<Id>>> batched_inputs;

template <typename Id>
void runBatched(benchmark::State & state, const Cell & cell, RunMemory & run_memory)
{
    const auto input = batched_inputs<Id>.get(
        fmt::format("{}{}/{}/{}", cell.type->name, cell.type->keys, cell.rows, cell.pieces),
        [&]
        {
            auto built = std::make_shared<BatchedInput<Id>>();
            for (size_t source = 0; source < batched_sources; ++source)
            {
                Rng rng(data_seed + source); // NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp): fixed seed
                built->sources.push_back(cell.type->make(cell.rows, rng));
                built->ids.push_back(makeSelector<Id>(cell.rows, cell.pieces, cell.distribution, selector_seed + source));
            }
            return std::shared_ptr<const BatchedInput<Id>>(std::move(built));
        });

    std::vector<const IColumn *> sources;
    std::vector<std::span<const Id>> ids;
    for (size_t source = 0; source < batched_sources; ++source)
    {
        sources.push_back(input->sources[source].get());
        ids.emplace_back(input->ids[source].data(), input->ids[source].size());
    }

    /// A caller counts the rows per piece before the call, so the count is timed too.
    timeScatter(
        state,
        run_memory,
        batched_sources * cell.rows,
        [&]
        {
            const auto counts = ColumnsScatter::countRowsPerPiece<Id>(ids, cell.pieces);
            return ColumnsScatter::scatter<Id>(sources, ids, counts);
        });
}
#endif

using RunCell = void (*)(benchmark::State &, const Cell &, RunMemory &);

template <typename Id>
void registerCell(const Cell & cell, RunCell run)
{
    const std::string name = fmt::format(
        "Scatter/arm:{}/id:{}/type:{}/pieces:{}/sel:{}/rows:{}{}",
        cell.arm,
        sizeof(Id) == 2 ? "u16" : "u64",
        cell.type->name,
        cell.pieces,
        distributionName(cell.distribution),
        cell.rows,
        cell.keys);
    auto run_memory = std::make_shared<RunMemory>();
    auto * registered = benchmark::RegisterBenchmark(
        name, [cell, run, run_memory](benchmark::State & state) { run(state, cell, *run_memory); });
    registered->MinTime(0.5)->UseRealTime();
    if (cell.threads > 1)
        registered->Threads(cell.threads);
}

void registerColumn(const TypeSpec & type, size_t pieces, Distribution distribution, size_t rows, int threads)
{
    const Cell cell{"column", &type, pieces, distribution, rows, threads, type.keys};
    registerCell<UInt64>(cell, runColumn<UInt64>);
#if SCATTER_BENCH_LEVEL >= 1
    registerCell<UInt16>(cell, runColumn<UInt16>);
#endif
}

void registerBlock(size_t pieces, size_t rows)
{
    const Cell cell{"block", &block_type, pieces, Distribution::Uniform, rows, 1, ""};
    registerCell<UInt64>(cell, runBlock<UInt64>);
#if SCATTER_BENCH_LEVEL >= 1
    registerCell<UInt16>(cell, runBlock<UInt16>);
#endif
}

#if SCATTER_BENCH_LEVEL >= 2
void registerBatched(const TypeSpec & type, size_t pieces)
{
    const Cell cell{"batched", &type, pieces, Distribution::Uniform, 65536, 1, fmt::format("{}/sources:{}", type.keys, batched_sources)};
    registerCell<UInt64>(cell, runBatched<UInt64>);
    registerCell<UInt16>(cell, runBatched<UInt16>);
}
#endif

void registerAll()
{
    for (const auto & type : matrixTypes())
        for (size_t rows : matrix_rows)
            for (size_t pieces : matrix_pieces)
                for (auto distribution : {Distribution::Uniform, Distribution::Skewed})
                    registerColumn(type, pieces, distribution, rows, 1);

    /// The thread sweep. Its cells with one thread are in the matrix above.
    for (const char * type : {"UInt64", "String/avg:8", "Nullable(String)/avg:8", "Array(UInt64)/avg:8"})
        for (size_t pieces : sweep_pieces)
            for (int threads : sweep_threads)
                registerColumn(findType(type), pieces, Distribution::Uniform, sweep_rows, threads);

    for (const auto & type : extraTypes())
        for (size_t rows : {65536uz, 1048576uz})
            for (size_t pieces : {16uz, 1024uz})
                registerColumn(type, pieces, Distribution::Uniform, rows, 1);
    /// `arrayFold` scatters into one piece per element of its longest array, which can be about 10^4 pieces.
    for (const char * type : {"UInt64", "String/avg:8"})
        registerColumn(findType(type), 8192, Distribution::Uniform, 1048576, 1);

    for (size_t rows : block_rows)
        for (size_t pieces : matrix_pieces)
            registerBlock(pieces, rows);

#if SCATTER_BENCH_LEVEL >= 2
    for (const char * type : {"UInt64", "String/avg:8"})
        for (size_t pieces : {16uz, 256uz, 1024uz, 4096uz})
            registerBatched(findType(type), pieces);
#endif
}

}

int main(int argc, char ** argv)
{
    benchmark::MaybeReenterWithoutASLR(argc, argv);
    registerAll();
    benchmark::Initialize(&argc, argv);
    if (benchmark::ReportUnrecognizedArguments(argc, argv))
        return 1;
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}
