#include <benchmark/benchmark.h>

#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnObject.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Squashing.h>
#include <Processors/Chunk.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/benchmarks/JemallocAllocationCounter.h>
#include <Common/typeid_cast.h>

#include <memory>
#include <optional>
#include <string>

using namespace DB;

namespace
{

constexpr size_t dynamic_path_count = 8;
/// The number of all dynamic paths in sparse `JSON`, where every source has only `dynamic_path_count` of them.
constexpr size_t sparse_path_count = 256;
constexpr size_t max_dynamic_subcolumns = 6;

using SourceColumns = VectorWithMemoryTracking<ColumnPtr>;

Field makeDynamicValue(size_t source, size_t row, size_t path)
{
    switch ((source + row + path) % 3)
    {
        case 0: return Field(UInt64(source * 1'000 + row * 10 + path));
        case 1: return Field("value_" + std::to_string((source + row + path) % 17));
        default: return Array{Field(UInt64(row)), Field(UInt64(path))};
    }
}

Map makeNestedMapValue(size_t source, size_t row)
{
    Map value;
    value.reserve(3);
    for (size_t element = 0; element != 3; ++element)
    {
        Array nested;
        nested.reserve(3);
        nested.emplace_back("value_" + std::to_string((source + row + element) % 17));
        nested.emplace_back(Null());
        nested.emplace_back("tail_" + std::to_string(element));
        value.emplace_back(Tuple{Field("key_" + std::to_string(element)), Field(std::move(nested))});
    }
    return value;
}

ColumnPtr makeNestedMapColumn(const DataTypePtr & type, size_t source, size_t rows)
{
    auto column = type->createColumn();
    for (size_t row = 0; row != rows; ++row)
        column->insert(makeNestedMapValue(source, row));
    return column;
}

ColumnPtr makeObjectColumn(const DataTypePtr & type, size_t source, size_t rows, bool sparse = false)
{
    auto column = type->createColumn();
    for (size_t row = 0; row != rows; ++row)
    {
        Object value;
        value.emplace("typed", makeNestedMapValue(source, row));
        /// Keep the path set fixed so the source-count dimension does not also change schema complexity.
        /// The merge limit is lower than this path count, which still exercises path ranking and shared-data statistics.
        /// Sparse `JSON` (like time-clustered events): every source has its own window of `dynamic_path_count` of
        /// `sparse_path_count` paths, so the result has many paths and every source has only a few of them.
        for (size_t path = 0; path != dynamic_path_count; ++path)
        {
            const size_t path_id = sparse ? (source * dynamic_path_count + path) % sparse_path_count : path;
            value.emplace("dynamic_" + std::to_string(path_id), makeDynamicValue(source, row, path));
        }
        column->insert(value);
    }
    return column;
}

SourceColumns makeSources(const DataTypePtr & type, size_t source_count, size_t rows, bool object, bool sparse = false)
{
    SourceColumns sources;
    sources.reserve(source_count);
    for (size_t source = 0; source != source_count; ++source)
    {
        sources.emplace_back(object ? makeObjectColumn(type, source, rows, sparse) : makeNestedMapColumn(type, source, rows));
    }
    return sources;
}

/// The owning source columns must outlive the returned pointers.
ColumnRawPtrs makeRawSources(const SourceColumns & sources)
{
    ColumnRawPtrs raw_sources;
    raw_sources.reserve(sources.size());
    for (const auto & source : sources)
        raw_sources.push_back(source.get());
    return raw_sources;
}

/// The number of element types besides `String` in the arrays of the `Array(Dynamic)` variant of the nested `Dynamic`.
constexpr size_t nested_dynamic_element_type_count = 6;

Field makeNestedDynamicElement(size_t element_type, size_t row)
{
    switch (element_type)
    {
        case 0: return Field(Int64(-10'000'000'000LL - static_cast<Int64>(row)));
        case 1: return Field(Float64(row) + 0.5);
        case 2: return Array{Field(UInt64(row)), Field(UInt64(row + 1))};
        case 3: return Array{Field("element_" + std::to_string(row % 17))};
        case 4: return Tuple{Field(UInt64(row)), Field("element_" + std::to_string(row % 17))};
        default: return Map{Tuple{Field("key_" + std::to_string(row % 5)), Field(UInt64(row))}};
    }
}

/// The values of a `Dynamic` column with leaf variants, a `Map` variant (it has statistics) and an `Array(Dynamic)`
/// variant, which `JSON` uses for arrays of values of different types. The arrays have a `String` and an element of one
/// of `nested_dynamic_element_type_count` other types, and every source has 2 of them. So the nested `Dynamic` of the
/// result has more variants than the nested `Dynamic` of every source, and the merge or the squashing recurses into it.
Field makeNestedDynamicValue(size_t source, size_t row)
{
    const size_t element_type = (source + (row / 4) % 2) % nested_dynamic_element_type_count;
    switch (row % 4)
    {
        case 0: return Field(Int64(-10'000'000'000LL - static_cast<Int64>(source * 1'000 + row)));
        case 1: return Field("value_" + std::to_string((source + row) % 17));
        case 2: return Array{Field("element_" + std::to_string(row % 17)), makeNestedDynamicElement(element_type, row)};
        default:
            return Map{Tuple{Field("key_" + std::to_string(row % 5)), Field(UInt64(row))}, Tuple{Field("source"), Field(UInt64(source))}};
    }
}

ColumnPtr makeNestedDynamicColumn(const DataTypePtr & type, size_t source, size_t rows)
{
    auto column = type->createColumn();
    for (size_t row = 0; row != rows; ++row)
        column->insert(makeNestedDynamicValue(source, row));
    return column;
}

SourceColumns makeNestedDynamicSources(const DataTypePtr & type, size_t first_source, size_t source_count, size_t rows)
{
    SourceColumns sources;
    sources.reserve(source_count);
    for (size_t source = first_source; source != first_source + source_count; ++source)
        sources.emplace_back(makeNestedDynamicColumn(type, source, rows));
    return sources;
}

void cacheOwnStatistics(IColumn & column)
{
    if (auto * object = typeid_cast<ColumnObject *>(&column))
        object->setStatistics(object->getOrCalculateStatistics());
    else if (auto * dynamic = typeid_cast<ColumnDynamic *>(&column))
        dynamic->setStatistics(dynamic->getOrCalculateStatistics());
    else if (auto * map = typeid_cast<ColumnMap *>(&column))
        map->setStatistics(map->getOrCalculateStatistics());
}

void cacheStatistics(IColumn & column)
{
    column.forEachMutableSubcolumnRecursively(cacheOwnStatistics);
    cacheOwnStatistics(column);
}

void cacheStatistics(SourceColumns & columns)
{
    for (auto & column : columns)
    {
        auto mutable_column = IColumn::mutate(std::move(column));
        cacheStatistics(*mutable_column);
        column = std::move(mutable_column);
    }
}

DataTypePtr getNestedMapType()
{
    return DataTypeFactory::instance().get("Map(String, Array(Nullable(String)))");
}

DataTypePtr getObjectType()
{
    return DataTypeFactory::instance().get("JSON(max_dynamic_types=8, max_dynamic_paths=8, typed Map(String, Array(Nullable(String))))");
}

DataTypePtr getSparseObjectType()
{
    return DataTypeFactory::instance().get("JSON(max_dynamic_types=8, max_dynamic_paths=256, typed Map(String, Array(Nullable(String))))");
}

void setJemallocAllocationCounters(benchmark::State & state, const std::optional<JemallocAllocationStats> & allocation_stats)
{
    if (!allocation_stats)
        return;

    state.counters["jemalloc_allocations"] = static_cast<double>(allocation_stats->allocations);
    state.counters["jemalloc_allocated_bytes"] = static_cast<double>(allocation_stats->allocated_bytes);
}

void BM_PrepareForSquashingNestedMap(benchmark::State & state)
{
    const auto type = getNestedMapType();
    const auto initial_column = makeNestedMapColumn(type, 0, state.range(1));
    const auto source_columns = makeSources(type, state.range(0), state.range(1), false);
    const auto sources = makeRawSources(source_columns);

    /// Run one untimed allocation probe. Exact jemalloc request counts require flushing the
    /// thread cache, so keeping the probe separate avoids perturbing the benchmark's timed loop.
    auto allocation_probe_destination = initial_column->cloneResized(initial_column->size());
    const auto allocation_stats = measureJemallocAllocations([&] { allocation_probe_destination->prepareForSquashing(sources, 1); });
    benchmark::DoNotOptimize(allocation_probe_destination);
    allocation_probe_destination.reset();
    setJemallocAllocationCounters(state, allocation_stats);

    for (auto _ [[maybe_unused]] : state)
    {
        state.PauseTiming();
        auto destination = initial_column->cloneResized(initial_column->size());
        state.ResumeTiming();

        /// This is the same shape as Squashing: a non-empty first chunk is the destination,
        /// and `sources` contains the additional chunks that will be appended to it.
        destination->prepareForSquashing(sources, 1);
        benchmark::DoNotOptimize(destination);

        state.PauseTiming();
        destination.reset();
        state.ResumeTiming();
    }
}

void BM_SquashNestedMap(benchmark::State & state)
{
    const auto type = getNestedMapType();
    /// Squashing uses the first chunk as the destination and appends the requested number of sources to it.
    const auto source_columns = makeSources(type, state.range(0) + 1, state.range(1), false);
    auto header = std::make_shared<const Block>(Block{{type->createColumn(), type, "value"}});

    Chunks allocation_probe_chunks;
    allocation_probe_chunks.reserve(source_columns.size());
    for (const auto & source : source_columns)
        allocation_probe_chunks.emplace_back(Columns{source->cloneResized(source->size())}, source->size());
    auto allocation_probe_squashing = std::make_unique<Squashing>(header, 0, 0);
    Chunk allocation_probe_result;
    const auto allocation_stats = measureJemallocAllocations(
        [&]
        {
            for (auto & chunk : allocation_probe_chunks)
                allocation_probe_squashing->add(std::move(chunk));
            allocation_probe_result = Squashing::squash(allocation_probe_squashing->flush(), header);
        });
    benchmark::DoNotOptimize(allocation_probe_result);
    allocation_probe_result.clear();
    allocation_probe_squashing.reset();
    setJemallocAllocationCounters(state, allocation_stats);

    for (auto _ [[maybe_unused]] : state)
    {
        state.PauseTiming();
        Chunks chunks;
        chunks.reserve(source_columns.size());
        for (const auto & source : source_columns)
            chunks.emplace_back(Columns{source->cloneResized(source->size())}, source->size());
        auto squashing = std::make_unique<Squashing>(header, 0, 0);
        state.ResumeTiming();

        for (auto & chunk : chunks)
            squashing->add(std::move(chunk));
        auto result = Squashing::squash(squashing->flush(), header);
        benchmark::DoNotOptimize(result);

        state.PauseTiming();
        result.clear();
        squashing.reset();
        state.ResumeTiming();
    }
}

void prepareForSquashingObject(benchmark::State & state, bool sparse)
{
    const auto type = sparse ? getSparseObjectType() : getObjectType();
    const auto initial_column = makeObjectColumn(type, 0, state.range(1), sparse);
    const auto source_columns = makeSources(type, state.range(0), state.range(1), true, sparse);
    const auto sources = makeRawSources(source_columns);

    auto allocation_probe_destination = initial_column->cloneResized(initial_column->size());
    const auto allocation_stats = measureJemallocAllocations([&] { allocation_probe_destination->prepareForSquashing(sources, 1); });
    benchmark::DoNotOptimize(allocation_probe_destination);
    allocation_probe_destination.reset();
    setJemallocAllocationCounters(state, allocation_stats);

    for (auto _ [[maybe_unused]] : state)
    {
        state.PauseTiming();
        auto destination = initial_column->cloneResized(initial_column->size());
        state.ResumeTiming();

        destination->prepareForSquashing(sources, 1);
        benchmark::DoNotOptimize(destination);

        state.PauseTiming();
        destination.reset();
        state.ResumeTiming();
    }
}

void BM_ChooseDynamicStructureForMergeObject(benchmark::State & state)
{
    const auto type = getObjectType();
    auto source_columns = makeSources(type, state.range(0), state.range(1), true);
    cacheStatistics(source_columns);
    const auto sources = makeRawSources(source_columns);

    auto allocation_probe_destination = type->createColumn();
    const auto allocation_stats = measureJemallocAllocations(
        [&] { allocation_probe_destination->chooseDynamicStructureForMerge(sources, max_dynamic_subcolumns); });
    benchmark::DoNotOptimize(allocation_probe_destination);
    allocation_probe_destination.reset();
    setJemallocAllocationCounters(state, allocation_stats);

    for (auto _ [[maybe_unused]] : state)
    {
        state.PauseTiming();
        auto destination = type->createColumn();
        state.ResumeTiming();

        destination->chooseDynamicStructureForMerge(sources, max_dynamic_subcolumns);
        benchmark::DoNotOptimize(destination);

        state.PauseTiming();
        destination.reset();
        state.ResumeTiming();
    }
}

void BM_TakeOrCalculateStatisticsFromObject(benchmark::State & state)
{
    const auto type = getObjectType();
    auto source_columns = makeSources(type, state.range(0), state.range(1), true);
    cacheStatistics(source_columns);
    const auto sources = makeRawSources(source_columns);

    auto allocation_probe_destination = type->createColumn();
    allocation_probe_destination->chooseDynamicStructureForMerge(sources, max_dynamic_subcolumns);
    const auto allocation_stats = measureJemallocAllocations([&] { allocation_probe_destination->takeOrCalculateStatisticsFrom(sources); });
    benchmark::DoNotOptimize(allocation_probe_destination);
    allocation_probe_destination.reset();
    setJemallocAllocationCounters(state, allocation_stats);

    for (auto _ [[maybe_unused]] : state)
    {
        state.PauseTiming();
        auto destination = type->createColumn();
        destination->chooseDynamicStructureForMerge(sources, max_dynamic_subcolumns);
        state.ResumeTiming();

        destination->takeOrCalculateStatisticsFrom(sources);
        benchmark::DoNotOptimize(destination);

        state.PauseTiming();
        destination.reset();
        state.ResumeTiming();
    }
}

void BM_TakeStatisticsForPartWritingObject(benchmark::State & state)
{
    const auto type = getObjectType();
    auto sources = makeSources(type, 1, state.range(0), true);
    cacheStatistics(sources);
    const IColumn * source = sources.front().get();

    /// MergeTreeDataPartWriterOnDisk propagates statistics from one block or sample column, MergedData and ColumnGatherer
    /// from the merged column.
    auto allocation_probe_destination = type->createColumn();
    allocation_probe_destination->takeExactDynamicStructureFrom(*source);
    const auto allocation_stats
        = measureJemallocAllocations([&] { allocation_probe_destination->takeOrCalculateStatisticsFrom(std::span(&source, 1)); });
    benchmark::DoNotOptimize(allocation_probe_destination);
    allocation_probe_destination.reset();
    setJemallocAllocationCounters(state, allocation_stats);

    for (auto _ [[maybe_unused]] : state)
    {
        state.PauseTiming();
        auto destination = type->createColumn();
        destination->takeExactDynamicStructureFrom(*source);
        state.ResumeTiming();

        destination->takeOrCalculateStatisticsFrom(std::span(&source, 1));
        benchmark::DoNotOptimize(destination);

        state.PauseTiming();
        destination.reset();
        state.ResumeTiming();
    }
}

void BM_PrepareForSquashingObject(benchmark::State & state) { prepareForSquashingObject(state, false); }

void BM_PrepareForSquashingSparseObject(benchmark::State & state) { prepareForSquashingObject(state, true); }

void mergePreparationObject(benchmark::State & state, bool sparse)
{
    const auto type = sparse ? getSparseObjectType() : getObjectType();
    auto source_columns = makeSources(type, state.range(0), state.range(1), true, sparse);
    cacheStatistics(source_columns);
    const auto sources = makeRawSources(source_columns);
    /// Sparse: no limit, so that the result has all paths of the sources and every source only a few of them.
    const std::optional<size_t> dynamic_subcolumns_limit = sparse ? std::nullopt : std::optional<size_t>(max_dynamic_subcolumns);

    auto allocation_probe_destination = type->createColumn();
    const auto allocation_stats = measureJemallocAllocations(
        [&]
        {
            allocation_probe_destination->chooseDynamicStructureForMerge(sources, dynamic_subcolumns_limit);
            allocation_probe_destination->takeOrCalculateStatisticsFrom(sources);
        });
    benchmark::DoNotOptimize(allocation_probe_destination);
    allocation_probe_destination.reset();
    setJemallocAllocationCounters(state, allocation_stats);

    for (auto _ [[maybe_unused]] : state)
    {
        state.PauseTiming();
        auto destination = type->createColumn();
        state.ResumeTiming();

        destination->chooseDynamicStructureForMerge(sources, dynamic_subcolumns_limit);
        destination->takeOrCalculateStatisticsFrom(sources);
        benchmark::DoNotOptimize(destination);

        state.PauseTiming();
        destination.reset();
        state.ResumeTiming();
    }
}

void BM_MergePreparationObject(benchmark::State & state) { mergePreparationObject(state, false); }

void BM_MergePreparationSparseObject(benchmark::State & state) { mergePreparationObject(state, true); }

DataTypePtr getNestedDynamicType() { return DataTypeFactory::instance().get("Dynamic"); }

void BM_PrepareForSquashingNestedDynamic(benchmark::State & state)
{
    const auto type = getNestedDynamicType();
    const auto initial_column = makeNestedDynamicColumn(type, 0, state.range(1));
    const auto source_columns = makeNestedDynamicSources(type, 1, state.range(0), state.range(1));
    const auto sources = makeRawSources(source_columns);

    auto allocation_probe_destination = initial_column->cloneResized(initial_column->size());
    const auto allocation_stats = measureJemallocAllocations([&] { allocation_probe_destination->prepareForSquashing(sources, 1); });
    benchmark::DoNotOptimize(allocation_probe_destination);
    allocation_probe_destination.reset();
    setJemallocAllocationCounters(state, allocation_stats);

    for (auto _ [[maybe_unused]] : state)
    {
        state.PauseTiming();
        auto destination = initial_column->cloneResized(initial_column->size());
        state.ResumeTiming();

        destination->prepareForSquashing(sources, 1);
        benchmark::DoNotOptimize(destination);

        state.PauseTiming();
        destination.reset();
        state.ResumeTiming();
    }
}

void BM_MergePreparationNestedDynamic(benchmark::State & state)
{
    const auto type = getNestedDynamicType();
    auto source_columns = makeNestedDynamicSources(type, 0, state.range(0), state.range(1));
    cacheStatistics(source_columns);
    const auto sources = makeRawSources(source_columns);

    auto allocation_probe_destination = type->createColumn();
    const auto allocation_stats = measureJemallocAllocations(
        [&]
        {
            allocation_probe_destination->chooseDynamicStructureForMerge(sources, std::nullopt);
            allocation_probe_destination->takeOrCalculateStatisticsFrom(sources);
        });
    benchmark::DoNotOptimize(allocation_probe_destination);
    allocation_probe_destination.reset();
    setJemallocAllocationCounters(state, allocation_stats);

    for (auto _ [[maybe_unused]] : state)
    {
        state.PauseTiming();
        auto destination = type->createColumn();
        state.ResumeTiming();

        destination->chooseDynamicStructureForMerge(sources, std::nullopt);
        destination->takeOrCalculateStatisticsFrom(sources);
        benchmark::DoNotOptimize(destination);

        state.PauseTiming();
        destination.reset();
        state.ResumeTiming();
    }
}

#define REGISTER_SOURCE_BENCHMARK(name, source_dimension) \
    BENCHMARK(name)->Args({1, 32})->Args({8, 32})->Args({32, 32})->ArgNames({source_dimension, "rows_per_source"})

/// With 256 sources every one of the 256 paths is in 8 sources.
#define REGISTER_SPARSE_SOURCE_BENCHMARK(name, source_dimension) \
    BENCHMARK(name)->Args({8, 32})->Args({32, 32})->Args({256, 32})->ArgNames({source_dimension, "rows_per_source"})

REGISTER_SOURCE_BENCHMARK(BM_PrepareForSquashingNestedMap, "additional_sources");
REGISTER_SOURCE_BENCHMARK(BM_SquashNestedMap, "additional_chunks");
REGISTER_SOURCE_BENCHMARK(BM_PrepareForSquashingObject, "additional_sources");
REGISTER_SOURCE_BENCHMARK(BM_ChooseDynamicStructureForMergeObject, "sources");
REGISTER_SOURCE_BENCHMARK(BM_TakeOrCalculateStatisticsFromObject, "sources");
REGISTER_SOURCE_BENCHMARK(BM_MergePreparationObject, "sources");
REGISTER_SPARSE_SOURCE_BENCHMARK(BM_PrepareForSquashingSparseObject, "additional_sources");
REGISTER_SPARSE_SOURCE_BENCHMARK(BM_MergePreparationSparseObject, "sources");

/// With 5 or more sources the nested `Dynamic` of the result has all element types, and every source has 2 of them.
#define REGISTER_NESTED_DYNAMIC_BENCHMARK(name, source_dimension) \
    BENCHMARK(name)->Args({1, 32})->Args({8, 32})->Args({32, 32})->Args({256, 32})->ArgNames({source_dimension, "rows_per_source"})

REGISTER_NESTED_DYNAMIC_BENCHMARK(BM_PrepareForSquashingNestedDynamic, "additional_sources");
REGISTER_NESTED_DYNAMIC_BENCHMARK(BM_MergePreparationNestedDynamic, "sources");

BENCHMARK(BM_TakeStatisticsForPartWritingObject)->Arg(32)->ArgName("rows");

#undef REGISTER_SOURCE_BENCHMARK
#undef REGISTER_SPARSE_SOURCE_BENCHMARK
#undef REGISTER_NESTED_DYNAMIC_BENCHMARK
}
