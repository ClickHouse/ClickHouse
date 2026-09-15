#include <benchmark/benchmark.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnsView.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/AdaptiveAggregationImpl.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Squashing.h>
#include <Processors/Chunk.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/benchmarks/JemallocAllocationCounter.h>
#include <Common/typeid_cast.h>

#include <cstring>
#include <memory>
#include <mutex>
#include <string>

using namespace DB;

namespace DB
{
void registerAggregateFunctionsAny(AggregateFunctionFactory & factory);
}

namespace
{

constexpr size_t dynamic_path_count = 8;
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

ColumnPtr makeObjectColumn(const DataTypePtr & type, size_t source, size_t rows)
{
    auto column = type->createColumn();
    for (size_t row = 0; row != rows; ++row)
    {
        Object value;
        value.emplace("typed", makeNestedMapValue(source, row));
        /// Keep the path set fixed so the source-count dimension does not also change schema complexity.
        /// The merge limit is lower than this path count, which still exercises path ranking and shared-data statistics.
        for (size_t path = 0; path != dynamic_path_count; ++path)
            value.emplace("dynamic_" + std::to_string(path), makeDynamicValue(source, row, path));
        column->insert(value);
    }
    return column;
}

SourceColumns makeSources(const DataTypePtr & type, size_t source_count, size_t rows, bool object)
{
    SourceColumns sources;
    sources.reserve(source_count);
    for (size_t source = 0; source != source_count; ++source)
    {
        sources.emplace_back(object ? makeObjectColumn(type, source, rows) : makeNestedMapColumn(type, source, rows));
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

void ensureAggregateFunctionsRegistered()
{
    static std::once_flag registered;
    std::call_once(registered, [] { registerAggregateFunctionsAny(AggregateFunctionFactory::instance()); });
}

AggregateDescription makeAggregateDescription(const DataTypePtr & argument_type)
{
    AggregateDescription description;
    AggregateFunctionProperties properties;
    description.function = AggregateFunctionFactory::instance().get("any", NullsAction::EMPTY, {argument_type}, {}, properties);
    description.argument_names = {"value"};
    description.column_name = "any(value)";
    return description;
}

Aggregator::Params makeAdaptiveAggregationParams(const AggregateDescriptions & aggregates)
{
    return Aggregator::Params(
        {"key"},
        aggregates,
        /*overflow_row_=*/false,
        /*max_rows_to_group_by_=*/0,
        OverflowMode::THROW,
        /*group_by_two_level_threshold_=*/0,
        /*group_by_two_level_threshold_bytes_=*/0,
        /*max_bytes_before_external_group_by_=*/0,
        /*empty_result_for_aggregation_by_empty_set_=*/false,
        /*tmp_data_scope_=*/nullptr,
        /*max_threads_=*/1,
        /*min_free_disk_space_=*/0,
        /*compile_aggregate_expressions_=*/false,
        /*min_count_to_compile_aggregate_expression_=*/0,
        /*max_block_size_=*/65536,
        /*enable_prefetch_=*/true,
        /*only_merge_=*/false,
        /*optimize_group_by_constant_keys_=*/true,
        /*min_hit_rate_to_use_consecutive_keys_optimization_=*/0.5,
        StatsCollectingParams{},
        /*enable_producing_buckets_out_of_order_in_aggregation_=*/false,
        /*serialize_string_with_zero_byte_=*/false,
        /*enable_parallel_single_level_merge_=*/false,
        /*enable_packed_string_keys_=*/true,
        /*enable_adaptive_aggregator_=*/true,
        /*adaptive_aggregator_freeze_threshold_=*/1,
        /*adaptive_aggregator_freeze_threshold_bytes_=*/0);
}

void setJemallocAllocationCounters(benchmark::State & state, const std::optional<JemallocAllocationStats> & allocation_stats)
{
    if (!allocation_stats)
        return;

    state.counters["jemalloc_allocations"] = static_cast<double>(allocation_stats->allocations);
    state.counters["jemalloc_allocated_bytes"] = static_cast<double>(allocation_stats->allocated_bytes);
}

MutableStagedChunkPtr makeAdaptiveStagedChunk(const DataTypePtr & argument_type, size_t chunk_number, size_t rows)
{
    auto chunk = std::make_shared<StagedChunk>();
    auto & keys = chunk->keys;
    keys.fixed_key_size = sizeof(UInt64);
    keys.routing_hashes.resize(rows);
    keys.key_bytes.resize(rows * sizeof(UInt64));
    for (size_t bucket = 0; bucket <= ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++bucket)
        keys.bucket_offsets[bucket] = static_cast<UInt32>(rows * bucket / ADAPTIVE_AGGREGATION_NUM_BUCKETS);
    for (size_t bucket = 0; bucket != ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++bucket)
    {
        for (size_t row = keys.bucket_offsets[bucket]; row != keys.bucket_offsets[bucket + 1]; ++row)
        {
            const UInt64 key = chunk_number * rows + row;
            keys.routing_hashes[row] = (static_cast<UInt64>(bucket) << 24) | (key & 0xFFFFFF);
            std::memcpy(keys.key_bytes.data() + row * sizeof(key), &key, sizeof(key));
        }
    }

    auto & argument_columns = chunk->payload.emplace<StagedChunk::AggregatePayload>().argument_columns;
    argument_columns.resize(2);
    argument_columns[1] = makeNestedMapColumn(argument_type, chunk_number, rows);
    return chunk;
}

void BM_PrepareForSquashingNestedMap(benchmark::State & state)
{
    const auto type = getNestedMapType();
    const auto initial_column = makeNestedMapColumn(type, 0, state.range(1));
    const auto sources = makeSources(type, state.range(0), state.range(1), false);

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

void BM_PrepareForSquashingObject(benchmark::State & state)
{
    const auto type = getObjectType();
    const auto initial_column = makeObjectColumn(type, 0, state.range(1));
    const auto sources = makeSources(type, state.range(0), state.range(1), true);

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
    const auto & source = sources.front();

    /// MergeTreeDataPartWriterOnDisk propagates statistics from one block or sample column.
    auto allocation_probe_destination = type->createColumn();
    allocation_probe_destination->takeExactDynamicStructureFrom(*source);
    const auto allocation_stats
        = measureJemallocAllocations([&] { allocation_probe_destination->takeOrCalculateStatisticsFrom(ColumnsView{source}); });
    benchmark::DoNotOptimize(allocation_probe_destination);
    allocation_probe_destination.reset();
    setJemallocAllocationCounters(state, allocation_stats);

    for (auto _ [[maybe_unused]] : state)
    {
        state.PauseTiming();
        auto destination = type->createColumn();
        destination->takeExactDynamicStructureFrom(*source);
        state.ResumeTiming();

        destination->takeOrCalculateStatisticsFrom(ColumnsView{source});
        benchmark::DoNotOptimize(destination);

        state.PauseTiming();
        destination.reset();
        state.ResumeTiming();
    }
}

void BM_TakeStatisticsFromSingleRawPtrObject(benchmark::State & state)
{
    const auto type = getObjectType();
    auto sources = makeSources(type, 1, state.range(0), true);
    cacheStatistics(sources);
    const auto & source = sources.front();

    /// MergedData and ColumnGatherer propagate statistics from a single column whose owner outlives the call.
    auto allocation_probe_destination = type->createColumn();
    allocation_probe_destination->takeExactDynamicStructureFrom(*source);
    const auto allocation_stats
        = measureJemallocAllocations([&] { allocation_probe_destination->takeOrCalculateStatisticsFrom(source.get()); });
    benchmark::DoNotOptimize(allocation_probe_destination);
    allocation_probe_destination.reset();
    setJemallocAllocationCounters(state, allocation_stats);

    for (auto _ [[maybe_unused]] : state)
    {
        state.PauseTiming();
        auto destination = type->createColumn();
        destination->takeExactDynamicStructureFrom(*source);
        state.ResumeTiming();

        destination->takeOrCalculateStatisticsFrom(source.get());
        benchmark::DoNotOptimize(destination);

        state.PauseTiming();
        destination.reset();
        state.ResumeTiming();
    }
}

void BM_AdaptiveAggregationCoalescingNestedMap(benchmark::State & state)
{
    /// The fixtures model the small staged chunks produced after adaptive key extraction. Flushing
    /// them uses the public production path which seals pending chunks and publishes the result.
    ensureAggregateFunctionsRegistered();
    const auto key_type = DataTypeFactory::instance().get("UInt64");
    const auto argument_type = getNestedMapType();
    const AggregateDescriptions aggregates{makeAggregateDescription(argument_type)};
    const Block header{{key_type->createColumn(), key_type, "key"}, {argument_type->createColumn(), argument_type, "value"}};
    const Aggregator aggregator(header, makeAdaptiveAggregationParams(aggregates));

    auto allocation_probe_session = std::make_shared<AdaptiveAggregationSession>();
    auto allocation_probe_producer = std::make_unique<AdaptiveAggregationProducer>(allocation_probe_session);
    allocation_probe_producer->pending_chunks.reserve(state.range(0));
    for (size_t chunk = 0; chunk != static_cast<size_t>(state.range(0)); ++chunk)
        allocation_probe_producer->pending_chunks.emplace_back(makeAdaptiveStagedChunk(argument_type, chunk, state.range(1)));
    const auto allocation_stats = measureJemallocAllocations([&] { aggregator.flushPendingChunks(*allocation_probe_producer); });
    benchmark::DoNotOptimize(allocation_probe_session->backlog.undrainedRecords());
    allocation_probe_producer.reset();
    allocation_probe_session.reset();
    setJemallocAllocationCounters(state, allocation_stats);

    for (auto _ [[maybe_unused]] : state)
    {
        state.PauseTiming();
        auto session = std::make_shared<AdaptiveAggregationSession>();
        auto producer = std::make_unique<AdaptiveAggregationProducer>(session);
        producer->pending_chunks.reserve(state.range(0));
        for (size_t chunk = 0; chunk != static_cast<size_t>(state.range(0)); ++chunk)
            producer->pending_chunks.emplace_back(makeAdaptiveStagedChunk(argument_type, chunk, state.range(1)));
        state.ResumeTiming();

        aggregator.flushPendingChunks(*producer);

        state.PauseTiming();
        benchmark::DoNotOptimize(session->backlog.undrainedRecords());
        producer.reset();
        session.reset();
        state.ResumeTiming();
    }
}

void BM_MergePreparationObject(benchmark::State & state)
{
    const auto type = getObjectType();
    auto source_columns = makeSources(type, state.range(0), state.range(1), true);
    cacheStatistics(source_columns);
    const auto sources = makeRawSources(source_columns);

    auto allocation_probe_destination = type->createColumn();
    const auto allocation_stats = measureJemallocAllocations(
        [&]
        {
            allocation_probe_destination->chooseDynamicStructureForMerge(sources, max_dynamic_subcolumns);
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

        destination->chooseDynamicStructureForMerge(sources, max_dynamic_subcolumns);
        destination->takeOrCalculateStatisticsFrom(sources);
        benchmark::DoNotOptimize(destination);

        state.PauseTiming();
        destination.reset();
        state.ResumeTiming();
    }
}

#define REGISTER_SOURCE_BENCHMARK(name, source_dimension) \
    BENCHMARK(name)->Args({1, 32})->Args({8, 32})->Args({32, 32})->ArgNames({source_dimension, "rows_per_source"})

REGISTER_SOURCE_BENCHMARK(BM_PrepareForSquashingNestedMap, "additional_sources");
REGISTER_SOURCE_BENCHMARK(BM_SquashNestedMap, "additional_chunks");
REGISTER_SOURCE_BENCHMARK(BM_PrepareForSquashingObject, "additional_sources");
REGISTER_SOURCE_BENCHMARK(BM_ChooseDynamicStructureForMergeObject, "sources");
REGISTER_SOURCE_BENCHMARK(BM_TakeOrCalculateStatisticsFromObject, "sources");
REGISTER_SOURCE_BENCHMARK(BM_MergePreparationObject, "sources");

BENCHMARK(BM_AdaptiveAggregationCoalescingNestedMap)
    ->Args({1, 32})
    ->Args({8, 32})
    ->Args({32, 32})
    ->ArgNames({"staged_chunks", "rows_per_chunk"});
BENCHMARK(BM_TakeStatisticsForPartWritingObject)->Arg(32)->ArgName("rows");
BENCHMARK(BM_TakeStatisticsFromSingleRawPtrObject)->Arg(32)->ArgName("rows");

#undef REGISTER_SOURCE_BENCHMARK

}
