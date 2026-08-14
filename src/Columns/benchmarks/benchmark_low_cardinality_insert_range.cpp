#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/assert_cast.h>
#include <base/types.h>

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdint>

using namespace DB;

namespace
{

constexpr size_t source_dictionary_size = 1ULL << 22;
constexpr size_t elements_per_operation = 1ULL << 18;
constexpr size_t source_index_begin = source_dictionary_size / 2;
constexpr size_t oversized_dictionary_size = 1ULL << 20;
constexpr UInt128 disjoint_key_prefix = UInt128{1} << 100;

enum class DestinationKeys
{
    Disjoint,
    FullyOverlapping,
    PartiallyOverlapping,
};

struct BenchmarkColumns
{
    DataTypePtr nested_type;
    MutableColumnPtr source;
    ColumnPtr destination_keys;
};

MutableColumnPtr makeSource(
    size_t dictionary_size,
    size_t elements,
    size_t distinct_indexes,
    size_t first_index,
    bool is_shared = false)
{
    auto nested_type = std::make_shared<DataTypeUInt128>();
    auto keys = ColumnUInt128::create(dictionary_size);
    auto & key_data = keys->getData();
    for (size_t i = 0; i < dictionary_size; ++i)
        key_data[i] = i;

    MutableColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(*nested_type, std::move(keys));
    MutableColumnPtr indexes = ColumnUInt32::create(elements);
    auto & index_data = assert_cast<ColumnUInt32 &>(*indexes).getData();
    for (size_t i = 0; i < elements; ++i)
        index_data[i] = static_cast<UInt32>(first_index + i % distinct_indexes);

    return ColumnLowCardinality::create(std::move(dictionary), std::move(indexes), is_shared);
}

ColumnPtr makeDestinationKeys(DestinationKeys kind, size_t distinct_indexes, size_t first_source_index)
{
    size_t overlapping_keys = 0;
    size_t disjoint_keys = 1ULL << 16;
    if (kind == DestinationKeys::FullyOverlapping)
    {
        overlapping_keys = distinct_indexes;
        disjoint_keys = 0;
    }
    else if (kind == DestinationKeys::PartiallyOverlapping)
    {
        overlapping_keys = distinct_indexes / 2;
        disjoint_keys = distinct_indexes - overlapping_keys;
    }

    auto keys = ColumnUInt128::create(1 + overlapping_keys + disjoint_keys);
    auto & data = keys->getData();
    data[0] = 0;
    for (size_t i = 0; i < overlapping_keys; ++i)
        data[1 + i] = first_source_index + i;
    for (size_t i = 0; i < disjoint_keys; ++i)
        data[1 + overlapping_keys + i] = disjoint_key_prefix + i;
    return keys;
}

BenchmarkColumns makeBenchmarkColumns(size_t repetitions, DestinationKeys destination_kind)
{
    const size_t distinct_indexes = elements_per_operation / repetitions;
    BenchmarkColumns columns;
    columns.nested_type = std::make_shared<DataTypeUInt128>();
    columns.source = makeSource(source_dictionary_size, elements_per_operation, distinct_indexes, source_index_begin);
    columns.destination_keys = makeDestinationKeys(destination_kind, distinct_indexes, source_index_begin);
    return columns;
}

MutableColumnPtr makeNonEmptyDestination(const DataTypePtr & nested_type, const IColumn & keys)
{
    auto dictionary_keys = keys.cloneResized(keys.size());
    MutableColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(*nested_type, std::move(dictionary_keys));
    MutableColumnPtr indexes = ColumnUInt8::create();
    assert_cast<ColumnUInt8 &>(*indexes).getData().push_back(UInt8{0});
    return ColumnLowCardinality::create(std::move(dictionary), std::move(indexes), /*is_shared=*/false);
}

void setThroughput(benchmark::State & state)
{
    const auto iterations = static_cast<int64_t>(state.iterations());
    const auto elements = static_cast<int64_t>(elements_per_operation);
    state.SetItemsProcessed(iterations * elements);
    state.SetBytesProcessed(iterations * elements * static_cast<int64_t>(sizeof(UInt32)));
}

void MapSparseOnly(benchmark::State & state)
{
    const size_t repetitions = static_cast<size_t>(state.range(0));
    const size_t distinct_indexes = elements_per_operation / repetitions;
    auto source = makeSource(source_dictionary_size, elements_per_operation, distinct_indexes, source_index_begin);
    const auto & low_cardinality_source = assert_cast<const ColumnLowCardinality &>(*source);

    for (auto _ : state)
    {
        auto encoded = low_cardinality_source.getMinimalDictionaryEncodedColumn(0, elements_per_operation);
        benchmark::DoNotOptimize(encoded.dictionary.get());
        benchmark::DoNotOptimize(encoded.indexes.get());
    }
    setThroughput(state);
}

void EmptyDestinationDifferentDictionary(benchmark::State & state)
{
    const size_t repetitions = static_cast<size_t>(state.range(0));
    auto columns = makeBenchmarkColumns(repetitions, DestinationKeys::Disjoint);
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(columns.nested_type);

    for (auto _ : state)
    {
        state.PauseTiming();
        auto destination = low_cardinality_type->createColumn();
        state.ResumeTiming();

        destination->insertRangeFrom(*columns.source, 0, elements_per_operation);
        benchmark::DoNotOptimize(destination->size());
    }
    setThroughput(state);
}

void EmptyDestinationMinimalDictionary(benchmark::State & state)
{
    const size_t repetitions = static_cast<size_t>(state.range(0));
    const size_t distinct_indexes = elements_per_operation / repetitions;
    auto nested_type = std::make_shared<DataTypeUInt128>();
    auto source = makeSource(
        /*dictionary_size=*/distinct_indexes + 1,
        elements_per_operation,
        distinct_indexes,
        /*first_index=*/1,
        /*is_shared=*/true);
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(nested_type);

    for (auto _ : state)
    {
        state.PauseTiming();
        auto destination = low_cardinality_type->createColumn();
        state.ResumeTiming();

        destination->insertRangeFrom(*source, 0, elements_per_operation);
        benchmark::DoNotOptimize(destination->size());
    }
    setThroughput(state);
}

void EmptyDestinationTinyRangeThenComputeHash(benchmark::State & state)
{
    constexpr size_t range_size = 1;
    auto nested_type = std::make_shared<DataTypeUInt128>();
    auto source = makeSource(
        oversized_dictionary_size,
        range_size,
        /*distinct_indexes=*/range_size,
        /*first_index=*/oversized_dictionary_size - 1);
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(nested_type);

    for (auto _ : state)
    {
        state.PauseTiming();
        auto destination = low_cardinality_type->createColumn();
        state.ResumeTiming();

        destination->insertRangeFrom(*source, 0, range_size);

        UInt32 hash = 0;
        destination->computeHashInto(0, range_size, &hash, /*initial=*/true);
        benchmark::DoNotOptimize(hash);
    }

    const auto iterations = static_cast<int64_t>(state.iterations());
    const auto elements = static_cast<int64_t>(range_size);
    state.SetItemsProcessed(iterations * elements);
    state.SetBytesProcessed(iterations * elements * static_cast<int64_t>(sizeof(UInt32)));
}

void benchmarkNonEmptyDestination(benchmark::State & state, DestinationKeys destination_kind)
{
    const size_t repetitions = static_cast<size_t>(state.range(0));
    auto columns = makeBenchmarkColumns(repetitions, destination_kind);

    for (auto _ : state)
    {
        state.PauseTiming();
        auto destination = makeNonEmptyDestination(columns.nested_type, *columns.destination_keys);
        state.ResumeTiming();

        destination->insertRangeFrom(*columns.source, 0, elements_per_operation);
        benchmark::DoNotOptimize(destination->size());
    }
    setThroughput(state);
}

void NonEmptyDisjointDictionaries(benchmark::State & state)
{
    benchmarkNonEmptyDestination(state, DestinationKeys::Disjoint);
}

void NonEmptyFullyOverlappingDictionaries(benchmark::State & state)
{
    benchmarkNonEmptyDestination(state, DestinationKeys::FullyOverlapping);
}

void NonEmptyPartiallyOverlapping(benchmark::State & state)
{
    benchmarkNonEmptyDestination(state, DestinationKeys::PartiallyOverlapping);
}

void SameDictionary(benchmark::State & state)
{
    const size_t repetitions = static_cast<size_t>(state.range(0));
    const size_t distinct_indexes = elements_per_operation / repetitions;
    auto source = makeSource(source_dictionary_size, elements_per_operation, distinct_indexes, source_index_begin);
    const auto & low_cardinality_source = assert_cast<const ColumnLowCardinality &>(*source);

    for (auto _ : state)
    {
        state.PauseTiming();
        MutableColumnPtr shared_dictionary = low_cardinality_source.getDictionaryPtr()->assumeMutable();
        MutableColumnPtr empty_indexes = ColumnUInt32::create();
        MutableColumnPtr destination = ColumnLowCardinality::create(
            std::move(shared_dictionary), std::move(empty_indexes), /*is_shared=*/false);
        const auto & low_cardinality_destination = assert_cast<const ColumnLowCardinality &>(*destination);
        if (&low_cardinality_source.getDictionary() != &low_cardinality_destination.getDictionary())
        {
            state.ResumeTiming();
            state.SkipWithError("SameDictionary fixture did not preserve dictionary identity");
            break;
        }
        state.ResumeTiming();

        destination->insertRangeFrom(*source, 0, elements_per_operation);
        benchmark::DoNotOptimize(destination->size());
    }
    setThroughput(state);
}

void SmallDenseDictionary(benchmark::State & state)
{
    constexpr size_t small_dictionary_size = 1ULL << 12;
    auto nested_type = std::make_shared<DataTypeUInt128>();
    auto source = makeSource(
        small_dictionary_size, elements_per_operation, small_dictionary_size - 1, /*first_index=*/1);
    auto destination_keys = makeDestinationKeys(DestinationKeys::Disjoint, 0, 0);

    for (auto _ : state)
    {
        state.PauseTiming();
        auto destination = makeNonEmptyDestination(nested_type, *destination_keys);
        state.ResumeTiming();

        destination->insertRangeFrom(*source, 0, elements_per_operation);
        benchmark::DoNotOptimize(destination->size());
    }
    setThroughput(state);
}

void applyRepetitionArguments(benchmark::internal::Benchmark * benchmark)
{
    benchmark->Arg(1)->Arg(2)->Arg(4);
}

BENCHMARK(MapSparseOnly)->Apply(applyRepetitionArguments);
BENCHMARK(EmptyDestinationDifferentDictionary)->Apply(applyRepetitionArguments);
BENCHMARK(EmptyDestinationMinimalDictionary)->Apply(applyRepetitionArguments);
BENCHMARK(EmptyDestinationTinyRangeThenComputeHash);
BENCHMARK(NonEmptyDisjointDictionaries)->Apply(applyRepetitionArguments);
BENCHMARK(NonEmptyFullyOverlappingDictionaries)->Apply(applyRepetitionArguments);
BENCHMARK(NonEmptyPartiallyOverlapping)->Apply(applyRepetitionArguments);
BENCHMARK(SameDictionary)->Apply(applyRepetitionArguments);
BENCHMARK(SmallDenseDictionary);

}
