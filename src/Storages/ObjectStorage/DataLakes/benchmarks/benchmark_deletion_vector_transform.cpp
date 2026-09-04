#include <Storages/ObjectStorage/DataLakes/DeletionVectorBitmap.h>
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Processors/Formats/IInputFormat.h>
#include <Storages/ObjectStorage/DataLakes/DeletionVectorTransform.h>

#include <benchmark/benchmark.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <string>


namespace
{

using ExcludedRows = DB::DeletionVectorTransform::ExcludedRows;

/// The type deletion vectors were stored in before, kept so that the baseline below measures the
/// implementation that actually shipped. Its `rb_contains` is backed by `roaring::Roaring64Map`,
/// which answers single-point lookups faster than the native 64-bit bitmap `ExcludedRows` now uses;
/// running the old algorithm against the new container would understate the old implementation.
using LegacyExcludedRows = DB::RoaringBitmapWithSmallSet<size_t, 32>;

constexpr UInt64 row_offset = 1'000'000'000;

/// Every third row is dropped by the emulated upstream filter. This has to stay coprime with every
/// deletion stride registered below: when the two resonate, all deleted row numbers land on rows
/// that are already gone and the index remapping path is never exercised.
constexpr size_t prefilter_stride = 3;

struct Dataset
{
    DB::Columns columns;
    std::unique_ptr<DB::IColumnFilter> applied_filter;
    std::unique_ptr<ExcludedRows> excluded_rows = std::make_unique<ExcludedRows>();
    /// Holds the same row numbers as `excluded_rows`, for the baseline.
    std::unique_ptr<LegacyExcludedRows> legacy_excluded_rows = std::make_unique<LegacyExcludedRows>();
    size_t logical_rows = 0;
    size_t physical_rows = 0;
    size_t expected_rows = 0;
};

Dataset makeDataset(size_t logical_rows, size_t deletion_stride, bool prefiltered, bool wide)
{
    Dataset dataset;
    dataset.logical_rows = logical_rows;

    if (prefiltered)
    {
        dataset.applied_filter = std::make_unique<DB::IColumnFilter>(logical_rows, 1);
        for (size_t i = 0; i < logical_rows; i += prefilter_stride)
            (*dataset.applied_filter)[i] = 0;
    }

    auto numbers = DB::ColumnUInt64::create();
    numbers->reserve(logical_rows);

    DB::MutableColumnPtr strings;
    const std::string payload(64, 'x');
    if (wide)
    {
        strings = DB::ColumnString::create();
        strings->reserve(logical_rows);
    }

    for (size_t i = 0; i < logical_rows; ++i)
    {
        if (dataset.applied_filter && !(*dataset.applied_filter)[i])
            continue;

        numbers->insertValue(i);
        if (strings)
            strings->insertData(payload.data(), payload.size());
        ++dataset.physical_rows;
    }

    dataset.columns.emplace_back(std::move(numbers));
    if (strings)
        dataset.columns.emplace_back(std::move(strings));

    /// Keep the bitmap in its roaring representation and put entries on both sides of the chunk.
    /// This also makes the benchmark exercise seeking to the chunk's range instead of starting at
    /// the first bitmap entry.
    for (size_t i = 0; i < 100; ++i)
    {
        dataset.excluded_rows->add(row_offset - 10'000 + i * 10);
        dataset.legacy_excluded_rows->add(row_offset - 10'000 + i * 10);
        dataset.excluded_rows->add(row_offset + logical_rows + 10'000 + i * 10);
        dataset.legacy_excluded_rows->add(row_offset + logical_rows + 10'000 + i * 10);
    }

    size_t deleted_physical_rows = 0;
    if (deletion_stride != 0)
    {
        for (size_t i = 0; i < logical_rows; i += deletion_stride)
        {
            dataset.excluded_rows->add(row_offset + i);
            dataset.legacy_excluded_rows->add(row_offset + i);
            if (!dataset.applied_filter || (*dataset.applied_filter)[i])
                ++deleted_physical_rows;
        }
    }

    dataset.expected_rows = dataset.physical_rows - deleted_physical_rows;
    return dataset;
}

DB::Chunk makeChunk(const Dataset & dataset)
{
    DB::Chunk chunk(dataset.columns, dataset.physical_rows);
    std::optional<DB::IColumnFilter> applied_filter;
    if (dataset.applied_filter)
        applied_filter.emplace(dataset.applied_filter->begin(), dataset.applied_filter->end());
    chunk.getChunkInfos().add(std::make_shared<DB::ChunkInfoRowNumbers>(row_offset, std::move(applied_filter)));
    return chunk;
}

/// The implementation before range scanning was introduced. Keeping it in this benchmark lets the
/// old and new algorithms run in the same binary, with identical compiler settings and input data.
void legacyTransform(DB::Chunk & chunk, const LegacyExcludedRows & excluded_rows)
{
    auto chunk_info = chunk.getChunkInfos().get<DB::ChunkInfoRowNumbers>();

    const size_t num_rows_before = chunk.getNumRows();
    size_t num_rows_after = num_rows_before;
    size_t index_in_chunk = 0;

    auto & applied_filter = chunk_info->applied_filter;
    const size_t num_indices = applied_filter.has_value() ? applied_filter->size() : num_rows_before;

    DB::IColumn::Filter filter(num_rows_before, true);
    for (size_t i = 0; i < num_indices; ++i)
    {
        if (!applied_filter.has_value() || applied_filter.value()[i])
        {
            if (excluded_rows.rb_contains(chunk_info->row_num_offset + i))
            {
                filter[index_in_chunk] = false;
                --num_rows_after;

                if (applied_filter.has_value())
                    applied_filter.value()[i] = false;
            }
            ++index_in_chunk;
        }
    }

    if (num_rows_after == num_rows_before)
        return;

    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->filter(filter, static_cast<ssize_t>(num_rows_after));

    if (!applied_filter.has_value())
        applied_filter.emplace(std::move(filter));

    chunk.setColumns(std::move(columns), num_rows_after);
}

bool sameFilter(const std::optional<DB::IColumnFilter> & lhs, const std::optional<DB::IColumnFilter> & rhs)
{
    if (lhs.has_value() != rhs.has_value())
        return false;
    if (!lhs.has_value())
        return true;
    return lhs->size() == rhs->size() && std::equal(lhs->begin(), lhs->end(), rhs->begin());
}

bool sameResult(const DB::Chunk & lhs, const DB::Chunk & rhs)
{
    if (lhs.getNumRows() != rhs.getNumRows() || lhs.getNumColumns() != rhs.getNumColumns())
        return false;

    for (size_t column = 0; column < lhs.getNumColumns(); ++column)
    {
        for (size_t row = 0; row < lhs.getNumRows(); ++row)
        {
            if (lhs.getColumns()[column]->compareAt(row, row, *rhs.getColumns()[column], 1) != 0)
                return false;
        }
    }

    /// Downstream steps recover `_row_number` from this mask, so a wrong index remapping has to
    /// fail here even when the surviving rows happen to come out right.
    const auto lhs_info = lhs.getChunkInfos().get<DB::ChunkInfoRowNumbers>();
    const auto rhs_info = rhs.getChunkInfos().get<DB::ChunkInfoRowNumbers>();
    if (!lhs_info || !rhs_info)
        return false;
    return lhs_info->row_num_offset == rhs_info->row_num_offset
        && sameFilter(lhs_info->applied_filter, rhs_info->applied_filter);
}

enum class Implementation : UInt8
{
    Legacy,
    RangeScan,
};

template <Implementation implementation>
void benchmarkDeletionVectorTransform(benchmark::State & state)
{
    const auto logical_rows = static_cast<size_t>(state.range(0));
    const auto deletion_stride = static_cast<size_t>(state.range(1));
    const bool prefiltered = state.range(2) != 0;
    const bool wide = state.range(3) != 0;
    const auto dataset = makeDataset(logical_rows, deletion_stride, prefiltered, wide);

    if (deletion_stride != 0 && dataset.expected_rows == dataset.physical_rows)
    {
        state.SkipWithError("Scenario deletes no rows: the deletion stride resonates with the prefilter stride");
        return;
    }

    auto legacy_chunk = makeChunk(dataset);
    auto range_scan_chunk = makeChunk(dataset);
    legacyTransform(legacy_chunk, *dataset.legacy_excluded_rows);
    DB::DeletionVectorTransform::transform(range_scan_chunk, *dataset.excluded_rows);
    if (legacy_chunk.getNumRows() != dataset.expected_rows || !sameResult(legacy_chunk, range_scan_chunk))
    {
        state.SkipWithError("Legacy and range-scan transforms produced different results");
        return;
    }

    for (auto _ : state)
    {
        state.PauseTiming();
        auto chunk = makeChunk(dataset);
        state.ResumeTiming();

        if constexpr (implementation == Implementation::Legacy)
            legacyTransform(chunk, *dataset.legacy_excluded_rows);
        else
            DB::DeletionVectorTransform::transform(chunk, *dataset.excluded_rows);

        benchmark::DoNotOptimize(chunk.getNumRows());
        benchmark::DoNotOptimize(chunk.getColumns().data());
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(dataset.logical_rows));
    state.counters["input_rows"] = static_cast<double>(dataset.physical_rows);
    state.counters["output_rows"] = static_cast<double>(dataset.expected_rows);
}

#define REGISTER_SCENARIO(rows, stride, prefiltered, wide) \
    BENCHMARK_TEMPLATE(benchmarkDeletionVectorTransform, Implementation::Legacy) \
        ->Args({rows, stride, prefiltered, wide}) \
        ->ArgNames({"rows", "delete_stride", "prefiltered", "wide"}); \
    BENCHMARK_TEMPLATE(benchmarkDeletionVectorTransform, Implementation::RangeScan) \
        ->Args({rows, stride, prefiltered, wide}) \
        ->ArgNames({"rows", "delete_stride", "prefiltered", "wide"})

REGISTER_SCENARIO(8'192, 0, 0, 0);
REGISTER_SCENARIO(65'536, 0, 0, 0);
REGISTER_SCENARIO(65'536, 4'096, 0, 0);
REGISTER_SCENARIO(65'536, 64, 0, 0);
REGISTER_SCENARIO(65'536, 16, 0, 0);
REGISTER_SCENARIO(65'536, 4, 0, 0);
REGISTER_SCENARIO(65'536, 2, 0, 0);
REGISTER_SCENARIO(65'536, 1, 0, 0);
REGISTER_SCENARIO(1'048'576, 0, 0, 0);
REGISTER_SCENARIO(1'048'576, 4'096, 0, 0);

/// With an upstream filter in place, every deleted row costs an index remapping. The dense strides
/// are the worst case for it: the range handed to `countBytesInFilter` is then only a byte or two.
REGISTER_SCENARIO(65'536, 0, 1, 0);
REGISTER_SCENARIO(65'536, 4'096, 1, 0);
REGISTER_SCENARIO(65'536, 64, 1, 0);
REGISTER_SCENARIO(65'536, 16, 1, 0);
REGISTER_SCENARIO(65'536, 4, 1, 0);
REGISTER_SCENARIO(65'536, 2, 1, 0);
REGISTER_SCENARIO(65'536, 1, 1, 0);

REGISTER_SCENARIO(65'536, 0, 0, 1);
REGISTER_SCENARIO(65'536, 4'096, 0, 1);
REGISTER_SCENARIO(65'536, 64, 0, 1);

#undef REGISTER_SCENARIO

}

BENCHMARK_MAIN();
