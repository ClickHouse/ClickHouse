#include <Processors/Merges/MaterializeMergedDataTransform.h>

#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <Processors/Merges/MergingSortedTransform.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void MaterializeMergedDataTransform::transform(Chunk & chunk)
{
    Stopwatch watch;

    auto info = chunk.getChunkInfos().extract<MergedDataMaterializationInfo>();
    if (!info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk has no deferred MergedData materialization plan");

    const size_t expected_rows = chunk.getNumRows();
    MergedData materialized_data(
        false,
        expected_rows,
        0,
        std::nullopt);
    materialized_data.initializeFromColumns(info->output_columns);

    if (info->runs.size() == 1)
    {
        const auto & run = info->runs.front();
        if (run.source >= info->sources.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid deferred materialization source {}", run.source);

        auto & source = info->sources[run.source];
        if (run.start == 0 && run.length == source.num_rows && run.length == expected_rows)
        {
            materialized_data.insertChunk(Chunk(std::move(source.columns), source.num_rows), source.num_rows);
        }
    }

    if (!materialized_data.mergedRows())
    {
        std::vector<ColumnRawPtrs> raw_sources;
        raw_sources.reserve(info->sources.size());
        for (const auto & source : info->sources)
        {
            auto & raw_columns = raw_sources.emplace_back();
            raw_columns.reserve(source.columns.size());
            for (const auto & column : source.columns)
                raw_columns.emplace_back(column.get());
        }

        for (const auto & run : info->runs)
        {
            if (run.source >= info->sources.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid deferred materialization source {}", run.source);

            const auto & source = info->sources[run.source];
            if (run.start > source.num_rows || run.length > source.num_rows - run.start)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Invalid deferred materialization range [{}, {}) for a source with {} rows",
                    run.start,
                    run.start + run.length,
                    source.num_rows);

            materialized_data.insertRows(raw_sources[run.source], run.start, run.length, source.num_rows);
        }
    }

    if (materialized_data.mergedRows() != expected_rows)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Deferred materialization produced {} rows instead of {}",
            materialized_data.mergedRows(),
            expected_rows);

    auto result = materialized_data.pull();
    materialized_bytes += materialized_data.totalAllocatedBytes();
    chunk.setColumns(result.detachColumns(), expected_rows);
    materialization_elapsed_ns += watch.elapsedNanoseconds();
}

void MaterializeMergedDataTransform::onFinish()
{
    stats->finishMaterializer(materialized_bytes, materialization_elapsed_ns);
}

}
