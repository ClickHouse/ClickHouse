#include <Storages/MergeTree/MergeTreeRestColumnsTransform.h>
#include <Storages/MergeTree/MergeTreeReadChunkInfo.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeRestColumnsTransform::MergeTreeRestColumnsTransform(
    Block input_header_,
    Block output_header_)
    : ISimpleTransform(std::move(input_header_), std::move(output_header_), /*skip_empty_chunks_=*/ false)
{
}

void MergeTreeRestColumnsTransform::transform(Chunk & chunk)
{
    auto read_chunk_info = chunk.getChunkInfos().get<MergeTreeReadChunkInfo>();
    if (!read_chunk_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeRestColumnsTransform: missing MergeTreeReadChunkInfo");

    if (!read_chunk_info->read_result)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeRestColumnsTransform: missing ReadResult in chunk info");

    /// Take ownership of the main reader when present (first chunk of a new task).
    if (read_chunk_info->rest_reader)
    {
        rest_reader = std::move(read_chunk_info->rest_reader);
        rest_range_reader.reset();
    }

    if (!rest_reader || rest_reader->getColumns().empty())
        return; /// No rest columns to read — pass through.

    /// Lazily create the MergeTreeRangeReader for the main reader.
    if (!rest_range_reader)
    {
        /// The prev_reader_header is the input header minus rest columns.
        /// It represents what previous prewhere steps produced.
        Block prev_reader_header = getInputPort().getHeader();
        for (const auto & col : rest_reader->getColumns())
            if (prev_reader_header.has(col.name))
                prev_reader_header.erase(col.name);

        rest_range_reader.emplace(
            rest_reader.get(),
            std::move(prev_reader_header),
            /*prewhere_info_=*/ nullptr,
            read_steps_performance_counters.getCountersForStep(0),
            /*main_reader_=*/ true,
            rest_reader->canReadIncompleteGranules());
    }

    auto & read_result = *read_chunk_info->read_result;
    size_t num_read_rows = 0;

    auto rest_columns = rest_range_reader->continueReadingChain(read_result, num_read_rows);

    if (rest_columns.empty())
        return; /// All rest columns absent in this part.

    /// If all requested columns are absent in part, num_read_rows will be 0.
    /// In this case, use the number of rows in the result to fill defaults.
    if (num_read_rows == 0)
        num_read_rows = read_result.num_rows;

    /// Fill virtual columns for the rest reader.
    rest_reader->fillVirtualColumns(rest_columns, num_read_rows);

    /// Fill missing columns (columns absent from the part).
    bool should_evaluate_missing_defaults = false;
    rest_reader->fillMissingColumns(rest_columns, should_evaluate_missing_defaults, num_read_rows);

    /// Apply the PREWHERE filter to newly read rest columns.
    /// `continueReadingChain` reads `total_rows_per_granule` rows,
    /// but we need `num_rows` (the filtered count).
    const auto & final_filter = read_result.getFinalFilter();
    if (read_result.num_rows != num_read_rows && final_filter.present())
    {
        MergeTreeRangeReader::filterColumns(rest_columns, final_filter);
    }

    /// Apply on-fly alter conversions.
    rest_reader->performRequiredConversions(rest_columns);

    /// Evaluate defaults for missing columns.
    if (should_evaluate_missing_defaults)
    {
        Block additional_columns = getInputPort().getHeader().cloneWithColumns(chunk.getColumns());
        rest_reader->evaluateMissingDefaults(additional_columns, rest_columns);
    }

    /// Replace the placeholder default columns in the chunk with real rest data.
    const auto & output_header = getOutputPort().getHeader();
    const auto & rest_sample_block = rest_range_reader->getReadSampleBlock();
    auto all_columns = chunk.detachColumns();

    for (size_t i = 0; i < rest_sample_block.columns(); ++i)
    {
        const auto & col_name = rest_sample_block.getByPosition(i).name;
        if (output_header.has(col_name))
        {
            size_t output_idx = output_header.getPositionByName(col_name);
            all_columns[output_idx] = std::move(rest_columns[i]);
        }
    }

    chunk.setColumns(std::move(all_columns), read_result.num_rows);
}

}
