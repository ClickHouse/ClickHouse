#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

String MergeTreeReadTask::Columns::dump() const
{
    WriteBufferFromOwnString s;
    for (size_t i = 0; i < pre_columns.size(); ++i)
    {
        s << "STEP " << i << ": " << pre_columns[i].toString() << "\n";
    }
    s << "COLUMNS: " << columns.toString() << "\n";
    return s.str();
}

MergeTreeReadTask::MergeTreeReadTask(
    InfoPtr info_,
    Readers readers_,
    MarkRanges mark_ranges_,
    MergeTreeBlockSizePredictorPtr size_predictor_)
    : info(std::move(info_))
    , readers(std::move(readers_))
    , mark_ranges(std::move(mark_ranges_))
    , size_predictor(std::move(size_predictor_))
{
}

MergeTreeReadTask::Readers MergeTreeReadTask::createReaders(
    const InfoPtr & read_info, const Extras & extras, const MarkRanges & ranges)
{
    Readers new_readers;

    auto create_reader = [&](const NamesAndTypesList & columns_to_read)
    {
        return read_info->data_part->getReader(
            columns_to_read, extras.storage_snapshot, ranges,
            extras.uncompressed_cache, extras.mark_cache,
            read_info->alter_conversions, extras.reader_settings, extras.value_size_map, extras.profile_callback);
    };

    new_readers.main = create_reader(read_info->task_columns.columns);

    /// Add lightweight delete filtering step
    if (extras.reader_settings.apply_deleted_mask && read_info->data_part->hasLightweightDelete())
        new_readers.prewhere.push_back(create_reader({LightweightDeleteDescription::FILTER_COLUMN}));

    for (const auto & pre_columns_per_step : read_info->task_columns.pre_columns)
        new_readers.prewhere.push_back(create_reader(pre_columns_per_step));

    return new_readers;
}

MergeTreeReadTask::RangeReaders MergeTreeReadTask::createRangeReaders(
    const Readers & task_readers,
    const PrewhereExprInfo & prewhere_actions,
    const Names & non_const_virtual_column_names)
{
    MergeTreeReadTask::RangeReaders new_range_readers;
    if (prewhere_actions.steps.size() != task_readers.prewhere.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "PREWHERE steps count mismatch, actions: {}, readers: {}",
            prewhere_actions.steps.size(), task_readers.prewhere.size());

    MergeTreeRangeReader * prev_reader = nullptr;
    bool last_reader = false;

    for (size_t i = 0; i < prewhere_actions.steps.size(); ++i)
    {
        last_reader = task_readers.main->getColumns().empty() && (i + 1 == prewhere_actions.steps.size());

        MergeTreeRangeReader current_reader(
            task_readers.prewhere[i].get(),
            prev_reader, prewhere_actions.steps[i].get(),
            last_reader, non_const_virtual_column_names);

        new_range_readers.prewhere.push_back(std::move(current_reader));
        prev_reader = &new_range_readers.prewhere.back();
    }

    if (!last_reader)
    {
        new_range_readers.main = MergeTreeRangeReader(task_readers.main.get(), prev_reader, nullptr, true, non_const_virtual_column_names);
    }
    else
    {
        /// If all columns are read by prewhere range readers than move last prewhere range reader to main.
        new_range_readers.main = std::move(new_range_readers.prewhere.back());
        new_range_readers.prewhere.pop_back();
    }

    return new_range_readers;
}

void MergeTreeReadTask::initializeRangeReaders(
    const PrewhereExprInfo & prewhere_actions,
    const Names & non_const_virtual_column_names)
{
    if (range_readers.main.isInitialized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Range reader is already initialized");

    range_readers = createRangeReaders(readers, prewhere_actions, non_const_virtual_column_names);
}

UInt64 MergeTreeReadTask::estimateNumRows(const BlockSizeParams & params) const
{
    if (!size_predictor)
    {
        if (params.preferred_block_size_bytes)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Size predictor is not set, it might lead to a performance degradation");
        return static_cast<size_t>(params.max_block_size_rows);
    }

    /// Calculates number of rows will be read using preferred_block_size_bytes.
    /// Can't be less than avg_index_granularity.
    size_t rows_to_read = size_predictor->estimateNumRows(params.preferred_block_size_bytes);
    if (!rows_to_read)
        return rows_to_read;

    auto total_row_in_current_granule = range_readers.main.numRowsInCurrentGranule();
    rows_to_read = std::max(total_row_in_current_granule, rows_to_read);

    if (params.preferred_max_column_in_block_size_bytes)
    {
        /// Calculates number of rows will be read using preferred_max_column_in_block_size_bytes.
        auto rows_to_read_for_max_size_column = size_predictor->estimateNumRowsForMaxSizeColumn(params.preferred_max_column_in_block_size_bytes);

        double filtration_ratio = std::max(params.min_filtration_ratio, 1.0 - size_predictor->filtered_rows_ratio);
        auto rows_to_read_for_max_size_column_with_filtration
            = static_cast<size_t>(rows_to_read_for_max_size_column / filtration_ratio);

        /// If preferred_max_column_in_block_size_bytes is used, number of rows to read can be less than current_index_granularity.
        rows_to_read = std::min(rows_to_read, rows_to_read_for_max_size_column_with_filtration);
    }

    auto unread_rows_in_current_granule = range_readers.main.numPendingRowsInCurrentGranule();
    if (unread_rows_in_current_granule >= rows_to_read)
        return rows_to_read;

    const auto & index_granularity = info->data_part->index_granularity;
    return index_granularity.countMarksForRows(range_readers.main.currentMark(), rows_to_read, range_readers.main.numReadRowsInCurrentGranule(), params.min_marks_to_read);
}

MergeTreeReadTask::BlockAndProgress MergeTreeReadTask::read(const BlockSizeParams & params)
{
    if (size_predictor)
        size_predictor->startBlock();

    UInt64 recommended_rows = estimateNumRows(params);
    UInt64 rows_to_read = std::max(static_cast<UInt64>(1), std::min(params.max_block_size_rows, recommended_rows));

    auto read_result = range_readers.main.read(rows_to_read, mark_ranges);

    /// All rows were filtered. Repeat.
    if (read_result.num_rows == 0)
        read_result.columns.clear();

    const auto & sample_block = range_readers.main.getSampleBlock();
    if (read_result.num_rows != 0 && sample_block.columns() != read_result.columns.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Inconsistent number of columns got from MergeTreeRangeReader. "
                        "Have {} in sample block and {} columns in list",
                        toString(sample_block.columns()), toString(read_result.columns.size()));

    /// TODO: check columns have the same types as in header.
    UInt64 num_filtered_rows = read_result.numReadRows() - read_result.num_rows;

    size_t num_read_rows = read_result.numReadRows();
    size_t num_read_bytes = read_result.numBytesRead();

    if (size_predictor)
    {
        size_predictor->updateFilteredRowsRation(read_result.numReadRows(), num_filtered_rows);
        if (!read_result.columns.empty())
            size_predictor->update(sample_block, read_result.columns, read_result.num_rows);
    }

    Block block;
    if (read_result.num_rows != 0)
        block = sample_block.cloneWithColumns(read_result.columns);

    BlockAndProgress res = {
        .block = std::move(block),
        .row_count = read_result.num_rows,
        .num_read_rows = num_read_rows,
        .num_read_bytes = num_read_bytes };

    return res;
}

}
