#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Common/Exception.h>
#include <IO/Operators.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

String MergeTreeReadTaskColumns::dump() const
{
    WriteBufferFromOwnString s;
    for (size_t i = 0; i < pre_columns.size(); ++i)
    {
        s << "STEP " << i << ": " << pre_columns[i].toString() << "\n";
    }
    s << "COLUMNS: " << columns.toString() << "\n";
    return s.str();
}

void MergeTreeReadTaskColumns::moveAllColumnsFromPrewhere()
{
    for (auto & step_columns : pre_columns)
        columns.splice(columns.end(), std::move(step_columns));

    pre_columns.clear();
}

bool MergeTreeReadTaskInfo::hasLightweightDelete() const
{
    return data_part->hasLightweightDelete();
}

MergeTreeReadTask::MergeTreeReadTask(
    MergeTreeReadTaskInfoPtr info_,
    Readers readers_,
    MarkRanges mark_ranges_,
    const BlockSizeParams & block_size_params_,
    MergeTreeBlockSizePredictorPtr size_predictor_)
    : info(std::move(info_))
    , readers(std::move(readers_))
    , mark_ranges(std::move(mark_ranges_))
    , block_size_params(block_size_params_)
    , size_predictor(std::move(size_predictor_))
{
}

MergeTreeReadTask::Readers MergeTreeReadTask::createReaders(
    const MergeTreeReadTaskInfoPtr & read_info, const Extras & extras, const MarkRanges & ranges)
{
    Readers new_readers;

    auto create_reader = [&](const NamesAndTypesList & columns_to_read, bool is_prewhere)
    {
        auto part_info = std::make_shared<LoadedMergeTreeDataPartInfoForReader>(read_info->data_part, read_info->alter_conversions);

        return createMergeTreeReader(
            part_info,
            columns_to_read,
            extras.storage_snapshot,
            ranges,
            read_info->const_virtual_fields,
            extras.uncompressed_cache,
            extras.mark_cache,
            is_prewhere ? nullptr : read_info->deserialization_prefixes_cache.get(),
            extras.reader_settings,
            extras.value_size_map,
            extras.profile_callback);
    };

    new_readers.main = create_reader(read_info->task_columns.columns, false);

    for (const auto & pre_columns_per_step : read_info->task_columns.pre_columns)
        new_readers.prewhere.push_back(create_reader(pre_columns_per_step, true));

    return new_readers;
}

MergeTreeReadersChain MergeTreeReadTask::createReadersChain(const Readers & task_readers, const PrewhereExprInfo & prewhere_actions, ReadStepsPerformanceCounters & read_steps_performance_counters)
{
    if (prewhere_actions.steps.size() != task_readers.prewhere.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "PREWHERE steps count mismatch, actions: {}, readers: {}",
            prewhere_actions.steps.size(), task_readers.prewhere.size());

    std::vector<MergeTreeRangeReader> range_readers;
    range_readers.reserve(prewhere_actions.steps.size() + 1);

    for (size_t i = 0; i < prewhere_actions.steps.size(); ++i)
    {
        range_readers.emplace_back(
            task_readers.prewhere[i].get(),
            (i == 0) ? Block{} : range_readers.back().getSampleBlock(),
            prewhere_actions.steps[i].get(),
            read_steps_performance_counters.getCountersForStep(i),
            /*main_reader_=*/ false);
    }

    if (!task_readers.main->getColumns().empty())
    {
        range_readers.emplace_back(
            task_readers.main.get(),
            range_readers.empty() ? Block{} : range_readers.back().getSampleBlock(),
            /*prewhere_info_=*/ nullptr,
            read_steps_performance_counters.getCountersForStep(range_readers.size()),
            /*main_reader_=*/ true);
    }

    return MergeTreeReadersChain{std::move(range_readers)};
}

void MergeTreeReadTask::initializeReadersChain(const PrewhereExprInfo & prewhere_actions, ReadStepsPerformanceCounters & read_steps_performance_counters)
{
    if (readers_chain.isInitialized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Range readers chain is already initialized");

    readers_chain = createReadersChain(readers, prewhere_actions, read_steps_performance_counters);
}

UInt64 MergeTreeReadTask::estimateNumRows() const
{
    if (!size_predictor)
    {
        if (block_size_params.preferred_block_size_bytes)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Size predictor is not set, it might lead to a performance degradation");
        return static_cast<size_t>(block_size_params.max_block_size_rows);
    }

    /// Calculates number of rows will be read using preferred_block_size_bytes.
    /// Can't be less than avg_index_granularity.
    size_t rows_to_read = size_predictor->estimateNumRows(block_size_params.preferred_block_size_bytes);
    if (!rows_to_read)
        return rows_to_read;

    auto total_row_in_current_granule = readers_chain.numRowsInCurrentGranule();
    rows_to_read = std::max(total_row_in_current_granule, rows_to_read);

    if (block_size_params.preferred_max_column_in_block_size_bytes)
    {
        /// Calculates number of rows will be read using preferred_max_column_in_block_size_bytes.
        auto rows_to_read_for_max_size_column
            = size_predictor->estimateNumRowsForMaxSizeColumn(block_size_params.preferred_max_column_in_block_size_bytes);

        double filtration_ratio = std::max(block_size_params.min_filtration_ratio, 1.0 - size_predictor->filtered_rows_ratio);
        auto rows_to_read_for_max_size_column_with_filtration
            = static_cast<size_t>(rows_to_read_for_max_size_column / filtration_ratio);

        /// If preferred_max_column_in_block_size_bytes is used, number of rows to read can be less than current_index_granularity.
        rows_to_read = std::min(rows_to_read, rows_to_read_for_max_size_column_with_filtration);
    }

    auto unread_rows_in_current_granule = readers_chain.numPendingRowsInCurrentGranule();
    if (unread_rows_in_current_granule >= rows_to_read)
        return rows_to_read;

    const auto & index_granularity = info->data_part->index_granularity;
    return index_granularity->countRowsForRows(readers_chain.currentMark(), rows_to_read, readers_chain.numReadRowsInCurrentGranule());
}

MergeTreeReadTask::BlockAndProgress MergeTreeReadTask::read()
{
    if (size_predictor)
        size_predictor->startBlock();

    UInt64 recommended_rows = estimateNumRows();
    UInt64 rows_to_read = std::max(static_cast<UInt64>(1), std::min(block_size_params.max_block_size_rows, recommended_rows));

    auto read_result = readers_chain.read(rows_to_read, mark_ranges);

    /// All rows were filtered. Repeat.
    if (read_result.num_rows == 0)
        read_result.columns.clear();

    const auto & sample_block = readers_chain.getSampleBlock();
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
    {
        for (const auto & column : read_result.columns)
            column->assumeMutableRef().shrinkToFit();
        block = sample_block.cloneWithColumns(read_result.columns);
    }

    BlockAndProgress res = {
        .block = std::move(block),
        .read_mark_ranges = read_result.read_mark_ranges,
        .row_count = read_result.num_rows,
        .num_read_rows = num_read_rows,
        .num_read_bytes = num_read_bytes };

    return res;
}

void MergeTreeReadTask::addPrewhereUnmatchedMarks(const MarkRanges & mark_ranges_)
{
    prewhere_unmatched_marks.insert(prewhere_unmatched_marks.end(), mark_ranges_.begin(), mark_ranges_.end());
}

}
