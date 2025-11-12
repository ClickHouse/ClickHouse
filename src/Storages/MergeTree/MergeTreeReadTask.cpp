#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeReaderIndex.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeReaderTextIndex.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/PatchParts/MergeTreePatchReader.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
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
        s << "STEP " << i << ":\n" << pre_columns[i].toString() << "\n";

    s << "MAIN:\n" << columns.toString() << "\n";

    for (size_t i = 0; i < patch_columns.size(); ++i)
        s << "PATCH " << i << ":\n" << patch_columns[i].toString() << "\n";

    return s.str();
}

Names MergeTreeReadTaskColumns::getAllColumnNames() const
{
    Names res;
    for (const auto & step_columns : pre_columns)
    {
        for (const auto & column : step_columns)
            res.push_back(column.name);
    }

    for (const auto & column : columns)
        res.push_back(column.name);

    return res;
}

void MergeTreeReadTaskColumns::moveAllColumnsFromPrewhere()
{
    for (auto & step_columns : pre_columns)
        columns.splice(columns.end(), std::move(step_columns));

    pre_columns.clear();
}

void MergeTreeReadTask::Readers::updateAllMarkRanges(const MarkRanges & ranges)
{
    main->updateAllMarkRanges(ranges);

    for (auto & reader : prewhere)
        reader->updateAllMarkRanges(ranges);
}

MergeTreeReadTask::MergeTreeReadTask(
    MergeTreeReadTaskInfoPtr info_,
    Readers readers_,
    MarkRanges mark_ranges_,
    std::vector<MarkRanges> patches_mark_ranges_,
    const BlockSizeParams & block_size_params_,
    MergeTreeBlockSizePredictorPtr size_predictor_)
    : info(std::move(info_))
    , readers(std::move(readers_))
    , mark_ranges(std::move(mark_ranges_))
    , patches_mark_ranges(std::move(patches_mark_ranges_))
    , block_size_params(block_size_params_)
    , size_predictor(std::move(size_predictor_))
{
}

/// Returns pointer to the index if all columns in the read step belongs to the read step for that index.
static const MergeTreeIndexWithCondition * getIndexForReadStep(const IndexReadTasks & index_read_tasks, const NamesAndTypesList & columns_to_read)
{
    if (index_read_tasks.empty())
        return nullptr;

    std::unordered_map<String, String> column_to_index;

    for (const auto & [index_name, index_task] : index_read_tasks)
    {
        for (const auto & column : index_task.columns)
            column_to_index[column.name] = index_name;
    }

    String index_for_step;
    String non_index_column;

    for (const auto & column : columns_to_read)
    {
        auto it = column_to_index.find(column.name);

        if (it == column_to_index.end())
        {
            non_index_column = column.name;
        }
        else if (index_for_step.empty())
        {
            index_for_step = it->second;
        }
        else if (index_for_step != it->second)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Found columns for multiple indexes ({} and {}) in one read step", index_for_step, it->second);
        }
    }

    if (!index_for_step.empty() && !non_index_column.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Found non-index column {} in read step for index {}", non_index_column, index_for_step);

    return index_for_step.empty() ? nullptr : &index_read_tasks.at(index_for_step).index;
}

MergeTreeReadTask::Readers MergeTreeReadTask::createReaders(
    const MergeTreeReadTaskInfoPtr & read_info,
    const Extras & extras,
    const MarkRanges & ranges,
    const std::vector<MarkRanges> & patches_ranges)
{
    Readers new_readers;

    auto create_reader = [&](const NamesAndTypesList & columns_to_read, bool is_prewhere)
    {
        auto part_info = std::make_shared<LoadedMergeTreeDataPartInfoForReader>(read_info->data_part, read_info->alter_conversions);

        return createMergeTreeReader(
            part_info,
            columns_to_read,
            extras.storage_snapshot,
            read_info->data_part->storage.getSettings(),
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

    bool is_vector_search = read_info->read_hints.vector_search_results.has_value();
    if (is_vector_search)
        new_readers.main->data_part_info_for_read->setReadHints(read_info->read_hints, read_info->task_columns.columns);

    for (const auto & pre_columns_per_step : read_info->task_columns.pre_columns)
    {
        if (const auto * index = getIndexForReadStep(read_info->index_read_tasks, pre_columns_per_step))
            new_readers.prewhere.push_back(createMergeTreeReaderIndex(new_readers.main.get(), *index, pre_columns_per_step));
        else
            new_readers.prewhere.push_back(create_reader(pre_columns_per_step, true));

        if (is_vector_search)
            new_readers.prewhere.back()->data_part_info_for_read->setReadHints(read_info->read_hints, pre_columns_per_step);
    }

    auto create_patch_reader = [&](size_t part_idx)
    {
        return createMergeTreeReader(
            read_info->patch_parts[part_idx].part,
            read_info->task_columns.patch_columns[part_idx],
            extras.storage_snapshot,
            read_info->data_part->storage.getSettings(),
            patches_ranges[part_idx],
            read_info->const_virtual_fields,
            extras.uncompressed_cache,
            extras.mark_cache,
            /*deserialization_prefixes_cache=*/ nullptr,
            extras.reader_settings,
            extras.value_size_map,
            extras.profile_callback);
    };

    for (size_t i = 0; i < read_info->patch_parts.size(); ++i)
    {
        new_readers.patches.push_back(getPatchReader(
            read_info->patch_parts[i],
            create_patch_reader(i),
            extras.patch_join_cache));
    }

    return new_readers;
}

MergeTreeReadersChain MergeTreeReadTask::createReadersChain(
    const Readers & task_readers,
    const PrewhereExprInfo & prewhere_actions,
    ReadStepsPerformanceCounters & read_steps_performance_counters)
{
    if (prewhere_actions.steps.size() != task_readers.prewhere.size())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "PREWHERE steps count mismatch, actions: {}, readers: {}",
            prewhere_actions.steps.size(), task_readers.prewhere.size());
    }

    std::vector<MergeTreeRangeReader> range_readers;

    size_t num_readers = prewhere_actions.steps.size() + task_readers.prewhere.size() + 1;
    range_readers.reserve(num_readers);

    if (task_readers.prepared_index)
    {
        range_readers.emplace_back(
            task_readers.prepared_index.get(),
            Block{},
            /*prewhere_info_=*/ nullptr,
            read_steps_performance_counters.getCounterForIndexStep(),
            /*main_reader_=*/ false);
    }

    size_t counter_idx = 0;
    for (size_t i = 0; i < prewhere_actions.steps.size(); ++i)
    {
        range_readers.emplace_back(
            task_readers.prewhere[i].get(),
            range_readers.empty() ? Block{} : range_readers.back().getSampleBlock(),
            prewhere_actions.steps[i].get(),
            read_steps_performance_counters.getCountersForStep(counter_idx++),
            /*main_reader_=*/ false);
    }

    if (!task_readers.main->getColumns().empty())
    {
        range_readers.emplace_back(
            task_readers.main.get(),
            range_readers.empty() ? Block{} : range_readers.back().getSampleBlock(),
            /*prewhere_info_=*/ nullptr,
            read_steps_performance_counters.getCountersForStep(counter_idx),
            /*main_reader_=*/ true);
    }

    return MergeTreeReadersChain{std::move(range_readers), task_readers.patches};
}

void MergeTreeReadTask::initializeReadersChain(
    const PrewhereExprInfo & prewhere_actions,
    MergeTreeIndexBuildContextPtr index_build_context,
    ReadStepsPerformanceCounters & read_steps_performance_counters)
{
    if (readers_chain.isInitialized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Range readers chain is already initialized");

    PrewhereExprInfo all_prewhere_actions;

    if (index_build_context)
        initializeIndexReader(*index_build_context);

    for (const auto & step : info->mutation_steps)
        all_prewhere_actions.steps.push_back(step);

    for (const auto & step : prewhere_actions.steps)
        all_prewhere_actions.steps.push_back(step);

    readers_chain = createReadersChain(readers, all_prewhere_actions, read_steps_performance_counters);
}

void MergeTreeReadTask::initializeIndexReader(const MergeTreeIndexBuildContext & index_build_context)
{
    /// Optionally initialize the index filter for the current read task. If the build context exists and contains
    /// relevant read ranges for the current part, retrieve or construct index filter for all involved skip indexes.
    /// This filter will later be used to filter granules during the first reading step.
    auto index_read_result = index_build_context.getPreparedIndexReadResult(*this);
    if (index_read_result)
        readers.prepared_index = std::make_unique<MergeTreeReaderIndex>(readers.main.get(), std::move(index_read_result));
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

    auto read_result = readers_chain.read(rows_to_read, mark_ranges, patches_mark_ranges);

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
        {
            /// We may have columns that has other references, usually it is a constant column that has been created during analysis
            /// (that will not be const here anymore, i.e. after materialize()), and we do not need to shrink it anyway.
            if (column->use_count() == 1)
                column->assumeMutableRef().shrinkToFit();
        }
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
