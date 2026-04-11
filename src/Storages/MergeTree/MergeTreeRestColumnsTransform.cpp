#include <Storages/MergeTree/MergeTreeRestColumnsTransform.h>
#include <Storages/MergeTree/MergeTreeReadChunkInfo.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/PatchParts/applyPatchesUtils.h>
#include <Core/Block.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeRestColumnsTransform::MergeTreeRestColumnsTransform(
    Block input_header_,
    Block output_header_,
    std::shared_ptr<std::atomic<size_t>> rest_bytes_counter_)
    : ISimpleTransform(std::move(input_header_), std::move(output_header_), /*skip_empty_chunks_=*/ false)
    , rest_bytes_counter(std::move(rest_bytes_counter_))
{
}

void MergeTreeRestColumnsTransform::acquireReadersFromChunkInfo(const MergeTreeReadChunkInfo & chunk_info)
{
    if (chunk_info.task_context == current_task_context)
        return;

    current_task_context = chunk_info.task_context;
    rest_range_reader.reset();

    /// Copy patch mark ranges from the task context -- we consume them in-place during readPatches.
    if (!current_task_context->rest_patches.empty())
    {
        rest_patches_results.assign(current_task_context->rest_patches.size(), {});
        rest_patches_mark_ranges = current_task_context->patches_mark_ranges;
    }
    else
    {
        rest_patches_results.clear();
        rest_patches_mark_ranges.clear();
    }
}

void MergeTreeRestColumnsTransform::ensureRangeReaderInitialized()
{
    if (rest_range_reader)
        return;

    const auto & rest_reader = current_task_context->rest_reader;

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

void MergeTreeRestColumnsTransform::advancePastSkippedResults(MergeTreeReadChunkInfo & chunk_info)
{
    for (auto & skipped_result : chunk_info.skipped_read_results)
    {
        size_t bytes_before = skipped_result->numBytesRead();
        size_t skipped_rows = 0;
        rest_range_reader->continueReadingChain(*skipped_result, skipped_rows);
        rest_bytes_counter->fetch_add(skipped_result->numBytesRead() - bytes_before);
    }
}

Columns MergeTreeRestColumnsTransform::readAndFilterRestColumns(
    ReadResult & read_result, size_t & num_read_rows, bool & should_evaluate_missing_defaults)
{
    const auto & rest_reader = current_task_context->rest_reader;

    size_t bytes_before = read_result.numBytesRead();
    num_read_rows = 0;
    auto rest_columns = rest_range_reader->continueReadingChain(read_result, num_read_rows);
    rest_bytes_counter->fetch_add(read_result.numBytesRead() - bytes_before);

    if (num_read_rows == 0)
        num_read_rows = read_result.num_rows;

    rest_reader->fillVirtualColumns(rest_columns, num_read_rows);

    should_evaluate_missing_defaults = false;
    rest_reader->fillMissingColumns(rest_columns, should_evaluate_missing_defaults, num_read_rows);

    /// Apply the PREWHERE filter to newly read rest columns.
    /// `continueReadingChain` reads `total_rows_per_granule` rows,
    /// but we need `num_rows` (the filtered count).
    const auto & final_filter = read_result.getFinalFilter();
    if (read_result.num_rows != num_read_rows && final_filter.present())
        MergeTreeRangeReader::filterColumns(rest_columns, final_filter);

    return rest_columns;
}

void MergeTreeRestColumnsTransform::applyPatchesAndConversions(
    ReadResult & read_result,
    const MergeTreeReadChunkInfo & chunk_info,
    Columns & rest_columns,
    bool should_evaluate_missing_defaults,
    const Chunk & chunk)
{
    const auto & rest_reader = current_task_context->rest_reader;
    const auto & rest_patch_readers = current_task_context->rest_patches;
    const bool has_rest_patches = !rest_patch_readers.empty();

    ColumnsForPatches rest_cfp;
    if (has_rest_patches)
    {
        const auto & rest_sample_block = rest_range_reader->getReadSampleBlock();

        /// readPatches needs a header that matches read_result.columns (prewhere chain output),
        /// not the rest reader's sample block, because PatchReaderJoin::readPatches does
        /// result_header.cloneWithColumns(main_result.columns) for position matching.
        const auto & patches_header = chunk_info.task_context->prewhere_sample_block;
        for (size_t i = 0; i < rest_patch_readers.size(); ++i)
        {
            auto & patch_results = rest_patches_results[i];

            while (!patch_results.empty()
                   && !rest_patch_readers[i]->needOldPatch(read_result, *patch_results.front()))
                patch_results.pop_front();

            const auto * last = patch_results.empty() ? nullptr : patch_results.back().get();
            auto new_patches = rest_patch_readers[i]->readPatches(
                rest_patches_mark_ranges[i], read_result, patches_header, last);
            patch_results.insert(patch_results.end(), new_patches.begin(), new_patches.end());
        }

        rest_cfp = getColumnsForPatches(rest_sample_block, rest_columns, rest_patch_readers);
    }

    auto apply_rest_patches = [&](ColumnForPatch::Order order)
    {
        if (!has_rest_patches)
            return;

        const auto & rest_sample_block = rest_range_reader->getReadSampleBlock();
        Block versions_block;
        applyPatchesToColumns(
            rest_patch_readers,
            rest_patches_results,
            rest_sample_block,
            rest_columns,
            versions_block,
            /*min_version=*/ {},
            /*max_version=*/ {},
            rest_cfp,
            {order},
            read_result.getColumnsForPatches());
    };

    apply_rest_patches(ColumnForPatch::Order::BeforeConversions);

    rest_reader->performRequiredConversions(rest_columns);

    apply_rest_patches(ColumnForPatch::Order::AfterConversions);

    if (should_evaluate_missing_defaults)
    {
        NameSet rest_column_names;
        for (const auto & col : rest_reader->getColumns())
            rest_column_names.insert(col.name);

        const auto & input_header = getInputPort().getHeader();
        Block additional_columns;
        const auto & chunk_columns = chunk.getColumns();
        for (size_t i = 0; i < input_header.columns(); ++i)
        {
            const auto & col = input_header.getByPosition(i);
            if (!rest_column_names.contains(col.name))
                additional_columns.insert({chunk_columns[i], col.type, col.name});
        }

        /// Include columns that were projected out by prewhere execution.
        for (const auto & col : read_result.getAdditionalColumns())
        {
            if (!additional_columns.has(col.name))
                additional_columns.insert(col);
        }

        addDummyColumnWithRowCount(additional_columns, read_result.num_rows);
        rest_reader->evaluateMissingDefaults(additional_columns, rest_columns);
    }

    apply_rest_patches(ColumnForPatch::Order::AfterEvaluatingDefaults);
}

void MergeTreeRestColumnsTransform::assembleOutputBlock(Chunk & chunk, Columns & rest_columns, size_t num_rows)
{
    const auto & input_header = getInputPort().getHeader();
    const auto & output_header = getOutputPort().getHeader();

    auto prewhere_columns = chunk.detachColumns();
    Columns all_columns(output_header.columns());

    for (size_t i = 0; i < input_header.columns(); ++i)
    {
        const auto & col_name = input_header.getByPosition(i).name;
        if (output_header.has(col_name))
            all_columns[output_header.getPositionByName(col_name)] = std::move(prewhere_columns[i]);
    }

    if (rest_range_reader)
    {
        const auto & rest_sample_block = rest_range_reader->getReadSampleBlock();
        for (size_t i = 0; i < rest_sample_block.columns(); ++i)
        {
            const auto & col_name = rest_sample_block.getByPosition(i).name;
            if (output_header.has(col_name))
                all_columns[output_header.getPositionByName(col_name)] = std::move(rest_columns[i]);
        }
    }

    chunk.setColumns(std::move(all_columns), num_rows);
}

void MergeTreeRestColumnsTransform::transform(Chunk & chunk)
{
    auto read_chunk_info = chunk.getChunkInfos().get<MergeTreeReadChunkInfo>();
    if (!read_chunk_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeRestColumnsTransform: missing MergeTreeReadChunkInfo");

    if (!read_chunk_info->read_result)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeRestColumnsTransform: missing ReadResult in chunk info");

    acquireReadersFromChunkInfo(*read_chunk_info);

    auto & read_result = *read_chunk_info->read_result;
    bool has_rest_columns = current_task_context
        && current_task_context->rest_reader
        && !current_task_context->rest_reader->getColumns().empty();

    Columns rest_columns;
    if (has_rest_columns)
    {
        ensureRangeReaderInitialized();
        advancePastSkippedResults(*read_chunk_info);

        size_t num_read_rows = 0;
        bool should_evaluate_missing_defaults = false;
        rest_columns = readAndFilterRestColumns(read_result, num_read_rows, should_evaluate_missing_defaults);

        applyPatchesAndConversions(
            read_result, *read_chunk_info, rest_columns,
            should_evaluate_missing_defaults, chunk);
    }

    assembleOutputBlock(chunk, rest_columns, read_result.num_rows);
}

}
