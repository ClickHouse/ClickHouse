#include <Storages/MergeTree/MergeTreePrewhereSource.h>
#include <Storages/MergeTree/MergeTreeReadChunkInfo.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Interpreters/ExpressionActions.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreePrewhereSource::MergeTreePrewhereSource(
    Block prewhere_header,
    MergeTreeReadPoolPtr pool_,
    size_t task_idx_,
    PrewhereExprInfo prewhere_actions_,
    MergeTreeReadTask::BlockSizeParams block_size_params_)
    : ISource(std::make_shared<const Block>(std::move(prewhere_header)))
    , pool(std::move(pool_))
    , task_idx(task_idx_)
    , prewhere_actions(std::move(prewhere_actions_))
    , block_size_params(block_size_params_)
{
}

Block MergeTreePrewhereSource::computePrewhereHeader(
    const Block & result_header,
    const Block & pool_header,
    const PrewhereExprInfo & prewhere_actions)
{
    /// Find columns that are required by any prewhere step.
    NameSet prewhere_required;
    for (const auto & step : prewhere_actions.steps)
    {
        if (step->actions)
        {
            for (const auto & name : step->actions->getRequiredColumns())
                prewhere_required.insert(name);
        }
    }

    /// Rest columns = pool header columns not required by any prewhere step.
    NameSet rest_columns;
    for (const auto & col : pool_header)
    {
        if (!prewhere_required.contains(col.name))
            rest_columns.insert(col.name);
    }

    /// Prewhere header = result header minus rest columns.
    Block prewhere_header;
    for (const auto & col : result_header)
    {
        if (!rest_columns.contains(col.name))
            prewhere_header.insert(col);
    }

    return prewhere_header;
}

bool MergeTreePrewhereSource::getNewTask()
{
    /// Discard accumulated skipped results from the previous task — they belong
    /// to the old rest reader which will be replaced by the new task's reader.
    skipped_read_results.clear();

    auto task = pool->getTask(task_idx, nullptr);
    if (!task)
        return false;

    current_task_info = task->getInfoPtr();
    current_readers = task->releaseReaders();
    mark_ranges = std::move(task->getMarkRanges());
    /// The full mark ranges go to the rest transform (aligned with patches vector
    /// which has entries for ALL patch parts). The prewhere chain uses
    /// patches_prewhere_ranges which is aligned with the patches_prewhere vector
    /// (may skip patch parts with no prewhere columns). These must be separate
    /// copies because readPatches consumes mark range entries in-place.
    rest_patches_mark_ranges = std::move(task->getPatchesMarkRanges());
    patches_mark_ranges = current_readers.patches_prewhere_ranges;

    /// Combine mutation steps (per-part on-fly mutations) with query prewhere steps.
    PrewhereExprInfo all_prewhere_actions;
    for (const auto & step : current_task_info->mutation_steps)
        all_prewhere_actions.steps.push_back(step);
    for (const auto & step : prewhere_actions.steps)
        all_prewhere_actions.steps.push_back(step);

    prewhere_chain = MergeTreeReadTask::createPrewhereReadersChain(
        current_readers, all_prewhere_actions, read_steps_performance_counters);

    need_new_task = false;
    return true;
}

std::optional<IProcessor::ReadProgress> MergeTreePrewhereSource::getReadProgress()
{
    /// Drain any bytes reported by the downstream RestColumnsTransform
    /// and fold them into our progress counters before the base class
    /// swaps them out. This ensures rest-column bytes appear in the
    /// query statistics reported to the client.
    size_t extra_bytes = rest_bytes_counter->exchange(0);
    if (extra_bytes > 0)
        progress(0, extra_bytes);

    return ISource::getReadProgress();
}

std::optional<Chunk> MergeTreePrewhereSource::tryGenerate()
{
    while (true)
    {
        if (need_new_task)
        {
            if (!getNewTask())
                return {}; /// No more tasks — finished.
        }

        if (mark_ranges.empty() && prewhere_chain.isCurrentRangeFinished())
        {
            need_new_task = true;
            continue;
        }

        /// Use a simple row count estimation for the first version.
        UInt64 rows_to_read = block_size_params.max_block_size_rows;

        auto read_result = prewhere_chain.read(rows_to_read, mark_ranges, patches_mark_ranges);

        if (read_result.num_rows == 0)
        {
            /// All rows were filtered by PREWHERE. Report progress, accumulate the
            /// ReadResult for the rest transform (so it can advance its stream past
            /// these filtered granules), and continue reading.
            progress(read_result.numReadRows(), read_result.numBytesRead());
            skipped_read_results.push_back(
                std::make_shared<MergeTreeRangeReader::ReadResult>(std::move(read_result)));
            continue;
        }

        const auto & sample_block = prewhere_chain.getSampleBlock();
        if (sample_block.columns() != read_result.columns.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Inconsistent number of columns from prewhere chain: expected {}, got {}",
                sample_block.columns(), read_result.columns.size());

        progress(read_result.numReadRows(), read_result.numBytesRead());

        /// Build chunk from prewhere columns.
        Columns ordered_columns;
        ordered_columns.reserve(getPort().getHeader().columns());

        const auto & output_header = getPort().getHeader();
        for (size_t i = 0; i < output_header.columns(); ++i)
        {
            const auto & col_name = output_header.getByPosition(i).name;
            if (sample_block.has(col_name))
            {
                size_t prewhere_idx = sample_block.getPositionByName(col_name);
                ordered_columns.push_back(read_result.columns[prewhere_idx]);
            }
            else
            {
                /// Rest column — fill with defaults. RestColumnsTransform will replace these.
                ordered_columns.push_back(
                    output_header.getByPosition(i).type->createColumnConstWithDefaultValue(read_result.num_rows));
            }
        }

        auto chunk = Chunk(std::move(ordered_columns), read_result.num_rows);

        /// Attach chunk info for RestColumnsTransform.
        auto chunk_info = std::make_shared<MergeTreeReadChunkInfo>();
        chunk_info->read_result = std::make_shared<MergeTreeRangeReader::ReadResult>(std::move(read_result));
        chunk_info->task_info = current_task_info;

        /// Pass the main reader on the first chunk of each task.
        /// RestColumnsTransform takes ownership and uses it for all subsequent chunks.
        if (current_readers.main)
            chunk_info->rest_reader = std::move(current_readers.main);

        /// Pass rest patch readers and their mark ranges on the first chunk of each task.
        /// Use rest_patches_mark_ranges (the unconsumed copy) because the prewhere chain
        /// has already consumed entries from patches_mark_ranges via readPatches.
        /// Filter out patch readers with empty column lists — these arise when
        /// `splitPatchColumnsForPrewhere` moved all columns to prewhere, leaving
        /// the rest reader with nothing to read.
        if (!current_readers.patches.empty())
        {
            MergeTreePatchReaders non_empty_patches;
            std::vector<MarkRanges> non_empty_ranges;
            for (size_t i = 0; i < current_readers.patches.size(); ++i)
            {
                if (current_readers.patches[i]->getReader()->getColumns().empty())
                    continue;
                non_empty_patches.push_back(std::move(current_readers.patches[i]));
                non_empty_ranges.push_back(std::move(rest_patches_mark_ranges[i]));
            }
            /// Clear moved-from vectors so subsequent chunks of the same task
            /// do not re-enter this block and dereference null shared_ptrs.
            current_readers.patches.clear();
            rest_patches_mark_ranges.clear();

            if (!non_empty_patches.empty())
            {
                chunk_info->rest_patches = std::move(non_empty_patches);
                chunk_info->patches_mark_ranges = std::move(non_empty_ranges);
            }
        }

        chunk_info->skipped_read_results = std::move(skipped_read_results);
        chunk_info->remaining_mark_ranges = mark_ranges;
        chunk.getChunkInfos().add(std::move(chunk_info));

        return chunk;
    }
}

}
