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
    MergeTreeReadTask::BlockSizeParams block_size_params_,
    MergeTreeIndexBuildContextPtr index_build_context_)
    : ISource(std::make_shared<const Block>(std::move(prewhere_header)))
    , pool(std::move(pool_))
    , task_idx(task_idx_)
    , prewhere_actions(std::move(prewhere_actions_))
    , block_size_params(block_size_params_)
    , index_build_context(std::move(index_build_context_))
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

    /// Initialize prepared skip/projection index before releasing readers,
    /// while the task still owns its readers and mark ranges.
    if (index_build_context)
        task->initializeIndexReader(index_build_context, nullptr);

    current_task_info = task->getInfoPtr();
    current_readers = task->releaseReaders();
    mark_ranges = std::move(task->getMarkRanges());
    patches_mark_ranges = current_readers.patches_prewhere_ranges;

    /// Combine mutation steps (per-part on-fly mutations) with query prewhere steps.
    PrewhereExprInfo all_prewhere_actions;
    for (const auto & step : current_task_info->mutation_steps)
        all_prewhere_actions.steps.push_back(step);
    for (const auto & step : prewhere_actions.steps)
        all_prewhere_actions.steps.push_back(step);

    prewhere_chain = MergeTreeReadTask::createPrewhereReadersChain(
        current_readers, all_prewhere_actions, read_steps_performance_counters);

    /// Build the shared task context for the downstream RestColumnsTransform.
    /// The prewhere chain consumes `patches_mark_ranges` during read(), so the
    /// rest transform needs its own copy via the task context.
    current_task_context = std::make_shared<MergeTreeReadTaskContext>();
    current_task_context->task_info = current_task_info;
    current_task_context->prewhere_sample_block = prewhere_chain.getSampleBlock();

    if (current_readers.main)
        current_task_context->rest_reader = std::move(current_readers.main);

    /// Filter out patch readers with empty column lists -- these arise when
    /// `splitPatchColumnsForPrewhere` moved all columns to prewhere, leaving
    /// the rest reader with nothing to read.
    auto rest_patches_mark_ranges = std::move(task->getPatchesMarkRanges());
    for (size_t i = 0; i < current_readers.patches.size(); ++i)
    {
        if (current_readers.patches[i]->getReader()->getColumns().empty())
            continue;
        current_task_context->rest_patches.push_back(std::move(current_readers.patches[i]));
        current_task_context->patches_mark_ranges.push_back(std::move(rest_patches_mark_ranges[i]));
    }
    current_readers.patches.clear();

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

        /// Build chunk: prewhere columns come from the read result,
        /// rest columns are filled with type defaults (replaced by RestColumnsTransform).
        const auto & output_header = getPort().getHeader();
        Columns ordered_columns;
        ordered_columns.reserve(output_header.columns());

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
                ordered_columns.push_back(
                    output_header.getByPosition(i).type->createColumnConstWithDefaultValue(read_result.num_rows));
            }
        }

        auto chunk = Chunk(std::move(ordered_columns), read_result.num_rows);

        /// Attach chunk info for RestColumnsTransform.
        auto chunk_info = std::make_shared<MergeTreeReadChunkInfo>();
        chunk_info->task_context = current_task_context;
        chunk_info->read_result = std::make_shared<MergeTreeRangeReader::ReadResult>(std::move(read_result));
        chunk_info->skipped_read_results = std::move(skipped_read_results);
        chunk_info->remaining_mark_ranges = mark_ranges;
        chunk.getChunkInfos().add(std::move(chunk_info));

        return chunk;
    }
}

}
