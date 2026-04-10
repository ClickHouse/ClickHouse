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
    size_t thread_idx_,
    PrewhereExprInfo prewhere_actions_,
    MergeTreeReadTask::BlockSizeParams block_size_params_)
    : ISource(std::make_shared<const Block>(std::move(prewhere_header)))
    , pool(std::move(pool_))
    , thread_idx(thread_idx_)
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
    auto task = pool->getTask(thread_idx, nullptr);
    if (!task)
        return false;

    current_task_info = task->getInfoPtr();
    current_readers = task->releaseReaders();
    mark_ranges = std::move(task->getMarkRanges());
    patches_mark_ranges = std::move(task->getPatchesMarkRanges());

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
            /// All rows were filtered by PREWHERE. Report progress and continue.
            progress(read_result.numReadRows(), read_result.numBytesRead());
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

        chunk_info->remaining_mark_ranges = mark_ranges;
        chunk.getChunkInfos().add(std::move(chunk_info));

        return chunk;
    }
}

}
