#include <cstddef>
#include <mutex>
#include <Processors/Transforms/CreatingSetsOnTheFlyTransform.h>
#include <Interpreters/Set.h>

#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Columns/IColumn.h>
#include <Core/ColumnWithTypeAndName.h>
#include <base/types.h>

namespace DB
{

namespace
{

std::vector<size_t> getColumnIndices(const Block & block, const Names & column_names)
{
    std::vector<size_t> indices;
    for (const auto & name : column_names)
        indices.push_back(block.getPositionByName(name));
    return indices;
}

Columns getColumnsByIndices(const Chunk & chunk, const std::vector<size_t> & indices)
{
    Columns columns;
    const Columns & all_cols = chunk.getColumns();
    for (const auto & index : indices)
        columns.push_back(all_cols.at(index));
    return columns;
}

ColumnsWithTypeAndName getColumnsByIndices(const Block & sample_block, const Chunk & chunk, const std::vector<size_t> & indices)
{
    Block block = sample_block.cloneEmpty();
    block.setColumns(getColumnsByIndices(chunk, indices));
    return block.getColumnsWithTypeAndName();
}

std::string formatBytesHumanReadable(size_t bytes)
{
    if (bytes >= 1_GiB)
        return fmt::format("{:.2f} GB", static_cast<double>(bytes) / 1_GiB);
    if (bytes >= 1_MiB)
        return fmt::format("{:.2f} MB", static_cast<double>(bytes) / 1_MiB);
    if (bytes >= 1_KiB)
        return fmt::format("{:.2f} KB", static_cast<double>(bytes) / 1_KiB);
    return fmt::format("{:.2f} B", static_cast<double>(bytes));
}

}


CreatingSetsOnTheFlyTransform::CreatingSetsOnTheFlyTransform(
    const Block & header_, const Names & column_names_, size_t num_streams_, SetWithStatePtr set_)
    : ISimpleTransform(header_, header_, true)
    , column_names(column_names_)
    , key_column_indices(getColumnIndices(inputs.front().getHeader(), column_names))
    , num_streams(num_streams_)
    , set(set_)
    , log(&Poco::Logger::get(getName()))
{
}

IProcessor::Status CreatingSetsOnTheFlyTransform::prepare()
{
    auto status = ISimpleTransform::prepare();

    if (status == Status::Finished && set && set->state == SetWithState::State::Creating)
    {
        if (input.isFinished())
        {
            set->finished_count++;
            if (set->finished_count == num_streams)
            {
                set->finishInsert();
                set->state = SetWithState::State::Finished;
                LOG_DEBUG(log, "{}: finish building set for [{}] with {} rows, set size is {}",
                    getDescription(), fmt::join(column_names, ", "), set->getTotalRowCount(), formatBytesHumanReadable(set->getTotalByteCount()));
            }
        }
        else
        {
            /// Should not happen because processor places before join that reads all the data
            /// But let's hanlde this case just for safety.
            set->state = SetWithState::State::Suspended;
            LOG_DEBUG(log, "{}: Processor finished, but not all input was read, cancelling building set after using {}",
                getDescription(), formatBytesHumanReadable(set->getTotalByteCount()));
        }
    }

    if (status == Status::Finished && set && set->state != SetWithState::State::Creating)
        /// Release pointer to make it possible destroy it by consumer
        set.reset();

    return status;
}

void CreatingSetsOnTheFlyTransform::transform(Chunk & chunk)
{
    if (!set || set->state != SetWithState::State::Creating)
    {
        if (set)
            set.reset();
        return;
    }

    if (chunk.getNumRows())
    {
        Columns key_columns = getColumnsByIndices(chunk, key_column_indices);
        size_t prev_size = set->getTotalByteCount();
        bool limit_exceeded = !set->insertFromBlock(key_columns);
        if (limit_exceeded)
        {
            auto prev_state = set->state.exchange(SetWithState::State::Suspended);
            if (prev_state == SetWithState::State::Creating)
            {
                LOG_DEBUG(log, "{}: set limit exceeded, give up building set, after using {} ({} -> {} bytes)",
                    getDescription(), formatBytesHumanReadable(set->getTotalByteCount()), prev_size, set->getTotalByteCount());
            }
            // TODO(@vdimir): set->clear() ?
            set.reset();
        }
    }
}

FilterBySetOnTheFlyTransform::FilterBySetOnTheFlyTransform(const Block & header_, const Names & column_names_, SetWithStatePtr set_)
    : ISimpleTransform(header_, header_, true)
    , column_names(column_names_)
    , key_column_indices(getColumnIndices(inputs.front().getHeader(), column_names))
    , set(set_)
    , log(&Poco::Logger::get(getName()))
{
    const auto & header = inputs.front().getHeader();
    for (size_t idx : key_column_indices)
        key_sample_block.insert(header.getByPosition(idx));
}

IProcessor::Status FilterBySetOnTheFlyTransform::prepare()
{
    auto status = ISimpleTransform::prepare();
    if (status == Status::Finished)
    {
        bool has_filter = set && set->state == SetWithState::State::Finished;
        if (has_filter)
        {
            LOG_DEBUG(log, "Finished {} by [{}]: consumed {} rows in total, {} rows bypassed, result {} rows, {}% filtered",
                Poco::toLower(getDescription()), fmt::join(column_names, ", "),
                stat.consumed_rows, stat.consumed_rows_before_set, stat.result_rows,
                static_cast<int>(100 - 100.0 * stat.result_rows / stat.consumed_rows));
        }
        else
        {
            LOG_DEBUG(log, "Finished {}: bypass {} rows", Poco::toLower(getDescription()), stat.consumed_rows);
        }

        /// Release set to free memory
        set = nullptr;
    }
    return status;
}

void FilterBySetOnTheFlyTransform::transform(Chunk & chunk)
{
    stat.consumed_rows += chunk.getNumRows();
    stat.result_rows += chunk.getNumRows();

    bool can_filter = set && set->state == SetWithState::State::Finished;
    // LOG_DEBUG(&Poco::Logger::get("XXXX"), "{}:{} {} / {} / {}", __FILE__, __LINE__, set->finished_count.load(), set->state.load(), set->getTotalRowCount());
    if (!can_filter)
        stat.consumed_rows_before_set += chunk.getNumRows();

    if (can_filter && chunk.getNumRows())
    {
        auto key_columns = getColumnsByIndices(key_sample_block, chunk, key_column_indices);
        ColumnPtr mask_col = set->execute(key_columns, false);
        const auto & mask = assert_cast<const ColumnUInt8 *>(mask_col.get())->getData();

        stat.result_rows -= chunk.getNumRows();

        Columns columns = chunk.detachColumns();
        size_t result_num_rows = 0;
        for (auto & col : columns)
        {
            col = col->filter(mask, 0);
            result_num_rows = col->size();
        }
        stat.result_rows += result_num_rows;

        chunk.setColumns(std::move(columns), result_num_rows);
    }
}

}
