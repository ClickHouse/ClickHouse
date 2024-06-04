#include <Processors/Transforms/CreateSetAndFilterOnTheFlyTransform.h>

#include <cstddef>
#include <mutex>

#include <Interpreters/Set.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnSparse.h>
#include <Core/ColumnWithTypeAndName.h>
#include <base/types.h>

#include <Poco/String.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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
    {
        auto col = recursiveRemoveSparse(all_cols.at(index));
        columns.push_back(std::move(col));
    }

    return columns;
}

ColumnsWithTypeAndName getColumnsByIndices(const Block & sample_block, const Chunk & chunk, const std::vector<size_t> & indices)
{
    Block block = sample_block.cloneEmpty();
    block.setColumns(getColumnsByIndices(chunk, indices));
    return block.getColumnsWithTypeAndName();
}

}

CreatingSetsOnTheFlyTransform::CreatingSetsOnTheFlyTransform(
    const Block & header_, const Names & column_names_, size_t num_streams_, SetWithStatePtr set_)
    : ISimpleTransform(header_, header_, true)
    , column_names(column_names_)
    , key_column_indices(getColumnIndices(inputs.front().getHeader(), column_names))
    , num_streams(num_streams_)
    , set(set_)
{
}

IProcessor::Status CreatingSetsOnTheFlyTransform::prepare()
{
    IProcessor::Status status = ISimpleTransform::prepare();

    if (!set || status != Status::Finished)
        /// Nothing to do with set
        return status;

    /// Finalize set
    if (set->state == SetWithState::State::Creating)
    {
        if (input.isFinished())
        {
            set->finished_count++;
            if (set->finished_count != num_streams)
                /// Not all instances of processor are finished
                return status;

            set->finishInsert();
            set->state = SetWithState::State::Finished;
            LOG_DEBUG(log, "{}: finish building set for [{}] with {} rows, set size is {}",
                getDescription(), fmt::join(column_names, ", "), set->getTotalRowCount(),
                formatReadableSizeWithBinarySuffix(set->getTotalByteCount()));
            set.reset();
        }
        else
        {
            /// Should not happen because processor inserted before join that reads all the data
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Processor finished, but not all input was read");
        }
    }

    return status;
}

void CreatingSetsOnTheFlyTransform::transform(Chunk & chunk)
{
    if (!set || set->state != SetWithState::State::Creating)
    {
        /// If set building suspended by another processor, release pointer
        if (set != nullptr)
            set.reset();
        return;
    }

    if (chunk.getNumRows())
    {
        Columns key_columns = getColumnsByIndices(chunk, key_column_indices);
        bool limit_exceeded = !set->insertFromColumns(key_columns);
        if (limit_exceeded)
        {
            auto prev_state = set->state.exchange(SetWithState::State::Suspended);
            /// Print log only after first state switch
            if (prev_state == SetWithState::State::Creating)
            {
                LOG_DEBUG(log, "{}: set limit exceeded, give up building set, after reading {} rows and using {}",
                    getDescription(), set->getTotalRowCount(), formatReadableSizeWithBinarySuffix(set->getTotalByteCount()));
            }
            /// Probaply we need to clear set here, because it's unneeded anymore
            /// But now `Set` doesn't have such method, so reset pointer in all processors and then it should be freed
            set.reset();
        }
    }
}

FilterBySetOnTheFlyTransform::FilterBySetOnTheFlyTransform(const Block & header_, const Names & column_names_, SetWithStatePtr set_)
    : ISimpleTransform(header_, header_, true)
    , column_names(column_names_)
    , key_column_indices(getColumnIndices(inputs.front().getHeader(), column_names))
    , set(set_)
{
    const auto & header = inputs.front().getHeader();
    for (size_t idx : key_column_indices)
        key_sample_block.insert(header.getByPosition(idx));
}

IProcessor::Status FilterBySetOnTheFlyTransform::prepare()
{
    auto status = ISimpleTransform::prepare();

    if (set && set->state == SetWithState::State::Suspended)
        set.reset();

    if (status == Status::Finished)
    {
        bool has_filter = set && set->state == SetWithState::State::Finished;
        if (has_filter)
        {
            LOG_DEBUG(log, "Finished {} by [{}]: consumed {} rows in total, {} rows bypassed, result {} rows, {:.2f}% filtered",
                Poco::toLower(getDescription()), fmt::join(column_names, ", "),
                stat.consumed_rows, stat.consumed_rows_before_set, stat.result_rows,
                stat.consumed_rows > 0 ? (100 - 100.0 * stat.result_rows / stat.consumed_rows) : 0);
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
            col = col->filter(mask, /* negative */ false);
            result_num_rows = col->size();
        }
        stat.result_rows += result_num_rows;

        chunk.setColumns(std::move(columns), result_num_rows);
    }
}

}
