#include <cstddef>
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


CreatingSetsOnTheFlyTransform::CreatingSetsOnTheFlyTransform(const Block & header_, const Names & column_names, SetPtr set_)
    : ISimpleTransform(header_, header_, true)
    , key_column_indices(getColumnIndices(inputs.front().getHeader(), column_names))
    , set(set_)
    , log(&Poco::Logger::get(getName()))
{
}

IProcessor::Status CreatingSetsOnTheFlyTransform::prepare()
{
    auto status = ISimpleTransform::prepare();
    return status;
}

void CreatingSetsOnTheFlyTransform::transform(Chunk & chunk)
{
    if (!set)
        return;

    if (chunk.getNumRows())
    {
        Columns key_cols = getColumnsByIndices(chunk, key_column_indices);
        bool limit_exceeded = !set->insertFromBlock(key_cols);
        if (limit_exceeded)
        {
            LOG_DEBUG(log, "Set limit exceeded, give up building set, after using {}", formatBytesHumanReadable(set->getTotalByteCount()));
            // set->clear();
            set.reset();
        }
    }

    if (input.isFinished())
    {
        set->finishInsert();
        LOG_DEBUG(log, "Finish building set with {} rows, set size is {}",
            set->getTotalRowCount(), formatBytesHumanReadable(set->getTotalByteCount()));

        /// Release pointer to make it possible destroy it by consumer
        set.reset();
    }
}

FilterBySetOnTheFlyTransform::FilterBySetOnTheFlyTransform(const Block & header_, const Names & column_names, SetPtr set_)
    : ISimpleTransform(header_, header_, true)
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
    return status;
}

void FilterBySetOnTheFlyTransform::transform(Chunk & chunk)
{

    if (!set)
        return;

    if (!set->isCreated())
        return;

    if (chunk.getNumRows())
    {
        auto key_columns = getColumnsByIndices(key_sample_block, chunk, key_column_indices);
        ColumnPtr mask_col = set->execute(key_columns, false);
        const auto & mask = assert_cast<const ColumnUInt8 *>(mask_col.get())->getData();

        Columns columns = chunk.detachColumns();
        size_t num_rows = 0;
        for (auto & col : columns)
        {
            col = col->filter(mask, 0);
            num_rows = col->size();
        }
        chunk.setColumns(std::move(columns), num_rows);
    }

    if (input.isFinished())
    {
        /// Release set to free memory
        set.reset();
    }
}

}
