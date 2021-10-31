#include <Processors/Transforms/SortingTransform.h>

#include <Core/SortDescription.h>
#include <Core/SortCursor.h>

#include <Common/formatReadable.h>
#include <Common/ProfileEvents.h>

#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>

#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>


namespace ProfileEvents
{
    extern const Event ExternalSortWritePart;
    extern const Event ExternalSortMerge;
}


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

MergeSorter::MergeSorter(Chunks chunks_, SortDescription & description_, size_t max_merged_block_size_, UInt64 limit_)
    : chunks(std::move(chunks_)), description(description_), max_merged_block_size(max_merged_block_size_), limit(limit_)
{
    Chunks nonempty_chunks;
    for (auto & chunk : chunks)
    {
        if (chunk.getNumRows() == 0)
            continue;

        cursors.emplace_back(chunk.getColumns(), description);
        has_collation |= cursors.back().has_collation;

        nonempty_chunks.emplace_back(std::move(chunk));
    }

    chunks.swap(nonempty_chunks);

    if (has_collation)
        queue_with_collation = SortingHeap<SortCursorWithCollation>(cursors);
    else if (description.size() > 1)
        queue_without_collation = SortingHeap<SortCursor>(cursors);
    else
        queue_simple = SortingHeap<SimpleSortCursor>(cursors);
}


Chunk MergeSorter::read()
{
    if (chunks.empty())
        return Chunk();

    if (chunks.size() == 1)
    {
        auto res = std::move(chunks[0]);
        chunks.clear();
        return res;
    }

    if (has_collation)
        return mergeImpl(queue_with_collation);
    else if (description.size() > 1)
        return mergeImpl(queue_without_collation);
    else
        return mergeImpl(queue_simple);
}


template <typename TSortingHeap>
Chunk MergeSorter::mergeImpl(TSortingHeap & queue)
{
    size_t num_columns = chunks[0].getNumColumns();
    MutableColumns merged_columns = chunks[0].cloneEmptyColumns();

    /// Reserve
    if (queue.isValid())
    {
        /// The expected size of output block is the same as input block
        size_t size_to_reserve = chunks[0].getNumRows();
        for (auto & column : merged_columns)
            column->reserve(size_to_reserve);
    }

    /// TODO: Optimization when a single block left.

    /// Take rows from queue in right order and push to 'merged'.
    size_t merged_rows = 0;
    while (queue.isValid())
    {
        auto current = queue.current();

        /// Append a row from queue.
        for (size_t i = 0; i < num_columns; ++i)
            merged_columns[i]->insertFrom(*current->all_columns[i], current->getRow());

        ++total_merged_rows;
        ++merged_rows;

        /// We don't need more rows because of limit has reached.
        if (limit && total_merged_rows == limit)
        {
            chunks.clear();
            break;
        }

        queue.next();

        /// It's enough for current output block but we will continue.
        if (merged_rows == max_merged_block_size)
            break;
    }

    if (!queue.isValid())
        chunks.clear();

    if (merged_rows == 0)
        return {};

    return Chunk(std::move(merged_columns), merged_rows);
}


SortingTransform::SortingTransform(
    const Block & header,
    const SortDescription & description_,
    size_t max_merged_block_size_, UInt64 limit_)
    : IProcessor({header}, {header})
    , description(description_)
    , max_merged_block_size(max_merged_block_size_)
    , limit(limit_)
{
    const auto & sample = inputs.front().getHeader();

    /// Replace column names to column position in sort_description.
    for (auto & column_description : description)
    {
        if (!column_description.column_name.empty())
        {
            column_description.column_number = sample.getPositionByName(column_description.column_name);
            column_description.column_name.clear();
        }
    }

    /// Remove constants from header and map old indexes to new.
    size_t num_columns = sample.columns();
    ColumnNumbers map(num_columns, num_columns);
    const_columns_to_remove.assign(num_columns, true);
    for (size_t pos = 0; pos < num_columns; ++pos)
    {
        const auto & column = sample.getByPosition(pos);
        if (!(column.column && isColumnConst(*column.column)))
        {
            map[pos] = header_without_constants.columns();
            header_without_constants.insert(column);
            const_columns_to_remove[pos] = false;
        }
    }

    /// Remove constants from column_description and remap positions.
    SortDescription description_without_constants;
    description_without_constants.reserve(description.size());
    for (const auto & column_description : description)
    {
        auto old_pos = column_description.column_number;
        auto new_pos = map[old_pos];
        if (new_pos < num_columns)
        {
            description_without_constants.push_back(column_description);
            description_without_constants.back().column_number = new_pos;
        }
    }

    description.swap(description_without_constants);
}

SortingTransform::~SortingTransform() = default;

IProcessor::Status SortingTransform::prepare()
{
    if (stage == Stage::Serialize)
    {
        if (!processors.empty())
            return Status::ExpandPipeline;

        auto status = prepareSerialize();
        if (status != Status::Finished)
            return status;

        stage = Stage::Consume;
    }

    if (stage == Stage::Consume)
    {
        auto status = prepareConsume();
        if (status != Status::Finished)
            return status;

        stage = Stage::Generate;
    }

    /// stage == Stage::Generate

    if (!generated_prefix || !chunks.empty())
        return Status::Ready;

    if (!processors.empty())
        return Status::ExpandPipeline;

    return prepareGenerate();
}

IProcessor::Status SortingTransform::prepareConsume()
{
    auto & input = inputs.front();
    auto & output = outputs.front();

    /// Check can output.

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (generated_chunk)
        output.push(std::move(generated_chunk));

    /// Check can input.
    if (!current_chunk)
    {
        if (input.isFinished())
            return Status::Finished;

        if (!input.hasData())
        {
            input.setNeeded();
            return Status::NeedData;
        }

        current_chunk = input.pull(true);
    }

    /// Now consume.
    return Status::Ready;
}

IProcessor::Status SortingTransform::prepareSerialize()
{
    auto & output = outputs.back();

    if (output.isFinished())
        return Status::Finished;

    if (!output.canPush())
        return Status::PortFull;

    if (current_chunk)
        output.push(std::move(current_chunk));

    if (merge_sorter)
        return Status::Ready;

    output.finish();
    return Status::Finished;
}

IProcessor::Status SortingTransform::prepareGenerate()
{
    auto & output = outputs.front();

    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

    if (!output.canPush())
        return Status::PortFull;

    if (merge_sorter)
    {
        if (!generated_chunk)
            return Status::Ready;

        output.push(std::move(generated_chunk));
        return Status::PortFull;
    }
    else
    {
        auto & input = inputs.back();

        if (generated_chunk)
            output.push(std::move(generated_chunk));

        if (input.isFinished())
        {
            output.finish();
            return Status::Finished;
        }

        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        auto chunk = input.pull();
        enrichChunkWithConstants(chunk);
        output.push(std::move(chunk));
        return Status::PortFull;
    }
}

void SortingTransform::work()
{
    if (stage == Stage::Consume)
        consume(std::move(current_chunk));

    if (stage == Stage::Serialize)
        serialize();

    if (stage == Stage::Generate)
        generate();
}

void SortingTransform::removeConstColumns(Chunk & chunk)
{
    size_t num_columns = chunk.getNumColumns();
    size_t num_rows = chunk.getNumRows();

    if (num_columns != const_columns_to_remove.size())
        throw Exception("Block has different number of columns with header: " + toString(num_columns)
                        + " vs " + toString(const_columns_to_remove.size()), ErrorCodes::LOGICAL_ERROR);

    auto columns = chunk.detachColumns();
    Columns column_without_constants;
    column_without_constants.reserve(header_without_constants.columns());

    for (size_t position = 0; position < num_columns; ++position)
    {
        if (!const_columns_to_remove[position])
            column_without_constants.push_back(std::move(columns[position]));
    }

    chunk.setColumns(std::move(column_without_constants), num_rows);
}

void SortingTransform::enrichChunkWithConstants(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    size_t num_result_columns = const_columns_to_remove.size();

    auto columns = chunk.detachColumns();
    Columns column_with_constants;
    column_with_constants.reserve(num_result_columns);

    const auto & header = inputs.front().getHeader();

    size_t next_non_const_column = 0;
    for (size_t i = 0; i < num_result_columns; ++i)
    {
        if (const_columns_to_remove[i])
            column_with_constants.emplace_back(header.getByPosition(i).column->cloneResized(num_rows));
        else
        {
            if (next_non_const_column >= columns.size())
                throw Exception("Can't enrich chunk with constants because run out of non-constant columns.",
                        ErrorCodes::LOGICAL_ERROR);

            column_with_constants.emplace_back(std::move(columns[next_non_const_column]));
            ++next_non_const_column;
        }
    }

    chunk.setColumns(std::move(column_with_constants), num_rows);
}

void SortingTransform::serialize()
{
    throw Exception("Method 'serialize' is not implemented for " + getName() + " processor", ErrorCodes::NOT_IMPLEMENTED);
}

}
