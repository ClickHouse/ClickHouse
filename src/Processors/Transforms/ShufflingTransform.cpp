#include <Processors/Transforms/ShufflingTransform.h>

#include <Core/ShuffleCursor.h>

#include <Common/formatReadable.h>
#include <Common/ProfileEvents.h>

#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>

#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>

#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

MergeShuffler::MergeShuffler(SharedHeader header, Chunks chunks_, size_t max_merged_block_size_, UInt64 limit_)
    : chunks(std::move(chunks_)), max_merged_block_size(max_merged_block_size_), limit(limit_)
{
    Chunks nonempty_chunks;
    size_t chunks_size = chunks.size();

    for (size_t chunk_index = 0; chunk_index < chunks_size; ++chunk_index)
    {
        auto & chunk = chunks[chunk_index];
        if (chunk.getNumRows() == 0)
            continue;

        /// Convert to full column, because sparse column has
        /// access to element in O(log(K)), where K is number of non-default rows,
        /// which can be inefficient.
        convertToFullIfSparse(chunk);

        /// Convert to full column, because some cursors expect non-contant columns
        convertToFullIfConst(chunk);

        cursors.emplace_back(*header, chunk.getColumns(), chunk.getNumRows(), chunk_index);

        nonempty_chunks.emplace_back(std::move(chunk));
    }

    chunks.swap(nonempty_chunks);

    queue = ShufflingQueue(cursors);
}


Chunk MergeShuffler::read()
{
    if (chunks.empty())
        return Chunk();

    if (chunks.size() == 1)
    {
        auto res = std::move(chunks[0]);
        chunks.clear();
        return res;
    }

    return mergeImpl();
}


Chunk MergeShuffler::mergeImpl()
{
    size_t num_columns = chunks[0].getNumColumns();
    MutableColumns merged_columns = chunks[0].cloneEmptyColumns();

    /// Reserve
    if (queue.isValid())
    {
        /// The size of output block will not be larger than the `max_merged_block_size`.
        /// If redundant memory space is reserved, `MemoryTracker` will count more memory usage than actual usage.
        size_t size_to_reserve = std::min(static_cast<size_t>(chunks[0].getNumRows()), max_merged_block_size);
        for (auto & column : merged_columns)
            column->reserve(size_to_reserve);
    }


    /// Take rows from queue in right order and push to 'merged'.
    size_t merged_rows = 0;
    while (queue.isValid())
    {
        auto [current_ptr, index_in_queue] = queue.current();
        auto & current = *current_ptr;

        /// Append rows from queue.
        for (size_t i = 0; i < num_columns; ++i)
        {
            merged_columns[i]->insertFrom(*current.all_columns[i], current.getRow());
        }

        ++total_merged_rows;
        ++merged_rows;

        /// We don't need more rows because of limit has reached.
        if (limit && total_merged_rows >= limit)
        {
            chunks.clear();
            break;
        }

        /// If there are still rows in current cursor, then increment its pos
        if (!current.isLast())
        {
            queue.incrementCursorPosByIndex(index_in_queue);
        }
        /// Otherwise remove it from the ShufflingQueue
        else
        {
            queue.removeFromIndex(index_in_queue);
        }

        /// It's enough for current output block but we will continue.
        if (merged_rows >= max_merged_block_size)
            break;
    }

    if (!queue.isValid())
        chunks.clear();

    if (merged_rows == 0)
        return {};

    return Chunk(std::move(merged_columns), merged_rows);
}

ShufflingTransform::ShufflingTransform(
    SharedHeader header,
    size_t max_merged_block_size_,
    UInt64 limit_,
    bool )
    : IProcessor({header}, {header})
    , max_merged_block_size(max_merged_block_size_)
    , limit(limit_)
{
    // LOG_INFO(getLogger("ShufflingTransform"), "ShufflingTransform::ShufflingTransform");
    const auto & sample = inputs.front().getHeader();

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

    
}

ShufflingTransform::~ShufflingTransform() = default;

IProcessor::Status ShufflingTransform::prepare()
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

IProcessor::Status ShufflingTransform::prepareConsume()
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

IProcessor::Status ShufflingTransform::prepareSerialize()
{
    auto & output = outputs.back();

    if (output.isFinished())
        return Status::Finished;

    if (!output.canPush())
        return Status::PortFull;

    if (current_chunk)
        output.push(std::move(current_chunk));

    if (merge_shuffler)
        return Status::Ready;

    output.finish();
    return Status::Finished;
}

IProcessor::Status ShufflingTransform::prepareGenerate()
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

    if (merge_shuffler)
    {
        if (!generated_chunk)
            return Status::Ready;

        output.push(std::move(generated_chunk));
        return Status::PortFull;
    }

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

void ShufflingTransform::work()
{
    if (stage == Stage::Consume)
        consume(std::move(current_chunk));

    if (stage == Stage::Serialize)
        serialize();

    if (stage == Stage::Generate)
        generate();
}

void ShufflingTransform::removeConstColumns(Chunk & chunk)
{
    size_t num_columns = chunk.getNumColumns();
    size_t num_rows = chunk.getNumRows();

    if (num_columns != const_columns_to_remove.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Block has different number of columns with header: {} vs {}",
                        num_columns, const_columns_to_remove.size());

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

void ShufflingTransform::enrichChunkWithConstants(Chunk & chunk)
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
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't enrich chunk with constants because run out of non-constant columns.");

            column_with_constants.emplace_back(std::move(columns[next_non_const_column]));
            ++next_non_const_column;
        }
    }

    chunk.setColumns(std::move(column_with_constants), num_rows);
}

void ShufflingTransform::serialize()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method 'serialize' is not implemented for {} processor", getName());
}

}
