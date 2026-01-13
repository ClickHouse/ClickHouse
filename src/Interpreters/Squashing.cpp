#include <utility>
#include <vector>
#include <Interpreters/Squashing.h>
#include <Interpreters/InsertDeduplication.h>
#include <Core/Block.h>
#include <Columns/ColumnSparse.h>
#include <Common/CurrentThread.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <base/defines.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

Squashing::Squashing(SharedHeader header_, size_t min_block_size_rows_, size_t min_block_size_bytes_,
                     size_t max_block_size_rows_, size_t max_block_size_bytes_, bool squash_with_strict_limits_)
    : header(header_) 
    , min_block_size_rows(min_block_size_rows_)
    , min_block_size_bytes(min_block_size_bytes_)
    , max_block_size_rows(max_block_size_rows_)
    , max_block_size_bytes(max_block_size_bytes_)
    , squash_with_strict_limits(squash_with_strict_limits_)
{
}

Chunk Squashing::flush()
{
    // Move all remaining pending data to accumulated (ignore thresholds)
    chassert(!accumulated);

    while (!pending.empty())
    {
        if (pending.peekFront().getNumRows() == 0)
        {
            pending.dropFront();
            continue;
        }  

        size_t rows = squash_with_strict_limits ? pending.peekFront().getNumRows() : 0;
        size_t bytes = squash_with_strict_limits ? pending.peekFront().bytes() : 0;
        auto result = pending.consumeUpTo(rows, bytes);

        accumulated.append(std::move(result.chunk), result.rows, result.bytes);
    }
    
    if (!accumulated)
        return {};

    return convertToChunk();
}

Chunk Squashing::squash(Chunk && input_chunk, SharedHeader header)
{
    if (!input_chunk)
        return std::move(input_chunk);

    auto squash_info = input_chunk.getChunkInfos().extract<ChunksToSquash>();

    if (!squash_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no ChunksToSquash in ChunkInfoPtr");

    return squash(std::move(squash_info->chunks), std::move(input_chunk.getChunkInfos()), header);
}

Chunk Squashing::squash(std::vector<Chunk> && input_chunks, Chunk::ChunkInfoCollection && infos, SharedHeader header)
{
    auto input_chunks_size = input_chunks.size();
    LOG_TEST(getLogger("squashing"), "input chunks count {}", input_chunks_size);

    Chunk::ChunkInfoCollection result_info;
    /// merge all infos before squashing the chunks in order to release original block in deduplication info
    for (auto & chunk : input_chunks)
    {
        LOG_TEST(getLogger("squashing"), "merge deduplication info debug: {}",
            chunk.getChunkInfos().get<DeduplicationInfo>() ? chunk.getChunkInfos().get<DeduplicationInfo>()->debug() : "null");
        result_info.mergeWith(std::move(chunk.getChunkInfos()));
    }
    LOG_TEST(getLogger("squashing"), "merge deduplication info debug: {}",
    infos.get<DeduplicationInfo>() ? infos.get<DeduplicationInfo>()->debug() : "null");
    result_info.mergeWith(std::move(infos));

    auto result = [](std::vector<Chunk> && input_chunks_) -> Chunk
    {
        /// TODO: We might need to change this behavior in case we are squashing with two mins
        /// because we cannot move the whole block if there is an offset and lenght that specify only a part of it
        if (input_chunks_.size() == 1)
            /// this is just optimization, no logic changes
            return std::move(input_chunks_.front());
        return Squashing::squash(std::move(input_chunks_));
    }(std::move(input_chunks));

    // Update original block in deduplication info after squashing
    if (auto deduplication_info = result_info.get<DeduplicationInfo>())
    {
        LOG_TEST(getLogger("squashing"), "Updating original block in deduplication info after squashing, rows: {}, input_chunks count {}, debug: {}",
            result.getNumRows(), input_chunks_size, deduplication_info->debug());
        deduplication_info->updateOriginalBlock(result, header);
    }

    result.setChunkInfos(std::move(result_info));

    chassert(result);
    return result;
}

Chunk Squashing::addAndGenerate(Chunk && input_chunk, bool flush_if_enough_size)
{
    add(std::move(input_chunk));
    return generate(flush_if_enough_size);
}

void Squashing::add(Chunk && input_chunk)
{
    pending.push(std::move(input_chunk));  
}

bool Squashing::canGenerate()
{
    size_t total_rows = accumulated.getRows() + pending.getRows();
    size_t total_bytes = accumulated.getBytes() + pending.getBytes();

    if (squash_with_strict_limits)
    {
        return  allMinReached(total_rows, total_bytes) || oneMaxReached(total_rows, total_bytes);
    }
    return oneMinReached(total_rows, total_bytes);
}

Chunk Squashing::generate(bool flush_if_enough_size)
{
    return squash_with_strict_limits ? generateUsingStrictBounds() : generateUsingOneMinBound(flush_if_enough_size);
}

Chunk Squashing::generateUsingStrictBounds()
{ 
    /// 1. Until the accumulated is larger the both mins continue getting the reference to the first chunk in the queue
    ///     Check what offset and lenght do we need to add it to accounted
    ///     if the lenght is equal to 0, then break
    ///     add the chunk to the accumulated with specified offset and lenght
    ///     if the offset and lenght do not exhaust the chunk break
    ///     otherwise pop it from the queue
    while(!pending.empty())
    {
        /// The behavior before, when add and generate were one action
        /// made the function exit early when an empty block was added
        /// However, now we might want just to skip such a block,
        /// because if user added a number of chunks, and only one of them
        /// is empty, they still probably want the rest of added to be processed 
        if (pending.peekFront().getNumRows() == 0)
        {
            pending.dropFront();
            continue;
        }

        size_t remaining_rows = max_block_size_rows;
        if (remaining_rows)
            remaining_rows -= accumulated.getRows();

        size_t remaining_bytes = max_block_size_bytes;
        if (remaining_bytes)
            remaining_bytes -= accumulated.getBytes();

        auto result = pending.consumeUpTo(remaining_rows, remaining_bytes);

        chassert(result.rows);
        chassert(result.bytes);

        accumulated.append(std::move(result.chunk), result.rows, result.bytes);

        if (allMinReached() || oneMaxReached())
           return convertToChunk();
    }

    return {};
}

Chunk Squashing::generateUsingOneMinBound(bool flush_if_enough_size)
{
    while(!pending.empty())
    {
        auto input_chunk = pending.pullFront();

        if (!input_chunk || input_chunk.getNumRows() == 0)
            continue;

        /// Just read block is already enough.
        if (oneMinReached(input_chunk))
        {
            /// If no accumulated data, return just read block.
            if (!accumulated || flush_if_enough_size)
            {
                accumulated.append(std::move(input_chunk));
                return convertToChunk();
            }

            /// Return accumulated data (maybe it has small size) and place new block to accumulated data.
            Chunk res_chunk = convertToChunk();
            accumulated.append(std::move(input_chunk));
            return res_chunk;
        }

        /// Accumulated block is already enough.
        if (oneMinReached())
        {
            /// Return accumulated data and place new block to accumulated data.
            Chunk res_chunk = convertToChunk();
            accumulated.append(std::move(input_chunk));
            return res_chunk;
        }

        /// Pushing data into accumulating vector
        accumulated.append(std::move(input_chunk));

        /// If accumulated data is big enough, we send it
        if (oneMinReached())
            return convertToChunk();
    }
    return {};
}

Chunk Squashing::convertToChunk()
{
    if (accumulated.empty())
        return {};

    auto info = std::make_shared<ChunksToSquash>();

    size_t total_rows = accumulated.getRows();
    info->chunks = accumulated.extract();

    // It is imortant that chunk is not empty, it has to have columns even if they are empty
    // Sometimes there are could be no columns in header but not empty rows in chunks
    // That happens when we intend to add defaults for the missing columns after
    auto aggr_chunk = Chunk(header->getColumns(), 0);
    if (header->columns() == 0)
        aggr_chunk = Chunk(header->getColumns(), total_rows);

    aggr_chunk.getChunkInfos().add(std::move(info));
    chassert(aggr_chunk);
    return aggr_chunk;
}

Chunk Squashing::squash(std::vector<Chunk> && input_chunks)
{
    if (input_chunks.empty())
        return {};

    std::vector<IColumn::MutablePtr> mutable_columns;
    size_t rows = 0;
    for (const Chunk & chunk : input_chunks)
        rows += chunk.getNumRows();

    {
        auto & first_chunk = input_chunks[0];
        Columns columns = first_chunk.detachColumns();
        mutable_columns.reserve(columns.size());
        for (auto & column : columns)
            mutable_columns.push_back(IColumn::mutate(std::move(column)));
    }

    size_t num_columns = mutable_columns.size();

    /// Collect the list of source columns for each column.
    std::vector<Columns> source_columns_list(num_columns);
    std::vector<UInt8> have_same_serialization(num_columns, true);

    for (size_t i = 0; i != num_columns; ++i)
        source_columns_list[i].reserve(input_chunks.size() - 1);

    for (size_t i = 1; i < input_chunks.size(); ++i) // We've already processed the first chunk above
    {
        auto columns = input_chunks[i].detachColumns();
        for (size_t j = 0; j != num_columns; ++j)
        {
            /// Need to check if there are any sparse columns in subcolumns,
            /// since `IColumn::isSparse` is not recursive but sparse column can be inside a tuple, for example.
            have_same_serialization[j] &= columns[j]->structureEquals(*mutable_columns[j]);
            source_columns_list[j].emplace_back(std::move(columns[j]));
        }
    }

    for (size_t i = 0; i != num_columns; ++i)
    {
        if (!have_same_serialization[i])
        {
            mutable_columns[i] = removeSpecialRepresentations(mutable_columns[i]->convertToFullColumnIfConst())->assumeMutable();
            for (auto & column : source_columns_list[i])
                column = removeSpecialRepresentations(column->convertToFullColumnIfConst());
        }

        /// We know all the data we will insert in advance and can make all necessary pre-allocations.
        mutable_columns[i]->prepareForSquashing(source_columns_list[i], /* factor */ 1);
        for (auto & source_column : source_columns_list[i])
        {
            auto column = std::move(source_column);
            mutable_columns[i]->insertRangeFrom(*column, 0, column->size());
        }
    }

    Chunk result;
    result.setColumns(std::move(mutable_columns), rows);

    chassert(result);
    return result;
}

bool Squashing::oneMinReached(size_t rows, size_t bytes) const
{
    return (!min_block_size_rows && !min_block_size_bytes)
        || (min_block_size_rows && rows >= min_block_size_rows)
        || (min_block_size_bytes && bytes >= min_block_size_bytes);
}

bool Squashing::oneMinReached() const
{
    return oneMinReached(accumulated.getRows(), accumulated.getBytes());
};

bool Squashing::oneMinReached(const Chunk & chunk) const
{
    return oneMinReached(chunk.getNumRows(), chunk.bytes());
}

bool Squashing::allMinReached() const
{
    return allMinReached(accumulated.getRows(), accumulated.getBytes());
}

bool Squashing::allMinReached(size_t rows, size_t bytes) const
{
    return (rows != 0 && bytes != 0) && rows >= min_block_size_rows && bytes >= min_block_size_bytes;
}

bool Squashing::oneMaxReached(size_t rows, size_t bytes) const
{
    return (max_block_size_rows && rows >= max_block_size_rows)
    || (max_block_size_bytes && bytes >= max_block_size_bytes);
}

bool Squashing::oneMaxReached() const
{
    return oneMaxReached(accumulated.getRows(), accumulated.getBytes());
}

void Squashing::AccumulatedChunks::append(Chunk && chunk)
{
    rows += chunk.getNumRows();
    bytes += chunk.bytes();
    chunks.push_back(std::move(chunk));
}

void Squashing::AccumulatedChunks::append(Chunk && chunk, size_t rows_to_add, size_t bytes_to_add)
{
    rows += rows_to_add;
    bytes += bytes_to_add;
    chunks.push_back(std::move(chunk));
}

Chunks Squashing::AccumulatedChunks::extract()
{
    rows = 0;
    bytes = 0;
    return std::move(chunks);
}

void Squashing::PendingQueue::push(Chunk && chunk)
{
    size_t rows = chunk.getNumRows();
    size_t bytes = chunk.bytes();
    chunks.push_front(std::move(chunk));
    total_rows += rows;
    total_bytes += bytes;
}

Chunk Squashing::PendingQueue::pullFront()
{
    auto result = std::move(chunks.front());
    total_rows -= result.getNumRows();
    total_bytes -= result.bytes();
    chunks.pop_front();
    return result;
}

std::pair<size_t, size_t> Squashing::PendingQueue::calculateConsumable(size_t max_rows, size_t max_bytes) const
{
    if (chunks.empty())
        return {0, 0};

    const Chunk & chunk = chunks.front();
    size_t total_rows_front = chunk.getNumRows();
    size_t total_bytes_front = chunk.bytes();
    double bytes_per_row = static_cast<double>(total_bytes_front) / static_cast<double>(total_rows_front);
    size_t available_rows = total_rows_front - offset_first;

    if (max_rows == 0 && max_bytes == 0)
        return {available_rows, available_rows * bytes_per_row};

    size_t rows_to_take = available_rows;

    if (max_rows != 0)
        rows_to_take = std::min( max_rows, rows_to_take);

    if (max_bytes != 0)
    {
        size_t rows_by_bytes = static_cast<size_t>(max_bytes / bytes_per_row);

        /// Allow at least one row if empty and cannot add anymore bytes
        if (rows_by_bytes == 0 && max_bytes > 0)
            rows_by_bytes = 1;

        rows_to_take = std::min(rows_by_bytes, rows_to_take);
    }
    
    auto bytes_to_take = static_cast<size_t>(rows_to_take * bytes_per_row);

    return {rows_to_take, bytes_to_take};
}

static Chunk sliceChunk(const Chunk & chunk, size_t offset, size_t length)
{
    if (!chunk.getChunkInfos().empty())
    {
        /// If there is information in chunk, as in DeduplicationInfo,
        /// this might brake the logic of algorithm, leading to erroneous behavior of the program
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunks for slicing in Squashing must have no additional information.");
    }

    Columns sliced_columns;
    sliced_columns.reserve(chunk.getNumColumns());
    for(const auto & col : chunk.getColumns())
    {
        auto sliced_col = col->cut(offset, length);
        sliced_columns.push_back(std::move(sliced_col));
    }

    Chunk result(std::move(sliced_columns), length);
    result.setChunkInfos((chunk.getChunkInfos().clone()));

    return result;
}

Squashing::PendingQueue::ConsumeResult Squashing::PendingQueue::consumeUpTo(size_t max_rows, size_t max_bytes)
{
    auto [rows_to_take, bytes_to_take] = calculateConsumable(max_rows, max_bytes);

    Chunk & front = chunks.front();
    size_t rows_in_front = front.getNumRows();
    size_t available_rows = rows_in_front - offset_first;
    chassert(available_rows >= rows_to_take);
    bool exhaust_chunk = (available_rows == rows_to_take);

    Chunk result_chunk;

    if (exhaust_chunk && offset_first == 0)
    {
        result_chunk = std::move(front);
        chunks.pop_front();
    }
    else
    {
       result_chunk = sliceChunk(front, offset_first, rows_to_take);

        if (exhaust_chunk)
            chunks.pop_front();
    }
    offset_first = (offset_first + rows_to_take) % rows_in_front;
    total_rows -= rows_to_take;
    total_bytes -= bytes_to_take;

    return {std::move(result_chunk), rows_to_take, bytes_to_take};
}

}
