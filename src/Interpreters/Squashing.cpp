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
    if (accumulated.getRows() == 0)
        return {};

    auto result = convertToChunk(extract());
    chassert(result);
    return result;        
}

/// TODO: add another input variable (bool), that will specify if we are 
/// squashing with new version, or with old version
Chunk Squashing::squash(Chunk && input_chunk, SharedHeader header)
{
    if (!input_chunk)
        return std::move(input_chunk);

    auto squash_info = input_chunk.getChunkInfos().extract<ChunksToSquash>();

    if (!squash_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no ChunksToSquash in ChunkInfoPtr");

    return squash(std::move(squash_info->chunks), std::move(input_chunk.getChunkInfos()), header);
}

/// TODO: add another input variable (bool), that will specify if we are 
/// squashing with new version, or with old version
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
    chunks_pending.push_back(std::move(input_chunk));
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
    while(!chunks_pending.empty())
    {
        /// The behavior before, when add and generate were one action
        /// made the function exit early when an empty block was added
        /// However, now we might want just to skip such a block,
        /// because if user added a number of chunks, and only one of them
        /// is empty, they still probably want the rest of added to be processed 
        if (!chunks_pending.front() || chunks_pending.front().getNumRows() == 0)
        {
            chunks_pending.pop_front();
            return {};
        }

        auto length = accumulated.findLengthPending(chunks_pending.front(), 
                                                    max_block_size_rows, 
                                                    max_block_size_bytes, 
                                                    offset_first_chunk_pending);

        if (length == 0)
        /// Signalled that we cannot add any more rows due to max limits
            return convertToChunk(extract());

        const Chunk & next_pending = chunks_pending.front();
        size_t remaining_rows = next_pending.getNumRows() - offset_first_chunk_pending;

        if (length == remaining_rows)
        {
            accumulated.appendChunkSliced(takeFrontPending(), length, offset_first_chunk_pending);
            offset_first_chunk_pending = 0;
        }
        else
        {
            accumulated.appendChunkSliced(next_pending.clone(), length, offset_first_chunk_pending);
            offset_first_chunk_pending += length;
        }

        if (allMinReached())
           return convertToChunk(extract());
    }

    return {};
}

Chunk Squashing::generateUsingOneMinBound(bool flush_if_enough_size)
{
    while(!chunks_pending.empty())
    {
        auto input_chunk = takeFrontPending();

        if (!input_chunk || input_chunk.getNumRows() == 0)
            return {};

        /// Just read block is already enough.
        if (oneMinReached(input_chunk))
        {
            /// If no accumulated data, return just read block.
            if (!accumulated || flush_if_enough_size)
            {
                accumulated.appendChunk(std::move(input_chunk));
                return convertToChunk(extract());
            }

            /// Return accumulated data (maybe it has small size) and place new block to accumulated data.
            Chunk res_chunk = convertToChunk(extract());
            accumulated.appendChunk(std::move(input_chunk));
            return res_chunk;
        }

        /// Accumulated block is already enough.
        if (oneMinReached())
        {
            /// Return accumulated data and place new block to accumulated data.
            Chunk res_chunk = convertToChunk(extract());
            accumulated.appendChunk(std::move(input_chunk));
            return res_chunk;
        }

        /// Pushing data into accumulating vector
        accumulated.appendChunk(std::move(input_chunk));

        /// If accumulated data is big enough, we send it
        if (oneMinReached())
            return convertToChunk(extract());
    }
    return {};
}

static Chunk sliceChunk(Chunk && chunk, size_t offset, size_t length)
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

Chunk Squashing::convertToChunk(CurrentData && data) const
{
    if (data.chunks_ready.empty())
        return {};

    auto info = std::make_shared<ChunksToSquash>();

    /// TODO: if offsets and lenghts are non-zero
    if (data.length_first_chunk != 0)
    {
        data.chunks_ready.front() = sliceChunk(std::move(data.chunks_ready.front()), data.offset_first_chunk, data.length_first_chunk);
    }

    if (data.length_last_chunk != 0)
    {
        data.chunks_ready.back() = sliceChunk(std::move(data.chunks_ready.back()), data.offset_last_chunk, data.length_last_chunk);
    }

    info->chunks = std::move(data.chunks_ready);

    // It is imortant that chunk is not empty, it has to have columns even if they are empty
    // Sometimes there are could be no columns in header but not empty rows in chunks
    // That happens when we intend to add defaults for the missing columns after
    auto aggr_chunk = Chunk(header->getColumns(), 0);
    if (header->columns() == 0)
        aggr_chunk = Chunk(header->getColumns(), data.getRows());

    aggr_chunk.getChunkInfos().add(std::move(info));
    chassert(aggr_chunk);
    return aggr_chunk;
}

/// TODO: add another input variable (bool), that will specify if we are 
/// squashing with new version, or with old version
/// We will also have to pass the offsets and lengths to this function, so that we can account for
/// these blocks OR we would be able to pass input chunks for which we already adjusted offsets and lengths
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
    return rows >= min_block_size_rows && bytes >= min_block_size_bytes;
}

void Squashing::CurrentData::appendChunk(Chunk && chunk)
{
    rows += chunk.getNumRows();
    bytes += chunk.bytes();
    chunks_ready.push_back(std::move(chunk));
}

Squashing::CurrentData Squashing::extract()
{
    auto result = std::move(accumulated);
    accumulated = {};
    return result;
}

Chunk Squashing::takeFrontPending()
{
    Chunk res = std::move(chunks_pending.front());
    chunks_pending.pop_front();
    return res;
}

size_t Squashing::CurrentData::findLengthPending(const Chunk & chunk, size_t max_rows, size_t max_bytes, size_t offset_pending) const
{    
    /// 2. How many lines is there in the first element of the queue available
    size_t total_rows_in_pending_chunk = chunk.getNumRows();
    size_t available_rows = total_rows_in_pending_chunk - offset_pending;

    /// 3. If max is disabled -- return offset = 0, and length equal all rows in the first element
    if (max_rows == 0 && max_bytes == 0)
        return available_rows;

    size_t result = available_rows;

    /// 4. How many lines we can add to accumulated until we reach max
    if (max_rows != 0)
    {
        size_t rows_until_max = max_rows - getRows();
        result = std::min(rows_until_max, result);
    }

    /// 5. if (max_block_size_bytes != 0) 
    ///     how much does one row on average weight in the front chunk
    ///     how many rows we can add until we reach max
    ///     
    ///     take min of current result and this number of lines
    if (max_bytes != 0)
    {
        size_t bytes_in_pending_chunk = chunk.bytes();
        double bytes_per_row = static_cast<double>(bytes_in_pending_chunk) / static_cast<double>(total_rows_in_pending_chunk);
        size_t bytes_until_max = max_bytes - getBytes();
        size_t rows_by_bytes = static_cast<size_t>(bytes_until_max / bytes_per_row);

        /// Allow at least one row if empty and cannot add anymore bytes
        if (rows_by_bytes == 0 && getRows() == 0 && getBytes() == 0)
        {
            rows_by_bytes = 1;
        }
        result = std::min(rows_by_bytes, result);
    }

    /// 6. Return the min number of rows allowed 
    return result;
}

void Squashing::CurrentData::appendChunkSliced(Chunk && chunk, size_t len, size_t offset_pending)
{
    /// 1. Add the first block in the queue to the chunks_ready
    ///     set offset of the last block to the offset of the first pending block
    ///     set the lenth of the last block to the lengh
    ///     increase the number of rows in the accumulated by len
    ///     increase the number of bytes in the accumulated by the average bytes per line * len
    chunks_ready.push_back(std::move(chunk));

    const auto & pending_chunk = chunks_ready.back();
    size_t total_rows_in_pending_chunk = pending_chunk.getNumRows();
    size_t bytes_in_pending_chunk = pending_chunk.bytes();
    double bytes_in_one_row_avg = static_cast<double>(bytes_in_pending_chunk) / static_cast<double>(total_rows_in_pending_chunk); 

    offset_last_chunk = offset_pending;
    length_last_chunk = len;

    rows += len;
    bytes += static_cast<size_t>(len * bytes_in_one_row_avg);

    /// 3. If accumulated is of size 1
    ///     set the first block offset/lenght to the last block offset/lenght
    ///     set the last block offset/lenght to 0
    if (chunks_ready.size() == 1)
    {
        offset_first_chunk = offset_last_chunk;
        length_first_chunk = length_last_chunk;
        offset_last_chunk = 0;
        length_last_chunk = 0;
    }
}

}
