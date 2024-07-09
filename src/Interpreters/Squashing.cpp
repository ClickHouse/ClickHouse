#include <vector>
#include <Interpreters/Squashing.h>
#include <Common/CurrentThread.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

Squashing::Squashing(Block header_, size_t min_block_size_rows_, size_t min_block_size_bytes_)
    : header(header_)
    , min_block_size_rows(min_block_size_rows_)
    , min_block_size_bytes(min_block_size_bytes_)
{
}

Chunk Squashing::flush()
{
    return convertToChunk(std::move(chunks_to_merge_vec));
}

Chunk Squashing::squash(Chunk && input_chunk)
{
    if (!input_chunk.hasChunkInfo())
        return Chunk();

    const auto *info = getInfoFromChunk(input_chunk);
    return squash(info->chunks);
}

Chunk Squashing::add(Chunk && input_chunk)
{
    if (!input_chunk)
        return {};

    /// Just read block is already enough.
    if (isEnoughSize(input_chunk.getNumRows(), input_chunk.bytes()))
    {
        /// If no accumulated data, return just read block.
        if (chunks_to_merge_vec.empty())
        {
            chunks_to_merge_vec.push_back(std::move(input_chunk));
            Chunk res_chunk = convertToChunk(std::move(chunks_to_merge_vec));
            chunks_to_merge_vec.clear();
            return res_chunk;
        }

        /// Return accumulated data (maybe it has small size) and place new block to accumulated data.
        Chunk res_chunk = convertToChunk(std::move(chunks_to_merge_vec));
        chunks_to_merge_vec.clear();
        changeCurrentSize(input_chunk.getNumRows(), input_chunk.bytes());
        chunks_to_merge_vec.push_back(std::move(input_chunk));
        return res_chunk;
    }

    /// Accumulated block is already enough.
    if (isEnoughSize(accumulated_size.rows, accumulated_size.bytes))
    {
        /// Return accumulated data and place new block to accumulated data.
        Chunk res_chunk = convertToChunk(std::move(chunks_to_merge_vec));
        chunks_to_merge_vec.clear();
        changeCurrentSize(input_chunk.getNumRows(), input_chunk.bytes());
        chunks_to_merge_vec.push_back(std::move(input_chunk));
        return res_chunk;
    }

    /// Pushing data into accumulating vector
    expandCurrentSize(input_chunk.getNumRows(), input_chunk.bytes());
    chunks_to_merge_vec.push_back(std::move(input_chunk));

    /// If accumulated data is big enough, we send it
    if (isEnoughSize(accumulated_size.rows, accumulated_size.bytes))
    {
        Chunk res_chunk = convertToChunk(std::move(chunks_to_merge_vec));
        changeCurrentSize(0, 0);
        chunks_to_merge_vec.clear();
        return res_chunk;
    }
    return {};
}

Chunk Squashing::convertToChunk(std::vector<Chunk> && chunks) const
{
    if (chunks.empty())
        return {};

    auto info = std::make_shared<ChunksToSquash>();
    info->chunks = std::move(chunks);

    chunks.clear();

    return Chunk(header.cloneEmptyColumns(), 0, info);
}

Chunk Squashing::squash(std::vector<Chunk> & input_chunks)
{
    Chunk accumulated_chunk;
    std::vector<IColumn::MutablePtr> mutable_columns = {};
    size_t rows = 0;
    for (const Chunk & chunk : input_chunks)
        rows += chunk.getNumRows();

    {
        auto & first_chunk = input_chunks[0];
        Columns columns = first_chunk.detachColumns();
        for (auto & column : columns)
        {
            mutable_columns.push_back(IColumn::mutate(std::move(column)));
            mutable_columns.back()->reserve(rows);
        }
    }

    for (size_t i = 1; i < input_chunks.size(); ++i) // We've already processed the first chunk above
    {
        Columns columns = input_chunks[i].detachColumns();
        for (size_t j = 0, size = mutable_columns.size(); j < size; ++j)
        {
            const auto source_column = columns[j];

            mutable_columns[j]->insertRangeFrom(*source_column, 0, source_column->size());
        }
    }
    accumulated_chunk.setColumns(std::move(mutable_columns), rows);
    return accumulated_chunk;
}

const ChunksToSquash* Squashing::getInfoFromChunk(const Chunk & chunk)
{
    const auto& info = chunk.getChunkInfo();
    const auto * agg_info = typeid_cast<const ChunksToSquash *>(info.get());

    if (!agg_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no ChunksToSquash in ChunkInfoPtr");

    return agg_info;
}

void Squashing::expandCurrentSize(size_t rows, size_t bytes)
{
    accumulated_size.rows += rows;
    accumulated_size.bytes += bytes;
}

void Squashing::changeCurrentSize(size_t rows, size_t bytes)
{
    accumulated_size.rows = rows;
    accumulated_size.bytes = bytes;
}

bool Squashing::isEnoughSize(size_t rows, size_t bytes) const
{
    return (!min_block_size_rows && !min_block_size_bytes)
        || (min_block_size_rows && rows >= min_block_size_rows)
        || (min_block_size_bytes && bytes >= min_block_size_bytes);
}
}
