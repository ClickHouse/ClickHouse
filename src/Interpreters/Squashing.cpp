#include <vector>
#include <Interpreters/Squashing.h>
#include "Common/Logger.h"
#include "Common/logger_useful.h"
#include <Common/CurrentThread.h>
#include <base/defines.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

Squashing::Squashing(Block header_, size_t min_block_size_rows_, size_t min_block_size_bytes_)
    : min_block_size_rows(min_block_size_rows_)
    , min_block_size_bytes(min_block_size_bytes_)
    , header(header_)
{
}

Chunk Squashing::flush()
{
    if (!accumulated)
        return {};

    auto result = convertToChunk(extract());
    chassert(result);
    return result;
}

Chunk Squashing::squash(Chunk && input_chunk)
{
    if (!input_chunk)
        return Chunk();

    auto squash_info = input_chunk.getChunkInfos().extract<ChunksToSquash>();

    if (!squash_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no ChunksToSquash in ChunkInfoPtr");

    return squash(std::move(squash_info->chunks), std::move(input_chunk.getChunkInfos()));
}

Chunk Squashing::add(Chunk && input_chunk)
{
    if (!input_chunk)
        return {};

    /// Just read block is already enough.
    if (isEnoughSize(input_chunk))
    {
        /// If no accumulated data, return just read block.
        if (!accumulated)
        {
            accumulated.add(std::move(input_chunk));
            return convertToChunk(extract());
        }

        /// Return accumulated data (maybe it has small size) and place new block to accumulated data.
        Chunk res_chunk = convertToChunk(extract());
        accumulated.add(std::move(input_chunk));
        return res_chunk;
    }

    /// Accumulated block is already enough.
    if (isEnoughSize())
    {
        /// Return accumulated data and place new block to accumulated data.
        Chunk res_chunk = convertToChunk(extract());
        accumulated.add(std::move(input_chunk));
        return res_chunk;
    }

    /// Pushing data into accumulating vector
    accumulated.add(std::move(input_chunk));

    /// If accumulated data is big enough, we send it
    if (isEnoughSize())
        return convertToChunk(extract());

    return {};
}

Chunk Squashing::convertToChunk(CurrentData && data) const
{
    if (data.chunks.empty())
        return {};

    auto info = std::make_shared<ChunksToSquash>();
    info->chunks = std::move(data.chunks);

    // It is imortant that chunk is not empty, it has to have columns even if they are empty
    // Sometimes there are could be no columns in header but not empty rows in chunks
    // That happens when we intend to add defaults for the missing columns after
    auto aggr_chunk = Chunk(header.getColumns(), 0);
    if (header.columns() == 0)
        aggr_chunk = Chunk(header.getColumns(), data.getRows());

    aggr_chunk.getChunkInfos().add(std::move(info));
    chassert(aggr_chunk);
    return aggr_chunk;
}

Chunk Squashing::squash(std::vector<Chunk> && input_chunks, Chunk::ChunkInfoCollection && infos)
{
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

    Chunk result;
    result.setColumns(std::move(mutable_columns), rows);
    result.setChunkInfos(infos);
    result.getChunkInfos().append(std::move(input_chunks.back().getChunkInfos()));

    chassert(result);
    return result;
}

bool Squashing::isEnoughSize(size_t rows, size_t bytes) const
{
    return (!min_block_size_rows && !min_block_size_bytes)
        || (min_block_size_rows && rows >= min_block_size_rows)
        || (min_block_size_bytes && bytes >= min_block_size_bytes);
}

bool Squashing::isEnoughSize() const
{
    return isEnoughSize(accumulated.getRows(), accumulated.getBytes());
};

bool Squashing::isEnoughSize(const Chunk & chunk) const
{
    return isEnoughSize(chunk.getNumRows(), chunk.bytes());
}

void Squashing::CurrentData::add(Chunk && chunk)
{
    rows += chunk.getNumRows();
    bytes += chunk.bytes();
    chunks.push_back(std::move(chunk));
}

Squashing::CurrentData Squashing::extract()
{
    auto result = std::move(accumulated);
    accumulated = {};
    return result;
}

}
