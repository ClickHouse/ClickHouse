#include <vector>
#include <Interpreters/Squashing.h>
#include <Common/CurrentThread.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ApplySquashing::ApplySquashing(Block header_)
    : header(header_)
{
}

Chunk ApplySquashing::add(Chunk && input_chunk)
{
    if (!input_chunk.hasChunkInfo())
        return Chunk();

    const auto *info = getInfoFromChunk(input_chunk);
    append(info->chunks);

    return std::move(accumulated_chunk);
}

void ApplySquashing::append(std::vector<Chunk> & input_chunks)
{
    accumulated_chunk = {};
    std::vector<IColumn::MutablePtr> mutable_columns = {};
    size_t rows = 0;
    for (const Chunk & chunk : input_chunks)
        rows += chunk.getNumRows();

    {
        auto & first_chunk = input_chunks[0];
        Columns columns = first_chunk.detachColumns();
        for (size_t i = 0; i < columns.size(); ++i)
        {
            mutable_columns.push_back(IColumn::mutate(std::move(columns[i])));
            mutable_columns[i]->reserve(rows);
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
}

const ChunksToSquash* ApplySquashing::getInfoFromChunk(const Chunk & chunk)
{
    const auto& info = chunk.getChunkInfo();
    const auto * agg_info = typeid_cast<const ChunksToSquash *>(info.get());

    if (!agg_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no ChunksToSquash in ChunkInfoPtr");

    return agg_info;
}

PlanSquashing::PlanSquashing(Block header_, size_t min_block_size_rows_, size_t min_block_size_bytes_)
    : min_block_size_rows(min_block_size_rows_)
    , min_block_size_bytes(min_block_size_bytes_)
    , header(header_)
{
}

Chunk PlanSquashing::flush()
{
    return convertToChunk(std::move(chunks_to_merge_vec));
}

Chunk PlanSquashing::add(Chunk && input_chunk)
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

Chunk PlanSquashing::convertToChunk(std::vector<Chunk> && chunks)
{
    if (chunks.empty())
        return {};

    auto info = std::make_shared<ChunksToSquash>();
    info->chunks = std::move(chunks);

    chunks.clear();

    return Chunk(header.cloneEmptyColumns(), 0, info);
}

void PlanSquashing::expandCurrentSize(size_t rows, size_t bytes)
{
    accumulated_size.rows += rows;
    accumulated_size.bytes += bytes;
}

void PlanSquashing::changeCurrentSize(size_t rows, size_t bytes)
{
    accumulated_size.rows = rows;
    accumulated_size.bytes = bytes;
}

bool PlanSquashing::isEnoughSize(size_t rows, size_t bytes) const
{
    return (!min_block_size_rows && !min_block_size_bytes)
        || (min_block_size_rows && rows >= min_block_size_rows)
        || (min_block_size_bytes && bytes >= min_block_size_bytes);
}
}
