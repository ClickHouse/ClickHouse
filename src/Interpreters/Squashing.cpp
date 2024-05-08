#include <Interpreters/Squashing.h>
#include <Common/CurrentThread.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

Squashing::Squashing(size_t min_block_size_rows_, size_t min_block_size_bytes_)
    : min_block_size_rows(min_block_size_rows_)
    , min_block_size_bytes(min_block_size_bytes_)
{
}

Block Squashing::add(Block && input_block)
{
    return addImpl<Block &&>(std::move(input_block));
}

Block Squashing::add(const Block & input_block)
{
    return addImpl<const Block &>(input_block);
}

/*
 * To minimize copying, accept two types of argument: const reference for output
 * stream, and rvalue reference for input stream, and decide whether to copy
 * inside this function. This allows us not to copy Block unless we absolutely
 * have to.
 */
template <typename ReferenceType>
Block Squashing::addImpl(ReferenceType input_block)
{
    /// End of input stream.
    if (!input_block)
    {
        Block to_return;
        std::swap(to_return, accumulated_block);
        return to_return;
    }

    /// Just read block is already enough.
    if (isEnoughSize(input_block))
    {
        /// If no accumulated data, return just read block.
        if (!accumulated_block)
        {
            return std::move(input_block);
        }

        /// Return accumulated data (maybe it has small size) and place new block to accumulated data.
        Block to_return = std::move(input_block);
        std::swap(to_return, accumulated_block);
        return to_return;
    }

    /// Accumulated block is already enough.
    if (isEnoughSize(accumulated_block))
    {
        /// Return accumulated data and place new block to accumulated data.
        Block to_return = std::move(input_block);
        std::swap(to_return, accumulated_block);
        return to_return;
    }

    append<ReferenceType>(std::move(input_block));
    if (isEnoughSize(accumulated_block))
    {
        Block to_return;
        std::swap(to_return, accumulated_block);
        return to_return;
    }

    /// Squashed block is not ready.
    return {};
}


template <typename ReferenceType>
void Squashing::append(ReferenceType input_block)
{
    if (!accumulated_block)
    {
        accumulated_block = std::move(input_block);
        return;
    }

    assert(blocksHaveEqualStructure(input_block, accumulated_block));

    for (size_t i = 0, size = accumulated_block.columns(); i < size; ++i)
    {
        const auto source_column = input_block.getByPosition(i).column;

        auto mutable_column = IColumn::mutate(std::move(accumulated_block.getByPosition(i).column));
        mutable_column->insertRangeFrom(*source_column, 0, source_column->size());
        accumulated_block.getByPosition(i).column = std::move(mutable_column);
    }
}


bool Squashing::isEnoughSize(const Block & block)
{
    size_t rows = 0;
    size_t bytes = 0;

    for (const auto & [column, type, name] : block)
    {
        if (!rows)
            rows = column->size();
        else if (rows != column->size())
            throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Sizes of columns doesn't match");

        bytes += column->byteSize();
    }

    return isEnoughSize(rows, bytes);
}


bool Squashing::isEnoughSize(size_t rows, size_t bytes) const
{
    return (!min_block_size_rows && !min_block_size_bytes)
        || (min_block_size_rows && rows >= min_block_size_rows)
        || (min_block_size_bytes && bytes >= min_block_size_bytes);
}

ApplySquashing::ApplySquashing(Block header_, size_t min_block_size_rows_, size_t min_block_size_bytes_)
    : min_block_size_rows(min_block_size_rows_)
    , min_block_size_bytes(min_block_size_bytes_)
    , header(std::move(header_))
{
}

Block ApplySquashing::add(Chunk && input_chunk)
{
    return addImpl(std::move(input_chunk));
}

Block ApplySquashing::addImpl(Chunk && input_chunk)
{
    if (!input_chunk.hasChunkInfo())
    {
        Block to_return;
        std::swap(to_return, accumulated_block);
        return to_return;
    }

    const auto *info = getInfoFromChunk(input_chunk);
    for (auto & chunk : info->chunks)
        append(chunk.clone());

    {
        Block to_return;
        std::swap(to_return, accumulated_block);
        return to_return;
    }
}

void ApplySquashing::append(Chunk && input_chunk)
{
    if (input_chunk.getNumColumns() == 0)
        return;
    if (!accumulated_block)
    {
        for (size_t i = 0; i < input_chunk.getNumColumns(); ++ i)
        {
            ColumnWithTypeAndName col = ColumnWithTypeAndName(input_chunk.getColumns()[i], header.getDataTypes()[i], header.getNames()[i]);
            accumulated_block.insert(accumulated_block.columns(), col);
        }
        return;
    }

    for (size_t i = 0, size = accumulated_block.columns(); i < size; ++i)
    {
        const auto source_column = input_chunk.getColumns()[i];

        auto mutable_column = IColumn::mutate(std::move(accumulated_block.getByPosition(i).column));
        mutable_column->insertRangeFrom(*source_column, 0, source_column->size());
        accumulated_block.getByPosition(i).column = std::move(mutable_column);
    }
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
    , header(std::move(header_))
{
}

Chunk PlanSquashing::add(Chunk && input_chunk)
{
    return addImpl(std::move(input_chunk));
}

Chunk PlanSquashing::addImpl(Chunk && input_chunk)
{
    if (!input_chunk)
    {
        Chunk res_chunk = convertToChunk(chunks_to_merge_vec);
        return res_chunk;
    }

    if (isEnoughSize(chunks_to_merge_vec))
        chunks_to_merge_vec.clear();

    if (input_chunk)
        chunks_to_merge_vec.push_back(std::move(input_chunk));

    if (isEnoughSize(chunks_to_merge_vec))
    {
        Chunk res_chunk = convertToChunk(chunks_to_merge_vec);
        return res_chunk;
    }
    return input_chunk;
}

Chunk PlanSquashing::convertToChunk(std::vector<Chunk> &chunks)
{
    if (chunks.empty())
        return {};

    auto info = std::make_shared<ChunksToSquash>();
    for (auto &chunk : chunks)
        info->chunks.push_back(std::move(chunk));

    chunks.clear(); // we can remove this

    return Chunk(header.cloneEmptyColumns(), 0, info);
}

bool PlanSquashing::isEnoughSize(const std::vector<Chunk> & chunks)
{
    size_t rows = 0;
    size_t bytes = 0;

    for (const Chunk & chunk : chunks)
    {
        rows += chunk.getNumRows();
        bytes += chunk.bytes();
    }

    return isEnoughSize(rows, bytes);
}

bool PlanSquashing::isEnoughSize(size_t rows, size_t bytes) const
{
    return (!min_block_size_rows && !min_block_size_bytes)
        || (min_block_size_rows && rows >= min_block_size_rows)
        || (min_block_size_bytes && bytes >= min_block_size_bytes);
}
}
