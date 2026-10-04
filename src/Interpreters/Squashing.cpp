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

Squashing::Squashing(SharedHeader header_, size_t min_block_size_rows_, size_t min_block_size_bytes_)
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
        chunk.setChunkInfos({});
    }
    LOG_TEST(getLogger("squashing"), "merge deduplication info debug: {}",
    infos.get<DeduplicationInfo>() ? infos.get<DeduplicationInfo>()->debug() : "null");
    result_info.mergeWith(std::move(infos));

    auto result = [](std::vector<Chunk> && input_chunks_) -> Chunk
    {
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

Chunk Squashing::add(Chunk && input_chunk, bool flush_if_enough_size)
{
    if (!input_chunk || input_chunk.getNumRows() == 0)
        return {};

    /// Just read block is already enough.
    if (isEnoughSize(input_chunk))
    {
        /// If no accumulated data, return just read block.
        if (!accumulated || flush_if_enough_size)
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
    auto aggr_chunk = Chunk(header->getColumns(), 0);
    if (header->columns() == 0)
        aggr_chunk = Chunk(header->getColumns(), data.getRows());

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
        /// Materialize ColumnConst before concatenation, because ColumnConst::insertRangeFrom
        /// ignores the source value and just increments the row count
        if (isColumnConst(*mutable_columns[i]))
        {
            mutable_columns[i] = IColumn::mutate(mutable_columns[i]->convertToFullColumnIfConst());
            for (auto & column : source_columns_list[i])
                column = column->convertToFullColumnIfConst();
        }
        if (!have_same_serialization[i])
        {
            mutable_columns[i] = IColumn::mutate(removeSpecialRepresentations(mutable_columns[i]->convertToFullColumnIfConst()));
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
