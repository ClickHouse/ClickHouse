#include <Storages/MeiliSearch/SinkMeiliSearch.h>
#include "Core/Field.h"
#include "IO/WriteBufferFromString.h"
#include "Processors/Formats/Impl/JSONRowOutputFormat.h"
#include "base/JSON.h"
#include "base/types.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int MEILISEARCH_EXCEPTION;
}

SinkMeiliSearch::SinkMeiliSearch(
    const MeiliSearchConfiguration & config_, const Block & sample_block_, ContextPtr local_context_, UInt64 max_block_size_)
    : SinkToStorage(sample_block_)
    , connection(config_)
    , local_context{local_context_}
    , max_block_size{max_block_size_}
    , sample_block{sample_block_}
{
}

String getStringRepresentation(const ColumnWithTypeAndName & col, size_t row)
{
    Field elem;
    if (col.column->size() <= row)
    {
        return "";
    }
    col.column->get(row, elem);
    if (elem.getType() == Field::Types::Int64)
    {
        return std::to_string(elem.get<Int64>());
    }
    else if (elem.getType() == Field::Types::UInt64)
    {
        return std::to_string(elem.get<UInt64>());
    }
    else if (elem.getType() == Field::Types::String)
    {
        return doubleQuoteString(elem.get<String>());
    }
    else if (elem.getType() == Field::Types::Float64)
    {
        return std::to_string(elem.get<Float64>());
    }
    return "";
}

String SinkMeiliSearch::getOneElement(const Block & block, int ind) const
{
    String ans = "{";
    int id = 0;
    for (const auto & col : block)
    {
        ans += doubleQuoteString(sample_block.getByPosition(id++).name) + ":" + getStringRepresentation(col, ind) + ",";
    }
    ans.back() = '}';
    return ans;
}

void SinkMeiliSearch::writeBlockData(const Block & block) const
{
    size_t max_col_size = 0;
    for (const auto & col : block)
    {
        max_col_size = std::max(max_col_size, col.column->size());
    }
    String json_array = "[";
    for (size_t i = 0; i < max_col_size; ++i)
    {
        json_array += getOneElement(block, i) + ",";
    }
    json_array.back() = ']';
    auto response = connection.updateQuery(json_array);
    JSON jres = JSON(response).begin();
    if (jres.getName() == "message")
    {
        throw Exception(ErrorCodes::MEILISEARCH_EXCEPTION, jres.getValue().toString());
    }
}

Blocks SinkMeiliSearch::splitBlocks(const Block & block, const size_t & max_rows) const
{
    /// Avoid Excessive copy when block is small enough
    if (block.rows() <= max_rows)
        return Blocks{std::move(block)};

    const size_t split_block_size = ceil(block.rows() * 1.0 / max_rows);
    Blocks split_blocks(split_block_size);

    for (size_t idx = 0; idx < split_block_size; ++idx)
        split_blocks[idx] = block.cloneEmpty();

    const size_t columns = block.columns();
    const size_t rows = block.rows();
    size_t offsets = 0;
    UInt64 limits = max_block_size;
    for (size_t idx = 0; idx < split_block_size; ++idx)
    {
        /// For last batch, limits should be the remain size
        if (idx == split_block_size - 1)
            limits = rows - offsets;
        for (size_t col_idx = 0; col_idx < columns; ++col_idx)
        {
            split_blocks[idx].getByPosition(col_idx).column = block.getByPosition(col_idx).column->cut(offsets, limits);
        }
        offsets += max_block_size;
    }

    return split_blocks;
}

void SinkMeiliSearch::consume(Chunk chunk)
{
    auto block = getHeader().cloneWithColumns(chunk.detachColumns());
    auto blocks = splitBlocks(block, max_block_size);
    for (const auto & b : blocks)
    {
        writeBlockData(b);
    }
}


}
