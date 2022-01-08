#include <Storages/MeiliSearch/SinkMeiliSearch.h>
#include "Core/Field.h"
#include "Formats/FormatFactory.h"
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

void extractData(std::string_view& view) {
    int ind = view.find("\"data\":") + 9;
    view.remove_prefix(ind);
    int bal = ind = 1;
    while (bal > 0) {
        if (view[ind] == '[') ++bal;
        else if (view[ind] == ']') --bal;
        ++ind;
    } 
    view.remove_suffix(view.size() - ind);
}

void SinkMeiliSearch::writeBlockData(const Block & block) const
{
    FormatSettings settings = getFormatSettings(local_context);
    settings.json.quote_64bit_integers = false;
    WriteBufferFromOwnString buf;
    auto writer = FormatFactory::instance().getOutputFormat("JSON", buf, sample_block, local_context, {}, settings);
    writer->write(block);
    writer->flush();
    writer->finalize();

    std::string_view vbuf(buf.str());
    extractData(vbuf);

    auto response = connection.updateQuery(vbuf);
    auto jres = JSON(response).begin();
    if (jres.getName() == "message")
        throw Exception(ErrorCodes::MEILISEARCH_EXCEPTION, jres.getValue().toString());
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
            split_blocks[idx].getByPosition(col_idx).column = block.getByPosition(col_idx).column->cut(offsets, limits);
        
        offsets += max_block_size;
    }

    return split_blocks;
}

void SinkMeiliSearch::consume(Chunk chunk)
{
    auto block = getHeader().cloneWithColumns(chunk.detachColumns());
    auto blocks = splitBlocks(block, max_block_size);
    for (const auto & b : blocks)
        writeBlockData(b);
}


}
