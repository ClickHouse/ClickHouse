#include "DictionarySourceBase.h"

namespace DB
{
DictionarySourceBase::DictionarySourceBase(const Block & header, size_t rows_count_, size_t max_block_size_)
    : SourceWithProgress(header), rows_count(rows_count_), max_block_size(max_block_size_)
{
}

Chunk DictionarySourceBase::generate()
{
    if (next_row == rows_count)
        return {};

    size_t size = std::min(max_block_size, rows_count - next_row);
    auto block = getBlock(next_row, size);
    next_row += size;
    return Chunk(block.getColumns(), size);
}

}
