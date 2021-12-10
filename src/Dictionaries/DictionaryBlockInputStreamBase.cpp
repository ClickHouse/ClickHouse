#include "DictionaryBlockInputStreamBase.h"

namespace DB
{
DictionaryBlockInputStreamBase::DictionaryBlockInputStreamBase(size_t rows_count_, size_t max_block_size_)
    : rows_count(rows_count_), max_block_size(max_block_size_)
{
}

Block DictionaryBlockInputStreamBase::readImpl()
{
    if (next_row == rows_count)
        return Block();

    size_t block_size = std::min(max_block_size, rows_count - next_row);
    Block block = getBlock(next_row, block_size);
    next_row += block_size;
    return block;
}

Block DictionaryBlockInputStreamBase::getHeader() const
{
    return getBlock(0, 0);
}

}
