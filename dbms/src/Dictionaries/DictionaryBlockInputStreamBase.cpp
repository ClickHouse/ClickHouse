#include <Dictionaries/DictionaryBlockInputStreamBase.h>

namespace DB
{

DictionaryBlockInputStreamBase::DictionaryBlockInputStreamBase(size_t rows_count, size_t max_block_size)
    : rows_count(rows_count), max_block_size(max_block_size), next_row(0)
{
}

Block DictionaryBlockInputStreamBase::readImpl()
{
    if (next_row == rows_count)
        return Block();

    size_t block_size = std::min<size_t>(max_block_size, rows_count - next_row);
    Block block = getBlock(next_row, block_size);
    next_row += block_size;
    return block;
}

Block DictionaryBlockInputStreamBase::getHeader() const
{
    return getBlock(0, 0);
}

}
