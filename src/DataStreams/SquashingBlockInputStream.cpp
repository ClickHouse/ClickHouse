#include <DataStreams/SquashingBlockInputStream.h>


namespace DB
{

SquashingBlockInputStream::SquashingBlockInputStream(
    const BlockInputStreamPtr & src, size_t min_block_size_rows, size_t min_block_size_bytes, bool reserve_memory)
    : header(src->getHeader()), transform(min_block_size_rows, min_block_size_bytes, reserve_memory)
{
    children.emplace_back(src);
}


Block SquashingBlockInputStream::readImpl()
{
    while (!all_read)
    {
        Block block = children[0]->read();
        if (!block)
            all_read = true;

        auto squashed_block = transform.add(std::move(block));
        if (squashed_block)
        {
            return squashed_block;
        }
    }
    return {};
}

}
