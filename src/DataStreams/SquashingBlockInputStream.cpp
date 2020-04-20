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
    if (all_read)
        return {};

    while (true)
    {
        Block block = children[0]->read();
        if (!block)
            all_read = true;

        SquashingTransform::Result result = transform.add(block.mutateColumns());
        if (result.ready)
        {
            if (result.columns.empty())
                return {};
            return header.cloneWithColumns(std::move(result.columns));
        }
    }
}

}
