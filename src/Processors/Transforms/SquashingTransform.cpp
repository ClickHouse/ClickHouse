#include <Processors/Transforms/SquashingTransform.h>

namespace DB
{

SquashingTransform::SquashingTransform(const Block & header_, size_t min_block_size_rows_, size_t min_block_size_bytes_)
    : ISimpleTransform(header_, header_, true)
    , header(header_), transformer(min_block_size_rows_, min_block_size_bytes_)
{
}
void SquashingTransform::transform(Chunk & chunk)
{
    auto columns = chunk.detachColumns();

    Block block = header.cloneWithoutColumns();
    block.setColumns(columns);

    if (auto squashed_block = transformer.add(std::move(block)))
        chunk.setColumns(squashed_block.getColumns(), squashed_block.rows());
}

}
