#include <DataStreams/materializeBlock.h>


namespace DB
{

Block materializeBlock(const Block & block)
{
    if (!block)
        return block;

    Block res = block;
    size_t columns = res.columns();
    for (size_t i = 0; i < columns; ++i)
    {
        auto & element = res.getByPosition(i);
        element.column = element.column->convertToFullColumnIfConst();
    }

    return res;
}

void materializeBlockInplace(Block & block)
{
    for (size_t i = 0; i < block.columns(); ++i)
        block.getByPosition(i).column = block.getByPosition(i).column->convertToFullColumnIfConst();
}

}
