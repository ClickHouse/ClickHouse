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
        auto & src = element.column;
        if (ColumnPtr converted = src->convertToFullColumnIfConst())
            src = converted;
    }

    return res;
}

}
