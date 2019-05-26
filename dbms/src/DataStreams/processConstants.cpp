#include <DataStreams/processConstants.h>

namespace DB
{

void removeConstantsFromBlock(Block & block)
{
    size_t columns = block.columns();
    size_t i = 0;
    while (i < columns)
    {
        if (block.getByPosition(i).column->isColumnConst())
        {
            block.erase(i);
            --columns;
        }
        else
            ++i;
    }
}


void removeConstantsFromSortDescription(const Block & header, SortDescription & description)
{
    description.erase(std::remove_if(description.begin(), description.end(),
        [&](const SortColumnDescription & elem)
        {
            if (!elem.column_name.empty())
                return header.getByName(elem.column_name).column->isColumnConst();
            else
                return header.safeGetByPosition(elem.column_number).column->isColumnConst();
        }), description.end());
}


void enrichBlockWithConstants(Block & block, const Block & header)
{
    size_t rows = block.rows();
    size_t columns = header.columns();

    for (size_t i = 0; i < columns; ++i)
    {
        const auto & col_type_name = header.getByPosition(i);
        if (col_type_name.column->isColumnConst())
            block.insert(i, {col_type_name.column->cloneResized(rows), col_type_name.type, col_type_name.name});
    }
}
}
