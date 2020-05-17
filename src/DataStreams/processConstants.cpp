#include <DataStreams/processConstants.h>

namespace DB
{

void removeConstantsFromBlock(Block & block)
{
    size_t columns = block.columns();
    size_t i = 0;
    while (i < columns)
    {
        if (block.getByPosition(i).column && isColumnConst(*block.getByPosition(i).column))
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
    /// Note: This code is not correct if column description contains column numbers instead of column names.
    /// Hopefully, everywhere where it is used, column description contains names.
    description.erase(std::remove_if(description.begin(), description.end(),
        [&](const SortColumnDescription & elem)
        {
            const auto & column = !elem.column_name.empty() ? header.getByName(elem.column_name)
                                                      : header.safeGetByPosition(elem.column_number);
            return column.column && isColumnConst(*column.column);
        }), description.end());
}


void enrichBlockWithConstants(Block & block, const Block & header)
{
    size_t rows = block.rows();
    size_t columns = header.columns();

    for (size_t i = 0; i < columns; ++i)
    {
        const auto & col_type_name = header.getByPosition(i);
        if (col_type_name.column && isColumnConst(*col_type_name.column))
            block.insert(i, {col_type_name.column->cloneResized(rows), col_type_name.type, col_type_name.name});
    }
}
}
