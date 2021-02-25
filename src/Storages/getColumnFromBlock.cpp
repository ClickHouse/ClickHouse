#include <Storages/getColumnFromBlock.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}

ColumnPtr getColumnFromBlock(const Block & block, const NameAndTypePair & column, bool is_compressed)
{
    /// Some subcolumns may exists in block as regular columns.
    /// E.g. parts of Nested as arrays with enabled setting 'flatten_nested'.
    if (block.has(column.name))
    {
        auto result_column = block.getByName(column.name).column;
        if (is_compressed)
            result_column = result_column->decompress();

        return result_column;
    }
    else if (auto name_in_storage = column.getNameInStorage();
        column.isSubcolumn() && block.has(name_in_storage))
    {
        auto result_column = block.getByName(name_in_storage).column;
        if (is_compressed)
            result_column = result_column->decompress();

        return column.getTypeInStorage()->getSubcolumn(column.getSubcolumnName(), *result_column);
    }

    throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE,
        "No such column {} in block. There are only columns: {}",
            column.name, toString(block.getNames()));
}

}
