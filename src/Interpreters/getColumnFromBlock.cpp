#include <Interpreters/getColumnFromBlock.h>
#include <Interpreters/castColumn.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
}

ColumnPtr tryGetColumnFromBlock(const Block & block, const NameAndTypePair & requested_column)
{
    const auto * elem = block.findByName(requested_column.getNameInStorage());
    if (!elem)
        return nullptr;

    DataTypePtr elem_type;
    ColumnPtr elem_column;

    if (requested_column.isSubcolumn())
    {
        auto subcolumn_name = requested_column.getSubcolumnName();
        elem_type = elem->type->tryGetSubcolumnType(subcolumn_name);
        elem_column = elem->type->tryGetSubcolumn(subcolumn_name, elem->column);

        if (!elem_type || !elem_column)
            return nullptr;
    }
    else
    {
        elem_type = elem->type;
        elem_column = elem->column;
    }

    return castColumn({elem_column, elem_type, ""}, requested_column.type);
}

ColumnPtr getColumnFromBlock(const Block & block, const NameAndTypePair & requested_column)
{
    auto result_column = tryGetColumnFromBlock(block, requested_column);
    if (!result_column)
        throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK,
            "Not found column or subcolumn {} in block. There are only columns: {}",
                requested_column.name, block.dumpNames());

    return result_column;
}

}
