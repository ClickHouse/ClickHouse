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

    auto elem_type = elem->type;
    auto elem_column = elem->column->decompress();

    if (requested_column.isSubcolumn())
    {
        auto subcolumn_name = requested_column.getSubcolumnName();
        elem_column = elem_type->tryGetSubcolumn(subcolumn_name, elem_column);
        elem_type = elem_type->tryGetSubcolumnType(subcolumn_name);

        if (!elem_type || !elem_column)
            return nullptr;
    }

    return castColumn({elem_column, elem_type, ""}, requested_column.type);
}

ColumnPtr tryGetSubcolumnFromBlock(const Block & block, const DataTypePtr & requested_column_type, const NameAndTypePair & requested_subcolumn)
{
    const auto * elem = block.findByName(requested_subcolumn.getNameInStorage());
    if (!elem)
        return nullptr;

    auto subcolumn_name = requested_subcolumn.getSubcolumnName();
    /// If requested subcolumn is dynamic, we should first perform cast and then
    /// extract the subcolumn, because the data of dynamic subcolumn can change after cast.
    if ((elem->type->hasDynamicSubcolumns() || requested_column_type->hasDynamicSubcolumns()) && !elem->type->equals(*requested_column_type))
    {
        auto casted_column = castColumn({elem->column, elem->type, ""}, requested_column_type);
        auto elem_column = requested_column_type->tryGetSubcolumn(subcolumn_name, casted_column);
        auto elem_type = requested_column_type->tryGetSubcolumnType(subcolumn_name);

        if (!elem_type || !elem_column)
            return nullptr;

        return elem_column;
    }

    auto elem_column = elem->type->tryGetSubcolumn(subcolumn_name, elem->column);
    auto elem_type = elem->type->tryGetSubcolumnType(subcolumn_name);

    if (!elem_type || !elem_column)
        return nullptr;

    return castColumn({elem_column, elem_type, ""}, requested_subcolumn.type);
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
