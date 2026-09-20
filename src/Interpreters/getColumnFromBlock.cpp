#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/getColumnFromBlock.h>

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

    if (!elem->column)
        return nullptr;

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
    bool is_dynamic = elem->type->hasDynamicStructure() || requested_column_type->hasDynamicStructure();

    /// Cast the parent to the requested type first, then extract, when types differ and either the
    /// subcolumn is dynamic (its data can change after cast) or the block's (older) type lacks it
    /// (metadata-only `ALTER MODIFY COLUMN T -> Nullable(T)`). Otherwise the subcolumn is readable
    /// from the block directly, so extract it below without casting the whole parent.
    auto source_column = elem->column->decompress()->convertToFullColumnIfConst();

    bool block_type_has_subcolumn = elem->type->tryGetSubcolumnType(subcolumn_name) != nullptr;
    if (!elem->type->equals(*requested_column_type) && (is_dynamic || !block_type_has_subcolumn))
    {
        auto cast_column = castColumn({source_column, elem->type, ""}, requested_column_type);
        auto elem_column = requested_column_type->tryGetSubcolumn(subcolumn_name, cast_column);
        auto elem_type = requested_column_type->tryGetSubcolumnType(subcolumn_name);

        if (!elem_type || !elem_column)
            return nullptr;

        /// Dynamic subcolumn data already matches after the cast; an extra cast could alter it.
        if (is_dynamic)
            return elem_column;

        return castColumn({elem_column, elem_type, ""}, requested_subcolumn.type);
    }

    auto elem_column = elem->type->tryGetSubcolumn(subcolumn_name, source_column);
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
