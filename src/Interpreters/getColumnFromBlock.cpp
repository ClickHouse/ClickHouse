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

    /// We must derive the subcolumn from the parent converted to the requested storage type,
    /// ONLY when the block's column type differs from the requested one AND either:
    ///  - the requested subcolumn is dynamic (its data can change after cast), or
    ///  - the block's (older) type lacks the requested subcolumn while the requested type has it,
    ///    i.e. after a metadata-only `ALTER MODIFY COLUMN` (e.g. `T` -> `Nullable(T)`,
    ///    or `Variant(...)` gaining an element). Extracting from the converted column yields the
    ///    correct value (e.g. an all-0 `.null` map).
    /// For subcolumns that already exist in the block's type (e.g. a tuple element after a sibling
    /// element's type changed), we must NOT cast the parent: a non-convertible sibling value
    /// would throw before we ever extract the still-readable subcolumn. Fall through to extract the
    /// subcolumn directly and cast only that.
    bool block_type_has_subcolumn = elem->type->tryGetSubcolumnType(subcolumn_name) != nullptr;
    if (!elem->type->equals(*requested_column_type) && (is_dynamic || !block_type_has_subcolumn))
    {
        if (is_dynamic)
        {
            /// Dynamic subcolumn data can change after the cast, so cast the whole parent and
            /// return the extracted subcolumn directly (an extra cast could alter the data).
            auto cast_column = castColumn({elem->column->decompress(), elem->type, ""}, requested_column_type);
            auto dyn_column = requested_column_type->tryGetSubcolumn(subcolumn_name, cast_column);
            if (!dyn_column)
                return nullptr;
            return dyn_column;
        }

        /// Casting the WHOLE parent can throw when a sibling element is non-convertible (e.g. a
        /// deep path `t.a.null` where sibling `t.b String -> UInt64` holds a non-numeric value),
        /// even though the requested subcolumn lives under a directly-readable branch. Descend to
        /// the deepest ancestor of the requested subcolumn that still resolves in the block's
        /// (older) type, cast only that branch to its requested type, then extract the remaining
        /// suffix from the converted branch.
        auto source_column = elem->column->decompress()->convertToFullColumnIfConst();

        /// Find the deepest existing ancestor: the longest strict '.'-prefix of `subcolumn_name`
        /// that resolves in the block's type. An empty ancestor means "the whole parent column".
        String ancestor;
        for (size_t pos = subcolumn_name.rfind('.'); pos != String::npos; pos = subcolumn_name.rfind('.', pos - 1))
        {
            auto prefix = subcolumn_name.substr(0, pos);
            if (elem->type->tryGetSubcolumnType(prefix))
            {
                ancestor = prefix;
                break;
            }
            if (pos == 0)
                break;
        }

        /// Column and type of the ancestor branch in both the block's (older) and requested types.
        ColumnPtr ancestor_column = source_column;
        DataTypePtr ancestor_type = elem->type;
        DataTypePtr requested_ancestor_type = requested_column_type;
        String remaining_subcolumn = subcolumn_name;

        if (!ancestor.empty())
        {
            ancestor_column = elem->type->tryGetSubcolumn(ancestor, source_column);
            ancestor_type = elem->type->tryGetSubcolumnType(ancestor);
            requested_ancestor_type = requested_column_type->tryGetSubcolumnType(ancestor);
            remaining_subcolumn = subcolumn_name.substr(ancestor.size() + 1);
        }

        if (!ancestor_column || !ancestor_type || !requested_ancestor_type)
            return nullptr;

        auto cast_column
            = castColumn({ancestor_column, ancestor_type, ""}, requested_ancestor_type)->convertToFullColumnIfConst();
        auto elem_column = requested_ancestor_type->tryGetSubcolumn(remaining_subcolumn, cast_column);
        auto elem_type = requested_ancestor_type->tryGetSubcolumnType(remaining_subcolumn);

        if (!elem_type || !elem_column)
            return nullptr;

        return castColumn({elem_column, elem_type, ""}, requested_subcolumn.type);
    }

    /// Unwrap a possible ColumnConst before extracting the subcolumn: subcolumn extraction
    /// (e.g. `SerializationNullable::enumerateStreams`) `assert_cast`s the column to its concrete
    /// class and would trip on a `ColumnConst` wrapper. This can happen when the column comes from
    /// an earlier pipeline step that produced a constant (e.g. an on-fly `UPDATE x = 0`).
    auto source_column = elem->column->decompress()->convertToFullColumnIfConst();
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
