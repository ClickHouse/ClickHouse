#include <Columns/ColumnArray.h>
#include <Columns/ColumnCompressed.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationArray.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/getColumnFromBlock.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
}

namespace
{

const Columns * getCompressedParts(const IColumn & column, size_t expected)
{
    const auto * compressed = typeid_cast<const ColumnCompressed *>(&column);
    if (!compressed || compressed->getParts().size() != expected)
        return nullptr;
    return &compressed->getParts();
}

/// Returns what `type.tryGetSubcolumn(subcolumn_name, column.decompress())` would, decompressing only the parts of
/// `column` that the subcolumn consists of, or nullptr if that needs the whole column.
ColumnPtr tryDecompressSubcolumn(const IDataType & type, const IColumn & column, std::string_view subcolumn_name)
{
    switch (type.getTypeId())
    {
        case TypeIndex::String:
        {
            const auto * parts = getCompressedParts(column, 1);
            if (parts && subcolumn_name == "size")
                return arrayOffsetsToSizes(*(*parts)[0]->decompress());
            return nullptr;
        }
        case TypeIndex::Array:
        {
            const auto * parts = getCompressedParts(column, 2);
            if (parts && subcolumn_name == "size0")
                return arrayOffsetsToSizes(*(*parts)[1]->decompress());
            return nullptr;
        }
        case TypeIndex::Nullable:
        {
            /// A `null` subcolumn of the nested type hides the null map, see `SerializationNullable::enumerateStreams`.
            const auto * parts = getCompressedParts(column, 2);
            if (parts && subcolumn_name == "null" && !assert_cast<const DataTypeNullable &>(type).getNestedType()->hasSubcolumn("null"))
                return (*parts)[1]->decompress();
            return nullptr;
        }
        case TypeIndex::Tuple:
        {
            /// A name without a dot can only be an element, not a subcolumn of one.
            if (subcolumn_name.contains('.'))
                return nullptr;
            const auto & names = assert_cast<const DataTypeTuple &>(type).getElementNames();
            const auto * parts = getCompressedParts(column, names.size());
            if (!parts)
                return nullptr;
            for (size_t i = 0; i < names.size(); ++i)
            {
                if (names[i] == subcolumn_name)
                    return (*parts)[i]->decompress();
            }
            return nullptr;
        }
        case TypeIndex::Map:
        {
            const auto * map_parts = getCompressedParts(column, 1);
            const auto * array_parts = map_parts ? getCompressedParts(*(*map_parts)[0], 2) : nullptr;
            if (!array_parts)
                return nullptr;
            const auto & offsets = (*array_parts)[1];
            if (subcolumn_name == "size0")
                return arrayOffsetsToSizes(*offsets->decompress());
            const auto * tuple_parts = getCompressedParts(*(*array_parts)[0], 2);
            if (!tuple_parts)
                return nullptr;
            if (subcolumn_name == "keys")
                return ColumnArray::create((*tuple_parts)[0]->decompress(), offsets->decompress());
            if (subcolumn_name == "values")
                return ColumnArray::create((*tuple_parts)[1]->decompress(), offsets->decompress());
            return nullptr;
        }
        default:
            return nullptr;
    }
}

}

ColumnPtr tryDecompressArrayOffsets(const IDataType & type, const IColumn & column)
{
    if (!isArray(type))
        return nullptr;
    const auto * parts = getCompressedParts(column, 2);
    return parts ? (*parts)[1]->decompress() : nullptr;
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
    bool block_type_has_subcolumn = elem->type->tryGetSubcolumnType(subcolumn_name) != nullptr;
    if (!elem->type->equals(*requested_column_type) && (is_dynamic || !block_type_has_subcolumn))
    {
        auto source_column = elem->column->decompress()->convertToFullColumnIfConst();
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

    auto elem_column = tryDecompressSubcolumn(*elem->type, *elem->column, subcolumn_name);
    if (!elem_column)
        elem_column = elem->type->tryGetSubcolumn(subcolumn_name, elem->column->decompress()->convertToFullColumnIfConst());
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
