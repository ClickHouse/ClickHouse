#include <Columns/ColumnMaterializationUtils.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnLowCardinality.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Processors/Chunk.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ColumnPtr recursiveRemoveSparse(const ColumnPtr & column)
{
    if (!column)
        return column;

    if (const auto * column_tuple = typeid_cast<const ColumnTuple *>(column.get()))
    {
        auto columns = column_tuple->getColumns();
        if (columns.empty())
            return column;

        for (auto & element : columns)
            element = recursiveRemoveSparse(element);

        return ColumnTuple::create(columns);
    }

    return column->convertToFullColumnIfSparse();
}

static ColumnPtr recursiveRemoveLowCardinality(const ColumnPtr & column, bool remove_native)
{
    ColumnPtr res = column;

    if (const auto * column_array = typeid_cast<const ColumnArray *>(column.get()))
    {
        const auto & data = column_array->getDataPtr();
        auto data_no_lc = recursiveRemoveLowCardinality(data, remove_native);

        if (data.get() != data_no_lc.get())
            res = ColumnArray::create(data_no_lc, column_array->getOffsetsPtr());
    }
    else if (const auto * column_const = typeid_cast<const ColumnConst *>(column.get()))
    {
        const auto & nested = column_const->getDataColumnPtr();
        auto nested_no_lc = recursiveRemoveLowCardinality(nested, remove_native);

        if (nested.get() != nested_no_lc.get())
            res = ColumnConst::create(nested_no_lc, column_const->size());
    }
    else if (const auto * column_tuple = typeid_cast<const ColumnTuple *>(column.get()))
    {
        auto columns = column_tuple->getColumns();
        if (columns.empty())
            return column;

        for (auto & element : columns)
            element = recursiveRemoveLowCardinality(element, remove_native);

        res = ColumnTuple::create(columns);
    }
    else if (const auto * column_map = typeid_cast<const ColumnMap *>(column.get()))
    {
        const auto & nested = column_map->getNestedColumnPtr();
        auto nested_no_lc = recursiveRemoveLowCardinality(nested, remove_native);

        if (nested.get() != nested_no_lc.get())
            res = ColumnMap::create(nested_no_lc);
    }
    /// Special case when column is a lazy argument of short circuit function.
    /// We should call recursiveRemoveLowCardinality on the result column
    /// when function will be executed.
    else if (const auto * column_function = typeid_cast<const ColumnFunction *>(column.get()))
    {
        if (column_function->isShortCircuitArgument())
            res = column_function->recursivelyConvertResultToFullColumnIfLowCardinality();
    }
    else if (const auto * column_low_cardinality = typeid_cast<const ColumnLowCardinality *>(column.get()))
    {
        if (remove_native || !column_low_cardinality->isNativeLowCardinality())
            res = column_low_cardinality->convertToFullColumn();
    }

    if (res != column)
    {
        /// recursiveRemoveLowCardinality() must not change the size of a passed column!
        if (res->size() != column->size())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "recursiveRemoveLowCardinality() somehow changed the size of column {}. Old size={}, new size={}. It's a bug",
                            column->getName(), column->size(), res->size());
        }
    }

    return res;
}

ColumnPtr recursiveRemoveLowCardinality(const ColumnPtr & column)
{
    return recursiveRemoveLowCardinality(column, true);
}

ColumnPtr recursiveRemoveNonNativeLowCardinality(const ColumnPtr & column)
{
    return recursiveRemoveLowCardinality(column, false);
}

ColumnPtr materializeColumn(const ColumnPtr & column)
{
    if (!column)
        return column;

    auto res = column->convertToFullColumnIfConst();
    res = recursiveRemoveSparse(res);
    res = recursiveRemoveNonNativeLowCardinality(res);
    return res;
}

void convertToFullIfSparse(Block & block)
{
    for (auto & column : block)
        column.column = recursiveRemoveSparse(column.column);
}

Block materializeBlock(const Block & block)
{
    Block res = block;
    materializeBlockInplace(res);
    return res;
}

void materializeBlockInplace(Block & block)
{
    for (size_t i = 0; i < block.columns(); ++i)
    {
        auto & column = block.getByPosition(i);
        column.column = materializeColumn(column.column);
    }
}

void materializeColumns(Columns & columns)
{
    for (auto & column : columns)
        column = materializeColumn(column);
}

void materializeChunk(Chunk & chunk)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    materializeColumns(columns);
    chunk.setColumns(std::move(columns), num_rows);
}

ColumnPtr convertToSerialization(const ColumnPtr & column, const IDataType & type, ISerialization::Kind kind)
{
    ColumnPtr res = column;

    if (kind != ISerialization::Kind::SPARSE)
    {
        res = recursiveRemoveSparse(res);
    }

    if (kind != ISerialization::Kind::LOW_CARDINALITY)
    {
        res = recursiveRemoveNonNativeLowCardinality(res);
    }
    else if (kind == ISerialization::Kind::LOW_CARDINALITY && !res->lowCardinality())
    {
        auto new_column = createEmptyLowCardinalityColumn(type, false);
        auto & column_lc = assert_cast<ColumnLowCardinality &>(*new_column);

        column_lc.insertRangeFromFullColumn(*res, 0, res->size());
        res = std::move(new_column);
    }

    return res;
}

void convertToSerializations(Block & block, const SerializationInfoByName & infos)
{
    for (auto & column : block)
    {
        auto kind = infos.getKind(column.name);
        column.column = convertToSerialization(column.column, *column.type, kind);
    }
}

}
