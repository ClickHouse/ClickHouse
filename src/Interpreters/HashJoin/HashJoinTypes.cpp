#include <Interpreters/HashJoin/HashJoinTypes.h>

#include <Columns/ColumnNullable.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/JoinUtils.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

void correctNullabilityInplace(ColumnWithTypeAndName & column, bool nullable)
{
    if (nullable)
    {
        JoinCommon::convertColumnToNullable(column);
    }
    else
    {
        /// We have to replace values masked by NULLs with defaults.
        if (column.column)
            if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(&*column.column))
                column.column = JoinCommon::filterWithBlanks(column.column, nullable_column->getNullMapColumn().getData(), true);

        JoinCommon::removeColumnNullability(column);
    }
}

}

Columns HashJoinTypes::materializeStoredBlock(StoredBlock & stored_block, const ColumnAccessIndexes & access_indexes)
{
    const auto & stored_columns = stored_block.columns;
    const auto & selector = stored_block.selector;

    MutableColumns row_store_columns;
    if (stored_block.hasRowStore())
    {
        if (selector.isContinuousRange())
        {
            auto [start, end] = selector.getRange();
            row_store_columns = stored_block.row_store->scatterRows(start, end - start);
        }
        else
            row_store_columns = stored_block.row_store->scatterRows(selector.getIndexes().getData());
        stored_block.row_store.reset();
    }

    Columns columnar_columns;
    columnar_columns.reserve(stored_block.columns.size());
    if (selector.size() == stored_block.blockRows())
        columnar_columns = stored_block.columns;
    else if (selector.isContinuousRange())
    {
        auto [start, end] = selector.getRange();
        for (const auto & c : stored_columns)
            columnar_columns.push_back(c->cut(start, end - start));
    }
    else
    {
        const auto & indexes = selector.getIndexes();
        for (const auto & c : stored_columns)
            columnar_columns.push_back(c->index(indexes, /*limit*/ 0));
    }

    if (access_indexes.empty())
        return columnar_columns;

    Columns result(access_indexes.size());
    for (size_t i = 0; i < access_indexes.size(); ++i)
    {
        const auto & access_index = access_indexes[i];
        if (access_index.type == ColumnAccessIndex::Type::RowStore)
            result[i] = std::move(row_store_columns[access_index.index]);
        else
            result[i] = std::move(columnar_columns[access_index.index]);
    }
    return result;
}

size_t HashJoinTypes::NullMapHolder::allocatedBytes() const
{
    if (!column)
        return 0;
    size_t rows = column->size();
    if (rows == 0)
        return 0;
    if (rows < selector_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The column size is smaller than the cached size");
    return column->allocatedBytes() * selector_rows / rows;
}

Block HashJoinTypes::restoreRightBlock(const Block & saved_block, const Block & right_sample_block)
{
    Block restored;
    for (const auto & sample_column : right_sample_block)
    {
        auto column = saved_block.getByName(sample_column.name);
        correctNullabilityInplace(column, isNullableOrLowCardinalityNullable(sample_column.type));
        restored.insert(std::move(column));
    }
    return restored;
}

}
