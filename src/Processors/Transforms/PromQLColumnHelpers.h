#pragma once

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>
#include <Common/typeid_cast.h>

namespace DB
{

/// The destination has the logical data type, but a pipeline can supply a
/// constant or sparse physical column. Unwrap it before copying one value.
inline void insertPromQLColumnValue(IColumn & destination, const IColumn & source, size_t row)
{
    const IColumn * value_column = &source;
    while (true)
    {
        if (const auto * const_column = typeid_cast<const ColumnConst *>(value_column))
        {
            value_column = &const_column->getDataColumn();
            row = 0;
        }
        else if (const auto * sparse_column = typeid_cast<const ColumnSparse *>(value_column))
        {
            row = sparse_column->getValueIndex(row);
            value_column = &sparse_column->getValuesColumn();
        }
        else
        {
            destination.insertFrom(*value_column, row);
            return;
        }
    }
}

}
