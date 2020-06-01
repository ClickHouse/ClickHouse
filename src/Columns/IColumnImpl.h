/**
  * This file implements template methods of IColumn that depend on other types
  * we don't want to include.
  * Currently, this is only the scatterImpl method that depends on PODArray
  * implementation.
  */

#pragma once

#include <Columns/IColumn.h>
#include <Common/PODArray.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

template <typename Derived>
std::vector<IColumn::MutablePtr> IColumn::scatterImpl(ColumnIndex num_columns,
                                             const Selector & selector) const
{
    size_t num_rows = size();

    if (num_rows != selector.size())
        throw Exception(
                "Size of selector: " + std::to_string(selector.size()) + " doesn't match size of column: " + std::to_string(num_rows),
                ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    std::vector<MutablePtr> columns(num_columns);
    for (auto & column : columns)
        column = cloneEmpty();

    {
        size_t reserve_size = num_rows * 1.1 / num_columns;    /// 1.1 is just a guess. Better to use n-sigma rule.

        if (reserve_size > 1)
            for (auto & column : columns)
                column->reserve(reserve_size);
    }

    for (size_t i = 0; i < num_rows; ++i)
        static_cast<Derived &>(*columns[selector[i]]).insertFrom(*this, i);

    return columns;
}

template <typename Derived>
std::vector<UInt8> IColumn::compareImpl(const Derived & rhs, size_t rhs_row_num, const std::vector<UInt8> & mask, int nan_direction_hint) const
{
    size_t rows_num = size();
    std::vector<UInt8> results(rows_num, 0);

    for (size_t i = 0; i < rows_num; ++i)
    {
        if (mask[i])
            results[i] = compareAt(i, rhs_row_num, rhs, nan_direction_hint);
    }
    return results;
}

}
