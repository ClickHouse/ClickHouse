#pragma once

#include <DataTypes/IDataType.h>
#include <Columns/ColumnObject.h>

namespace DB
{

/// Object column can store only limited number of paths as subcolumns.
/// If this limit is reached, all other paths are stored together in a
/// single column called shared data.
/// This function collects all the paths stored in Object column into
/// separate columns (except typed paths, they are not returned and
/// should be processed separately).
std::vector<std::pair<String, ColumnPtr>> flattenPaths(const ColumnObject & object_column);

/// Insert data from flattened representation of an Object column to a usual Object column.
void unflattenAndInsertPaths(const std::vector<String> & flattened_paths, std::vector<ColumnPtr> && flattened_columns, ColumnObject & object_column, size_t num_rows);

}
