#pragma once

#include <DataTypes/IDataType.h>
#include <Columns/ColumnDynamic.h>

namespace DB
{

/// Dynamic column can store only limited number of types as subcolumns.
/// If this limit is reached, all other types are stored together in a single
/// column called shared variant.
/// This struct represents flattened version of a Dynamic column where
/// all types are stored separately
struct FlattenedDynamicColumn
{
    /// List of types that are present in the Dynamic column.
    DataTypes types;
    /// Columns with data for each type.
    /// columns[i] contains values of type types[i].
    std::vector<ColumnPtr> columns;
    /// Column with indexes of types for each row in Dynamic column.
    ColumnPtr indexes_column;
    /// Type of indexes column is dynamic and depends on the number of types.
    DataTypePtr indexes_type;
};

/// Create flattened Dynamic column representation.
FlattenedDynamicColumn flattenDynamicColumn(const ColumnDynamic & dynamic_column);

/// Insert data from flattened Dynamic column representation to a usual Dynamic column
void unflattenDynamicColumn(FlattenedDynamicColumn && flattened_column, ColumnDynamic & dynamic_column);

/// Type of indexes for flattened Dynamic column is dynamic and can be UInt8/UInt16/UInt32/UInt64
/// depending on the number of unique indexes.
DataTypePtr getIndexesTypeForFlattenedDynamicColumn(size_t max_index);

/// Iterate over indexes and calculate the total number of occurrences for each index.
std::vector<size_t> getLimitsForFlattenedDynamicColumn(const IColumn & indexes_column, size_t num_types);

}
