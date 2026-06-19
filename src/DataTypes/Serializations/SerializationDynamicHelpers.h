#pragma once

#include <DataTypes/IDataType.h>
#include <Columns/ColumnDynamic.h>

#include <optional>

namespace DB
{

class DataTypeVariant;

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

/// Iterate over indexes and calculate the total number of occurrences for each index.
std::vector<size_t> getLimitsForFlattenedDynamicColumn(const IColumn & indexes_column, size_t num_types);

/// Dynamic subcolumn lookup should treat nested `JSON` types with the same schema as compatible
/// even if only `max_dynamic_paths`/`max_dynamic_types` differ.
bool areDynamicSubcolumnTypesCompatible(const DataTypePtr & lhs, const DataTypePtr & rhs);

/// Dynamic storage variants must preserve exact storage type identity where it
/// affects serialization. For example, `JSON(max_dynamic_paths=0)` and
/// `JSON(max_dynamic_paths=1)` can expose compatible subcolumns, but they are
/// distinct stored `Dynamic` variants.
bool areDynamicStorageTypesCompatible(const DataTypePtr & existing_type, const DataTypePtr & inserted_type);

/// Find a matching variant discriminator for requested Dynamic subcolumn type.
/// Exact name matches win; otherwise we fall back to compatible nested `JSON` types.
std::optional<ColumnVariant::Discriminator> findVariantDiscriminatorForDynamicSubcolumn(
    const DataTypeVariant & variant_type,
    const DataTypePtr & requested_type);

}
