#pragma once

#include <Core/BlockMissingValues.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/IDataType.h>
#include <Common/PODArray_fwd.h>

namespace DB
{

class NestedColumnExtractHelper;

struct RequestedTypeCastSettings
{
    bool null_as_default = false;
    /// The format's case-insensitive column matching, applied to tuple elements as well.
    bool case_insensitive = false;
    /// A NULL tuple requested as a non-Nullable tuple is its default also without `null_as_default`.
    bool null_tuple_as_default = false;
    /// Named tuples are matched by name even with no name in common; otherwise they are paired as `CAST` pairs them.
    bool tuple_elements_by_name = true;
    /// Otherwise no value is converted: the types may differ only by Nullable, removed with `null_as_default`, and LowCardinality.
    bool allow_types_conversion = true;
};

/// Convert a column produced by an input format to the type the caller requested in the header.
/// Formats with a fixed source schema decode into their own types, so a column has to be converted
/// before it leaves the format. Otherwise it contradicts the type it is declared with, and every
/// reader downstream interprets its memory as the wrong type.
/// Named tuples are matched by name, as a file is read by its schema: a missing element gets its type's default, others are dropped.
/// A NULL row's value is never converted: the row gets the requested type's default, at the top level marked in `block_missing_values`.
/// Does nothing when the types already match. On failure the column name and both types are added
/// to the message.
void castColumnToRequestedType(ColumnWithTypeAndName & column, const DataTypePtr & requested_type,
    const RequestedTypeCastSettings & settings = {}, size_t column_i = 0, BlockMissingValues * block_missing_values = nullptr);

/// Same, but only the rows that `null_map` leaves unset are converted; the others get the default of `requested_type`.
void castColumnToRequestedType(ColumnWithTypeAndName & column, const DataTypePtr & requested_type,
    const PaddedPODArray<UInt8> & null_map, const RequestedTypeCastSettings & settings = {});

/// Defaults of `header_column`'s type for a column the data lacks, marked as missing; THERE_IS_NO_COLUMN unless `allow_missing_columns`.
/// A subcolumn of a column that `nested` extracts from gets, as its outer arrays, the arrays of the deepest column on its path that `nested` has.
ColumnPtr createMissingColumn(const ColumnWithTypeAndName & header_column, size_t num_rows, bool allow_missing_columns, size_t column_i,
    BlockMissingValues * block_missing_values, NestedColumnExtractHelper * nested = nullptr);

/// Gives the named tuple `column` the top-level elements of the named tuple `requested_type` (both possibly Nullable): those it has keep
/// their type, missing ones are filled with defaults, and the others are dropped. Does nothing unless both are named tuples.
void matchTupleElementsByName(ColumnWithTypeAndName & column, const DataTypePtr & requested_type);

}
