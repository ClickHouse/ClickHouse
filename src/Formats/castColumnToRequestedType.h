#pragma once

#include <Core/BlockMissingValues.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/IDataType.h>
#include <Common/PODArray_fwd.h>

namespace DB
{

struct RequestedTypeCastSettings
{
    /// `input_format_null_as_default`: a NULL where the requested type has no Nullable becomes its default.
    bool null_as_default = false;
    /// The format's case-insensitive column matching, applied to tuple elements as well.
    bool case_insensitive = false;
    /// A NULL tuple requested as a non-Nullable tuple is its default also without `null_as_default`.
    bool null_tuple_as_default = false;
};

/// Convert a column produced by an input format to the type the caller requested in the header.
/// Formats with a fixed source schema decode into their own types, so a column has to be converted
/// before it leaves the format. Otherwise it contradicts the type it is declared with, and every
/// reader downstream interprets its memory as the wrong type.
/// Named tuples are matched by name, as a file is read by its schema: an element the column does not have gets the
/// default of its type, the others are dropped. The rows of a NULL tuple are not converted; they get the default of
/// the requested type. With `block_missing_values`, a NULL replaced by a default at the top level is marked as missing.
/// Does nothing when the types already match. On failure the column name and both types are added
/// to the message.
void castColumnToRequestedType(ColumnWithTypeAndName & column, const DataTypePtr & requested_type,
    const RequestedTypeCastSettings & settings = {}, size_t column_i = 0, BlockMissingValues * block_missing_values = nullptr);

/// Same, but only the rows that `null_map` leaves unset are converted; the others get the default of `requested_type`.
void castColumnToRequestedType(ColumnWithTypeAndName & column, const DataTypePtr & requested_type,
    const PaddedPODArray<UInt8> & null_map, const RequestedTypeCastSettings & settings = {});

/// The column for `header_column` that the data does not have: the defaults of its type, marked as missing; or
/// THERE_IS_NO_COLUMN unless `allow_missing_columns`.
ColumnPtr createMissingColumn(const ColumnWithTypeAndName & header_column, size_t num_rows, bool allow_missing_columns, size_t column_i,
    BlockMissingValues * block_missing_values);

/// Gives a named tuple `column` (possibly Nullable) the elements of the named tuple `requested_type` (possibly
/// Nullable) at the top level: an element it has keeps its type, a missing one is filled with defaults, and the
/// others are dropped. Does nothing unless both are named tuples.
void matchTupleElementsByName(ColumnWithTypeAndName & column, const DataTypePtr & requested_type);

}
