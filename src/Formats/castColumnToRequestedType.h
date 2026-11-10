#pragma once

#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/IDataType.h>

namespace DB
{

/// Convert a column produced by an input format to the type the caller requested in the header.
/// Formats with a fixed source schema decode into their own types, so a column has to be converted
/// before it leaves the format. Otherwise it contradicts the type it is declared with, and every
/// reader downstream interprets its memory as the wrong type.
/// Named tuples are matched by name, as a file is read by its schema.
/// Does nothing when the types already match. On failure the column name and both types are added
/// to the message.
void castColumnToRequestedType(ColumnWithTypeAndName & column, const DataTypePtr & requested_type);

/// Gives a named tuple `column` (possibly Nullable) the elements of the named tuple `requested_type` (possibly
/// Nullable) at the top level: an element it has keeps its type, a missing one is filled with defaults, and the
/// others are dropped. Does nothing unless both are named tuples.
void matchTupleElementsByName(ColumnWithTypeAndName & column, const DataTypePtr & requested_type);

}
