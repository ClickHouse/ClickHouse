#pragma once

#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/IDataType.h>

namespace DB
{

/// Convert a column produced by an input format to the type the caller requested in the header.
/// Formats with a fixed source schema decode into their own types, so a column has to be converted
/// before it leaves the format. Otherwise it contradicts the type it is declared with, and every
/// reader downstream interprets its memory as the wrong type.
/// Does nothing when the types already match. On failure the column name and both types are added
/// to the message.
void castColumnToRequestedType(ColumnWithTypeAndName & column, const DataTypePtr & requested_type);

}
