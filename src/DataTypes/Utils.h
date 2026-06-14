#pragma once

#include <DataTypes/IDataType.h>
#include <Core/Names.h>

namespace DB
{

/** Returns true if from_type can be safely cast to to_type.
  *
  * Examples:
  * From type UInt8 to type UInt16 returns true.
  * From type UInt16 to type UInt8 returns false.
  * From type String to type LowCardinality(String) returns true.
  * From type LowCardinality(String) to type String returns true.
  * From type String to type UInt8 returns false.
  */
bool canBeSafelyCast(const DataTypePtr & from_type, const DataTypePtr & to_type);

/** Drop the given leaf substream paths from a named-Tuple-shaped type, recursing through
  * Array / Nullable / Map / nested Tuple wrappers. Returns the narrowed type.
  *
  * `expired_substreams` contains dotted storage names rooted at `column_name`
  * (e.g. for `column_name = "data"`, an entry like "data.c2s.gold" drops `gold`
  * from inside `data.c2s`).
  *
  * Returns nullptr if every leaf of the type is in `expired_substreams` — caller treats
  * this as "the entire column is expired" and removes it.
  */
DataTypePtr narrowDataTypeByExpiredSubstreams(
    const DataTypePtr & full_type,
    const String & column_name,
    const NameSet & expired_substreams);

}
