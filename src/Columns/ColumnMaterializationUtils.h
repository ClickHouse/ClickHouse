#pragma once

#include <Columns/IColumn_fwd.h>
#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

class Block;
class IDataType;
class SerializationInfoByName;

/// Coerce a column to the in-memory representation required to write it with the given serialization kind.
/// When @low_cardinality is set, builds a non-native ColumnLowCardinality from the full column
/// (SerializationLowCardinality requires a ColumnLowCardinality on the write path). Otherwise,
/// any stray non-native LowCardinality representation is removed.
ColumnPtr convertToSerialization(const ColumnPtr & column, const IDataType & type, bool low_cardinality);

/// Applies convertToSerialization to every column of @block according to @infos.
void convertToSerializations(Block & block, const SerializationInfoByName & infos);

}
