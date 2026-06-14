#pragma once

#include <Columns/IColumn_fwd.h>
#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

class Block;
class IDataType;
class SerializationInfoByName;

/// Coerce a column to the in-memory representation required to write it with the given serialization kind.
/// For the LOW_CARDINALITY kind, builds a non-native ColumnLowCardinality from the full column
/// (SerializationLowCardinality requires a ColumnLowCardinality on the write path). For other kinds,
/// any stray non-native LowCardinality representation is removed.
ColumnPtr convertToSerialization(const ColumnPtr & column, const IDataType & type, const ISerialization::KindStack & kind_stack);
void convertToSerializations(Block & block, const SerializationInfoByName & infos);

}
