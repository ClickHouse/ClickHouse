#pragma once
#include <Columns/IColumn.h>
#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

class Block;
class Chunk;
class SerializationInfoByName;

/// Remove Sparse recursively from all nested columns.
ColumnPtr recursiveRemoveSparse(const ColumnPtr & column);

/// Remove LowCardinality recursively from all nested columns.
ColumnPtr recursiveRemoveLowCardinality(const ColumnPtr & column);
ColumnPtr recursiveRemoveNonNativeLowCardinality(const ColumnPtr & column);

/// Recursively remove all serialization modifiers from column.
ColumnPtr materializeColumn(const ColumnPtr & column);

void convertToFullIfSparse(Block & block);
Block materializeBlock(const Block & block);
void materializeBlockInplace(Block & block);

void materializeColumns(Columns & columns);
void materializeChunk(Chunk & chunk);

ColumnPtr convertToSerialization(const ColumnPtr & column, const IDataType & type, ISerialization::Kind kind);
void convertToSerializations(Block & block, const SerializationInfoByName & infos);

}
