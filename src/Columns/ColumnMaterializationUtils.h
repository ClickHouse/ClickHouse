#pragma once
#include <Columns/IColumn.h>

namespace DB
{

class Block;
class Chunk;

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

}
