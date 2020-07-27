#pragma once

#include <Interpreters/IJoin.h>

namespace DB
{

struct ColumnWithTypeAndName;
class Block;
class IColumn;
using ColumnRawPtrs = std::vector<const IColumn *>;

namespace JoinCommon
{

void convertColumnToNullable(ColumnWithTypeAndName & column, bool low_card_nullability = false);
void convertColumnsToNullable(Block & block, size_t starting_pos = 0);
void removeColumnNullability(ColumnWithTypeAndName & column);
Columns materializeColumns(const Block & block, const Names & names);
ColumnRawPtrs materializeColumnsInplace(Block & block, const Names & names);
ColumnRawPtrs getRawPointers(const Columns & columns);
void removeLowCardinalityInplace(Block & block);

/// Split key and other columns by keys name list
void splitAdditionalColumns(const Block & sample_block, const Names & key_names, Block & block_keys, Block & block_others);
ColumnRawPtrs extractKeysForJoin(const Block & block_keys, const Names & key_names_right);

/// Throw an exception if blocks have different types of key columns. Compare up to Nullability.
void checkTypesOfKeys(const Block & block_left, const Names & key_names_left, const Block & block_right, const Names & key_names_right);

void createMissedColumns(Block & block);
void joinTotals(const Block & totals, const Block & columns_to_add, const Names & key_names_right, Block & block);

}

}
