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

void convertColumnToNullable(ColumnWithTypeAndName & column);
void convertColumnsToNullable(Block & block, size_t starting_pos = 0);
void removeColumnNullability(ColumnWithTypeAndName & column);
ColumnRawPtrs temporaryMaterializeColumns(const Block & block, const Names & names, Columns & materialized);
void removeLowCardinalityInplace(Block & block);

/// Split key and other columns by keys name list
ColumnRawPtrs extractKeysForJoin(const Names & key_names_right, const Block & right_sample_block,
                                 Block & sample_block_with_keys, Block & sample_block_with_columns_to_add);

/// Throw an exception if blocks have different types of key columns. Compare up to Nullability.
void checkTypesOfKeys(const Block & block_left, const Names & key_names_left, const Block & block_right, const Names & key_names_right);

void createMissedColumns(Block & block);
void joinTotals(const Block & totals, const Block & columns_to_add, const Names & key_names_right, Block & block);

}

}
