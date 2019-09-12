#pragma once

#include <memory>
#include <vector>

#include <Core/Names.h>
#include <Columns/IColumn.h>

namespace DB
{

struct ColumnWithTypeAndName;
class Block;
class IColumn;
using ColumnRawPtrs = std::vector<const IColumn *>;

class IJoin
{
public:
    virtual ~IJoin() = default;

    /// Add block of data from right hand of JOIN.
    /// @returns false, if some limit was exceeded and you should not insert more data.
    virtual bool addJoinedBlock(const Block & block) = 0;

    /// Join the block with data from left hand of JOIN to the right hand data (that was previously built by calls to addJoinedBlock).
    /// Could be called from different threads in parallel.
    virtual void joinBlock(Block & block) = 0;

    virtual bool hasTotals() const { return false; }
    virtual void setTotals(const Block & block) = 0;
    virtual void joinTotals(Block & block) const = 0;

    virtual size_t getTotalRowCount() const = 0;
};

using JoinPtr = std::shared_ptr<IJoin>;


namespace JoinCommon
{

void convertColumnToNullable(ColumnWithTypeAndName & column);
void convertColumnsToNullable(Block & block, size_t starting_pos = 0);
ColumnRawPtrs temporaryMaterializeColumns(const Block & block, const Names & names, Columns & materialized);

/// Split key and other columns by keys name list
ColumnRawPtrs extractKeysForJoin(const Names & key_names_right, const Block & right_sample_block,
                                 Block & sample_block_with_keys, Block & sample_block_with_columns_to_add);

/// Throw an exception if blocks have different types of key columns. Compare up to Nullability.
void checkTypesOfKeys(const Block & block_left, const Names & key_names_left, const Block & block_right, const Names & key_names_right);

void createMissedColumns(Block & block);

}

}
