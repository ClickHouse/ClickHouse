#pragma once

#include <memory>
#include <vector>

#include <Core/Names.h>
#include <Columns/IColumn.h>
#include <DataStreams/IBlockStream_fwd.h>

namespace DB
{

class Block;
struct ExtraBlock;

class IJoin
{
public:
    virtual ~IJoin() = default;

    /// Add block of data from right hand of JOIN.
    /// @returns false, if some limit was exceeded and you should not insert more data.
    virtual bool addJoinedBlock(const Block & block, bool check_limits = true) = 0;

    /// Join the block with data from left hand of JOIN to the right hand data (that was previously built by calls to addJoinedBlock).
    /// Could be called from different threads in parallel.
    virtual void joinBlock(Block & block, std::shared_ptr<ExtraBlock> & not_processed) = 0;

    virtual bool hasTotals() const = 0;
    virtual void setTotals(const Block & block) = 0;
    virtual void joinTotals(Block & block) const = 0;

    virtual size_t getTotalRowCount() const = 0;
    virtual size_t getTotalByteCount() const = 0;
    virtual bool alwaysReturnsEmptySet() const { return false; }

    virtual BlockInputStreamPtr createStreamWithNonJoinedRows(const Block &, UInt64) const { return {}; }
    virtual bool hasStreamWithNonJoinedRows() const { return false; }
};

using JoinPtr = std::shared_ptr<IJoin>;

}
