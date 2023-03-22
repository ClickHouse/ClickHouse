#pragma once

#include <memory>
#include <vector>

#include <Core/Names.h>
#include <Core/Block.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>

namespace DB
{

class Block;

struct ExtraBlock;
using ExtraBlockPtr = std::shared_ptr<ExtraBlock>;

class TableJoin;
class NotJoinedBlocks;

enum class JoinPipelineType
{
    /*
     * Right stream processed first, then when join data structures are ready, the left stream is processed using it.
     * The pipeline is not sorted.
     */
    FillRightFirst,

    /*
     * Only the left stream is processed. Right is already filled.
     */
    FilledRight,

    /*
     * The pipeline is created from the left and right streams processed with merging transform.
     * Left and right streams have the same priority and are processed simultaneously.
     * The pipelines are sorted.
     */
    YShaped,
};

class IJoin
{
public:
    virtual ~IJoin() = default;

    virtual const TableJoin & getTableJoin() const = 0;

    /// Add block of data from right hand of JOIN.
    /// @returns false, if some limit was exceeded and you should not insert more data.
    virtual bool addJoinedBlock(const Block & block, bool check_limits = true) = 0; /// NOLINT

    virtual void checkTypesOfKeys(const Block & block) const = 0;

    /// Join the block with data from left hand of JOIN to the right hand data (that was previously built by calls to addJoinedBlock).
    /// Could be called from different threads in parallel.
    virtual void joinBlock(Block & block, std::shared_ptr<ExtraBlock> & not_processed) = 0;

    /** Set/Get totals for right table
      * Keep "totals" (separate part of dataset, see WITH TOTALS) to use later.
      */
    virtual void setTotals(const Block & block) { totals = block; }
    virtual const Block & getTotals() const { return totals; }

    /// Number of rows/bytes stored in memory
    virtual size_t getTotalRowCount() const = 0;
    virtual size_t getTotalByteCount() const = 0;

    /// Returns true if no data to join with.
    virtual bool alwaysReturnsEmptySet() const = 0;

    /// StorageJoin/Dictionary is already filled. No need to call addJoinedBlock.
    /// Different query plan is used for such joins.
    virtual bool isFilled() const { return pipelineType() == JoinPipelineType::FilledRight; }
    virtual JoinPipelineType pipelineType() const { return JoinPipelineType::FillRightFirst; }

    // That can run FillingRightJoinSideTransform parallelly
    virtual bool supportParallelJoin() const { return false; }

    virtual std::shared_ptr<NotJoinedBlocks>
    getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const = 0;

private:
    Block totals;
};


using JoinPtr = std::shared_ptr<IJoin>;

}
