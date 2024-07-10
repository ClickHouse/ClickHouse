#pragma once

#include <memory>
#include <vector>

#include <Core/Names.h>
#include <Core/Block.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

struct ExtraBlock;
using ExtraBlockPtr = std::shared_ptr<ExtraBlock>;

class TableJoin;
class NotJoinedBlocks;
class IBlocksStream;
using IBlocksStreamPtr = std::shared_ptr<IBlocksStream>;

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

enum class JoinPipelineType : uint8_t
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

    virtual std::string getName() const = 0;

    virtual const TableJoin & getTableJoin() const = 0;

    /// Returns true if clone is supported
    virtual bool isCloneSupported() const
    {
        return false;
    }

    /// Clone underlyhing JOIN algorithm using table join, left sample block, right sample block
    virtual std::shared_ptr<IJoin> clone(const std::shared_ptr<TableJoin> & table_join_,
        const Block & left_sample_block_,
        const Block & right_sample_block_) const
    {
        (void)(table_join_);
        (void)(left_sample_block_);
        (void)(right_sample_block_);
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Clone method is not supported for {}", getName());
    }

    /// Add block of data from right hand of JOIN.
    /// @returns false, if some limit was exceeded and you should not insert more data.
    virtual bool addBlockToJoin(const Block & block, bool check_limits = true) = 0; /// NOLINT

    /* Some initialization may be required before joinBlock() call.
     * It's better to done in in constructor, but left block exact structure is not known at that moment.
     * TODO: pass correct left block sample to the constructor.
     */
    virtual void initialize(const Block & /* left_sample_block */) {}

    virtual void checkTypesOfKeys(const Block & block) const = 0;

    /// Join the block with data from left hand of JOIN to the right hand data (that was previously built by calls to addBlockToJoin).
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

    /// StorageJoin/Dictionary is already filled. No need to call addBlockToJoin.
    /// Different query plan is used for such joins.
    virtual bool isFilled() const { return pipelineType() == JoinPipelineType::FilledRight; }
    virtual JoinPipelineType pipelineType() const { return JoinPipelineType::FillRightFirst; }

    // That can run FillingRightJoinSideTransform parallelly
    virtual bool supportParallelJoin() const { return false; }
    virtual bool supportTotals() const { return true; }

    /// Peek next stream of delayed joined blocks.
    virtual IBlocksStreamPtr getDelayedBlocks() { return nullptr; }
    virtual bool hasDelayedBlocks() const { return false; }
    virtual void tryRerangeRightTableData() {}

    virtual IBlocksStreamPtr
        getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const = 0;

private:
    Block totals;
};

class IBlocksStream
{
public:
    /// Returns empty block on EOF
    Block next()
    {
        if (finished)
            return {};

        if (Block res = nextImpl())
            return res;

        finished = true;
        return {};
    }

    virtual ~IBlocksStream() = default;

    bool isFinished() const { return finished; }

protected:
    virtual Block nextImpl() = 0;

    std::atomic_bool finished{false};

};

}
