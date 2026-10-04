#pragma once

#include <memory>
#include <optional>
#include <string>

#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <Core/Joins.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Processors/QueryPlan/Profiling/Metrics/StepAnalyzeInfo.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

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

class IJoinResult;
using JoinResultPtr = std::unique_ptr<IJoinResult>;

class IJoinResult
{
public:
    virtual ~IJoinResult() = default;

    struct JoinResultBlock
    {
        Block block;
        /// Pointer to next block to process, if any.
        /// Should be used once we got last result and is_last is true.
        ScatteredBlock * next_block = nullptr;
        bool is_last = true;
    };

    virtual JoinResultBlock next() = 0;

    /// Right table rows matched while producing the result. Only meaningful once the result is exhausted.
    /// Empty when the probe never counted matches, so its zero would be structural rather than measured.
    virtual std::optional<size_t> getMatchedRightRows() const { return 0; }

    static JoinResultPtr createFromBlock(Block block);
};

/// Folds one `getMatchedRightRows()` into a running total. Empty absorbs: a total counts every match
/// only if every part of it did.
inline void addMatchedRightRows(std::optional<size_t> & total, std::optional<size_t> part)
{
    if (!part)
        total.reset();
    else if (total)
        *total += *part;
}

class QueryPipelineBuilder;

/// Lets only `QueryPipelineBuilder` number the build streams.
class JoinBuildStreamKey
{
    friend class QueryPipelineBuilder;
    JoinBuildStreamKey() = default;
};

/// Says which build stream a build-side call works for, and whether the join checks the size limits.
///
/// The pipeline fills a join from N concurrent `FillingRightJoinSideTransform`s only when
/// `supportParallelJoin` is true. It gives them the streams 0 to N - 1 (`forStream`), with N at most
/// `getMaxBuildThreads` when that is not zero. Calls with the same stream never overlap. Calls with
/// different streams can, so a join may keep unsynchronized state per stream, indexed by `getStream`.
///
/// A join that inserts into another join passes the context of the stream it works for, or
/// `callerChecksLimits` of it. That covers the caller's own block, every block it converts, replays,
/// re-buckets or drains during the call, and the inserts that `requestSpill` starts. It holds even when
/// the join serializes those inserts under a lock of its own.
///
/// `serial` claims that no other insert into the join can overlap this one. Only an owner that fills a
/// join one call at a time uses it. That is a single filler, join by layers, `StorageJoin` (its inserts
/// take the table's write lock), and code that runs while no build stream can run (`onBuildPhaseFinish`,
/// after the build phase).
class JoinBuildContext
{
public:
    /// Stream `stream` of the `num_streams` streams that fill one join concurrently.
    static JoinBuildContext forStream(JoinBuildStreamKey, size_t stream, size_t num_streams);

    static JoinBuildContext serial() { return JoinBuildContext(0, 1, /*join_checks_limits_=*/true); }

    /// The same stream, but the join does not check `max_rows_in_join` and `max_bytes_in_join`. For a
    /// caller that checks the limits itself, or that re-inserts blocks the limits already admitted.
    JoinBuildContext callerChecksLimits() const { return JoinBuildContext(stream, num_streams, /*join_checks_limits_=*/false); }

    size_t getStream() const { return stream; }
    /// The number of streams the pipeline started; 1 for `serial`. Wrappers forward it, so it is not the
    /// receiver's real concurrency.
    size_t getNumStreams() const { return num_streams; }
    bool joinChecksLimits() const { return join_checks_limits; }

private:
    JoinBuildContext(UInt32 stream_, UInt32 num_streams_, bool join_checks_limits_)
        : stream(stream_), num_streams(num_streams_), join_checks_limits(join_checks_limits_)
    {
    }

    UInt32 stream;
    UInt32 num_streams;
    bool join_checks_limits;
};

class IJoin
{
public:
    virtual ~IJoin() = default;

    virtual std::string getName() const = 0;

    virtual std::string getAlgorithm() const = 0;

    virtual const TableJoin & getTableJoin() const = 0;

    /// The `join_any_take_last_row` setting: for `ANY` joins it selects the last matching right-side
    /// row instead of the first one. It is not part of `TableJoin`, it is baked into the concrete
    /// algorithm, so algorithms that honor it expose it here. Algorithms for which the setting is
    /// meaningless keep the default.
    virtual bool anyTakeLastRow() const { return false; }

    /// Returns true if clone is supported
    virtual bool isCloneSupported() const
    {
        return false;
    }

    /// Clone underlying JOIN algorithm using table join, left sample block, right sample block
    virtual std::shared_ptr<IJoin> clone(const std::shared_ptr<TableJoin> & table_join_,
        SharedHeader left_sample_block_,
        SharedHeader right_sample_block_) const
    {
        (void)table_join_;
        (void)left_sample_block_;
        (void)right_sample_block_;
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Clone method is not supported for {}", getName());
    }

    virtual std::shared_ptr<IJoin> cloneNoParallel(const std::shared_ptr<TableJoin> & table_join_,
        SharedHeader left_sample_block_,
        SharedHeader right_sample_block_) const { return clone(table_join_, left_sample_block_, right_sample_block_); }

    /// Add block of data from right hand of JOIN.
    /// `num_rows` is the number of rows in `block`. It differs from `Block::rows` only for a block
    /// without columns, for example when `PREWHERE` consumes every right-side column of a `CROSS JOIN`.
    /// `context` says which build stream makes the call and whether the join checks the size limits.
    /// @returns false if the caller should stop adding blocks, for example because a limit was exceeded.
    virtual bool addBlockToJoin(const Block & block, size_t num_rows, JoinBuildContext context) = 0;

    /* Some initialization may be required before joinBlock() call.
     * It's better to done in in constructor, but left block exact structure is not known at that moment.
     * TODO: pass correct left block sample to the constructor.
     */
    virtual void initialize(const Block & /* left_sample_block */) {}

    virtual void checkTypesOfKeys(const Block & block) const = 0;

    /// Join the block with data from left hand of JOIN to the right hand data (that was previously built by calls to addBlockToJoin).
    /// Could be called from different threads in parallel.
    virtual JoinResultPtr joinBlock(Block block) = 0;

    /** Set/Get totals for right table
      * Keep "totals" (separate part of dataset, see WITH TOTALS) to use later.
      */
    virtual void setTotals(const Block & block) { totals = block; }
    virtual const Block & getTotals() const { return totals; }

    /// Number of rows/bytes stored in memory
    virtual size_t getTotalRowCount() const = 0;
    virtual size_t getTotalByteCount() const = 0;
    virtual StepAnalysisReport getAnalysisReport() const = 0;

    /// Returns true if no data to join with.
    virtual bool alwaysReturnsEmptySet() const = 0;

    /// StorageJoin/Dictionary is already filled. No need to call addBlockToJoin.
    /// Different query plan is used for such joins.
    virtual bool isFilled() const { return pipelineType() == JoinPipelineType::FilledRight; }
    virtual JoinPipelineType pipelineType() const { return JoinPipelineType::FillRightFirst; }

    // That can run FillingRightJoinSideTransform parallelly
    virtual bool supportParallelJoin() const { return false; }

    /// Upper bound on the number of build streams. The pipeline reads it only when `supportParallelJoin`
    /// is true and the right side has no totals. Zero leaves the choice to the pipeline.
    virtual size_t getMaxBuildThreads() const { return 0; }

    /// Peek next stream of delayed joined blocks.
    virtual IBlocksStreamPtr getDelayedBlocks() { return nullptr; }
    virtual bool hasDelayedBlocks() const { return false; }

    virtual IBlocksStreamPtr
        getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const = 0;

    virtual bool supportParallelNonJoinedBlocksProcessing() const { return false; }
    /// This serves as a runtime check in JoiningTransform to decide whether to utilize the parallel processing of
    /// non-joined blocks. Only relevant for joins that support parallel processing of non-joined blocks.
    /// If the join supports parallel processing, it can still decide during build phase whether to utilize it or not.
    /// The decision should be done at latest in onBuildPhaseFinish, after that the returned value should not change.
    /// This is important for SpillingHashJoin, which can change algorithms runtime, and parallel non-joined blocks
    /// processing depends on the algorithm used.
    virtual bool isParallelNonJoinedProcessingEnabled() const { return supportParallelNonJoinedBlocksProcessing(); }

    /// Get non-joined blocks for a specific stream partition
    /// stream_idx is in [0, num_streams), each stream must produce a disjoint subset of rows
    /// Default: stream 0 returns everything, others return nothing
    virtual IBlocksStreamPtr getNonJoinedBlocks(
        const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size,
        size_t stream_idx, size_t /*num_streams*/) const
    {
        if (stream_idx != 0)
            return {};
        return getNonJoinedBlocks(left_sample_block, result_sample_block, max_block_size);
    }

    /// Whether the join emits left rows in their original stream order. Read-in-order relies on
    /// this to keep the left sort property, so the default is fail-closed: a join has to opt in.
    virtual bool preservesLeftBlockOrder() const { return false; }

    /// Notify the join that the query plan requires left-side read-in-order preservation.
    /// SpillingHashJoin overrides this to forbid switching to GraceHashJoin at runtime.
    virtual void keepLeftPipelineInOrder() {}

    /// Spilling under memory pressure, driven by `MemorySpillScheduler`. Asked once while the pipeline is
    /// built, so do not look at runtime state here.
    virtual bool canSpillToDisk() const { return false; }
    /// How many bytes of the right side are still sitting in memory and could go to disk.
    virtual size_t getSpillableBytes() const { return 0; }
    /// Move the right side to disk at the next opportunity, at the latest when the build phase ends.
    /// The stream that `context` names calls it from its own job, never while one of its own inserts runs.
    /// The call can also come before that stream's first insert, or after its last one, even after
    /// `onBuildPhaseFinish`. A join may insert on behalf of the stream here only while the build phase runs.
    virtual void requestSpill(JoinBuildContext /*context*/) { }

    /// Called by `FillingRightJoinSideTransform` after all data is inserted in join.
    virtual void onBuildPhaseFinish() { }

    /// Called by `JoiningTransform` when every probe stream has consumed its whole left input.
    /// Not called when the probe is cut short (LIMIT, cancellation).
    /// `matched_right_rows` is the number of right table rows matched across every probe stream,
    /// empty if any of them did not count matches.
    virtual void onProbePhaseFinish(std::optional<size_t> /*matched_right_rows*/) { }

    /// Called by `FillingRightJoinSideTransform` after `onBuildPhaseFinish` if the join has
    /// a post build optimization step.
    virtual bool hasPostBuildPhase() const { return false; }
    virtual void runPostBuildPhase() { }

    /// Enables lazy columns indexing optimization on hash join variants
    virtual void setEnableLazyColumnsIndexing(bool /*value*/) { }

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

        if (Block res = nextImpl(); !res.empty())
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
