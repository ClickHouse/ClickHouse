#pragma once

#include <Columns/ColumnNullable.h>
#include <Columns/IColumn.h>
#include <Common/HyperLogLogCounter.h>
#include <Common/PODArray.h>
#include <Common/SharedMutex.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Interpreters/JoinUtils.h>
#include <QueryPipeline/SizeLimits.h>
#include <base/types.h>

#include <atomic>
#include <exception>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace DB
{

struct StoredBlock;

/// The distinct-key sketch of one build stream and one `ON` disjunct (the live mode of `HyperLogLogCounter`). The input is
/// the low 32 bits of the hash that the map computes for the key; `insert` applies `fmix32` itself.
using JoinKeySketch = LiveHyperLogLogCounter;

/** What a waiting block keeps so that it can be inserted later: what the insert reads from the incoming block. Built in a
  * local list and appended last, so a record in a list is always complete. Freed when the block is inserted or scattered.
  */
struct WaitingBlockRecord
{
    /// One per `ON` disjunct.
    struct Clause
    {
        /// The prepared key columns, by pointer. `key_holders` below keeps them alive.
        ColumnRawPtrs key_columns;
        /// The positions in `key_holders` of the key columns of this disjunct.
        std::vector<size_t> key_positions;
        ConstNullMapPtr null_map = nullptr;
        ColumnPtr null_map_holder;
        JoinCommon::JoinMask join_mask;
        /// A row with a NULL key was skipped. A single-row table keeps such a block.
        bool skipped_null_key = false;

        /// Only with several build streams (`HashJoinBuildState::partitioned`).
        /// 1 = the row is not inserted. Empty when no row is skipped.
        PaddedPODArray<UInt8> skip;
        /// Bits 16..31 of the map hash; the scatter turns it into the partition id in place.
        PaddedPODArray<UInt16> route;
        /// The keys of `hashed` and packed `String` maps, built once by `PreparedJoinKeys::buildAllKeys`.
        PaddedPODArray<UInt128> key128;
        /// The map hash of `String` and `FixedString` keys, so the owner hashes a string once.
        PaddedPODArray<UInt64> hash64;
    };

    StoredBlock * stored = nullptr;
    /// The node of `stored` in the list of its build stream, to drop it.
    std::list<StoredBlock>::iterator stored_it;
    /// When the stored block was dropped at once (`block_dropped`): its selector and number, without columns.
    std::optional<StoredBlock> dropped_block;
    UInt32 block_no = 0;
    size_t rows = 0;
    /// The block keeps a `NullMapHolder` or per-row flags, so it is never dropped.
    bool keeps_block = false;
    /// The stored block was dropped at once (a key-only table whose blocks nobody keeps): only this record waits.
    bool block_dropped = false;
    Columns key_holders;
    /// For each of `key_holders`, the position in the stored block's `columns` it was prepared from, or -1. A shrink of the
    /// stored block prepares those keys again from the new columns, so both copies do not stay alive.
    std::vector<Int32> key_holder_sources;
    std::vector<Clause> clauses;
    /// The bytes of this record, counted in the join's allocated bytes until the record is freed.
    size_t allocated_bytes = 0;
    /// The bytes of its `String` keys, counted in `HashJoinBuildState::string_key_bytes` until the record is freed.
    size_t string_key_bytes = 0;
    /// The bytes of the `NullMapHolder`s of its stored block.
    size_t nullmap_bytes = 0;
    /// What this record added to `HashJoinBuildState::waiting_bytes`.
    size_t counted_waiting_bytes = 0;

    [[nodiscard]] size_t computeAllocatedBytes() const;
};

using WaitingBlockList = std::list<WaitingBlockRecord>;

/// Why the wait of a table ended. Logged once per build.
enum class JoinWaitEnd : uint8_t
{
    NotYet,
    BuildFinish,
    RowLimit,
    ByteLimit,
    RepeatsOnly,
    RememberedVerdict,
    Backstop,
    MemoryBound,
};

/** The plan of a partitioned build: chosen on the host that runs it, when the wait ends.
  * See `choosePartitionPlan`.
  */
struct JoinPartitionPlan
{
    size_t partition_bits = 0;
    size_t cells_per_partition = 0;
    bool robin_hood = false;
    /// The estimate E, and where it came from: "cache", "sketch" or "rows".
    size_t estimate = 0;
    const char * estimate_source = "rows";
};

/** Chooses the partitions of a partitioned build when its wait ends. A build that ends at build finish takes the fewest
  * partitions whose map with a reserve margin fits 0.8 of the L2 cache. An early build takes
  * max(256, next_pow2(2 * num_build_streams)) partitions. Both are capped by `maxJoinPartitions`.
  * The average key count then sizes the common buffer for ordinary linear probing.
  * `buffer_bytes_for_reserve(r, robin_hood)` is the buffer that the grower of a partition map allocates for `r` keys, with
  * or without the Robin Hood mode.
  */
JoinPartitionPlan choosePartitionPlan(
    size_t estimate,
    const char * estimate_source,
    size_t waiting_rows,
    size_t num_build_streams,
    size_t parallel_hash_join_threshold,
    bool early,
    bool robin_hood_setting,
    size_t (*buffer_bytes_for_reserve)(size_t keys, bool robin_hood));

/// The L1-based limit on the number of build partitions.
size_t maxJoinPartitions();

/** The state of a join's build while it waits, and the rounds of scatter and owners. Owned by `HashJoin::RightTableData`.
  * A join of the `Join` engine has none: its blocks go straight in.
  */
struct HashJoinBuildState
{
    struct Stream
    {
        WaitingBlockList waiting;
        /// One per `ON` disjunct; published through `sketches` once it exists.
        std::vector<std::unique_ptr<JoinKeySketch>> sketch_storage;
        std::vector<std::atomic<JoinKeySketch *>> sketches;
        std::atomic<size_t> rows = 0;
    };

    /// The live estimate of one `ON` disjunct.
    struct Clause
    {
        std::unique_ptr<JoinKeySketch> merged;
        std::atomic<size_t> estimate = 0;
        /// The live test of the current batch.
        bool baseline_taken = false;
        size_t prev_rows = 0;
        size_t prev_estimate = 0;
    };

    /// Several build streams: the wait ends in a partitioned build (scatter and owners). One stream: in a replay.
    std::atomic<bool> partitioned = false;
    /// The join can end its wait before build finish: it drops blocks, or it has a size limit.
    bool can_end_early = false;
    /// `ANY`, `SEMI`, `ANTI` without `join_any_take_last_row` or per-row flags, or key-only tables nobody keeps the blocks of.
    bool drops_blocks = false;
    std::atomic<bool> flags_per_cell = false;
    /// The limits that end the wait in time. A wrapper that checks the limits itself passes its own (`setWaitLimits`).
    SizeLimits wait_limits;
    /// One build stream: the wait ends when the upper bound of the bytes reaches it (`setWaitMemoryBound`). 0 = none.
    size_t memory_bound_bytes = 0;
    /// The backstop: a quarter of the budget, for joins that drop blocks. 0 = none.
    size_t backstop_bytes = 0;
    /// The statistics cache says the last run's live test fired.
    bool remembered_repeats_dominate = false;
    /// A cached key count, for sizing the tables only.
    std::atomic<size_t> cached_keys = 0;
    /// The bytes of the `String` keys of the waiting blocks: the arena copies them for the new keys.
    std::atomic<size_t> string_key_bytes = 0;

    std::vector<Stream> streams;
    std::vector<Clause> clauses;

    /// Taken shared by every `addBlockToJoin` of a join that `can_end_early`, and exclusively by a round.
    SharedMutex round_mutex;
    /// The stream that sees a rule fire claims the round.
    std::atomic<bool> round_claimed = false;
    std::atomic<bool> end_wait = false;
    JoinWaitEnd wait_end = JoinWaitEnd::NotYet;
    /// The first round has run: blocks of one stream go straight in; several streams wait for the next batch.
    std::atomic<bool> built_early = false;
    std::atomic<bool> refreshing = false;
    /// The waiting rows when the sketches were last merged into the estimates.
    std::atomic<size_t> last_refresh_rows = 0;
    /// Set by any exception that leaves `addBlockToJoin` of a waiting join, and by a failed round.
    std::atomic<bool> build_failed = false;
    std::exception_ptr failure;
    std::mutex failure_mutex;
    /// `break` of a limit: every stream stores its next block and stops.
    std::atomic<bool> stop_reading = false;
    /// The build stopped at `max_execution_time` with `timeout_overflow_mode = 'break'`. A probe throws `QUERY_WAS_CANCELLED`.
    std::atomic<bool> cancelled = false;
    /// A stored block was freed during the build: the statistics keep a remembered verdict only then.
    std::atomic<bool> freed_a_block = false;
    /// The bytes of the waiting blocks and their records and holders: what the backstop compares.
    std::atomic<size_t> waiting_bytes = 0;
    /// A wrapper's `dropRightBlocksKeptForAnotherAlgorithm` before the build ran; the finish step runs it.
    bool drop_kept_blocks_requested = false;
    std::atomic<bool> finished = false;

    /// One byte per registered block, set when a table entry points into it (`markReferenced`).
    std::unique_ptr<std::atomic<UInt8>[]> referenced;
    size_t referenced_size = 0;
    void markReferenced(UInt32 block_no) const
    {
        if (!referenced[block_no].load(std::memory_order_relaxed))
            referenced[block_no].store(1, std::memory_order_relaxed);
    }

    size_t waitingRows() const;
};

}
