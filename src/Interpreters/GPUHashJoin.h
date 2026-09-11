#pragma once

#include "config.h"

#if USE_GPU

#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <Interpreters/IJoin.h>
#include <Common/Logger.h>

#include <atomic>
#include <mutex>
#include <vector>

namespace DB
{

class TableJoin;

/** `ALL INNER JOIN` on a single fixed-width integer key, run on a CUDA GPU with cuDF's
  * `cudf::hash_join`.
  *
  * Chosen only by name - `join_algorithm = 'gpu_hash'` - and never by `default` or `auto`, because
  * whether it is faster than `parallel_hash` depends on how much of the query's data has to cross
  * the PCIe link, which the planner does not know. When it is asked for and the join is one it
  * cannot do, `chooseJoinAlgorithm` throws the same "none of the algorithms enabled" it throws for
  * every other algorithm that does not apply; there is deliberately no fallback here, so that a
  * query which asked for the device either uses it or fails.
  *
  * What crosses the link, and what does not, is the whole of the design. The right table's key and
  * payload columns are sent to the device as they arrive and stay there. Of a left block only the
  * key column is sent; the probe brings back the index of the probe-side row of each matching pair,
  * and the left block's own columns are then indexed on the host with `IColumn::index`. So the left
  * side - which is normally the large one - is read once from host memory and never transferred,
  * while the right side is transferred once and gathered on the device. The alternative, sending the
  * left payload over and gathering it there, would move the larger of the two tables across the
  * narrower of the two paths to memory.
  *
  * One probe block produces exactly one result block, however many rows that is. `HashJoin` splits
  * its output at `max_joined_block_rows`, and this does not: the device computes a whole block's
  * matches in one call, so there is nothing to resume from and cutting the result up would mean
  * holding it and handing it out in pieces. A join with a badly skewed key can therefore emit a
  * block far larger than `max_block_size`, which is the first thing to fix if this path grows past
  * a prototype.
  *
  * The restrictions of this first cut - one key column pair, both keys of the same non-nullable
  * integer type, every output column of either side a non-nullable fixed-width numeric type - are
  * `isSupported`, and each of them is about a layout or a comparison rather than about what the
  * device can compute. `Nullable` needs a validity bitmask, `LowCardinality` a dictionary, `String`
  * a column of offsets plus characters instead of one contiguous run of values, and a float key is
  * compared by cuDF with IEEE equality where ClickHouse compares its bytes.
  */
class GPUHashJoin final : public IJoin
{
public:
    /// Whether this join is one the device can do, decided on the join's shape and on the column
    /// types alone, while the algorithm is being picked. Says nothing about whether the machine has
    /// a usable device - that is `GPU::deviceProbeError`, and the caller checks it separately so
    /// that a query which fits but has no device to run on says so instead of failing with
    /// "no algorithm applies".
    static bool isSupported(const TableJoin & table_join, const Block & left_sample_block, const Block & right_sample_block);

    GPUHashJoin(std::shared_ptr<TableJoin> table_join_, SharedHeader left_sample_block_, SharedHeader right_sample_block_);

    ~GPUHashJoin() override;

    /// Holds a device resource, and there is no use for a second name for one.
    GPUHashJoin(const GPUHashJoin &) = delete;
    GPUHashJoin & operator=(const GPUHashJoin &) = delete;

    std::string getName() const override { return "GPUHashJoin"; }

    const TableJoin & getTableJoin() const override { return *table_join; }

    /// One device, one hash table, one stream: a second copy of this join would contend for the
    /// same device rather than use a second one, and the plan optimizations that clone a join do it
    /// to run the copies in parallel.
    bool isCloneSupported() const override { return false; }

    bool addBlockToJoin(const Block & block, bool check_limits) override;

    using IJoin::addBlockToJoin;

    void checkTypesOfKeys(const Block & block) const override;

    using IJoin::joinBlock;

    JoinResultPtr joinBlock(Block block) override;

    void onBuildPhaseFinish() override;

    /// Rows of the right table on the device, and the bytes they occupy there. The bytes are device
    /// memory, which no memory limit of the server's knows about - the same caveat the GPU
    /// aggregation's partial result carries - but they are what `max_bytes_in_join` is checked
    /// against, so that the setting still bounds the right table on this path.
    size_t getTotalRowCount() const override { return build_rows.load(std::memory_order_relaxed); }
    size_t getTotalByteCount() const override { return build_bytes.load(std::memory_order_relaxed); }

    StepAnalysisReport getAnalysisReport() const override;

    /// An `INNER JOIN` whose right table has no rows returns nothing whatever the left table holds,
    /// and `JoiningTransform` uses this to stop reading the left side at all. Both of its call sites
    /// ask only once the right side has finished, so the count below is the final one.
    bool alwaysReturnsEmptySet() const override { return build_rows.load(std::memory_order_relaxed) == 0; }

    /// The build phase fills one hash table on one device through one handle, so there is nothing
    /// for a second filling thread to do but wait on the mutex below.
    bool supportParallelJoin() const override { return false; }

    /// An `INNER JOIN` emits no row for an unmatched right row, so there is no second stream.
    IBlocksStreamPtr getNonJoinedBlocks(const Block &, const Block &, UInt64) const override { return nullptr; }

private:
    /// Builds the hash table over what has been sent so far, after which nothing more may be added.
    /// Idempotent: `onBuildPhaseFinish` calls it so that the build's cost is not charged to the
    /// first probe, and `joinBlock` calls it in case the pipeline never did.
    void finishBuildUnlocked();

    /// The result block for one probe: the left block's columns indexed with `probe_indices`, then
    /// the right table's payload columns as the device gathered them, then the right key columns the
    /// query asks for. Every one of them has as many rows as `probe_indices` has indices, and the
    /// structure is the same whether or not there were any matches - which is what makes the empty
    /// block this produces at planning time the header every real block will have.
    Block assembleOutputBlock(const Block & probe_block, const ColumnPtr & probe_indices, Columns gathered_payloads) const;

    const std::shared_ptr<TableJoin> table_join;

    /// The left and right headers as `chooseJoinAlgorithm` saw them, which is after the planner's
    /// converting actions - so the two key columns already have the same type here.
    const Block left_sample_block;
    const Block right_sample_block;

    const String key_name_left;
    const String key_name_right;

    /// The right table's columns other than the key: what is sent to the device as the build side's
    /// payload, gathered there, and read back. In this order, which is also the order they take in
    /// the output block.
    Block build_payload_header;

    /// The right key columns the query asks for, and the left column each of them is taken from. An
    /// `INNER JOIN` on equality makes the two equal row by row, so these are copies of left columns
    /// and are not read from the device at all. `HashJoin` restores them the same way.
    Block required_right_keys;
    std::vector<String> required_right_keys_sources;

    /// `ClickHouseGPUElementType` values, kept as `int` so that this header does not have to carry
    /// the boundary's enumerators - the same reason `GPU::SumAccumulator` does.
    int key_element_type;
    size_t key_element_size;
    std::vector<int> payload_element_types;
    std::vector<size_t> payload_element_sizes;

    /// Guards the handle. The build side is filled by one thread, but `joinBlock` is called from
    /// every probe stream at once, and a probe's result lives in the handle between the probe and
    /// the copy out - so the two together are one critical section. cuDF would allow the probes
    /// themselves to run concurrently; serializing them costs little, because they all queue on the
    /// one stream of the one device anyway.
    std::mutex device_mutex;

    /// The build side and the hash table, on the device. Owned - see the destructor.
    void * handle = nullptr;

    /// Atomic, unlike everything else about the build side, because `alwaysReturnsEmptySet` is read
    /// from `JoiningTransform::prepare` - which the executor calls over and over, on every probe
    /// thread - and taking `device_mutex` there would make those calls wait behind a probe that is
    /// on the device. Only `addBlockToJoin` writes them, and it holds the mutex while it does.
    std::atomic<size_t> build_rows{0};
    std::atomic<size_t> build_bytes{0};

    bool build_finished = false;

    /// For `EXPLAIN ANALYZE`. How many rows of the left table were probed with; how many of them
    /// matched is deliberately not reported, because the device hands back one index per matching
    /// pair and counting the distinct probe rows among them would be a pass over the whole output
    /// for a number nothing needs.
    std::atomic<UInt64> probe_rows_total{0};

    LoggerPtr log;
};

}

#endif
