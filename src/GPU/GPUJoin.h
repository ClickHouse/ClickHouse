#pragma once

#include "config.h"

#if USE_GPU

#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <GPU/GPUHashTable.h>
#include <Interpreters/IJoin.h>
#include <Common/Logger.h>

#include <atomic>
#include <optional>
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
    static bool isSupported(const TableJoin & table_join, const Block & left_sample_block, const Block & right_sample_block);

    GPUHashJoin(std::shared_ptr<TableJoin> table_join_, SharedHeader left_sample_block_, SharedHeader right_sample_block_);

    ~GPUHashJoin() override = default;

    GPUHashJoin(const GPUHashJoin &) = delete;
    GPUHashJoin & operator=(const GPUHashJoin &) = delete;

    std::string getName() const override { return "GPUHashJoin"; }

    const TableJoin & getTableJoin() const override { return *table_join; }

    bool isCloneSupported() const override { return false; }

    bool addBlockToJoin(const Block & block, bool check_limits) override;

    using IJoin::addBlockToJoin;

    void checkTypesOfKeys(const Block & block) const override;

    using IJoin::joinBlock;

    JoinResultPtr joinBlock(Block block) override;

    void onBuildPhaseFinish() override;

    size_t getTotalRowCount() const override { return build_rows.load(std::memory_order_relaxed); }
    size_t getTotalByteCount() const override { return build_bytes.load(std::memory_order_relaxed); }

    StepAnalysisReport getAnalysisReport() const override;

    bool alwaysReturnsEmptySet() const override { return build_rows.load(std::memory_order_relaxed) == 0; }

    bool supportParallelJoin() const override { return false; }

    IBlocksStreamPtr getNonJoinedBlocks(const Block &, const Block &, UInt64) const override { return nullptr; }

private:
    void finishBuildUnlocked();

    Block assembleOutputBlock(const Block & probe_block, const ColumnPtr & probe_indices, Columns gathered_payloads) const;

    const std::shared_ptr<TableJoin> table_join;

    const Block left_sample_block;
    const Block right_sample_block;

    const String key_name_left;
    const String key_name_right;

    Block build_payload_header;

    Block required_right_keys;
    std::vector<String> required_right_keys_sources;

    std::mutex device_mutex;

    std::optional<GPU::HashTable> hash_table;

    std::atomic<size_t> build_rows{0};
    std::atomic<size_t> build_bytes{0};

    std::atomic<UInt64> probe_rows_total{0};

    LoggerPtr log;
};

}

#endif
