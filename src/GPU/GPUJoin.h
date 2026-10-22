#pragma once

#include "config.h"

#if USE_GPU

#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <GPU/GPUMemory.h>
#include <GPU/GPUTypes.cuh>
#include <Interpreters/IJoin.h>
#include <Common/Logger.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

namespace DB
{

class TableJoin;

namespace GPU
{
class CudfHashJoin;
}

/** `ALL INNER JOIN` on a single integer key, whose hash table is built and probed on a CUDA GPU with cuDF's
  * `cudf::hash_join`.
  *
  * Chosen only by name - `join_algorithm = 'gpu_hash'` - and never by `default` or `auto`, because whether it is faster
  * than `parallel_hash` depends on how much crosses the PCIe link, which the planner does not know. When it is asked for
  * and the join is one it cannot do, `chooseJoinAlgorithm` throws the same "none of the algorithms enabled" it throws
  * for every other algorithm that does not apply; there is deliberately no fallback.
  *
  * Only keys cross the link. The right table's blocks are kept on the host, and its keys are sent to the device once,
  * when the build ends, where the hash table is built over them. Of a left block only the key column is sent; the probe
  * brings back the row index on either side of each matching pair, and the columns of both sides are gathered on the
  * host with `IColumn::index`. So a column of any type is joined, and what crosses per left row is its key and 8 bytes
  * per match.
  *
  * The blocks of the left table are probed at once, each on a CUDA stream of its own thread, as the hash table is only
  * read once built. One probe block produces exactly one result block, however many rows that is: the device computes a
  * whole block's matches in one call.
  */
class GPUHashJoin final : public IJoin
{
public:
    static bool isSupported(const TableJoin & table_join, const Block & left_sample_block, const Block & right_sample_block);

    GPUHashJoin(std::shared_ptr<TableJoin> table_join_, SharedHeader left_sample_block_, SharedHeader right_sample_block_);

    ~GPUHashJoin() override;

    GPUHashJoin(const GPUHashJoin &) = delete;
    GPUHashJoin & operator=(const GPUHashJoin &) = delete;

    std::string getName() const override { return "GPUHashJoin"; }

    std::string getAlgorithm() const override { return toString(JoinAlgorithm::GPU_HASH); }

    const TableJoin & getTableJoin() const override { return *table_join; }

    bool isCloneSupported() const override { return false; }

    bool addBlockToJoin(const Block & block, size_t num_rows, JoinBuildContext context) override;

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
    /// Concatenates the right table's blocks and builds the hash table over their keys, once.
    void finishBuildUnlocked();

    Block assembleOutputBlock(const Block & probe_block, const ColumnPtr & probe_indices, const ColumnPtr & build_indices) const;

    const std::shared_ptr<TableJoin> table_join;

    const Block left_sample_block;
    const Block right_sample_block;

    const String key_name_left;
    const String key_name_right;
    const GPU::GPUElementType key_type;

    /// The right table's columns that the result takes.
    Block build_payload_header;

    Block required_right_keys;
    std::vector<String> required_right_keys_sources;

    std::mutex device_mutex;

    /// The right table's blocks, of its key and payload columns, until the build ends, and then all of them as one.
    Blocks build_blocks;
    Block build_block;
    bool built = false;

    GPU::DeviceBuffer build_keys;
    std::unique_ptr<GPU::CudfHashJoin> hash_join;

    std::atomic<size_t> build_rows{0};
    std::atomic<size_t> build_bytes{0};

    std::atomic<UInt64> probe_rows_total{0};

    LoggerPtr log;
};

}

#endif
