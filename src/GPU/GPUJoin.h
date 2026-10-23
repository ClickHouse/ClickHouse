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
  * Only keys cross the link. The right table's columns are kept on the host, gathered into one block as its blocks
  * arrive, and its keys go to the device as they arrive, where the hash table is built over them when the build ends. Of a left block only the key column is sent; the probe
  * brings back the row index on either side of each matching pair, and the columns of both sides are gathered on the
  * host with `IColumn::index`. So a column of any type is joined, and what crosses per left row is its key and 8 bytes
  * per match.
  *
  * The blocks of the left table are probed at once, each on a CUDA stream of its own thread, as the hash table is only
  * read once built. The device computes a whole block's matches in one call, and the result hands them out in blocks of
  * at most `max_joined_block_rows`.
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

    /// Each probe launches several kernels and waits for the device twice, which costs as much as the keys of a block
    /// of 65536 rows take to match.
    size_t probeBlockRows() const override { return 1UL << 20; }

    IBlocksStreamPtr getNonJoinedBlocks(const Block &, const Block &, UInt64) const override { return nullptr; }

private:
    class Result;

    /// Builds the hash table over the keys sent so far, once.
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

    /// The right table's payload columns of each block as it arrives, kept as they are, and the block of them, put
    /// together once the build ends, a column at a time in parallel: the threads of the right side add their blocks under
    /// one lock, which a copy of their columns would hold for as long as the copy takes.
    std::vector<Columns> build_chunks;
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
