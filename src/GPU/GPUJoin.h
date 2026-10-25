#pragma once

#include "config.h"

#if USE_GPU

#include <Core/Block.h>
#include <Core/Block_fwd.h>
#include <GPU/DeviceKeySet.h>
#include <GPU/GPUDeviceColumns.h>
#include <GPU/GPUMemory.h>
#include <GPU/GPUTypes.cuh>
#include <Interpreters/HashTablesStatistics.h>
#include <Interpreters/IJoin.h>
#include <Common/Logger.h>

#include <atomic>
#include <condition_variable>
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
  *
  * On the device - with `gpu_hash_join_on_device`, where every column of both sides is of a fixed width the device holds
  * - the columns of both sides are on the device too, and so is the result: the right table's columns are put together
  * there once the build ends, a left block's columns go there unless they are there already, and the columns of the
  * result are gathered there, a chunk of columns on the device for the steps above, see `DeviceChunkInfo`. So a chain of
  * joins and the reads and aggregation around it keep their rows on the device, and only what the steps on the CPU take
  * comes back.
  */
class GPUHashJoin final : public IJoin, public GPU::IDeviceKeySetProvider
{
public:
    static bool isSupported(const TableJoin & table_join, const Block & left_sample_block, const Block & right_sample_block);

    /// Whether a supported join of these would keep its columns on the device, see above: `gpu_hash_join_on_device` is
    /// set in the query, and every column of the left table and of the result's share of the right one is of a fixed
    /// width the device holds.
    static bool runsOnDevice(const TableJoin & table_join, const Block & left_sample_block, const Block & right_sample_block);

    GPUHashJoin(
        std::shared_ptr<TableJoin> table_join_,
        SharedHeader left_sample_block_,
        SharedHeader right_sample_block_,
        HashJoinStatsCollectingParams stats_collecting_params_ = {});

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

    bool acceptsDeviceChunks() const override { return on_device; }
    bool addDeviceChunkToJoin(const Chunk & chunk, const Block & header, JoinBuildContext context) override;
    JoinResultPtr joinDeviceChunk(const Chunk & chunk, const Block & header) override;

    void onBuildPhaseFinish() override;
    void onProbePhaseFinish(std::optional<size_t> matched_right_rows) override;

    size_t getTotalRowCount() const override { return build_rows.load(std::memory_order_relaxed); }
    size_t getTotalByteCount() const override { return build_bytes.load(std::memory_order_relaxed); }

    StepAnalysisReport getAnalysisReport() const override;

    bool alwaysReturnsEmptySet() const override { return build_rows.load(std::memory_order_relaxed) == 0; }

    bool supportParallelJoin() const override { return false; }

    /// Each probe launches several kernels and waits for the device twice, which costs as much as the keys of a block
    /// of 65536 rows take to match.
    size_t probeBlockRows() const override { return 1UL << 20; }

    IBlocksStreamPtr getNonJoinedBlocks(const Block &, const Block &, UInt64) const override { return nullptr; }

    /// The key of the left table, by which a read of it may filter its rows with the right table's keys, see `DeviceKeySet`.
    const String & leftKeyName() const { return key_name_left; }
    GPU::GPUElementType keyElementType() const override { return key_type; }
    /// The right table's keys, once the build ended: a read of the left table waits here for them.
    std::shared_ptr<const GPU::DeviceKeySet> waitForKeySet(const std::function<bool()> & stop) const override;

private:
    class Result;
    class DeviceResult;

    /// Builds the hash table over the keys sent so far, once.
    void finishBuild();

    /// After the hash table: the build marked as ended, for the probes and the waits on it.
    void markBuilt();

    /// Of a join on the device: the keys and the columns of the result of the right table's chunks, put together on the
    /// device in the order of the chunks.
    struct BuildChunk;
    void finishBuildOnDevice(const std::vector<std::unique_ptr<BuildChunk>> & chunks, size_t rows);

    /// Of a join on the device: the result of the left table's rows, which are the columns of `header` either on the
    /// device, `device_input`, or in `host_block`.
    JoinResultPtr probeOnDevice(const GPU::GPUDeviceColumns * device_input, const Block & host_block, const Block & header, size_t rows);

    /// Adds a block of the right table to the list, and counts it against the limits.
    bool addBuildChunk(std::unique_ptr<BuildChunk> chunk, size_t num_rows, size_t bytes, JoinBuildContext context);

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

    /// A block of the right table as it arrived: its keys and the columns the result takes of it, in host memory, or, of
    /// a join on the device, on the device: the keys and then the columns of `build_payload_header`.
    struct BuildChunk
    {
        ColumnPtr keys;
        Columns payload;
        std::shared_ptr<const GPU::GPUDeviceColumns> device;
        BuildChunk * next = nullptr;
    };

    /// Whether the columns of both sides and of the result are on the device, see above.
    bool on_device = false;
    /// Of a join on the device: what the device holds the values of each column of `build_payload_header` as.
    std::vector<GPU::GPUElementType> build_payload_types;
    /// Of a join on the device: the columns of `build_payload_header` of every row of the right table.
    std::vector<std::unique_ptr<GPU::DeviceBuffer>> build_columns;

    /// The blocks of the right table, which its threads add at once, without a lock, to the front of a list; they are put
    /// together once the build ends, the payload a column at a time in parallel, and the keys sent to the device in one
    /// copy.
    std::atomic<BuildChunk *> build_chunks{nullptr};
    Block build_block;
    /// Set once the hash table is built, before `onBuildPhaseFinish` returns and so before the left table is probed.
    std::atomic<bool> built{false};

    GPU::DeviceBuffer build_keys;
    std::unique_ptr<GPU::CudfHashJoin> hash_join;
    /// The build keys as a set, made once the first read of the left table asks for them, see `waitForKeySet`.
    mutable std::once_flag key_set_once;
    mutable std::shared_ptr<const GPU::DeviceKeySet> key_set;
    mutable std::mutex built_mutex;
    mutable std::condition_variable built_changed;

    std::atomic<size_t> build_rows{0};
    std::atomic<size_t> build_bytes{0};

    std::atomic<UInt64> probe_rows_total{0};

    /// The sizes of the hash table and of its matches, told to the statistics the planner orders joins by, as `HashJoin`
    /// tells them: a join on the GPU that told none left the order to change from one run to the next.
    const HashJoinStatsCollectingParams stats_collecting_params;
    bool probe_phase_finished = false;
    std::optional<size_t> hash_table_matches;

    LoggerPtr log;
};

}

#endif
