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

class GPUHashJoin final : public IJoin
{
public:
    static bool isSupported(const TableJoin & table_join, const Block & left_sample_block, const Block & right_sample_block);

    GPUHashJoin(std::shared_ptr<TableJoin> table_join_, SharedHeader left_sample_block_, SharedHeader right_sample_block_);

    ~GPUHashJoin() override = default;

    GPUHashJoin(const GPUHashJoin &) = delete;
    GPUHashJoin & operator=(const GPUHashJoin &) = delete;

    std::string getName() const override { return "GPUHashJoin"; }
    std::string getAlgorithm() const override { return toString(JoinAlgorithm::GPU_HASH); }

    const TableJoin & getTableJoin() const override { return *table_join; }

    bool isCloneSupported() const override { return false; }

    bool addBlockToJoin(const Block & block, bool check_limits) override;

    using IJoin::addBlockToJoin;

    void checkTypesOfKeys(const Block & block) const override;

    using IJoin::joinBlock;

    JoinResultPtr joinBlock(Block block) override;

    void onBuildPhaseFinish() override;

    void setTotals(const Block & block) override;
    const Block & getTotals() const override { return totals; }

    size_t getTotalRowCount() const override { return build_rows.load(std::memory_order_relaxed); }
    size_t getTotalByteCount() const override { return build_bytes.load(std::memory_order_relaxed); }

    StepAnalysisReport getAnalysisReport() const override;

    bool alwaysReturnsEmptySet() const override { return build_rows.load(std::memory_order_relaxed) == 0; }

    bool supportParallelJoin() const override { return true; }

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

    std::mutex totals_mutex;
    Block totals;

    std::optional<GPU::HashTable> hash_table;

    std::atomic<size_t> build_rows{0};
    std::atomic<size_t> build_bytes{0};

    std::atomic<UInt64> probe_rows_total{0};

    LoggerPtr log;
};

}

#endif
