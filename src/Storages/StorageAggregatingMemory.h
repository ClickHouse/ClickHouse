#pragma once

#include <atomic>
#include <optional>
#include <mutex>

#include <ext/shared_ptr_helper.h>

#include <Core/NamesAndTypes.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Storages/IStorage.h>
#include <DataStreams/IBlockOutputStream.h>

#include <Common/MultiVersion.h>

namespace DB
{

/** TODO: describe this memory storage
  */
class StorageAggregatingMemory final : public ext::shared_ptr_helper<StorageAggregatingMemory>, public IStorage
{
friend class AggregatingOutputStream;
friend struct ext::shared_ptr_helper<StorageAggregatingMemory>;

public:
    String getName() const override { return "AggregatingMemory"; }

    size_t getSize() const { return data.get()->size(); }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    /// Smaller blocks (e.g. 64K rows) are better for CPU cache.
    bool prefersLargeBlocks() const override { return false; }

    bool noPushingToViews() const override { return true; }

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) override;

    void drop() override;

    void mutate(const MutationCommands & commands, ContextPtr context) override;

    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &) override;

    std::optional<UInt64> totalRows(const Settings &) const override;
    std::optional<UInt64> totalBytes(const Settings &) const override;

    void delayReadForGlobalSubqueries() { delay_read_for_global_subqueries = false; }

private:
    /// MultiVersion data storage, so that we can copy the list of blocks to readers.
    MultiVersion<Blocks> data;

    mutable std::mutex mutex;

    bool delay_read_for_global_subqueries = false;

    std::atomic<size_t> total_size_bytes = 0;
    std::atomic<size_t> total_size_rows = 0;

    Block src_sample_block;

    AggregatingTransformParamsPtr aggregator_transform;

    ExpressionAnalysisResult analysis_result;

    std::shared_ptr<ManyAggregatedData> many_data;

protected:
    StorageAggregatingMemory(const StorageID & table_id_, ConstraintsDescription constraints_, const ASTCreateQuery & query, ContextPtr context_);
};

}
