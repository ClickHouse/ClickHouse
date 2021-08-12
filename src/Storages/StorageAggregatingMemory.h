#pragma once

#include <atomic>
#include <optional>
#include <mutex>
#include <shared_mutex>

#include <common/shared_ptr_helper.h>

#include <Core/NamesAndTypes.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Processors/QueryPipeline.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Storages/IStorage.h>
#include <DataStreams/IBlockOutputStream.h>

namespace DB
{

/** Special storage, allows incremental data aggregation in memory.
  * Creates and stores all the data in the Aggregator,
  * allowing reads and writes to it.
  */
class StorageAggregatingMemory final : public shared_ptr_helper<StorageAggregatingMemory>, public IStorage
{
    friend class AggregatingMemorySink;
    friend struct shared_ptr_helper<StorageAggregatingMemory>;

public:
    String getName() const override { return "AggregatingMemory"; }

    void startup() override;

    StorageMetadataPtr getInMemoryMetadataPtrForInsert() const override { return src_metadata_snapshot; }

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

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context) override;

    void drop() override;

    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &) override;

    std::optional<UInt64> totalRows(const Settings &) const override;

    std::optional<UInt64> totalBytes(const Settings &) const override;

protected:
    /// Create ManyData, and assign zero state if needed.
    void initState(ContextPtr context);

private:
    Poco::Logger * log;

    SelectQueryDescription select_query;

    StorageMetadataPtr src_metadata_snapshot;

    AggregatingTransformParamsPtr aggregator_transform_params;
    std::shared_ptr<ManyAggregatedData> many_data;

    /// Reads can be done in parallel, while new blocks must be merged sequentially.
    mutable std::shared_mutex rwlock;

protected:
    StorageAggregatingMemory(const StorageID & table_id_, const ColumnsDescription & columns_, ConstraintsDescription constraints_, const ASTCreateQuery & query, ContextPtr context_);
};

}
