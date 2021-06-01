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

namespace DB
{

/** Special storage, allows incremental data aggregation in memory.
  * Creates and stores all the data in the Aggregator,
  * allowing reads and writes to it.
  */
class StorageAggregatingMemory final : public ext::shared_ptr_helper<StorageAggregatingMemory>, public IStorage
{
friend class AggregatingOutputStream;
friend struct ext::shared_ptr_helper<StorageAggregatingMemory>;

public:
    String getName() const override { return "AggregatingMemory"; }

    void startup() override;

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

    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &) override;

    // TODO implement totalRows and totalBytes using data from Aggregator (if possible)
    // std::optional<UInt64> totalRows(const Settings &) const override;
    // std::optional<UInt64> totalBytes(const Settings &) const override;

protected:
    /* Initialize engine before executing any queries. This initialization is proceeding
     * in a lazy manner, exactly once. This function will do nothing if engine is already
     * initialized.
     */
    void lazyInit();

    /// Create ManyData, and assign zero state if needed.
    void initState(ContextPtr context);

private:
    Poco::Logger * log;

    mutable std::mutex mutex;
    std::atomic<bool> is_initialized{false};

    SelectQueryDescription select_query;
    ContextPtr constructor_context;
    ConstraintsDescription constructor_constraints;

    StoragePtr source_storage;
    Block src_block_header;
    StorageMetadataPtr src_metadata_snapshot;

    AggregatingTransformParamsPtr aggregator_transform;

    std::unique_ptr<SelectQueryExpressionAnalyzer> query_analyzer;
    ExpressionAnalysisResult analysis_result;

    std::shared_ptr<ManyAggregatedData> many_data;

protected:
    StorageAggregatingMemory(const StorageID & table_id_, ConstraintsDescription constraints_, const ASTCreateQuery & query, ContextPtr context_);
};

}
