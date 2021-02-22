#pragma once

#include <atomic>
#include <optional>
#include <mutex>

#include <ext/shared_ptr_helper.h>

#include <Core/NamesAndTypes.h>
#include <Storages/IStorage.h>
#include <DataStreams/IBlockOutputStream.h>

#include <Common/MultiVersion.h>

namespace DB
{

/** TODO: describe this memory storage
  */
class StorageAggregatingMemory final : public ext::shared_ptr_helper<StorageAggregatingMemory>, public IStorage
{
friend class MemoryBlockOutputStream;
friend struct ext::shared_ptr_helper<StorageAggregatingMemory>;

public:
    String getName() const override { return "AggregatingMemory"; }

    size_t getSize() const { return data.get()->size(); }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    bool supportsParallelInsert() const override { return true; }
    bool supportsSubcolumns() const override { return true; }

    /// Smaller blocks (e.g. 64K rows) are better for CPU cache.
    bool prefersLargeBlocks() const override { return false; }

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, const Context & context) override;

    void drop() override;

    void mutate(const MutationCommands & commands, const Context & context) override;

    void truncate(const ASTPtr &, const StorageMetadataPtr &, const Context &, TableExclusiveLockHolder &) override;

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

protected:
    StorageAggregatingMemory(const StorageID & table_id_, ColumnsDescription columns_description_, ConstraintsDescription constraints_);
};

}
