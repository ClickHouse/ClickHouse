#pragma once

#include <mutex>

#include <ext/shared_ptr_helper.h>

#include <Core/NamesAndTypes.h>
#include <Storages/IStorage.h>
#include <DataStreams/IBlockOutputStream.h>


namespace DB
{

/** Implements storage in the RAM.
  * Suitable for temporary data.
  * It does not support keys.
  * Data is stored as a set of blocks and is not stored anywhere else.
  */
class StorageMemory final : public ext::shared_ptr_helper<StorageMemory>, public IStorage
{
friend class MemoryBlockInputStream;
friend class MemoryBlockOutputStream;
friend struct ext::shared_ptr_helper<StorageMemory>;

public:
    String getName() const override { return "Memory"; }

    size_t getSize() const { return data.size(); }

    Pipes read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, const Context & context) override;

    void drop() override;

    void truncate(const ASTPtr &, const StorageMetadataPtr &, const Context &, TableExclusiveLockHolder &) override;

    std::optional<UInt64> totalRows() const override;
    std::optional<UInt64> totalBytes() const override;

private:
    /// The data itself. `list` - so that when inserted to the end, the existing iterators are not invalidated.
    BlocksList data;

    mutable std::mutex mutex;

protected:
    StorageMemory(const StorageID & table_id_, ColumnsDescription columns_description_, ConstraintsDescription constraints_);
};

}
