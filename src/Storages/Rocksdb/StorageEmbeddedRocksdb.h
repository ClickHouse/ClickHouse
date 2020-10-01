#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>
#include <Storages/TableLockHolder.h>
#include <Disks/IDisk.h>

#include <shared_mutex>

#include "rocksdb/db.h"
#include "rocksdb/table.h"

namespace DB
{

class Context;

class StorageEmbeddedRocksdb final : public ext::shared_ptr_helper<StorageEmbeddedRocksdb>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageEmbeddedRocksdb>;
    friend class EmbeddedRocksdbBlockOutputStream;
public:
    std::string getName() const override { return "EmbeddedRocksdb"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, const Context & context) override;
    void truncate(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, const Context &, TableExclusiveLockHolder &) override;

protected:
    StorageEmbeddedRocksdb(const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata,
        bool attach,
        Context & context_,
        const String & primary_key_);

private:
    const String primary_key;
    using RocksdbPtr = std::unique_ptr<rocksdb::DB>;
    RocksdbPtr rocksdb_ptr;
    String rocksdb_dir;
    mutable std::shared_mutex rwlock;

    void initDb();
};
}
