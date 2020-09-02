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

struct Rocksdb
{
    rocksdb::DB * rocksdb;
    explicit Rocksdb(rocksdb::DB * rocksdb_) : rocksdb{rocksdb_} {}
    Rocksdb(const Rocksdb &) = delete;
    Rocksdb & operator=(const Rocksdb &) = delete;

    void shutdown()
    {
        if (rocksdb)
        {
           rocksdb->Close();
           delete rocksdb;
        }
    }
 };

class StorageEmbeddedRocksdb final : public ext::shared_ptr_helper<StorageEmbeddedRocksdb>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageEmbeddedRocksdb>;
    friend class EmbeddedRocksdbBlockOutputStream;
public:
    std::string getName() const override { return "EmbeddedRocksdb"; }

    ~StorageEmbeddedRocksdb() override;

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
    StorageEmbeddedRocksdb(const StorageFactory::Arguments & args);

private:
    using RocksdbPtr = std::shared_ptr<Rocksdb>;
    String rocksdb_dir;
    RocksdbPtr rocksdb_ptr;
    mutable std::shared_mutex rwlock;

    void initDb();
};
}
