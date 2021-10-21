#pragma once

#include <memory>
#include <common/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace rocksdb
{
    class DB;
}


namespace DB
{

class Context;

class StorageEmbeddedRocksDB final : public shared_ptr_helper<StorageEmbeddedRocksDB>, public IStorage
{
    friend struct shared_ptr_helper<StorageEmbeddedRocksDB>;
    friend class EmbeddedRocksDBSource;
    friend class EmbeddedRocksDBBlockOutputStream;
    friend class EmbeddedRocksDBBlockInputStream;
public:
    std::string getName() const override { return "EmbeddedRocksDB"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;
    void truncate(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr, TableExclusiveLockHolder &) override;

    bool supportsParallelInsert() const override { return true; }
    bool supportsIndexForIn() const override { return true; }
    bool mayBenefitFromIndexForIn(
        const ASTPtr & node, ContextPtr /*query_context*/, const StorageMetadataPtr & /*metadata_snapshot*/) const override
    {
        return node->getColumnName() == primary_key;
    }

    bool storesDataOnDisk() const override { return true; }
    Strings getDataPaths() const override { return {rocksdb_dir}; }

protected:
    StorageEmbeddedRocksDB(const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata,
        bool attach,
        ContextPtr context_,
        const String & primary_key_);

private:
    const String primary_key;
    using RocksDBPtr = std::unique_ptr<rocksdb::DB>;
    RocksDBPtr rocksdb_ptr;
    String rocksdb_dir;

    void initDB();
};
}
