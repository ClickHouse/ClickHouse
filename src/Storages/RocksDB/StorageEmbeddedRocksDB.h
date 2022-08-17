#pragma once

#include <memory>
#include <shared_mutex>
#include <Storages/IStorage.h>
#include <Interpreters/IKeyValueEntity.h>
#include <rocksdb/status.h>


namespace rocksdb
{
    class DB;
    class Statistics;
}


namespace DB
{

class Context;

/// Wrapper for rocksdb storage.
/// Operates with rocksdb data structures via rocksdb API (holds pointer to rocksdb::DB inside for that).
/// Storage have one primary key.
/// Values are serialized into raw strings to store in rocksdb.
class StorageEmbeddedRocksDB final : public IStorage, public IKeyValueEntity, WithContext
{
    friend class EmbeddedRocksDBSink;
public:
    StorageEmbeddedRocksDB(const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata,
        bool attach,
        ContextPtr context_,
        const String & primary_key_,
        Int32 ttl_ = 0,
        bool read_only = false);

    std::string getName() const override { return "EmbeddedRocksDB"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;
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

    std::shared_ptr<rocksdb::Statistics> getRocksDBStatistics() const;
    std::vector<rocksdb::Status> multiGet(const std::vector<rocksdb::Slice> & slices_keys, std::vector<String> & values) const;
    Names getPrimaryKey() const override { return {primary_key}; }

    Chunk getByKeys(const ColumnsWithTypeAndName & keys, PaddedPODArray<UInt8> & null_map, const Names &) const override;

    Block getSampleBlock(const Names &) const override;

    /// Return chunk with data for given serialized keys.
    /// If out_null_map is passed, fill it with 1/0 depending on key was/wasn't found. Result chunk may contain default values.
    /// If out_null_map is not passed. Not found rows excluded from result chunk.
    Chunk getBySerializedKeys(
        const std::vector<std::string> & keys,
        PaddedPODArray<UInt8> * out_null_map) const;

private:
    const String primary_key;
    using RocksDBPtr = std::unique_ptr<rocksdb::DB>;
    RocksDBPtr rocksdb_ptr;
    mutable std::shared_mutex rocksdb_ptr_mx;
    String rocksdb_dir;
    Int32 ttl;
    bool read_only;

    void initDB();
};
}
