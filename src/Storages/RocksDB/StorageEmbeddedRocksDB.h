#pragma once

#include <memory>
#include <Common/MultiVersion.h>
#include <Common/SharedMutex.h>
#include <Interpreters/IKeyValueEntity.h>
#include <rocksdb/status.h>
#include <Storages/IStorage.h>
#include <Storages/RocksDB/EmbeddedRocksDBSink.h>
#include <Storages/RocksDB/EmbeddedRocksDBBulkSink.h>
#include <Storages/RocksDB/RocksDBSettings.h>


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
    friend class EmbeddedRocksDBBulkSink;
    friend class ReadFromEmbeddedRocksDB;
public:
    StorageEmbeddedRocksDB(const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata,
        LoadingStrictnessLevel mode,
        ContextPtr context_,
        std::unique_ptr<RocksDBSettings> settings_,
        const String & primary_key_,
        Int32 ttl_ = 0,
        String rocksdb_dir_ = "",
        bool read_only_ = false);

    ~StorageEmbeddedRocksDB() override;

    std::string getName() const override { return "EmbeddedRocksDB"; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context, bool async_insert) override;
    void truncate(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr, TableExclusiveLockHolder &) override;

    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;
    void mutate(const MutationCommands &, ContextPtr) override;
    void drop() override;
    void alter(const AlterCommands & params, ContextPtr query_context, AlterLockHolder &) override;

    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        bool cleanup,
        ContextPtr context) override;

    bool supportsParallelInsert() const override { return true; }

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

    bool supportsDelete() const override { return true; }

    /// To turn on the optimization optimize_trivial_approximate_count_query=1 should be set for a query.
    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override { return true; }

    std::optional<UInt64> totalRows(const Settings & settings) const override;

    std::optional<UInt64> totalBytes(const Settings & settings) const override;

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr /* context */) const override;

    const RocksDBSettings & getSettings() const { return *storage_settings.get(); }

    void setSettings(std::unique_ptr<RocksDBSettings> && settings_) { storage_settings.set(std::move(settings_)); }

private:
    SinkToStoragePtr getSink(ContextPtr context, const StorageMetadataPtr & metadata_snapshot);

    LoggerPtr log;

    MultiVersion<RocksDBSettings> storage_settings;
    const String primary_key;

    using RocksDBPtr = std::unique_ptr<rocksdb::DB>;
    RocksDBPtr rocksdb_ptr;

    mutable SharedMutex rocksdb_ptr_mx;
    String rocksdb_dir;
    Int32 ttl;
    bool read_only;

    void initDB();
};
}
