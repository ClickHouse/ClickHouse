#pragma once

#include <Poco/Redis/Redis.h>
#include <Storages/IStorage.h>
#include <Storages/RedisCommon.h>
#include <Interpreters/IKeyValueEntity.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/MutationCommands.h>

namespace DB
{
/* Implements storage in the Redis.
 * Use ENGINE = Redis(host:port[, db_index[, password[, pool_size]]]) PRIMARY KEY(key);
 */
class StorageRedis : public IStorage, public IKeyValueEntity, WithContext
{
public:
    StorageRedis(
        const StorageID & table_id_,
        const RedisConfiguration & configuration_,
        ContextPtr context_,
        const StorageInMemoryMetadata & storage_metadata,
        const String & primary_key_);

    std::string getName() const override { return "Redis"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        ContextPtr context,
        bool /*async_insert*/) override;

    void truncate(const ASTPtr &,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr,
        TableExclusiveLockHolder &) override;

    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;
    void mutate(const MutationCommands &, ContextPtr) override;

    Names getPrimaryKey() const override { return {primary_key}; }

    /// Return chunk with data for given serialized keys.
    /// If out_null_map is passed, fill it with 1/0 depending on key was/wasn't found. Result chunk may contain default values.
    /// If out_null_map is not passed. Not found rows excluded from result chunk.
    Chunk getBySerializedKeys(
        const std::vector<std::string> & keys,
        PaddedPODArray<UInt8> * out_null_map) const;

    Chunk getBySerializedKeys(
        const RedisArray & keys,
        PaddedPODArray<UInt8> * out_null_map) const;

    std::pair<RedisIterator, RedisArray> scan(RedisIterator iterator, const String & pattern, uint64_t max_count);

    RedisArray multiGet(const RedisArray & keys) const;
    void multiSet(const RedisArray & data) const;
    RedisInteger multiDelete(const RedisArray & keys) const;

    Chunk getByKeys(const ColumnsWithTypeAndName & keys, PaddedPODArray<UInt8> & null_map, const Names &) const override;

    Block getSampleBlock(const Names &) const override;

private:
    StorageID table_id;
    RedisConfiguration configuration;

    LoggerPtr log;
    RedisPoolPtr pool;

    const String primary_key;
};

}
