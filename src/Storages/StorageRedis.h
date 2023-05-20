#pragma once

#include <Poco/Redis/Redis.h>
#include <Storages/IStorage.h>
#include <Dictionaries/RedisSource.h>

namespace DB
{
/* Implements storage in the Redis.
 * Use ENGINE = Redis(host:port, db_index, password, storage_type);
 * Read only.
 */
class StorageRedis : public IStorage
{
public:
    StorageRedis(
        const StorageID & table_id_,
        const RedisConfiguration & configuration_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment_);

    std::string getName() const override { return "Redis"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        ContextPtr context) override;

    static RedisConfiguration getConfiguration(ASTs engine_args, ContextPtr context);

private:
    StorageID table_id;
    RedisConfiguration configuration;

    ColumnsDescription columns;
    ConstraintsDescription constraints;

    String comment;
    RedisPoolPtr pool;
};

}
