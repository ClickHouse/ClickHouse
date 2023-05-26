#include <Storages/StorageRedis.h>
#include <Storages/StorageFactory.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/KVStorageUtils.h>

#include <unordered_set>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/Pipe.h>
#include <Common/logger_useful.h>
#include <Common/parseAddress.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_REDIS_STORAGE_TYPE;
    extern const int NOT_IMPLEMENTED;
}

StorageRedis::StorageRedis(
    const StorageID & table_id_,
    const RedisConfiguration & configuration_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment_)
    : IStorage(table_id_)
    , table_id(table_id_)
    , configuration(configuration_)
    , log(&Poco::Logger::get("StorageRedis"))
{
    pool = std::make_shared<RedisPool>(configuration.pool_size);
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment_);
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageRedis::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    auto connection = getRedisConnection(pool, configuration);
    storage_snapshot->check(column_names);

    Block sample_block;
    RedisColumnTypes redis_types;
    auto all_columns = storage_snapshot->metadata->getColumns().getNamesOfPhysical();

    for (const String & column_name : column_names)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);
        sample_block.insert({column_data.type, column_data.name});
        redis_types.push_back(getRedisColumnType(configuration.storage_type, all_columns, column_name));
    }

    FieldVectorPtr fields;
    bool all_scan = false;

    String primary_key = all_columns.at(0);
    auto primary_key_data_type = sample_block.getByName(primary_key).type;

    std::tie(fields, all_scan) = getFilterKeys(primary_key, primary_key_data_type, query_info, context);

    if (all_scan)
    {
        RedisCommand command_for_keys("KEYS");
        /// generate keys by table name prefix
//        command_for_keys << table_id.getTableName() + ":" + serializeStorageType(configuration.storage_type) + ":*";
        command_for_keys << "*";

        auto all_keys = connection->client->execute<RedisArray>(command_for_keys);

        if (all_keys.isNull() || all_keys.size() == 0)
            return {};

        Pipes pipes;

        size_t num_keys = all_keys.size();
        size_t num_threads = std::min<size_t>(num_streams, all_keys.size());

        num_threads = std::min<size_t>(num_threads, configuration.pool_size);
        assert(num_keys <= std::numeric_limits<uint32_t>::max());

        for (size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx)
        {
            size_t begin = num_keys * thread_idx / num_threads;
            size_t end = num_keys * (thread_idx + 1) / num_threads;

            RedisArray keys;
            for (size_t pos=begin; pos<std::min(end, num_keys); pos++)
                keys.add(all_keys.get<RedisBulkString>(pos));

            if (configuration.storage_type == RedisStorageType::HASH_MAP)
            {
                keys = *getRedisHashMapKeys(connection, keys);
            }

            delete connection.release();

            /// TODO reduce keys copy
            pipes.emplace_back(std::make_shared<RedisSource>(
                getRedisConnection(pool, configuration), keys,
                configuration.storage_type, sample_block, redis_types, max_block_size));
        }
        return Pipe::unitePipes(std::move(pipes));
    }
    else
    {
        if (fields->empty())
            return {};

        Pipes pipes;

        size_t num_keys = fields->size();
        size_t num_threads = std::min<size_t>(num_streams, fields->size());

        num_threads = std::min<size_t>(num_threads, configuration.pool_size);
        assert(num_keys <= std::numeric_limits<uint32_t>::max());

        for (size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx)
        {
            size_t begin = num_keys * thread_idx / num_threads;
            size_t end = num_keys * (thread_idx + 1) / num_threads;

            RedisArray keys;
            for (size_t pos=begin; pos<std::min(end, num_keys); pos++)
                keys.add(fields->at(pos).get<String>());

            if (configuration.storage_type == RedisStorageType::HASH_MAP)
            {
                keys = *getRedisHashMapKeys(connection, keys);
            }

            delete connection.release();

            pipes.emplace_back(std::make_shared<RedisSource>(
                getRedisConnection(pool, configuration), keys,
                configuration.storage_type, sample_block, redis_types, max_block_size));
        }
        return Pipe::unitePipes(std::move(pipes));
    }
}


SinkToStoragePtr StorageRedis::write(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    ContextPtr /*context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Write is unsupported for StorageRedis");
}

void registerStorageRedis(StorageFactory & factory)
{
    factory.registerStorage(
        "Redis",
        [](const StorageFactory::Arguments & args)
        {
            auto configuration = getRedisConfiguration(args.engine_args, args.getLocalContext());

            checkRedisTableStructure(args.columns, configuration);

            return std::make_shared<StorageRedis>(
                args.table_id,
                configuration,
                args.columns,
                args.constraints,
                args.comment);
        },
        {
            .source_access_type = AccessType::REDIS,
        });
}

}
