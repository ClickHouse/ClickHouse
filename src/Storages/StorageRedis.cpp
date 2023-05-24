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

/// TODO make "password", "db_index", "storage_type", "pool_size" optional
RedisConfiguration StorageRedis::getConfiguration(ASTs engine_args, ContextPtr context)
{
    RedisConfiguration configuration;

    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, context))
    {
        validateNamedCollection(
            *named_collection,
            ValidateKeysMultiset<RedisEqualKeysSet>{"host", "port", "hostname", "password", "db_index", "storage_type", "pool_size"},
            {});

        configuration.host = named_collection->getAny<String>({"host", "hostname"});
        configuration.port = static_cast<uint32_t>(named_collection->get<UInt64>("port"));
        configuration.password = named_collection->get<String>("password");
        configuration.db_index = static_cast<uint32_t>(named_collection->get<UInt64>({"db_index"}));
        configuration.storage_type = parseStorageType(named_collection->getOrDefault<String>("storage_type", ""));
        configuration.pool_size = static_cast<uint32_t>(named_collection->get<UInt64>("pool_size"));
    }
    else
    {
        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);

        /// 6379 is the default Redis port.
        auto parsed_host_port = parseAddress(checkAndGetLiteralArgument<String>(engine_args[0], "host:port"), 6379);

        configuration.host = parsed_host_port.first;
        configuration.port = parsed_host_port.second;
        configuration.db_index = static_cast<uint32_t>(checkAndGetLiteralArgument<UInt64>(engine_args[1], "db_index"));
        configuration.password = checkAndGetLiteralArgument<String>(engine_args[2], "password");
        configuration.storage_type = parseStorageType(checkAndGetLiteralArgument<String>(engine_args[3], "storage_type"));
        configuration.pool_size = static_cast<uint32_t>(checkAndGetLiteralArgument<UInt64>(engine_args[4], "pool_size"));
    }

    if (configuration.storage_type == RedisStorageType::UNKNOWN)
        throw Exception(ErrorCodes::INVALID_REDIS_STORAGE_TYPE, "Invalid Redis storage type");

    context->getRemoteHostFilter().checkHostAndPort(configuration.host, toString(configuration.port));
    return configuration;
}

void registerStorageRedis(StorageFactory & factory)
{
    factory.registerStorage(
        "Redis",
        [](const StorageFactory::Arguments & args)
        {
            auto configuration = StorageRedis::getConfiguration(args.engine_args, args.getLocalContext());

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
