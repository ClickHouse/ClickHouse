#include <Storages/StorageRedis.h>
#include <Storages/StorageFactory.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <unordered_set>
#include <Core/Settings.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/Pipe.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/parseAddress.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
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
    , columns(columns_)
    , constraints(constraints_)
    , comment(comment_)
{
    pool = std::make_shared<RedisPool>(configuration.pool_size);
}

Pipe StorageRedis::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    auto connection = getRedisConnection(pool, configuration);

    storage_snapshot->check(column_names);

    Block sample_block;
    for (const String & column_name : column_names)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);
        sample_block.insert({column_data.type, column_data.name});
    }

    RedisArray keys;
    RedisCommand command_for_keys("KEYS");
    /// generate keys by table name prefix
    command_for_keys << table_id.getTableName() + ":" + toString(configuration.storage_type) + ":*";

    /// Get only keys for specified storage type.
    auto all_keys = connection->client->execute<RedisArray>(command_for_keys);
    return Pipe(std::make_shared<RedisSource>(std::move(connection), all_keys, configuration.storage_type, sample_block, max_block_size));
}


SinkToStoragePtr StorageRedis::write(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    ContextPtr /*context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method write is unsupported for StorageRedis");
}

RedisConfiguration StorageRedis::getConfiguration(ASTs engine_args, ContextPtr context)
{
    RedisConfiguration configuration;

    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, context))
    {
        validateNamedCollection(
            *named_collection,
            ValidateKeysMultiset<RedisEqualKeysSet>{"host", "port", "hostname", "password", "db_index", "storage_type"},
            {});

        configuration.host = named_collection->getAny<String>({"host", "hostname"});
        configuration.port = static_cast<uint32_t>(named_collection->get<UInt64>("port"));
        configuration.password = named_collection->get<String>("password");
        configuration.db_index = static_cast<uint32_t>(named_collection->get<UInt64>({"db_index"}));
        configuration.storage_type = toRedisStorageType(named_collection->getOrDefault<String>("storage_type", ""));
        configuration.pool_size = 16; /// TODO
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
        configuration.storage_type = toRedisStorageType(checkAndGetLiteralArgument<String>(engine_args[3], "storage_type"));
        configuration.pool_size = 16; /// TODO
    }

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

            return std::make_shared<StorageRedis>(
                args.table_id,
                configuration,
                args.columns,
                args.constraints,
                args.comment);
        },
        {
            .source_access_type = AccessType::Redis,
        });
}

}
