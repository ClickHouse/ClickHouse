#include "StorageRedis.h"
#include <Common/parseAddress.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTLiteral.h>

#include <Storages/StorageFactory.h>

#include "Poco/Redis/Redis.h"
#include "Poco/Redis/Type.h"
#include "Poco/Redis/Exception.h"
#include <Poco/Redis/Array.h>
#include <Poco/Redis/Client.h>
#include <Poco/Redis/Command.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Common/logger_useful.h>
#include "Dictionaries/RedisSource.h"

namespace DB
{

static constexpr size_t REDIS_MAX_BLOCK_SIZE = DEFAULT_BLOCK_SIZE;
static constexpr size_t REDIS_LOCK_ACQUIRE_TIMEOUT_MS = 5000;
namespace ErrorCodes
{
    extern const int INTERNAL_REDIS_ERROR;
    extern const int TIMEOUT_EXCEEDED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

StorageRedis::StorageRedis(
        const StorageID & table_id_,
        const std::string & host_,
        const UInt16 & port_,
        const UInt32 & db_index_,
        const std::string & password_,
        const RedisStorageType & storage_type_,
        const String & primary_key_column_name_,
        const String & secondary_key_column_name_,
        const String & value_column_name_,
        ContextPtr context_,
        const std::string & options_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment)
    : IStorage(table_id_)
    , host(host_)
    , port(port_)
    , db_index(db_index_)
    , password(password_)
    , storage_type(storage_type_)
    , context(context_)
    , options(options_)
    , pool(std::make_shared<Pool>(context->getSettingsRef().redis_connection_pool_size))
    , primary_key_column_name(primary_key_column_name_)
    , secondary_key_column_name(secondary_key_column_name_)
    , value_column_name(value_column_name_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}

static String storageTypeToKeyType(RedisStorageType type)
{
    switch (type)
    {
        case RedisStorageType::SIMPLE:
            return "string";
        case RedisStorageType::HASH_MAP:
            return "hash";
        default:
            return "none";
    }

    __builtin_unreachable();
}

Poco::Redis::Array StorageRedis::getKeys(ConnectionPtr & connection)
{
    RedisCommand command_for_keys("KEYS");
    command_for_keys << "*";

    auto all_keys = connection->client->execute<Poco::Redis::Array>(command_for_keys);
    if (all_keys.isNull())
        return Poco::Redis::Array{};

    Poco::Redis::Array keys;
    auto key_type = storageTypeToKeyType(storage_type);
    for (auto && key : all_keys)
        if (key_type == connection->client->execute<String>(RedisCommand("TYPE").addRedisType(key)))
                keys.addRedisType(key);

    if (storage_type == RedisStorageType::HASH_MAP)
    {
        Poco::Redis::Array hkeys;
        for (const auto & key : keys)
        {
            RedisCommand command_for_secondary_keys("HKEYS");
            command_for_secondary_keys.addRedisType(key);

            auto secondary_keys = connection->client->execute<Poco::Redis::Array>(command_for_secondary_keys);

            Poco::Redis::Array primary_with_secondary;
            primary_with_secondary.addRedisType(key);
            for (const auto & secondary_key : secondary_keys)
            {
                primary_with_secondary.addRedisType(secondary_key);
                if (primary_with_secondary.size() == REDIS_MAX_BLOCK_SIZE + 1)
                {
                    hkeys.add(primary_with_secondary);
                    primary_with_secondary.clear();
                    primary_with_secondary.addRedisType(key);
                }
            }

            if (primary_with_secondary.size() > 1)
                hkeys.add(primary_with_secondary);
        }
        keys = hkeys;
    }
    return keys;
}

Pipe StorageRedis::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info_*/,
    ContextPtr /*context_*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size_*/,
    unsigned /*num_streams*/)
{
    auto connection = getConnection();
    storage_snapshot->check(column_names);
    Block sample_block;
    Poco::Redis::Array keys = getKeys(connection);
    std::vector<bool> selected_columns(3, false);
    for (const String & column_name : column_names)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);
        sample_block.insert({ column_data.type, column_data.name });
        if (column_data.name == primary_key_column_name)
            selected_columns[0] = true;
        else if (column_data.name == secondary_key_column_name)
            selected_columns[1] = true;
        else if (column_data.name == value_column_name)
            selected_columns[2] = true;
    }
    return Pipe(std::make_shared<RedisSource>(std::move(connection), keys, storage_type, sample_block, REDIS_MAX_BLOCK_SIZE, selected_columns));
}

StorageRedisConfiguration StorageRedis::getConfiguration(ASTs engine_args, ContextPtr context)
{
    StorageRedisConfiguration configuration;
    if (auto named_collection = getExternalDataSourceConfiguration(engine_args, context))
    {
        auto [common_configuration, storage_specific_args, _] = named_collection.value();
        configuration.set(common_configuration);

        for (const auto & [arg_name, arg_value] : storage_specific_args)
        {   
            if (arg_name == "options")
                configuration.host = arg_value->as<ASTLiteral>()->value.safeGet<String>();
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Unexpected key-value argument."
                        "Got: {}, but expected one of:"
                        "host, port, database_id, password, storage_type, options.", arg_name);
        }
    }
    else 
    {
        if (engine_args.size() < 4 || engine_args.size() > 5)
            throw Exception(
                "Storage Redis requires from 4 to 5 parameters: MongoDB('host:port', 'database_id', 'password', 'storage_type', [, 'options']).",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);


        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);

        /// 6379 is the default Redis port.
        auto parsed_host_port = parseAddress(engine_args[0]->as<ASTLiteral &>().value.safeGet<String>(), 6379);

        configuration.host = parsed_host_port.first;
        configuration.port = parsed_host_port.second;
        configuration.db_index = engine_args[1]->as<ASTLiteral &>().value.safeGet<UInt32>();
        configuration.password = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        auto st_type = engine_args[3]->as<ASTLiteral &>().value.safeGet<String>();

        configuration.storage_type = StorageRedisConfiguration::RedisStorageType::UNKNOWN;
        if (st_type == "SIMPLE")
            configuration.storage_type = StorageRedisConfiguration::RedisStorageType::SIMPLE;
        else if (st_type == "HASH_MAP")
            configuration.storage_type = StorageRedisConfiguration::RedisStorageType::HASH_MAP;

        if (engine_args.size() >= 5)
            configuration.options = engine_args[4]->as<ASTLiteral &>().value.safeGet<String>();
    }
    return configuration;
}


void registerStorageRedis(StorageFactory & factory)
{
    factory.registerStorage("Redis", [](const StorageFactory::Arguments & args)
    {
        auto configuration = StorageRedis::getConfiguration(args.engine_args, args.getLocalContext());
        std::string primary_key;
        std::string secondary_key;
        std::string value;
        if (configuration.storage_type == StorageRedisConfiguration::RedisStorageType::HASH_MAP) {
            primary_key = args.columns.getNamesOfPhysical()[0];
            secondary_key = args.columns.getNamesOfPhysical()[1];
            value = args.columns.getNamesOfPhysical()[2];
        } else {
            secondary_key = args.columns.getNamesOfPhysical()[0];
            value = args.columns.getNamesOfPhysical()[1];
        }
        return std::make_shared<StorageRedis>(
            args.table_id,
            configuration.host,
            configuration.port,
            configuration.db_index,
            configuration.password,
            static_cast<RedisStorageType>(configuration.storage_type),
            primary_key,
            secondary_key,
            value,
            args.getContext(),
            configuration.options,
            args.columns,
            args.constraints,
            args.comment);
    },
    {
        .source_access_type = AccessType::REDIS,
    });
}

ConnectionPtr StorageRedis::getConnection() const
{
    ClientPtr client;
    bool flag = pool->tryBorrowObject(client,
        [] { return std::make_unique<Poco::Redis::Client>(); },
        REDIS_LOCK_ACQUIRE_TIMEOUT_MS);
    if (!flag)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED,
            "Could not get connection from pool, timeout exceeded {} seconds",
            REDIS_LOCK_ACQUIRE_TIMEOUT_MS);
    if (!client->isConnected())
    {
        try
        {
            client->connect(host, port);
            if (!password.empty())
            {
                RedisCommand command("AUTH");
                command << password;
                String reply = client->execute<String>(command);
                if (reply != "OK")
                    throw Exception(ErrorCodes::INTERNAL_REDIS_ERROR,
                        "Authentication failed with reason {}", reply);
            }

            if (db_index != 0)
            {
                RedisCommand command("SELECT");
                command << std::to_string(db_index);
                String reply = client->execute<String>(command);
                if (reply != "OK")
                    throw Exception(ErrorCodes::INTERNAL_REDIS_ERROR,
                        "Selecting database with index {} failed with reason {}",
                        db_index, reply);
            }
        }
        catch (...)
        {
            if (client->isConnected())
                client->disconnect();

            pool->returnObject(std::move(client));
            throw;
        }
    }
    return std::make_unique<Connection>(pool, std::move(client));
}

}
