#include "RedisDictionarySource.h"
#include "DictionarySourceFactory.h"
#include "DictionaryStructure.h"
#include "registerDictionaries.h"

#include <Poco/Redis/Array.h>
#include <Poco/Redis/Client.h>
#include <Poco/Redis/Command.h>
#include <Poco/Redis/Type.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Interpreters/Context.h>
#include <QueryPipeline/QueryPipeline.h>

#include <IO/WriteHelpers.h>

#include "RedisSource.h"

namespace DB
{
    namespace ErrorCodes
    {
        extern const int UNSUPPORTED_METHOD;
        extern const int INVALID_CONFIG_PARAMETER;
        extern const int INTERNAL_REDIS_ERROR;
        extern const int LOGICAL_ERROR;
        extern const int TIMEOUT_EXCEEDED;
    }

    static RedisStorageType parseStorageType(const String & storage_type_str)
    {
        if (storage_type_str == "hash_map")
            return RedisStorageType::HASH_MAP;
        else if (!storage_type_str.empty() && storage_type_str != "simple")
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Unknown storage type {} for Redis dictionary", storage_type_str);

        return RedisStorageType::SIMPLE;
    }

    void registerDictionarySourceRedis(DictionarySourceFactory & factory)
    {
        auto create_table_source = [=](const DictionaryStructure & dict_struct,
                                    const Poco::Util::AbstractConfiguration & config,
                                    const String & config_prefix,
                                    Block & sample_block,
                                    ContextPtr global_context,
                                    const std::string & /* default_database */,
                                    bool /* created_from_ddl */) -> DictionarySourcePtr {

            auto redis_config_prefix = config_prefix + ".redis";

            auto host = config.getString(redis_config_prefix + ".host");
            auto port = config.getUInt(redis_config_prefix + ".port");
            global_context->getRemoteHostFilter().checkHostAndPort(host, toString(port));

            RedisDictionarySource::Configuration configuration =
            {
                .host = host,
                .port = static_cast<UInt16>(port),
                .db_index = config.getUInt(redis_config_prefix + ".db_index", 0),
                .password = config.getString(redis_config_prefix + ".password", ""),
                .storage_type = parseStorageType(config.getString(redis_config_prefix + ".storage_type", "")),
                .pool_size = config.getUInt(redis_config_prefix + ".pool_size", 16),
            };

            return std::make_unique<RedisDictionarySource>(dict_struct, configuration, sample_block);
        };

        factory.registerSource("redis", create_table_source);
    }

    static constexpr size_t REDIS_MAX_BLOCK_SIZE = DEFAULT_BLOCK_SIZE;
    static constexpr size_t REDIS_LOCK_ACQUIRE_TIMEOUT_MS = 5000;

    RedisDictionarySource::RedisDictionarySource(
        const DictionaryStructure & dict_struct_,
        const Configuration & configuration_,
        const Block & sample_block_)
        : dict_struct{dict_struct_}
        , configuration(configuration_)
        , pool(std::make_shared<Pool>(configuration.pool_size))
        , sample_block{sample_block_}
    {
        if (dict_struct.attributes.size() != 1)
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                "Invalid number of non key columns for Redis source: {}, expected 1",
                DB::toString(dict_struct.attributes.size()));

        if (configuration.storage_type == RedisStorageType::HASH_MAP)
        {
            if (!dict_struct.key)
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                    "Redis source with storage type \'hash_map\' must have key");

            if (dict_struct.key->size() != 2)
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                    "Redis source with storage type \'hash_map\' requires 2 keys");
            // suppose key[0] is primary key, key[1] is secondary key

            for (const auto & key : *dict_struct.key)
                if (!isInteger(key.type) && !isString(key.type))
                    throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                        "Redis source supports only integer or string key, but key '{}' of type {} given",
                        key.name,
                        key.type->getName());
        }
    }

    RedisDictionarySource::RedisDictionarySource(const RedisDictionarySource & other)
        : RedisDictionarySource(other.dict_struct, other.configuration, other.sample_block)
    {
    }

    RedisDictionarySource::~RedisDictionarySource() = default;

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

    QueryPipeline RedisDictionarySource::loadAll()
    {
        auto connection = getConnection();

        RedisCommand command_for_keys("KEYS");
        command_for_keys << "*";

        /// Get only keys for specified storage type.
        auto all_keys = connection->client->execute<RedisArray>(command_for_keys);
        if (all_keys.isNull())
            return QueryPipeline(std::make_shared<RedisSource>(
                std::move(connection), RedisArray{},
                configuration.storage_type, sample_block, REDIS_MAX_BLOCK_SIZE));

        RedisArray keys;
        auto key_type = storageTypeToKeyType(configuration.storage_type);
        for (auto && key : all_keys)
            if (key_type == connection->client->execute<String>(RedisCommand("TYPE").addRedisType(key)))
                keys.addRedisType(key);

        if (configuration.storage_type == RedisStorageType::HASH_MAP)
        {
            RedisArray hkeys;
            for (const auto & key : keys)
            {
                RedisCommand command_for_secondary_keys("HKEYS");
                command_for_secondary_keys.addRedisType(key);

                auto secondary_keys = connection->client->execute<RedisArray>(command_for_secondary_keys);

                RedisArray primary_with_secondary;
                primary_with_secondary.addRedisType(key);
                for (const auto & secondary_key : secondary_keys)
                {
                    primary_with_secondary.addRedisType(secondary_key);
                    /// Do not store more than max_block_size values for one request.
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

        return QueryPipeline(std::make_shared<RedisSource>(
            std::move(connection), std::move(keys),
            configuration.storage_type, sample_block, REDIS_MAX_BLOCK_SIZE));
    }

    QueryPipeline RedisDictionarySource::loadIds(const std::vector<UInt64> & ids)
    {
        auto connection = getConnection();

        if (configuration.storage_type == RedisStorageType::HASH_MAP)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Cannot use loadIds with 'hash_map' storage type");

        if (!dict_struct.id)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'id' is required for selective loading");

        RedisArray keys;

        for (UInt64 id : ids)
            keys << DB::toString(id);

        return QueryPipeline(std::make_shared<RedisSource>(
            std::move(connection), std::move(keys),
            configuration.storage_type, sample_block, REDIS_MAX_BLOCK_SIZE));
    }

    QueryPipeline RedisDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
    {
        auto connection = getConnection();

        if (key_columns.size() != dict_struct.key->size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The size of key_columns does not equal to the size of dictionary key");

        RedisArray keys;
        for (auto row : requested_rows)
        {
            RedisArray key;
            for (size_t i = 0; i < key_columns.size(); ++i)
            {
                const auto & type = dict_struct.key->at(i).type;
                if (isInteger(type))
                    key << DB::toString(key_columns[i]->get64(row));
                else if (isString(type))
                    key << get<const String &>((*key_columns[i])[row]);
                else
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected type of key in Redis dictionary");
            }

            keys.add(key);
        }

        return QueryPipeline(std::make_shared<RedisSource>(
            std::move(connection), std::move(keys),
            configuration.storage_type, sample_block, REDIS_MAX_BLOCK_SIZE));
    }

    String RedisDictionarySource::toString() const
    {
        return "Redis: " + configuration.host + ':' + DB::toString(configuration.port);
    }

    RedisDictionarySource::ConnectionPtr RedisDictionarySource::getConnection() const
    {
        ClientPtr client;
        bool ok = pool->tryBorrowObject(client,
            [] { return std::make_unique<Poco::Redis::Client>(); },
            REDIS_LOCK_ACQUIRE_TIMEOUT_MS);

        if (!ok)
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED,
                "Could not get connection from pool, timeout exceeded {} seconds",
                REDIS_LOCK_ACQUIRE_TIMEOUT_MS);

        if (!client->isConnected())
        {
            try
            {
                client->connect(configuration.host, configuration.port);

                if (!configuration.password.empty())
                {
                    RedisCommand command("AUTH");
                    command << configuration.password;
                    String reply = client->execute<String>(command);
                    if (reply != "OK")
                        throw Exception(ErrorCodes::INTERNAL_REDIS_ERROR,
                            "Authentication failed with reason {}", reply);
                }

                if (configuration.db_index != 0)
                {
                    RedisCommand command("SELECT");
                    command << std::to_string(configuration.db_index);
                    String reply = client->execute<String>(command);
                    if (reply != "OK")
                        throw Exception(ErrorCodes::INTERNAL_REDIS_ERROR,
                            "Selecting database with index {} failed with reason {}",
                            configuration.db_index, reply);
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
