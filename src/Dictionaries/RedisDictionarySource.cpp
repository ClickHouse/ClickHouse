#include <Dictionaries/RedisDictionarySource.h>
#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/DictionaryStructure.h>

#include <Columns/IColumn.h>
#include <Interpreters/Context.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/RemoteHostFilter.h>

#include <IO/WriteHelpers.h>

#include <Dictionaries/RedisSource.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int UNSUPPORTED_METHOD;
        extern const int INVALID_CONFIG_PARAMETER;
        extern const int LOGICAL_ERROR;
    }

    void registerDictionarySourceRedis(DictionarySourceFactory & factory)
    {
        auto create_table_source = [=](const String & /*name*/,
                                    const DictionaryStructure & dict_struct,
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

            RedisConfiguration configuration =
            {
                .host = host,
                .port = static_cast<UInt16>(port),
                .db_index = config.getUInt(redis_config_prefix + ".db_index", DEFAULT_REDIS_DB_INDEX),
                .password = config.getString(redis_config_prefix + ".password", DEFAULT_REDIS_PASSWORD),
                .storage_type = parseStorageType(config.getString(redis_config_prefix + ".storage_type", "")),
                .pool_size = config.getUInt(redis_config_prefix + ".pool_size", DEFAULT_REDIS_POOL_SIZE),
            };

            return std::make_unique<RedisDictionarySource>(dict_struct, configuration, sample_block);
        };

        factory.registerSource("redis", create_table_source);
    }

    RedisDictionarySource::RedisDictionarySource(
        const DictionaryStructure & dict_struct_,
        const RedisConfiguration & configuration_,
        const Block & sample_block_)
        : dict_struct{dict_struct_}
        , configuration(configuration_)
        , pool(std::make_shared<RedisPool>(configuration.pool_size))
        , sample_block{sample_block_}
    {
        if (dict_struct.attributes.size() != 1)
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                "Invalid number of non key columns for Redis source: {}, expected 1",
                dict_struct.attributes.size());

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

    QueryPipeline RedisDictionarySource::loadAll()
    {
        auto connection = getRedisConnection(pool, configuration);

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
            keys = *getRedisHashMapKeys(connection, keys);
        }

        return QueryPipeline(std::make_shared<RedisSource>(
            std::move(connection), std::move(keys),
            configuration.storage_type, sample_block, REDIS_MAX_BLOCK_SIZE));
    }

    QueryPipeline RedisDictionarySource::loadIds(const std::vector<UInt64> & ids)
    {
        auto connection = getRedisConnection(pool, configuration);

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
        auto connection = getRedisConnection(pool, configuration);

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
                    key << (*key_columns[i])[row].safeGet<String>();
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

}
