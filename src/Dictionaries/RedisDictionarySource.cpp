#include "RedisDictionarySource.h"
#include "DictionarySourceFactory.h"
#include "DictionaryStructure.h"
#include "registerDictionaries.h"

namespace DB
{

void registerDictionarySourceRedis(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const DictionaryStructure & dict_struct,
                                   const Poco::Util::AbstractConfiguration & config,
                                   const String & config_prefix,
                                   Block & sample_block,
                                   const Context & /* context */,
                                   const std::string & /* default_database */,
                                   bool /* check_config */) -> DictionarySourcePtr {
        return std::make_unique<RedisDictionarySource>(dict_struct, config, config_prefix + ".redis", sample_block);
    };
    factory.registerSource("redis", create_table_source);
}

}


#include <Poco/Redis/Array.h>
#include <Poco/Redis/Client.h>
#include <Poco/Redis/Command.h>
#include <Poco/Redis/Type.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <IO/WriteHelpers.h>
#include <Common/FieldVisitors.h>

#include "RedisBlockInputStream.h"


namespace DB
{
    namespace ErrorCodes
    {
        extern const int UNSUPPORTED_METHOD;
        extern const int INVALID_CONFIG_PARAMETER;
        extern const int INTERNAL_REDIS_ERROR;
        extern const int LOGICAL_ERROR;
    }


    static const size_t max_block_size = 8192;

    RedisDictionarySource::RedisDictionarySource(
            const DictionaryStructure & dict_struct_,
            const String & host_,
            UInt16 port_,
            UInt8 db_index_,
            const String & password_,
            RedisStorageType storage_type_,
            const Block & sample_block_)
            : dict_struct{dict_struct_}
            , host{host_}
            , port{port_}
            , db_index{db_index_}
            , password{password_}
            , storage_type{storage_type_}
            , sample_block{sample_block_}
            , client{std::make_shared<Poco::Redis::Client>(host, port)}
    {
        if (dict_struct.attributes.size() != 1)
            throw Exception{"Invalid number of non key columns for Redis source: " +
                            DB::toString(dict_struct.attributes.size()) + ", expected 1",
                            ErrorCodes::INVALID_CONFIG_PARAMETER};

        if (storage_type == RedisStorageType::HASH_MAP)
        {
            if (!dict_struct.key)
                throw Exception{"Redis source with storage type \'hash_map\' must have key",
                                ErrorCodes::INVALID_CONFIG_PARAMETER};

            if (dict_struct.key->size() != 2)
                throw Exception{"Redis source with storage type \'hash_map\' requires 2 keys",
                                ErrorCodes::INVALID_CONFIG_PARAMETER};
            // suppose key[0] is primary key, key[1] is secondary key

            for (const auto & key : *dict_struct.key)
                if (!isInteger(key.type) && !isString(key.type))
                    throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                        "Redis source supports only integer or string key, but key '{}' of type {} given", key.name, key.type->getName());
        }

        if (!password.empty())
        {
            RedisCommand command("AUTH");
            command << password;
            String reply = client->execute<String>(command);
            if (reply != "OK")
                throw Exception{"Authentication failed with reason "
                     + reply, ErrorCodes::INTERNAL_REDIS_ERROR};
        }

        if (db_index != 0)
        {
            RedisCommand command("SELECT");
            command << std::to_string(db_index);
            String reply = client->execute<String>(command);
            if (reply != "OK")
                throw Exception{"Selecting database with index " + DB::toString(db_index)
                    + " failed with reason " + reply, ErrorCodes::INTERNAL_REDIS_ERROR};
        }
    }


    RedisDictionarySource::RedisDictionarySource(
            const DictionaryStructure & dict_struct_,
            const Poco::Util::AbstractConfiguration & config_,
            const String & config_prefix_,
            Block & sample_block_)
            : RedisDictionarySource(
            dict_struct_,
            config_.getString(config_prefix_ + ".host"),
            config_.getUInt(config_prefix_ + ".port"),
            config_.getUInt(config_prefix_ + ".db_index", 0),
            config_.getString(config_prefix_ + ".password",""),
            parseStorageType(config_.getString(config_prefix_ + ".storage_type", "")),
            sample_block_)
    {
    }


    RedisDictionarySource::RedisDictionarySource(const RedisDictionarySource & other)
            : RedisDictionarySource{other.dict_struct,
                                    other.host,
                                    other.port,
                                    other.db_index,
                                    other.password,
                                    other.storage_type,
                                    other.sample_block}
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

    BlockInputStreamPtr RedisDictionarySource::loadAll()
    {
        if (!client->isConnected())
            client->connect(host, port);

        RedisCommand command_for_keys("KEYS");
        command_for_keys << "*";

        /// Get only keys for specified storage type.
        auto all_keys = client->execute<RedisArray>(command_for_keys);
        if (all_keys.isNull())
            return std::make_shared<RedisBlockInputStream>(client, RedisArray{}, storage_type, sample_block, max_block_size);

        RedisArray keys;
        auto key_type = storageTypeToKeyType(storage_type);
        for (const auto & key : all_keys)
            if (key_type == client->execute<String>(RedisCommand("TYPE").addRedisType(key)))
                keys.addRedisType(std::move(key));

        if (storage_type == RedisStorageType::HASH_MAP)
        {
            RedisArray hkeys;
            for (const auto & key : keys)
            {
                RedisCommand command_for_secondary_keys("HKEYS");
                command_for_secondary_keys.addRedisType(key);

                auto secondary_keys = client->execute<RedisArray>(command_for_secondary_keys);

                RedisArray primary_with_secondary;
                primary_with_secondary.addRedisType(key);
                for (const auto & secondary_key : secondary_keys)
                {
                    primary_with_secondary.addRedisType(secondary_key);
                    /// Do not store more than max_block_size values for one request.
                    if (primary_with_secondary.size() == max_block_size + 1)
                    {
                        hkeys.add(primary_with_secondary);
                        primary_with_secondary.clear();
                        primary_with_secondary.addRedisType(key);
                    }
                }

                if (primary_with_secondary.size() > 1)
                    hkeys.add(std::move(primary_with_secondary));
            }

            keys = std::move(hkeys);
        }

        return std::make_shared<RedisBlockInputStream>(client, std::move(keys), storage_type, sample_block, max_block_size);
    }


    BlockInputStreamPtr RedisDictionarySource::loadIds(const std::vector<UInt64> & ids)
    {
        if (!client->isConnected())
            client->connect(host, port);

        if (storage_type == RedisStorageType::HASH_MAP)
            throw Exception{"Cannot use loadIds with 'hash_map' storage type", ErrorCodes::UNSUPPORTED_METHOD};

        if (!dict_struct.id)
            throw Exception{"'id' is required for selective loading", ErrorCodes::UNSUPPORTED_METHOD};

        RedisArray keys;

        for (UInt64 id : ids)
            keys << DB::toString(id);

        return std::make_shared<RedisBlockInputStream>(client, std::move(keys), storage_type, sample_block, max_block_size);
    }

    BlockInputStreamPtr RedisDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
    {
        if (!client->isConnected())
            client->connect(host, port);

        if (key_columns.size() != dict_struct.key->size())
            throw Exception{"The size of key_columns does not equal to the size of dictionary key", ErrorCodes::LOGICAL_ERROR};

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
                    key << get<String>((*key_columns[i])[row]);
                else
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected type of key in Redis dictionary");
            }

            keys.add(key);
        }

        return std::make_shared<RedisBlockInputStream>(client, std::move(keys), storage_type, sample_block, max_block_size);
    }


    String RedisDictionarySource::toString() const
    {
        return "Redis: " + host + ':' + DB::toString(port);
    }

    RedisStorageType RedisDictionarySource::parseStorageType(const String & storage_type_str)
    {
        if (storage_type_str == "hash_map")
            return RedisStorageType::HASH_MAP;
        else if (!storage_type_str.empty() && storage_type_str != "simple")
            throw Exception("Unknown storage type " + storage_type_str + " for Redis dictionary", ErrorCodes::INVALID_CONFIG_PARAMETER);

        return RedisStorageType::SIMPLE;
    }
}
