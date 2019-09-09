#include "RedisDictionarySource.h"
#include "DictionarySourceFactory.h"
#include "DictionaryStructure.h"

namespace DB
{
    namespace ErrorCodes
    {
        extern const int SUPPORT_IS_DISABLED;
    }

    void registerDictionarySourceRedis(DictionarySourceFactory & factory)
    {
        auto createTableSource = [=](const DictionaryStructure & dict_struct,
                                     const Poco::Util::AbstractConfiguration & config,
                                     const std::string & config_prefix,
                                     Block & sample_block,
                                     const Context & /* context */) -> DictionarySourcePtr {
#if USE_POCO_REDIS
        return std::make_unique<RedisDictionarySource>(dict_struct, config, config_prefix + ".redis", sample_block);
#else
        (void)dict_struct;
        (void)config;
        (void)config_prefix;
        (void)sample_block;
        throw Exception{"Dictionary source of type `redis` is disabled because poco library was built without redis support.",
                        ErrorCodes::SUPPORT_IS_DISABLED};
#endif
        };
        factory.registerSource("redis", createTableSource);
    }

}


#if USE_POCO_REDIS

#    include <Poco/Redis/Array.h>
#    include <Poco/Redis/Client.h>
#    include <Poco/Redis/Command.h>
#    include <Poco/Redis/Type.h>
#    include <Poco/Util/AbstractConfiguration.h>

#    include <Common/FieldVisitors.h>
#    include <IO/WriteHelpers.h>

#    include "RedisBlockInputStream.h"


namespace DB
{
    namespace ErrorCodes
    {
        extern const int UNSUPPORTED_METHOD;
        extern const int CANNOT_SELECT;
        extern const int INVALID_CONFIG_PARAMETER;
    }


    static const size_t max_block_size = 8192;


    RedisDictionarySource::RedisDictionarySource(
            const DictionaryStructure & dict_struct,
            const std::string & host,
            UInt16 port,
            UInt8 db_index,
            RedisStorageType::Id storage_type,
            const Block & sample_block)
            : dict_struct{dict_struct}
            , host{host}
            , port{port}
            , db_index{db_index}
            , storage_type{storage_type}
            , sample_block{sample_block}
            , client{std::make_shared<Poco::Redis::Client>(host, port)}
    {
        if (dict_struct.attributes.size() != 1)
            throw Exception{"Invalid number of non key columns for Redis source: " +
                            DB::toString(dict_struct.attributes.size()) + ", expected 1",
                            ErrorCodes::INVALID_CONFIG_PARAMETER};

        if (storage_type == RedisStorageType::HASH_MAP)
        {
            if (!dict_struct.key.has_value())
                throw Exception{"Redis source with storage type \'hash_map\' must have key",
                                ErrorCodes::INVALID_CONFIG_PARAMETER};
            if (dict_struct.key.value().size() > 2)
                throw Exception{"Redis source with complex keys having more than 2 attributes are unsupported",
                                ErrorCodes::INVALID_CONFIG_PARAMETER};
            // suppose key[0] is primary key, key[1] is secondary key
        }

        if (db_index != 0)
        {
            Poco::Redis::Command command("SELECT");
            command << static_cast<Int64>(db_index);
            std::string reply = client->execute<std::string>(command);
            if (reply != "+OK\r\n")
                throw Exception{"Selecting db with index " + DB::toString(db_index) + " failed with reason " + reply,
                                ErrorCodes::CANNOT_SELECT};
        }
    }


    RedisDictionarySource::RedisDictionarySource(
            const DictionaryStructure & dict_struct,
            const Poco::Util::AbstractConfiguration & config,
            const std::string & config_prefix,
            Block & sample_block)
            : RedisDictionarySource(
            dict_struct,
            config.getString(config_prefix + ".host"),
            config.getUInt(config_prefix + ".port"),
            config.getUInt(config_prefix + ".db_index", 0),
            parseStorageType(config.getString(config_prefix + ".storage_type", "")),
            sample_block)
    {
    }


    RedisDictionarySource::RedisDictionarySource(const RedisDictionarySource & other)
            : RedisDictionarySource{other.dict_struct,
                                    other.host,
                                    other.port,
                                    other.db_index,
                                    other.storage_type,
                                    other.sample_block}
    {
    }


    RedisDictionarySource::~RedisDictionarySource() = default;


    BlockInputStreamPtr RedisDictionarySource::loadAll()
    {
        Poco::Redis::Command command_for_keys("KEYS");
        command_for_keys << "*";

        Poco::Redis::Array keys = client->execute<Poco::Redis::Array>(command_for_keys);

        if (storage_type == RedisStorageType::HASH_MAP && dict_struct.key->size() == 2)
        {
            Poco::Redis::Array hkeys;
            for (const auto & key : keys)
            {
                Poco::Redis::Command command_for_secondary_keys("HKEYS");
                command_for_secondary_keys.addRedisType(key);

                Poco::Redis::Array reply_for_primary_key = client->execute<Poco::Redis::Array>(command_for_secondary_keys);

                Poco::Redis::Array primary_with_secondary;
                primary_with_secondary.addRedisType(key);
                for (const auto & secondary_key : reply_for_primary_key)
                    primary_with_secondary.addRedisType(secondary_key);

                hkeys.add(primary_with_secondary);
            }
            keys = hkeys;
        }

        return std::make_shared<RedisBlockInputStream>(client, std::move(keys), sample_block, max_block_size);
    }


    BlockInputStreamPtr RedisDictionarySource::loadIds(const std::vector<UInt64> & ids)
    {
        if (storage_type != RedisStorageType::SIMPLE)
            throw Exception{"Cannot use loadIds with \'simple\' storage type", ErrorCodes::UNSUPPORTED_METHOD};

        if (!dict_struct.id)
            throw Exception{"'id' is required for selective loading", ErrorCodes::UNSUPPORTED_METHOD};

        Poco::Redis::Array keys;

        for (UInt64 id : ids)
            keys << DB::toString(id);

        return std::make_shared<RedisBlockInputStream>(client, std::move(keys), sample_block, max_block_size);
    }

    std::string RedisDictionarySource::toString() const
    {
        return "Redis: " + host + ':' + DB::toString(port);
    }

    RedisStorageType::Id RedisDictionarySource::parseStorageType(const std::string & storage_type)
    {
        RedisStorageType::Id storage_type_id = RedisStorageType::valueOf(storage_type);
        if (storage_type_id == RedisStorageType::UNKNOWN)
            storage_type_id = RedisStorageType::SIMPLE;
        return storage_type_id;
    }
}

#endif
