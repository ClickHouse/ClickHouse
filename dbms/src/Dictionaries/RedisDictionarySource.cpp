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
            const DictionaryStructure & dict_struct_,
            const std::string & host_,
            UInt16 port_,
            UInt8 db_index_,
            RedisStorageType::Id storage_type_,
            const Block & sample_block_)
            : dict_struct{dict_struct_}
            , host{host_}
            , port{port_}
            , db_index{db_index_}
            , storage_type{storage_type_}
            , sample_block{sample_block_}
            , client{std::make_shared<Poco::Redis::Client>(host, port)}
    {
        if (dict_struct.attributes.size() != 1)
            throw Exception{"Invalid number of non key columns for Redis source: " +
                            DB::toString(dict_struct.attributes.size()) + ", expected 1",
                            ErrorCodes::INVALID_CONFIG_PARAMETER};

        {
        if (storage_type == RedisStorageType::HASH_MAP)
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
            const DictionaryStructure & dict_struct_,
            const Poco::Util::AbstractConfiguration & config_,
            const std::string & config_prefix_,
            Block & sample_block_)
            : RedisDictionarySource(
            dict_struct_,
            config_.getString(config_prefix_ + ".host"),
            config_.getUInt(config_prefix_ + ".port"),
            config_.getUInt(config_prefix_ + ".db_index", 0),
            parseStorageType(config_.getString(config_prefix_ + ".storage_type", "")),
            sample_block_)
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

    static std::string storageTypeToKeyType(RedisStorageType::Id type)
    {
        switch (type)
        {
            case RedisStorageType::Id::SIMPLE:
                return "string";
            case RedisStorageType::Id::HASH_MAP:
                return "hash";
            default:
                return "none";
        }

        __builtin_unreachable();       
    }

    BlockInputStreamPtr RedisDictionarySource::loadAll()
    {
        Poco::Redis::Command command_for_keys("KEYS");
        command_for_keys << "*";

        /// Get only keys for specified storage type.
        auto all_keys = client->execute<Poco::Redis::Array>(command_for_keys);
        Poco::Redis::Array keys;
        auto key_type = storageTypeToKeyType(storage_type);
        for (auto & key : all_keys)
            if (key_type == client->execute<std::string>(Poco::Redis::Command("TYPE").addRedisType(key)))
                keys.addRedisType(std::move(key));

        if (storage_type == RedisStorageType::HASH_MAP && !keys.isNull())
        {
            Poco::Redis::Array hkeys;
            for (const auto & key : keys)
            {
                Poco::Redis::Command command_for_secondary_keys("HKEYS");
                command_for_secondary_keys.addRedisType(key);

                auto secondary_keys = client->execute<Poco::Redis::Array>(command_for_secondary_keys);

                Poco::Redis::Array primary_with_secondary;
                primary_with_secondary.addRedisType(key);
                for (const auto & secondary_key : secondary_keys)
                    primary_with_secondary.addRedisType(secondary_key);

                hkeys.add(std::move(primary_with_secondary));
            }
            keys = std::move(hkeys);
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
