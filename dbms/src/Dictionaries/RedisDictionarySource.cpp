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
#    include <Poco/Redis/AsyncReader.h>
#    include <Poco/Redis/Client.h>
#    include <Poco/Redis/Command.h>
#    include <Poco/Redis/Error.h>
#    include <Poco/Redis/Exception.h>
#    include <Poco/Redis/RedisEventArgs.h>
#    include <Poco/Redis/RedisStream.h>
#    include <Poco/Redis/Type.h>
#    include <Poco/Util/AbstractConfiguration.h>
#    include <Poco/Version.h>

#    include <IO/WriteHelpers.h>
#    include <Common/FieldVisitors.h>
#    include <ext/enumerate.h>
#    include "RedisBlockInputStream.h"


namespace
{
    template <class K, class V>
    Poco::Redis::Array makeResult(const K & keys, const V & values) {
        Poco::Redis::Array result;
        result << keys << values;
        return result;
    }
}


namespace DB
{
    namespace ErrorCodes
    {
        extern const int UNSUPPORTED_METHOD;
    }


    static const size_t max_block_size = 8192;


    RedisDictionarySource::RedisDictionarySource(
            const DictionaryStructure & dict_struct,
            const std::string & host,
            UInt16 port,
            const Block & sample_block)
            : dict_struct{dict_struct}
            , host{host}
            , port{port}
            , sample_block{sample_block}
            , client{std::make_shared<Poco::Redis::Client>(host, port)}
    {
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
            sample_block)
    {
    }


    RedisDictionarySource::RedisDictionarySource(const RedisDictionarySource & other)
            : RedisDictionarySource{other.dict_struct,
                                    other.host,
                                    other.port,
                                    other.sample_block}
    {
    }


    RedisDictionarySource::~RedisDictionarySource() = default;


    BlockInputStreamPtr RedisDictionarySource::loadAll()
    {
        Int64 cursor = 0;
        Poco::Redis::Array keys;

        do
        {
            Poco::Redis::Array commandForKeys;
            commandForKeys << "SCAN" << cursor << "COUNT 1000";

            Poco::Redis::Array replyForKeys = client->execute<Poco::Redis::Array>(commandForKeys);
            cursor = replyForKeys.get<Int64>(0);

            Poco::Redis::Array response = replyForKeys.get<Poco::Redis::Array>(1);
            if (response.isNull())
                continue;

            for (const Poco::Redis::RedisType::Ptr & key : response)
                keys.addRedisType(key);
        }
        while (cursor != 0);

        Poco::Redis::Array commandForValues;
        commandForValues << "MGET";
        for (const Poco::Redis::RedisType::Ptr & key : keys)
            commandForValues.addRedisType(key);

        Poco::Redis::Array values = client->execute<Poco::Redis::Array>(commandForValues);

        return std::make_shared<RedisBlockInputStream>(makeResult(keys, values), sample_block, max_block_size);
    }


    BlockInputStreamPtr RedisDictionarySource::loadIds(const std::vector<UInt64> & ids)
    {
        if (!dict_struct.id)
            throw Exception{"'id' is required for selective loading", ErrorCodes::UNSUPPORTED_METHOD};

        Poco::Redis::Array keys;
        Poco::Redis::Array command;
        command << "MGET";

        for (const UInt64 id : ids)
        {
            keys << static_cast<Int64>(id);
            command << static_cast<Int64>(id);
        }

        Poco::Redis::Array values = client->execute<Poco::Redis::Array>(command);

        return std::make_shared<RedisBlockInputStream>(makeResult(keys, values), sample_block, max_block_size);
    }


    std::string RedisDictionarySource::toString() const
    {
        return "Redis: " + host + ':' + DB::toString(port);
    }

}

#endif
