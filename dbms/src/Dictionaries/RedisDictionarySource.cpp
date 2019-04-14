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


namespace DB
{
    namespace ErrorCodes
    {
        extern const int UNSUPPORTED_METHOD;
        extern const int CANNOT_SELECT;
    }


    static const size_t max_block_size = 8192;


    RedisDictionarySource::RedisDictionarySource(
            const DictionaryStructure & dict_struct,
            const std::string & host,
            UInt16 port,
            UInt8 db_index,
            const Block & sample_block)
            : dict_struct{dict_struct}
            , host{host}
            , port{port}
            , db_index{db_index}
            , sample_block{sample_block}
            , client{std::make_shared<Poco::Redis::Client>(host, port)}
    {
        if (db_index != 0)
        {
            Poco::Redis::Array command;
            command << "SELECT" << static_cast<Int64>(db_index);
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
            sample_block)
    {
    }


    RedisDictionarySource::RedisDictionarySource(const RedisDictionarySource & other)
            : RedisDictionarySource{other.dict_struct,
                                    other.host,
                                    other.port,
                                    other.db_index,
                                    other.sample_block}
    {
    }


    RedisDictionarySource::~RedisDictionarySource() = default;


    BlockInputStreamPtr RedisDictionarySource::loadAll()
    {
        Poco::Redis::Array commandForKeys;
        commandForKeys << "KEYS" << "*";

        Poco::Redis::Array keys = client->execute<Poco::Redis::Array>(commandForKeys);

        return std::make_shared<RedisBlockInputStream>(client, std::move(keys), sample_block, max_block_size);
    }


    BlockInputStreamPtr RedisDictionarySource::loadIds(const std::vector<UInt64> & ids)
    {
        if (!dict_struct.id)
            throw Exception{"'id' is required for selective loading", ErrorCodes::UNSUPPORTED_METHOD};

        Poco::Redis::Array keys;

        for (UInt64 id : ids)
            keys << static_cast<Int64>(id);

        return std::make_shared<RedisBlockInputStream>(client, std::move(keys), sample_block, max_block_size);
    }


    std::string RedisDictionarySource::toString() const
    {
        return "Redis: " + host + ':' + DB::toString(port);
    }

}

#endif
