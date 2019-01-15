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
        return std::make_shared<RedisBlockInputStream>(client, sample_block, max_block_size);
    }

/*
    BlockInputStreamPtr RedisDictionarySource::loadIds(const std::vector<UInt64> & ids)
    {
        if (!dict_struct.id)
            throw Exception{"'id' is required for selective loading", ErrorCodes::UNSUPPORTED_METHOD};

        Poco::Redis::Array ids_array(new Poco::Redis::Array);
        for (const UInt64 id : ids)
            ids_array->add(DB::toString(id), Int32(id));

        cursor->query().selector().addNewDocument(dict_struct.id->name).add("$in", ids_array);

        return std::make_shared<RedisBlockInputStream>(connection, sample_block, max_block_size);
    }


    BlockInputStreamPtr RedisDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
    {
        if (!dict_struct.key)
            throw Exception{"'key' is required for selective loading", ErrorCodes::UNSUPPORTED_METHOD};

        Poco::Redis::Array::Ptr keys_array(new Poco::Redis::Array);

        for (const auto row_idx : requested_rows)
        {
            auto & key = keys_array->addNewDocument(DB::toString(row_idx));

            for (const auto attr : ext::enumerate(*dict_struct.key))
            {
                switch (attr.second.underlying_type)
                {
                    case AttributeUnderlyingType::UInt8:
                    case AttributeUnderlyingType::UInt16:
                    case AttributeUnderlyingType::UInt32:
                    case AttributeUnderlyingType::UInt64:
                    case AttributeUnderlyingType::UInt128:
                    case AttributeUnderlyingType::Int8:
                    case AttributeUnderlyingType::Int16:
                    case AttributeUnderlyingType::Int32:
                    case AttributeUnderlyingType::Int64:
                    case AttributeUnderlyingType::Decimal32:
                    case AttributeUnderlyingType::Decimal64:
                    case AttributeUnderlyingType::Decimal128:
                        key.add(attr.second.name, Int32(key_columns[attr.first]->get64(row_idx)));
                        break;

                    case AttributeUnderlyingType::Float32:
                    case AttributeUnderlyingType::Float64:
                        key.add(attr.second.name, applyVisitor(FieldVisitorConvertToNumber<Float64>(), (*key_columns[attr.first])[row_idx]));
                        break;

                    case AttributeUnderlyingType::String:
                        String _str(get<String>((*key_columns[attr.first])[row_idx]));
                        /// Convert string to ObjectID
                        if (attr.second.is_object_id)
                        {
                            Poco::Redis::ObjectId::Ptr _id(new Poco::Redis::ObjectId(_str));
                            key.add(attr.second.name, _id);
                        }
                        else
                        {
                            key.add(attr.second.name, _str);
                        }
                        break;
                }
            }
        }

        /// If more than one key we should use $or
        cursor->query().selector().add("$or", keys_array);

        return std::make_shared<RedisBlockInputStream>(connection, sample_block, max_block_size);
    }
*/

    std::string RedisDictionarySource::toString() const
    {
        return "Redis: " + host + ':' + DB::toString(port);
    }

}

#endif
