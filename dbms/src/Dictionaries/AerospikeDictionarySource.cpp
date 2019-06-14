#include "DictionarySourceFactory.h"
#include "DictionaryStructure.h"

#include "AerospikeDictionarySource.h"

#if USE_AEROSPIKE

#include "AerospikeBlockInputStream.h"
#include "Aerospike.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int EXTERNAL_DICTIONARY_ERROR;
}

void registerDictionarySourceAerospike(DictionarySourceFactory & factory)
{
    auto create_source = [=](const DictionaryStructure & dict_struct,
                            const Poco::Util::AbstractConfiguration & config,
                            const std::string & config_prefix,
                            Block & sample_block,
                            const Context &) -> DictionarySourcePtr
    {
        return std::make_unique<AerospikeDictionarySource>(dict_struct, config, config_prefix + ".aerospike", sample_block);
    };
    factory.registerSource("aerospike", create_source);
}


AerospikeDictionarySource::AerospikeDictionarySource(
    const DB::DictionaryStructure & dict_struct,
    const std::string & host,
    UInt16 port,
    const std::string & namespace_name,
    const std::string & set_name,
    as_config * /*uselessConfig*/,
    const DB::Block & sample_block)
    : dict_struct{dict_struct}
    , host{host}
    , port{port}
    , namespace_name{namespace_name}
    , set_name{set_name}
    , sample_block{sample_block}
    , client(std::make_shared<Aerospike>(host.c_str(), port))
{}

static const size_t max_block_size = 8192;

AerospikeDictionarySource::AerospikeDictionarySource(
    const DictionaryStructure & dict_struct,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    Block & sample_block)
    : AerospikeDictionarySource(
          dict_struct,
          config.getString(config_prefix + ".host"),
          config.getUInt(config_prefix + ".port"),
          config.getString(config_prefix + ".namespace"),
          config.getString(config_prefix + ".set"),
          nullptr,
          sample_block)
{
}

AerospikeDictionarySource::AerospikeDictionarySource(const DB::AerospikeDictionarySource & other)
    : dict_struct{other.dict_struct}
    , host{other.host}
    , port{other.port}
    , namespace_name{other.namespace_name}
    , set_name{other.set_name}
    , client{other.client}
{
}

AerospikeDictionarySource::~AerospikeDictionarySource() = default;

struct SpecificKeys
{
    Aerospike::Keys keys;
    const char * namespace_name;
    const char * set_name;
};

BlockInputStreamPtr AerospikeDictionarySource::loadAll()
{
    auto scanner_callback = [](const as_val * p_val, void * udata)
    {
        /// scan callback returned null - scan is complete
        if (!p_val)
            return true;

        as_record * record = as_record_fromval(p_val);

        /// scan callback returned non-as_record object
        if (!record)
            return false;

        SpecificKeys & s_keys = *(static_cast<SpecificKeys *>(udata));
        s_keys.keys.emplace_back(AllocateKey(record->key, s_keys.namespace_name, s_keys.set_name));
        return true;
    };

    SpecificKeys s_keys{{}, namespace_name.c_str(), set_name.c_str()};

    Aerospike::Scan scan(namespace_name.c_str(), set_name.c_str());

    if (!scan.scanForeach(*client, scanner_callback, &s_keys))
        throw Exception{client->lastErrorMessage(), ErrorCodes::EXTERNAL_DICTIONARY_ERROR};

    return std::make_shared<AerospikeBlockInputStream>(
        *client, std::move(s_keys.keys), sample_block, max_block_size, namespace_name, set_name);
}

BlockInputStreamPtr AerospikeDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    Aerospike::Keys keys;
    for (UInt64 id : ids)
        keys.emplace_back(as_key_new_int64(namespace_name.c_str(), set_name.c_str(), id));

    return std::make_shared<AerospikeBlockInputStream>(
        *client, std::move(keys), sample_block, max_block_size, namespace_name, set_name);
}

std::string AerospikeDictionarySource::toString() const
{
    return "Aerospike: " + namespace_name + '.' + set_name + ',' + host + ':' + std::to_string(port);
}

}

#else

namespace DB
{
namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

void registerDictionarySourceAerospike(DictionarySourceFactory & factory)
{
    auto create_source = [](
        const DictionaryStructure &, const Poco::Util::AbstractConfiguration &,
        const std::string &, Block &, const Context &) -> DictionarySourcePtr
    {
        throw Exception{"Dictionary source of type `aerospike` is disabled because ClickHouse was built without aerospike support.",
                        ErrorCodes::SUPPORT_IS_DISABLED};
    };

    factory.registerSource("aerospike", create_source);
}

}

#endif
