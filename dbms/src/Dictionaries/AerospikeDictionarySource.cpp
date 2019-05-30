#include "AerospikeDictionarySource.h"
#include "DictionarySourceFactory.h"
#include "DictionaryStructure.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

void registerDictionarySourceAerospike(DictionarySourceFactory & factory)
{
    auto createTableSource = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 const Context & /* context */) -> DictionarySourcePtr {
#if USE_AEROSPIKE
        return std::make_unique<AerospikeDictionarySource>(dict_struct, config, config_prefix + ".aerospike", sample_block);
#else
        (void)dict_struct;
        (void)config;
        (void)config_prefix;
        (void)sample_block;
        throw Exception{"Dictionary source of type `aerospike` is disabled because ClickHouse was built without aerospike support.",
                        ErrorCodes::SUPPORT_IS_DISABLED};
#endif
    };
    factory.registerSource("aerospike", createTableSource);
}

}

#if USE_AEROSPIKE

#    include <aerospike/aerospike.h>
#    include <aerospike/aerospike_scan.h>
#    include <aerospike/as_record.h>
#    include <aerospike/as_scan.h>
#    include "AerospikeBlockInputStream.h"
#    include "DictionaryStructure.h"

namespace DB
{
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
{
    as_config config;
    as_config_init(&config);
    as_config_add_host(&config, host.c_str(), port);
    aerospike_init(&client, &config);
    // Check connection
    as_error err;
    fprintf(stderr, "namespace: %s set: %s", namespace_name.c_str(), set_name.c_str());
    if (aerospike_connect(&client, &err) != AEROSPIKE_OK)
    {
        fprintf(stderr, "error(%d) %s at [%s:%d] \n", err.code, err.message, err.file, err.line);
    }
    else
    {
        fprintf(stderr, "Connected to Aerospike. Host: %s port: %u", host.c_str(), port);
    }
}

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

std::unique_ptr<as_key> AllocateKey(const as_key & key, const char * namespace_name, const char * set_name)
{
    std::unique_ptr<as_key> result;
    switch (as_val_type(const_cast<as_key_value*>(&key.value)))
    {
        case AS_INTEGER:
        {
            return std::unique_ptr<as_key>(as_key_new_int64(namespace_name, set_name, key.value.integer.value));
        }
        case AS_STRING:
        {
            size_t value_size = key.value.string.len;
            char * v = static_cast<char *>(cf_malloc(value_size + 1));
            memcpy(v, key.value.string.value, value_size);
            v[value_size] = 0;
            return std::unique_ptr<as_key>(as_key_new_str(namespace_name, set_name, v));
        }
        default:
        {
            size_t value_size = key.value.string.len;
            void * v = cf_malloc(value_size);
            memcpy(v, key.value.bytes.value, value_size);
            return std::unique_ptr<as_key>(as_key_new_rawp(namespace_name, set_name, static_cast<uint8_t *>(v), value_size, true));
        }
    }
}

AerospikeDictionarySource::~AerospikeDictionarySource() = default;

struct AerospikeSpecificKeys
{
    std::vector<std::unique_ptr<as_key>> * keys_ptrs;
    const char * namespace_name;
    const char * set_name;
};

BlockInputStreamPtr AerospikeDictionarySource::loadAll()
{
    as_scan scanner;
    as_scan_init(&scanner, namespace_name.c_str(), set_name.c_str());
    as_error err;
    std::vector<std::unique_ptr<as_key>> keys;

    auto scannerCallback = [](const as_val * p_val, void * udata)
    {
        if (!p_val)
        {
            printf("scan callback returned null - scan is complete");
            return true;
        }
        as_record * record = as_record_fromval(p_val);
        if (!record)
        {
            printf("scan callback returned non-as_record object");
            return false;
        }
        AerospikeSpecificKeys& global_keys_object = *(static_cast<AerospikeSpecificKeys*>(udata));
        global_keys_object.keys_ptrs->emplace_back(AllocateKey(record->key, global_keys_object.namespace_name, global_keys_object.set_name));
        return true;
    };
    AerospikeSpecificKeys keys_object = {&keys, namespace_name.c_str(), set_name.c_str()};
    if (aerospike_scan_foreach(&client, &err, nullptr, &scanner, scannerCallback, static_cast<void *>(&keys_object)) != AEROSPIKE_OK)
    {
        fprintf(stderr, "SCAN ERROR(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
    }
    return std::make_shared<AerospikeBlockInputStream>(client, std::move(keys), sample_block, max_block_size, namespace_name, set_name);
}

BlockInputStreamPtr AerospikeDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    std::vector<std::unique_ptr<as_key>> keys;
    for (UInt64 id : ids)
    {
        keys.emplace_back(as_key_new_int64(namespace_name.c_str(), set_name.c_str(), id));
    }
    return std::make_shared<AerospikeBlockInputStream>(client, std::move(keys), sample_block, max_block_size, namespace_name, set_name);
}

std::string AerospikeDictionarySource::toString() const
{
    return "Aerospike: " + namespace_name + '.' + set_name + ',' + host + ':' + std::to_string(port); }
}

#endif
