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
        throw Exception{"Dictionary source of type `aerospike` is disabled because poco library was built without aerospike support.",
                        ErrorCodes::SUPPORT_IS_DISABLED};
#endif
        };
        factory.registerSource("aerospike", createTableSource);
    }

}

#if USE_AEROSPIKE

#    include "AerospikeBlockInputStream.h"
#    include <aerospike/aerospike.h>
#    include <aerospike/aerospike_scan.h>
#    include <aerospike/as_scan.h>
#    include <aerospike/as_record.h>

namespace DB
{
    AerospikeDictionarySource::AerospikeDictionarySource(
        const DB::DictionaryStructure& dict_struct,
        const std::string& host,
        UInt16 port,
        as_config * /*uselessConfig*/,
        const DB::Block& sample_block)
        : dict_struct{dict_struct}
        , host{host}
        , port{port}
        , sample_block{sample_block}
    {
        as_config config;
        as_config_init(&config);
        as_config_add_host(&config, host.c_str(), port);
        aerospike_init(&client, &config);
        // Check connection
        as_error err;
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
            nullptr,
            sample_block)
    {
    }

    AerospikeDictionarySource::AerospikeDictionarySource(const DB::AerospikeDictionarySource& other)
        : dict_struct{other.dict_struct}
        , host{other.host}
        , port{other.port}
        , client{other.client}
    {
    }

    const static char* default_namespace_name = "test";
    const static char* default_set_name = "test_set";

    std::unique_ptr<as_key> AllocateKey(const as_key& key)
    {
        std::unique_ptr<as_key> result;
        switch (as_val_type(&key.value))
        {
            case AS_INTEGER: {
                fprintf(stderr, "AllocateKey INTEGER\n");
                return std::unique_ptr<as_key>(as_key_new_int64(default_namespace_name, default_set_name, key.value.integer.value));
                // *value = (as_val*)as_integer_new(key.value.integer.value);
                break;
            }
            case AS_STRING: {
                fprintf(stderr, "AllocateKey STRING\n");
                size_t value_size = key.value.string.len;
                char* v = static_cast<char*>(cf_malloc(value_size + 1));
                memcpy(v, key.value.string.value, value_size);
                v[value_size] = 0;
                return std::unique_ptr<as_key>(as_key_new_str(default_namespace_name, default_set_name, v));
                // *value = (as_val*)as_string_new_wlen(v, value_size, true);
                break;
            }
            default: {
                size_t value_size = key.value.string.len;
                void* v = cf_malloc(value_size);
                memcpy(v, key.value.bytes.value, value_size);
                return std::unique_ptr<as_key>(as_key_new_rawp(default_namespace_name, default_set_name, (uint8_t*)v, value_size, true));
                fprintf(stderr, "AllocateKey DEFAULT\n");
                break;
            }
        }
        return result;
    }

    AerospikeDictionarySource::~AerospikeDictionarySource() = default;

    BlockInputStreamPtr AerospikeDictionarySource::loadAll()
    {
        as_scan scanner;
        as_scan_init(&scanner, "test", "test_set");
        as_error err;
        std::vector<std::unique_ptr<as_key>> keys;

        auto scannerCallback = [] (const as_val * p_val, void * keys)
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
            std::vector<std::unique_ptr<as_key>>& tmp = *static_cast<std::vector<std::unique_ptr<as_key>>*>(keys);
            tmp.emplace_back(AllocateKey(record->key));
            return true;
        };
        if (aerospike_scan_foreach(&client, &err, nullptr, &scanner, scannerCallback, static_cast<void*>(&keys)) != AEROSPIKE_OK)
        {
            fprintf(stderr, "SCAN ERROR(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
        }
        return std::make_shared<AerospikeBlockInputStream>(client, std::move(keys), sample_block, max_block_size);
    }
    
    BlockInputStreamPtr AerospikeDictionarySource::loadIds(const std::vector<UInt64> & ids)
    {
        std::vector<std::unique_ptr<as_key>> keys;
        for (UInt64 id : ids)
        {
            keys.emplace_back(as_key_new_int64(default_namespace_name, default_set_name, id));
        }
        return std::make_shared<AerospikeBlockInputStream>(client, std::move(keys), sample_block, max_block_size);
    }

    std::string AerospikeDictionarySource::toString() const
    {
        return "Aerospike: "; // +  bla bla bla
    }
}

#endif

