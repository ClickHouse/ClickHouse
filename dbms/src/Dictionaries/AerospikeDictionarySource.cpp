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
// #include <aerospike/as_key.h>

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
        // std::vector<as_host> hosts;
        // hosts.assign(static_cast<as_host *>(config->hosts->list), static_cast<as_host *>(config->hosts->list) + config->hosts->size);
        aerospike_init(&client, &config);
        
        // Check connection
        as_error err;
        if (aerospike_connect(&client, &err) != AEROSPIKE_OK) {
                fprintf(stderr, "error(%d) %s at [%s:%d] \n", err.code, err.message, err.file, err.line);
        } else {
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

    AerospikeDictionarySource::~AerospikeDictionarySource() = default;

    BlockInputStreamPtr AerospikeDictionarySource::loadAll() {
        as_scan scanner;
        as_scan_init(&scanner, "test", "test_set");
        as_error err;
        std::vector<std::unique_ptr<as_bytes>> keys;

        auto scannerCallback = [] (const as_val * p_val, void *keys) {
            if (!p_val) {
                printf("scan callback returned null - scan is complete");
                return true;
            }
            as_record * record = as_record_fromval(p_val);
            if (!record) {
                printf("scan callback returned non-as_record object");
                return false;
            }
            // std::vector<as_key>* tmp = reinterpret_cast<std::vector<as_key>*>(keys);
            std::vector<std::unique_ptr<as_bytes>>& tmp = *static_cast<std::vector<std::unique_ptr<as_bytes>>*>(keys);
            tmp.emplace_back(as_bytes_new_wrap(record->key.value.bytes.value, record->key.value.bytes.size, true)); // record->key -- correct object. FIXME: push_back and pointers casting
            return true;
        };

        if (aerospike_scan_foreach(&client, &err, nullptr, &scanner, scannerCallback, static_cast<void*>(&keys)) != AEROSPIKE_OK) {
            printf("error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
        }
        // fprintf(stderr, "KEY VALUE: %s \n", keys[0]->value.string.value);
        as_key key;
        as_key_init_raw(&key, "test", "test_set", keys[0]->value, keys[0]->size);
        fprintf(stderr, "KEY VALUE: %s \n", key.value.string.value);
        return std::make_shared<AerospikeBlockInputStream>(client, std::vector<as_key>(), sample_block, max_block_size);
    }

    std::string AerospikeDictionarySource::toString() const {
        return "Aerospike: "; // +  bla bla bla
    }
}

#endif

