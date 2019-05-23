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
        as_config * config,
        const DB::Block& sample_block)
        : dict_struct{dict_struct}
        , host{host}
        , port{port}
        , sample_block{sample_block}
    {
        // std::vector<as_host> hosts;
        // hosts.assign(static_cast<as_host *>(config->hosts->list), static_cast<as_host *>(config->hosts->list) + config->hosts->size);
        aerospike_init(&client, config);
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
        std::vector<as_key> keys;

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
            static_cast<std::vector<as_key>*>(keys)->push_back(record->key);
            return true;
        };

        if (aerospike_scan_foreach(&client, &err, nullptr, &scanner, scannerCallback, static_cast<void*>(&keys)) != AEROSPIKE_OK) {
            printf("error(%d) %s at [%s:%d]", err.code, err.message, err.file, err.line);
        }
        return std::make_shared<AerospikeBlockInputStream>(client, std::move(keys), sample_block, max_block_size);
    }

    std::string AerospikeDictionarySource::toString() const {
        return "Aerospike: "; // +  bla bla bla
    }
}

#endif

