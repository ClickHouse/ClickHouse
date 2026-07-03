#include <Core/Settings.h>
#include <Dictionaries/HashedDictionary.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <Dictionaries/ClickHouseDictionarySource.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool dictionary_use_async_executor;
    extern const SettingsSeconds max_execution_time;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNSUPPORTED_METHOD;
}

void registerDictionaryHashed(DictionaryFactory & factory)
{
    auto create_layout = [](const std::string & full_name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr,
                             ContextPtr global_context,
                             DictionaryKeyType dictionary_key_type,
                             bool sparse) -> DictionaryPtr
    {
        if (dictionary_key_type == DictionaryKeyType::Simple && dict_struct.key)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'key' is not supported for simple key hashed dictionary");
        if (dictionary_key_type == DictionaryKeyType::Complex && dict_struct.id)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'id' is not supported for complex key hashed dictionary");

        if (dict_struct.range_min || dict_struct.range_max)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "{}: elements .structure.range_min and .structure.range_max should be defined only "
                "for a dictionary of layout 'range_hashed'",
                full_name);

        const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);
        const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
        const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);

        std::string dictionary_layout_name;

        if (dictionary_key_type == DictionaryKeyType::Simple)
            dictionary_layout_name = sparse ? "sparse_hashed" : "hashed";
        else
            dictionary_layout_name = sparse ? "complex_key_sparse_hashed" : "complex_key_hashed";

        const std::string dictionary_layout_prefix = ".layout." + dictionary_layout_name;
        const bool preallocate = config.getBool(config_prefix + dictionary_layout_prefix + ".preallocate", false);
        if (preallocate)
            LOG_WARNING(getLogger("HashedDictionary"), "'prellocate' attribute is obsolete, consider looking at 'shards'");

        Int64 shards = config.getInt(config_prefix + dictionary_layout_prefix + ".shards", 1);
        if (shards <= 0 || shards > 128)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,"{}: SHARDS parameter should be within [1, 128]", full_name);

        Int64 shard_load_queue_backlog = config.getInt(config_prefix + dictionary_layout_prefix + ".shard_load_queue_backlog", 10000);
        if (shard_load_queue_backlog <= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,"{}: SHARD_LOAD_QUEUE_BACKLOG parameter should be greater then zero", full_name);

        float max_load_factor = static_cast<float>(config.getDouble(config_prefix + dictionary_layout_prefix + ".max_load_factor", 0.5));
        if (max_load_factor < 0.5f || max_load_factor > 0.99f)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}: max_load_factor parameter should be within [0.5, 0.99], got {}", full_name, max_load_factor);

        ContextMutablePtr context = copyContextAndApplySettingsFromDictionaryConfig(global_context, config, config_prefix);
        const auto & settings = context->getSettingsRef();

        const auto * clickhouse_source = dynamic_cast<const ClickHouseDictionarySource *>(source_ptr.get());
        bool use_async_executor = clickhouse_source && clickhouse_source->isLocal() && settings[Setting::dictionary_use_async_executor];

        HashedDictionaryConfiguration configuration{
            static_cast<UInt64>(shards),
            static_cast<UInt64>(shard_load_queue_backlog),
            max_load_factor,
            require_nonempty,
            dict_lifetime,
            use_async_executor,
            std::chrono::seconds(settings[Setting::max_execution_time].totalSeconds()),
        };

        if (source_ptr->hasUpdateField() && shards > 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}: SHARDS parameter does not supports for updatable source (UPDATE_FIELD)", full_name);

        if (dictionary_key_type == DictionaryKeyType::Simple)
        {
            if (sparse)
            {
                if (shards > 1)
                    return std::make_unique<HashedDictionary<DictionaryKeyType::Simple, true, true>>(dict_id, dict_struct, std::move(source_ptr), configuration);
                return std::make_unique<HashedDictionary<DictionaryKeyType::Simple, true, false>>(
                    dict_id, dict_struct, std::move(source_ptr), configuration);
            }

            if (shards > 1)
                return std::make_unique<HashedDictionary<DictionaryKeyType::Simple, false, true>>(
                    dict_id, dict_struct, std::move(source_ptr), configuration);
            return std::make_unique<HashedDictionary<DictionaryKeyType::Simple, false, false>>(
                dict_id, dict_struct, std::move(source_ptr), configuration);
        }

        if (sparse)
        {
            if (shards > 1)
                return std::make_unique<HashedDictionary<DictionaryKeyType::Complex, true, true>>(
                    dict_id, dict_struct, std::move(source_ptr), configuration);
            return std::make_unique<HashedDictionary<DictionaryKeyType::Complex, true, false>>(
                dict_id, dict_struct, std::move(source_ptr), configuration);
        }

        if (shards > 1)
            return std::make_unique<HashedDictionary<DictionaryKeyType::Complex, false, true>>(
                dict_id, dict_struct, std::move(source_ptr), configuration);
        return std::make_unique<HashedDictionary<DictionaryKeyType::Complex, false, false>>(
            dict_id, dict_struct, std::move(source_ptr), configuration);
    };

    factory.registerLayout("hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e, ContextPtr global_context, bool /*created_from_ddl*/){ return create_layout(a, b, c, d, std::move(e), global_context, DictionaryKeyType::Simple, /* sparse = */ false); }, false);
    factory.registerLayout("sparse_hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e, ContextPtr global_context, bool /*created_from_ddl*/){ return create_layout(a, b, c, d, std::move(e), global_context, DictionaryKeyType::Simple, /* sparse = */ true); }, false);
    factory.registerLayout("complex_key_hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e, ContextPtr global_context, bool /*created_from_ddl*/){ return create_layout(a, b, c, d, std::move(e), global_context, DictionaryKeyType::Complex, /* sparse = */ false); }, true);
    factory.registerLayout("complex_key_sparse_hashed",
        [=](auto && a, auto && b, auto && c, auto && d, DictionarySourcePtr e, ContextPtr global_context, bool /*created_from_ddl*/){ return create_layout(a, b, c, d, std::move(e), global_context, DictionaryKeyType::Complex, /* sparse = */ true); }, true);

}

}
