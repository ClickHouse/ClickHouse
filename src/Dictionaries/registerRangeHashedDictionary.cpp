#include <Dictionaries/RangeHashedDictionary.h>

#include <Core/Settings.h>
#include <Dictionaries/DictionarySource.h>
#include <Dictionaries/ClickHouseDictionarySource.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <Dictionaries/DictionaryFactory.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool dictionary_use_async_executor;
}

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int BAD_ARGUMENTS;
}

template <DictionaryKeyType dictionary_key_type>
static DictionaryPtr createRangeHashedDictionary(const std::string & full_name,
                            const DictionaryStructure & dict_struct,
                            const Poco::Util::AbstractConfiguration & config,
                            const std::string & config_prefix,
                            ContextPtr global_context,
                            DictionarySourcePtr source_ptr)
{
    static constexpr auto layout_name = dictionary_key_type == DictionaryKeyType::Simple ? "range_hashed" : "complex_key_range_hashed";

    if constexpr (dictionary_key_type == DictionaryKeyType::Simple)
    {
        if (dict_struct.key)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'key' is not supported for dictionary of layout 'range_hashed'");
    }
    else
    {
        if (dict_struct.id)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'id' is not supported for dictionary of layout 'complex_key_range_hashed'");
    }

    if (!dict_struct.range_min || !dict_struct.range_max)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "{}: dictionary of layout '{}' requires .structure.range_min and .structure.range_max",
            full_name,
            layout_name);

    const auto dict_id = StorageID::fromDictionaryConfig(config, config_prefix);
    const DictionaryLifetime dict_lifetime{config, config_prefix + ".lifetime"};
    const bool require_nonempty = config.getBool(config_prefix + ".require_nonempty", false);

    String dictionary_layout_prefix = config_prefix + ".layout." + layout_name;
    const bool convert_null_range_bound_to_open = config.getBool(dictionary_layout_prefix + ".convert_null_range_bound_to_open", true);
    String range_lookup_strategy = config.getString(dictionary_layout_prefix + ".range_lookup_strategy", "min");
    RangeHashedDictionaryLookupStrategy lookup_strategy = RangeHashedDictionaryLookupStrategy::min;

    if (range_lookup_strategy == "min")
        lookup_strategy = RangeHashedDictionaryLookupStrategy::min;
    else if (range_lookup_strategy == "max")
        lookup_strategy = RangeHashedDictionaryLookupStrategy::max;

    auto context = copyContextAndApplySettingsFromDictionaryConfig(global_context, config, config_prefix);
    const auto * clickhouse_source = dynamic_cast<const ClickHouseDictionarySource *>(source_ptr.get());
    bool use_async_executor = clickhouse_source && clickhouse_source->isLocal() && context->getSettingsRef()[Setting::dictionary_use_async_executor];

    RangeHashedDictionaryConfiguration configuration
    {
        .convert_null_range_bound_to_open = convert_null_range_bound_to_open,
        .lookup_strategy = lookup_strategy,
        .require_nonempty = require_nonempty,
        .use_async_executor = use_async_executor,
    };

    DictionaryPtr result = std::make_unique<RangeHashedDictionary<dictionary_key_type>>(
        dict_id,
        dict_struct,
        std::move(source_ptr),
        dict_lifetime,
        configuration);

    return result;
}

void registerDictionaryRangeHashed(DictionaryFactory & factory)
{
    auto create_layout_simple = [=](const std::string & full_name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr,
                             ContextPtr global_context,
                             bool /*created_from_ddl*/) -> DictionaryPtr
    {
        return createRangeHashedDictionary<DictionaryKeyType::Simple>(full_name, dict_struct, config, config_prefix, global_context, std::move(source_ptr));
    };

    factory.registerLayout("range_hashed", create_layout_simple, false);

    auto create_layout_complex = [=](const std::string & full_name,
                             const DictionaryStructure & dict_struct,
                             const Poco::Util::AbstractConfiguration & config,
                             const std::string & config_prefix,
                             DictionarySourcePtr source_ptr,
                             ContextPtr global_context,
                             bool /*created_from_ddl*/) -> DictionaryPtr
    {
        return createRangeHashedDictionary<DictionaryKeyType::Complex>(full_name, dict_struct, config, config_prefix, global_context, std::move(source_ptr));
    };

    factory.registerLayout("complex_key_range_hashed", create_layout_complex, true);
}

}
