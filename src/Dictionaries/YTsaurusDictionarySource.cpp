#include "config.h"

#if USE_YTSAURUS
#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/YTsaurusDictionarySource.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/YTsaurusSource.h>
#include <Storages/YTsaurus/StorageYTsaurus.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <Core/Settings.h>
#include <Common/parseRemoteDescription.h>

#include <boost/algorithm/string/split.hpp>

#endif

namespace DB
{

namespace ErrorCodes
{
    #if USE_YTSAURUS
    extern const int UNSUPPORTED_METHOD;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_STORAGE;
    #else
    extern const int SUPPORT_IS_DISABLED;
    #endif
}

namespace Setting
{
    extern const SettingsBool allow_experimental_ytsaurus_dictionary_source;
}


void registerDictionarySourceYTsaurus(DictionarySourceFactory & factory)
{
    #if USE_YTSAURUS
    auto create_dictionary_source = [](
        const String& /*name*/,
        const DictionaryStructure & dict_struct,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & root_config_prefix,
        Block & sample_block,
        ContextPtr context,
        const std::string & /* default_database */,
        bool created_from_ddl) -> DictionarySourcePtr
    {
        if (!context->getSettingsRef()[Setting::allow_experimental_ytsaurus_dictionary_source])
            throw Exception(ErrorCodes::UNKNOWN_STORAGE, "Dictionary source YTsaurus is experimental. "
                "Set `allow_experimental_ytsaurus_dictionary_source` setting to enable it");

        const auto config_prefix = root_config_prefix + ".ytsaurus";
        auto configuration = std::make_shared<YTsaurusStorageConfiguration>();
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(config_prefix, keys);
        for (const auto & key : keys)
        {
            if (!YTsaurusSettings::hasBuiltin(key))
            {
                continue;
            }
            configuration->settings.set(key, config.getString(config_prefix + "." + key));
        }
        auto named_collection = created_from_ddl ? tryGetNamedCollectionWithOverrides(config, config_prefix, context) : nullptr;
        if (named_collection)
        {
            configuration->settings.loadFromNamedCollection(*named_collection);
        }

        boost::split(configuration->http_proxy_urls, config.getString(config_prefix + ".http_proxy_urls"), [](char c) { return c == '|'; });
        configuration->cypress_path = config.getString(config_prefix + ".cypress_path");
        configuration->oauth_token = config.getString(config_prefix + ".oauth_token");
        BlockPtr table_sample_block = std::make_shared<Block>();
        if (dict_struct.id)
        {
            table_sample_block->insert(ColumnWithTypeAndName(dict_struct.id->type, dict_struct.id->name));
        }
        if (dict_struct.key)
        {
            for (const auto & attr : dict_struct.key.value())
            {
                table_sample_block->insert(ColumnWithTypeAndName(attr.type, attr.name));
            }
        }
        for (const auto & attr : dict_struct.attributes)
        {
            table_sample_block->insert(ColumnWithTypeAndName(attr.type, attr.name));
        }

        return std::make_unique<YTsarususDictionarySource>(context, dict_struct, std::move(configuration), std::move(sample_block), std::move(table_sample_block));
    };

    #else
    auto create_dictionary_source = [](
        const String& /*name*/,
        const DictionaryStructure & /* dict_struct */,
        const Poco::Util::AbstractConfiguration & /* config */,
        const std::string & /* root_config_prefix */,
        Block & /* sample_block */,
        ContextPtr /* context */,
        const std::string & /* default_database */,
        bool /* created_from_ddl */) -> DictionarySourcePtr
    {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
        "Dictionary source of type `ytsaurus` is disabled because ClickHouse was built without YTsaurus support.");
    };
    #endif

    factory.registerSource("ytsaurus", create_dictionary_source);
}

#if USE_YTSAURUS
static const UInt64 max_block_size = 8192;


YTsarususDictionarySource::YTsarususDictionarySource(
    ContextPtr context_,
    const DictionaryStructure & dict_struct_,
    std::shared_ptr<YTsaurusStorageConfiguration> configuration_,
    Block sample_block_,
    SharedHeader table_sample_block_)
    : context(context_)
    , dict_struct{dict_struct_}
    , configuration{configuration_}
    , sample_block{sample_block_}
    , table_sample_block{table_sample_block_}
    , client(new YTsaurusClient(context, {.http_proxy_urls = configuration->http_proxy_urls, .oauth_token = configuration->oauth_token}))
{
}

YTsarususDictionarySource::YTsarususDictionarySource(const YTsarususDictionarySource & other)
    : YTsarususDictionarySource{other.context, other.dict_struct, other.configuration, other.sample_block, other.table_sample_block}
{
}

YTsarususDictionarySource::~YTsarususDictionarySource() = default;

QueryPipeline YTsarususDictionarySource::loadAll()
{
    return QueryPipeline(YTsaurusSourceFactory::createSource(client, {.cypress_path = configuration->cypress_path, .settings = configuration->settings}, table_sample_block, max_block_size));
}

QueryPipeline YTsarususDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    if (!dict_struct.id)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'id' is required for selective loading");

    if (!supportsSelectiveLoad())
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Can't make selective update of YTsaurus dictionary because data source doesn't supports lookups.");

    auto block = blockForIds(dict_struct, ids);
    return QueryPipeline(YTsaurusSourceFactory::createSource(client, {.cypress_path = configuration->cypress_path, .settings = configuration->settings, .lookup_input_block = std::move(block)}, table_sample_block, max_block_size));
}

QueryPipeline YTsarususDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    if (!supportsSelectiveLoad())
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Can't make selective update of YTsaurus dictionary because data source doesn't supports lookups.");

    if (!dict_struct.key)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'key' is required for selective loading");

    if (key_columns.size() != dict_struct.key->size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The size of key_columns does not equal to the size of dictionary key");

    auto block = blockForKeys(dict_struct, key_columns, requested_rows);
    return QueryPipeline(YTsaurusSourceFactory::createSource(client, {.cypress_path = configuration->cypress_path, .settings = configuration->settings, .lookup_input_block = std::move(block)}, table_sample_block, max_block_size));
}

bool YTsarususDictionarySource::supportsSelectiveLoad() const
{
    return client->getNodeType(configuration->cypress_path) == YTsaurusNodeType::DYNAMIC_TABLE;
}


std::string YTsarususDictionarySource::toString() const
{
    return fmt::format("YTsaurus: {}", configuration->cypress_path);
}
#endif

}
