#include "config.h"

#if USE_YTSAURUS
#include <memory>
#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/YTsaurusDictionarySource.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/YTsaurusSource.h>
#include <Storages/YTsaurus/StorageYTsaurus.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <Core/Settings.h>
#include <Common/parseRemoteDescription.h>
#include <Common/Throttler.h>

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


namespace YTsaurusSetting
{
    extern const YTsaurusSettingsBool encode_utf8;
    extern const YTsaurusSettingsBool enable_heavy_proxy_redirection;
    extern const YTsaurusSettingsUInt64 lookup_throttler_max_requests_per_second;
    extern const YTsaurusSettingsUInt64 lookup_max_rows_per_query;
}


namespace Setting
{
    extern const SettingsBool allow_experimental_ytsaurus_dictionary_source;
}

void registerDictionarySourceYTsaurus(DictionarySourceFactory & factory);
void registerDictionarySourceYTsaurus(DictionarySourceFactory & factory)
{
    #if USE_YTSAURUS
    auto create_dictionary_source = [](
        const String & name,
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
        std::shared_ptr<YTsaurusStorageConfiguration> configuration = nullptr;
        auto named_collection = created_from_ddl ? tryGetNamedCollectionWithOverrides(config, config_prefix, context) : nullptr;
        if (named_collection)
        {

            YTsaurusSettings settings;
            configuration = std::make_shared<YTsaurusStorageConfiguration>(StorageYTsaurus::processNamedCollectionResult(*named_collection, settings, true));
        }
        else
        {
            configuration = std::make_shared<YTsaurusStorageConfiguration>();
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
            boost::split(configuration->http_proxy_urls, config.getString(config_prefix + ".http_proxy_urls"), [](char c) { return c == '|'; });
            configuration->cypress_path = config.getString(config_prefix + ".cypress_path");
            configuration->oauth_token = config.getString(config_prefix + ".oauth_token");
            if (config.has(config_prefix + ".ytsaurus_columns_description"))
                configuration->ytsaurus_columns_description = config.getString(config_prefix + ".ytsaurus_columns_description");
        }

        return std::make_unique<YTsarususDictionarySource>(context, dict_struct, std::move(configuration), sample_block, name);
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

    factory.registerSource("ytsaurus", create_dictionary_source, Documentation{
        .description = "Reads dictionary data from a YTsaurus cluster."
            " This source is experimental; set the `allow_experimental_ytsaurus_dictionary_source` setting to enable it."
#if !USE_YTSAURUS
            " Currently unavailable, because this ClickHouse build does not include YTsaurus support."
#endif
        ,
        .syntax = "SOURCE(YTSAURUS(http_proxy_urls 'url' cypress_path '//path' oauth_token 'token'))",
        .related = {}});
}

#if USE_YTSAURUS
static const UInt64 max_block_size = 8192;


static ThrottlerPtr makeLookupThrottler(const YTsaurusSettings & settings)
{
    auto max_lookups_per_sec = settings[YTsaurusSetting::lookup_throttler_max_requests_per_second].value;
    if (!max_lookups_per_sec)
        return nullptr;
    /// The `YTsaurusLookupThrottled` event is incremented by `YTsaurusClient::lookupRows` based on the return value of
    /// `Throttler::throttle`, so that it counts only requests that were actually blocked, not every request passing through.
    return std::make_shared<Throttler>(max_lookups_per_sec);
}

YTsarususDictionarySource::YTsarususDictionarySource(
    ContextPtr context_,
    const DictionaryStructure & dict_struct_,
    std::shared_ptr<YTsaurusStorageConfiguration> configuration_,
    const Block & sample_block_,
    const String & name_)
    : context(context_)
    , dict_struct{dict_struct_}
    , configuration{configuration_}
    , sample_block{std::make_shared<Block>(sample_block_)}
    , client(new YTsaurusClient(context,
        {
            .http_proxy_urls = configuration->http_proxy_urls,
            .oauth_token = configuration->oauth_token,
            .encode_utf8 = configuration->settings[YTsaurusSetting::encode_utf8],
            .enable_heavy_proxy_redirection = configuration->settings[YTsaurusSetting::enable_heavy_proxy_redirection],
        }))
    , lookup_throttler(makeLookupThrottler(configuration->settings))
    , name(name_)
{
}

YTsarususDictionarySource::YTsarususDictionarySource(const YTsarususDictionarySource & other)
    : YTsarususDictionarySource{other.context, other.dict_struct, other.configuration, *other.sample_block, other.name}
{
}

YTsarususDictionarySource::~YTsarususDictionarySource() = default;

BlockIO YTsarususDictionarySource::loadAll()
{
    BlockIO io;
    io.pipeline = QueryPipeline(YTsaurusSourceFactory::createPipe(
          client
        , configuration->cypress_path
        , {
              .settings = configuration->settings
            , .select_rows_columns = configuration->ytsaurus_columns_description
            , .check_types_allow_nullable = true
            , .lookup_throttler = lookup_throttler

        }
        , sample_block
        , max_block_size
        // TODO enable parallelization for reads from dictionary
        , 1));
    return io;
}

BlockIO YTsarususDictionarySource::loadIds(const VectorWithMemoryTracking<UInt64> & ids)
{
    if (!dict_struct.id)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'id' is required for selective loading");

    if (!supportsSelectiveLoad())
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Can't make selective update of YTsaurus dictionary because data source doesn't supports lookups.");

    BlockIO io;
    auto ids_vectors = divideVectorByChunkSize(ids, configuration->settings[YTsaurusSetting::lookup_max_rows_per_query]);
    VectorWithMemoryTracking<Block> lookup_blocks;
    for (const auto & ids_chunk : ids_vectors)
    {
        lookup_blocks.push_back(blockForIds(dict_struct, ids_chunk));
    }
    io.pipeline = QueryPipeline(YTsaurusSourceFactory::createPipe(
        client
        , configuration->cypress_path
        , {
              .settings = configuration->settings
            , .lookup_input_blocks = std::move(lookup_blocks)
            , .check_types_allow_nullable = true
            , .lookup_throttler = lookup_throttler
            }
        , sample_block
        , max_block_size
        // Parallel reads supported only for static tables
        , 1
    ));
    return io;
}

BlockIO YTsarususDictionarySource::loadKeys(const Columns & key_columns, const VectorWithMemoryTracking<size_t> & requested_rows)
{
    if (!supportsSelectiveLoad())
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Can't make selective update of YTsaurus dictionary because data source doesn't supports lookups.");

    if (!dict_struct.key)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "'key' is required for selective loading");

    if (key_columns.size() != dict_struct.key->size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The size of key_columns does not equal to the size of dictionary key");

    /// `blockForKeys` scans the whole key columns on every call, so it is called only once for all
    /// requested rows, and the resulting block is sliced into chunks; this keeps the total work
    /// linear in the number of requested rows regardless of `lookup_max_rows_per_query`.
    VectorWithMemoryTracking<Block> lookup_blocks;
    if (!requested_rows.empty())
    {
        const Block block_for_all_keys = blockForKeys(dict_struct, key_columns, requested_rows);
        const size_t chunk_size = configuration->settings[YTsaurusSetting::lookup_max_rows_per_query];
        const size_t total_rows = block_for_all_keys.rows();
        if (!chunk_size || total_rows <= chunk_size)
            lookup_blocks.push_back(block_for_all_keys);
        else
            for (size_t offset = 0; offset < total_rows; offset += chunk_size)
                lookup_blocks.push_back(block_for_all_keys.cloneWithCutColumns(offset, std::min(chunk_size, total_rows - offset)));
    }

    BlockIO io;
    io.pipeline = QueryPipeline(YTsaurusSourceFactory::createPipe(
          client
        , configuration->cypress_path
        , {
              .settings = configuration->settings
            , .lookup_input_blocks = std::move(lookup_blocks)
            , .check_types_allow_nullable = true
            , .lookup_throttler = lookup_throttler
        }
         , sample_block
         , max_block_size
         // Parallel reads supported only for static tables
         , 1
    ));
    return io;
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
