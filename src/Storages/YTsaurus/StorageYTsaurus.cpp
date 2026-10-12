#include "config.h"

#if USE_YTSAURUS

#include <Interpreters/Context.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/StorageFactory.h>
#include <Storages/YTsaurus/StorageYTsaurus.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Common/ErrorCodes.h>
#include <Core/Settings.h>
#include <Processors/Sources/YTsaurusSource.h>
#include <Core/YTsaurus/YTsaurusClient.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <QueryPipeline/Pipe.h>

#include <boost/algorithm/string/split.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_STORAGE;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_ytsaurus_table_engine;
}

namespace YTsaurusSetting
{
    extern const YTsaurusSettingsBool encode_utf8;
    extern const YTsaurusSettingsBool enable_heavy_proxy_redirection;
}


StorageYTsaurus::StorageYTsaurus(
    const StorageID & table_id_,
    YTsaurusStorageConfiguration configuration_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment)
    : IStorage{table_id_}
    , cypress_path(std::move(configuration_.cypress_path))
    , settings(configuration_.settings)
    , client_connection_info{
        .http_proxy_urls = std::move(configuration_.http_proxy_urls),
        .oauth_token = std::move(configuration_.oauth_token),
        .encode_utf8 = settings[YTsaurusSetting::encode_utf8],
        .enable_heavy_proxy_redirection = settings[YTsaurusSetting::enable_heavy_proxy_redirection],
    }
    , log(getLogger("StorageYTsaurus(" + table_id_.table_name + ")"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageYTsaurus::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    storage_snapshot->check(column_names);

    BlockPtr sample_block = std::make_shared<Block>();
    ColumnsDescription columns_description = storage_snapshot->metadata->getColumns();
    for (const String & column_name : column_names)
    {
        auto column_data = columns_description.getPhysical(column_name);
        sample_block->insert({ column_data.type, column_data.name });
    }

    YTsaurusClientPtr client(new YTsaurusClient(context, client_connection_info));
    return YTsaurusSourceFactory::createPipe(client, cypress_path, {.settings = settings}, sample_block, max_block_size, num_streams);
}

YTsaurusStorageConfiguration StorageYTsaurus::processNamedCollectionResult(
    const NamedCollection & named_collection, const YTsaurusSettings& settings, bool is_for_dictionary = false)
{
    ValidateKeysMultiset<ExternalDatabaseEqualKeysSet> required_arguments = {"http_proxy_urls", "cypress_path", "oauth_token"};
    ValidateKeysMultiset<ExternalDatabaseEqualKeysSet> optional_arguments = {"ytsaurus_columns_description"};
    if (is_for_dictionary)
    {
        for (const auto & name : settings.getAllRegisteredNames())
        {
            optional_arguments.insert(name);
        }
        optional_arguments.insert("name");
    }
    validateNamedCollection<ValidateKeysMultiset<ExternalDatabaseEqualKeysSet>>(named_collection, required_arguments, optional_arguments);


    YTsaurusStorageConfiguration configuration{.settings = settings};
    configuration.settings.loadFromNamedCollection(named_collection);

    boost::split(configuration.http_proxy_urls, named_collection.get<String>("http_proxy_urls"), [](char c) { return c == '|'; });
    configuration.cypress_path = named_collection.get<String>("cypress_path");
    configuration.oauth_token = named_collection.get<String>("oauth_token");

    if (is_for_dictionary)
    {
        auto column_description = named_collection.getOrDefault<String>("ytsaurus_columns_description", "");
        if (!column_description.empty())
            configuration.ytsaurus_columns_description = std::move(column_description);
    }
    return configuration;
}

YTsaurusStorageConfiguration StorageYTsaurus::getConfiguration(ASTs engine_args, const YTsaurusSettings & settings, ContextPtr context, const StorageID * table_id)
{
    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, context, true, nullptr, table_id))
    {
        return StorageYTsaurus::processNamedCollectionResult(*named_collection, settings);
    }
    YTsaurusStorageConfiguration configuration{.settings = settings};
    if (engine_args.size() == 3)
    {
        for (auto & engine_arg : engine_args)
        {
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);
        }
        boost::split(configuration.http_proxy_urls, checkAndGetLiteralArgument<String>(engine_args[0], "http_proxy_urls"), [](char c) { return c == '|'; });
        configuration.cypress_path = checkAndGetLiteralArgument<String>(engine_args[1], "cypress_path");
        configuration.oauth_token = checkAndGetLiteralArgument<String>(engine_args[2], "oauth_token");
    }
    else
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Incorrect Ytsarurus table schema. Expected YTsaurus(http_proxy_url, cypress_path, oauth_token)");
    return configuration;
}

void registerStorageYTsaurus(StorageFactory & factory)
{
    factory.registerStorage("YTsaurus", [](const StorageFactory::Arguments & args)
    {
        if (args.mode <= LoadingStrictnessLevel::CREATE && !args.getLocalContext()->getSettingsRef()[Setting::allow_experimental_ytsaurus_table_engine])
            throw Exception(ErrorCodes::UNKNOWN_STORAGE, "Table engine YTsaurus is experimental. "
                "Set `allow_experimental_ytsaurus_table_engine` setting to enable it");
        return std::make_shared<StorageYTsaurus>(
            args.table_id,
            StorageYTsaurus::getConfiguration(args.engine_args, YTsaurusSettings::createFromQuery(*args.storage_def), args.getLocalContext(), &args.table_id),
            args.columns,
            args.constraints,
            args.comment);
    },
    {
        .supports_settings = true,
        .source_access_type = AccessTypeObjects::Source::YTSAURUS,
        .has_builtin_setting_fn = YTsaurusSettings::hasBuiltin
    });
}

}
#endif
