#include "config.h"

#if USE_YTSAURUS

#include <Parsers/ASTIdentifier.h>
#include <Storages/StorageFactory.h>
#include <Storages/Ytsaurus/StorageYtsaurus.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Common/ErrorCodes.h>
#include <Core/Settings.h>
#include <Processors/Sources/YtsaurusSource.h>
#include <Core/Ytsaurus/YtsaurusClient.h>
#include <Interpreters/evaluateConstantExpression.h>


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


StorageYtsaurus::StorageYtsaurus(
    const StorageID & table_id_,
    YtsaurusStorageConfiguration configuration_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment)
    : IStorage{table_id_}
    , configuration{std::move(configuration_)}
    , log(getLogger(" (" + table_id_.table_name + ")"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageYtsaurus::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    Block sample_block;
    for (const String & column_name : column_names)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);
        sample_block.insert({ column_data.type, column_data.name });
    }

    ytsaurus::YtsaurusClient::ConnectionInfo connection_info{.base_uri = configuration.base_uri, .auth_token = configuration.auth_token};
    ytsaurus::YtsaurusClientPtr client = std::make_unique<ytsaurus::YtsaurusClient>(connection_info);

    auto ptr = YtsaurusSourceFactory::createSource(std::move(client), configuration.path, sample_block, max_block_size);

    return Pipe(ptr);
}

YtsaurusStorageConfiguration StorageYtsaurus::getConfiguration(ASTs engine_args, ContextPtr context)
{
    YtsaurusStorageConfiguration configuration;
    for (auto & engine_arg : engine_args)
        engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);
    if (engine_args.size() == 3)
    {
        configuration.base_uri = checkAndGetLiteralArgument<String>(engine_args[0], "base_uri");
        configuration.path = checkAndGetLiteralArgument<String>(engine_args[1], "path");
        configuration.auth_token = checkAndGetLiteralArgument<String>(engine_args[2], "auth_token");
    }
    else
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Incorrect Ytsarurus table schema. Expected Ytsaurus(<base_url>, <yt_path>, <auth_token>)");
    return configuration;
}

void registerStorageYtsaurus(StorageFactory & factory)
{
    factory.registerStorage("Ytsaurus", [](const StorageFactory::Arguments & args)
    {
        if (args.mode <= LoadingStrictnessLevel::CREATE && !args.getLocalContext()->getSettingsRef()[Setting::allow_experimental_ytsaurus_table_engine])
            throw Exception(ErrorCodes::UNKNOWN_STORAGE, "Table engine Ytsaurus is experimental."
                "Set `allow_experimental_ytsaurus_table_engine` setting to enable it");
        return std::make_shared<StorageYtsaurus>(
            args.table_id,
            StorageYtsaurus::getConfiguration(args.engine_args, args.getLocalContext()),
            args.columns,
            args.constraints,
            args.comment);
    },
    {
        .source_access_type = AccessType::YTSAURUS,
    });
}

}
#endif
