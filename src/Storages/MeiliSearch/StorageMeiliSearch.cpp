#include <Core/Types.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/Formats/IOutputFormat.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/IStorage.h>
#include <Storages/MeiliSearch/MeiliSearchConnection.h>
#include <Storages/MeiliSearch/SinkMeiliSearch.h>
#include <Storages/MeiliSearch/SourceMeiliSearch.h>
#include <Storages/MeiliSearch/StorageMeiliSearch.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <base/logger_useful.h>
#include <Common/parseAddress.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_QUERY_PARAMETER;
}

StorageMeiliSearch::StorageMeiliSearch(
    const StorageID & table_id,
    const MeiliSearchConfiguration & config_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment)
    : IStorage(table_id), config{config_}, log(&Poco::Logger::get("StorageMeiliSearch (" + table_id.table_name + ")"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}

String convertASTtoStr(ASTPtr ptr)
{
    WriteBufferFromOwnString out;
    IAST::FormatSettings settings(out, true);
    settings.identifier_quoting_style = IdentifierQuotingStyle::BackticksMySQL;
    settings.always_quote_identifiers = IdentifierQuotingStyle::BackticksMySQL != IdentifierQuotingStyle::None;
    ptr->format(settings);
    return out.str();
}

ASTPtr getFunctionParams(ASTPtr node, const String & name)
{
    if (!node)
        return nullptr;

    const auto * ptr = node->as<ASTFunction>();
    if (ptr && ptr->name == name)
    {
        if (node->children.size() == 1)
            return node->children[0];
        else
            return nullptr;
    }
    for (const auto & next : node->children)
    {
        auto res = getFunctionParams(next, name);
        if (res != nullptr)
            return res;
    }
    return nullptr;
}

Pipe StorageMeiliSearch::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    ASTPtr original_where = query_info.query->clone()->as<ASTSelectQuery &>().where();
    ASTPtr query_params = getFunctionParams(original_where, "meiliMatch");


    std::unordered_map<String, String> kv_pairs_params;
    if (query_params)
    {
        LOG_TRACE(log, "Query params: {}", convertASTtoStr(query_params));
        for (const auto & el : query_params->children)
        {
            auto str = el->getColumnName();
            auto it = find(str.begin(), str.end(), '=');
            if (it == str.end())
                throw Exception("meiliMatch function must have parameters of the form \'key=value\'", ErrorCodes::BAD_QUERY_PARAMETER);

            String key(str.begin() + 1, it);
            String value(it + 1, str.end() - 1);
            kv_pairs_params[key] = value;
        }
    }
    else
    {
        LOG_TRACE(log, "Query params: none");
    }

    for (const auto & el : kv_pairs_params)
        LOG_TRACE(log, "Parsed parameter: key = " + el.first + ", value = " + el.second);

    auto sample_block = metadata_snapshot->getSampleBlockForColumns(column_names, getVirtuals(), getStorageID());

    return Pipe(std::make_shared<MeiliSearchSource>(config, sample_block, max_block_size, kv_pairs_params));
}

SinkToStoragePtr StorageMeiliSearch::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    LOG_TRACE(log, "Trying update index: " + config.index);
    return std::make_shared<SinkMeiliSearch>(config, metadata_snapshot->getSampleBlock(), local_context);
}

MeiliSearchConfiguration getConfiguration(ASTs engine_args, ContextPtr context)
{
    if (auto named_collection = getExternalDataSourceConfiguration(engine_args, context))
    {
        auto [common_configuration, storage_specific_args] = named_collection.value();
        String url = common_configuration.addresses_expr;
        String index = common_configuration.table;
        String key = common_configuration.password;
        for (const auto & [arg_name, arg_value] : storage_specific_args)
        {
            if (arg_name == "url")
            {
                if (!url.empty())
                    throw Exception(
                        "2 times given 'url' value, old = " + url + " new = " + arg_value->as<ASTLiteral>()->value.safeGet<String>(),
                        ErrorCodes::BAD_ARGUMENTS);
                url = arg_value->as<ASTLiteral>()->value.safeGet<String>();
            }
            else if (arg_name == "key" || arg_name == "password")
            {
                if (!key.empty())
                    throw Exception(
                        "2 times given 'key' value, old = " + key + " new = " + arg_value->as<ASTLiteral>()->value.safeGet<String>(),
                        ErrorCodes::BAD_ARGUMENTS);
                key = arg_value->as<ASTLiteral>()->value.safeGet<String>();
            }
            else
                throw Exception("Unexpected key-value argument", ErrorCodes::BAD_ARGUMENTS);
        }
        if (url.empty() || index.empty())
        {
            throw Exception("Storage MeiliSearch requires 3 parameters: {url, index, [key]}", ErrorCodes::BAD_ARGUMENTS);
        }
        return MeiliSearchConfiguration(url, index, key);
    }
    else
    {
        if (engine_args.size() != 3)
        {
            throw Exception(
                "Storage MeiliSearch requires 3 parameters: MeiliSearch('url', 'index', 'key')",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        String url = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
        String index = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        String key = engine_args[2]->as<ASTLiteral &>().value.safeGet<String>();
        return MeiliSearchConfiguration(url, index, key);
    }
}

void registerStorageMeiliSearch(StorageFactory & factory)
{
    factory.registerStorage(
        "MeiliSearch",
        [](const StorageFactory::Arguments & args) {
            auto config = getConfiguration(args.engine_args, args.getLocalContext());
            return StorageMeiliSearch::create(args.table_id, config, args.columns, args.constraints, args.comment);
        },
        {
            .source_access_type = AccessType::MEILISEARCH,
        });
}


}
