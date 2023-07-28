#include <memory>
#include <Interpreters/evaluateConstantExpression.h>
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
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Common/logger_useful.h>
#include <Common/parseAddress.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Storages/MeiliSearch/MeiliSearchColumnDescriptionFetcher.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_QUERY_PARAMETER;
    extern const int BAD_ARGUMENTS;
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

    if (columns_.empty())
    {
        auto columns = getTableStructureFromData(config);
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}

ColumnsDescription StorageMeiliSearch::getTableStructureFromData(const MeiliSearchConfiguration & config_)
{
    MeiliSearchColumnDescriptionFetcher fetcher(config_);
    fetcher.addParam(doubleQuoteString("limit"), "1");
    return fetcher.fetchColumnsDescription();
}

String convertASTtoStr(ASTPtr ptr)
{
    WriteBufferFromOwnString out;
    IAST::FormatSettings settings(
        out, /*one_line*/ true, /*hilite*/ false,
        /*always_quote_identifiers*/ IdentifierQuotingStyle::BackticksMySQL != IdentifierQuotingStyle::None,
        /*identifier_quoting_style*/ IdentifierQuotingStyle::BackticksMySQL);
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
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    ASTPtr original_where = query_info.query->clone()->as<ASTSelectQuery &>().where();
    ASTPtr query_params = getFunctionParams(original_where, "meiliMatch");

    MeiliSearchSource::QueryRoute route = MeiliSearchSource::QueryRoute::documents;

    std::unordered_map<String, String> kv_pairs_params;
    if (query_params)
    {
        route = MeiliSearchSource::QueryRoute::search;
        LOG_TRACE(log, "Query params: {}", convertASTtoStr(query_params));
        for (const auto & el : query_params->children)
        {
            auto str = el->getColumnName();
            auto it = std::find(str.begin(), str.end(), '=');
            if (it == str.end())
                throw Exception(ErrorCodes::BAD_QUERY_PARAMETER, "meiliMatch function must have parameters of the form \'key=value\'");

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
        LOG_TRACE(log, "Parsed parameter: key = {}, value = {}", el.first, el.second);

    auto sample_block = storage_snapshot->getSampleBlockForColumns(column_names);

    return Pipe(std::make_shared<MeiliSearchSource>(config, sample_block, max_block_size, route, kv_pairs_params));
}

SinkToStoragePtr StorageMeiliSearch::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool /*async_insert*/)
{
    LOG_TRACE(log, "Trying update index: {}", config.index);
    return std::make_shared<SinkMeiliSearch>(config, metadata_snapshot->getSampleBlock(), local_context);
}

MeiliSearchConfiguration StorageMeiliSearch::getConfiguration(ASTs engine_args, ContextPtr context)
{
    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, context))
    {
        validateNamedCollection(*named_collection, {"url", "index"}, {"key"});

        String url = named_collection->get<String>("url");
        String index = named_collection->get<String>("index");
        String key = named_collection->getOrDefault<String>("key", "");

        if (url.empty() || index.empty())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Storage MeiliSearch requires 3 parameters: MeiliSearch('url', 'index', 'key'= \"\")");
        }

        return MeiliSearchConfiguration(url, index, key);
    }
    else
    {
        if (engine_args.size() < 2 || 3 < engine_args.size())
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Storage MeiliSearch requires 3 parameters: MeiliSearch('url', 'index', 'key'= \"\")");
        }

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);

        String url = checkAndGetLiteralArgument<String>(engine_args[0], "url");
        String index = checkAndGetLiteralArgument<String>(engine_args[1], "index");
        String key;
        if (engine_args.size() == 3)
            key = checkAndGetLiteralArgument<String>(engine_args[2], "key");
        return MeiliSearchConfiguration(url, index, key);
    }
}

void registerStorageMeiliSearch(StorageFactory & factory)
{
    factory.registerStorage(
        "MeiliSearch",
        [](const StorageFactory::Arguments & args)
        {
            auto config = StorageMeiliSearch::getConfiguration(args.engine_args, args.getLocalContext());
            return std::make_shared<StorageMeiliSearch>(args.table_id, config, args.columns, args.constraints, args.comment);
        },
        {
            .supports_schema_inference = true,
            .source_access_type = AccessType::MEILISEARCH,
        });
}


}
