#include <TableFunctions/TableFunctionURL.h>

#include <TableFunctions/registerTableFunctions.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/Web/Configuration.h>
#include <Storages/StorageURLCluster.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromVector.h>
#include <Storages/HivePartitioningUtils.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_url_wildcard_from_index_pages;
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsBool parallel_replicas_for_cluster_engines;
    extern const SettingsString cluster_for_parallel_replicas;
    extern const SettingsParallelReplicasMode parallel_replicas_mode;
    extern const SettingsString url_base;
}

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{
    void checkExperimentalURLWildcardFromIndexPages(const ContextPtr & context)
    {
        if (context->getSettingsRef()[Setting::allow_experimental_url_wildcard_from_index_pages])
            return;

        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Wildcard expansion for `url` from HTTP index pages is experimental. "
            "Set `allow_experimental_url_wildcard_from_index_pages = 1` to enable it");
    }

    ASTs makeWebObjectStorageEngineArgs(
        const String & source,
        const String & format,
        const String & structure,
        const String & compression_method,
        const HTTPHeaderEntries & headers)
    {
        ASTs engine_args;
        engine_args.emplace_back(make_intrusive<ASTLiteral>(source));
        engine_args.emplace_back(make_intrusive<ASTLiteral>(format));

        if (structure != "auto" || compression_method != "auto")
            engine_args.emplace_back(make_intrusive<ASTLiteral>(structure));
        if (compression_method != "auto")
            engine_args.emplace_back(make_intrusive<ASTLiteral>(compression_method));

        if (!headers.empty())
        {
            ASTs header_equals;
            header_equals.reserve(headers.size());
            for (const auto & [header_name, header_value] : headers)
            {
                ASTs equals_args;
                equals_args.emplace_back(make_intrusive<ASTLiteral>(header_name));
                equals_args.emplace_back(make_intrusive<ASTLiteral>(header_value));
                header_equals.emplace_back(makeASTOperator("equals", std::move(equals_args)));
            }

            auto headers_list = make_intrusive<ASTExpressionList>();
            headers_list->children = std::move(header_equals);

            auto headers_func = make_intrusive<ASTFunction>();
            headers_func->name = "headers";
            headers_func->arguments = headers_list;
            headers_func->children.push_back(headers_func->arguments);
            engine_args.emplace_back(std::move(headers_func));
        }

        return engine_args;
    }
}

VectorWithMemoryTracking<size_t> TableFunctionURL::skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr) const
{
    auto & table_function_node = query_node_table_function->as<TableFunctionNode &>();
    auto & table_function_arguments_nodes = table_function_node.getArguments().getNodes();
    size_t table_function_arguments_size = table_function_arguments_nodes.size();

    VectorWithMemoryTracking<size_t> result;

    for (size_t i = 0; i < table_function_arguments_size; ++i)
    {
        auto * function_node = table_function_arguments_nodes[i]->as<FunctionNode>();
        if (function_node && function_node->getFunctionName() == "headers")
            result.push_back(i);
    }

    return result;
}

void TableFunctionURL::parseArguments(const ASTPtr & ast, ContextPtr context)
{
    /// Clone ast function, because we can modify it's arguments like removing headers.
    ITableFunctionFileLike::parseArguments(ast->clone(), context);
}

void TableFunctionURL::parseArgumentsImpl(ASTs & args, const ContextPtr & context)
{
    if (auto named_collection = tryGetNamedCollectionWithOverrides(args, context))
    {
        StorageURL::processNamedCollectionResult(configuration, *named_collection);

        filename = configuration.url;
        structure = configuration.structure;
        compression_method = configuration.compression_method;

        format = configuration.format;

        StorageURL::evalArgsAndCollectHeaders(args, configuration.headers, context);
    }
    else
    {
        size_t count = StorageURL::evalArgsAndCollectHeaders(args, configuration.headers, context);
        /// ITableFunctionFileLike cannot parse headers argument, so remove it.
        ASTPtr headers_ast;
        if (count != args.size())
        {
            chassert(count + 1 == args.size());
            headers_ast = args.back();
            args.pop_back();
        }

        ITableFunctionFileLike::parseArgumentsImpl(args, context);

        if (headers_ast)
            args.push_back(headers_ast);
    }

    /// Resolve relative URLs against the url_base setting.
    const auto & url_base = context->getSettingsRef()[Setting::url_base].value;
    filename = StorageURL::resolveURLBase(filename, url_base);
    configuration.url = filename;

    /// Re-derive format from the resolved URL if still auto, because the original
    /// filename may have been a relative reference (e.g. "?x=1") with no extension.
    /// `resolveURLBase` tolerates malformed inputs via string manipulation, so the resolved URL
    /// may contain characters that `Poco::URI` rejects. Fall back to "auto" instead of throwing.
    if (format == "auto")
    {
        try
        {
            format = FormatFactory::instance().tryGetFormatFromFileName(Poco::URI(filename).getPath()).value_or("auto");
        }
        catch (const Poco::Exception &) // NOLINT(bugprone-empty-catch)
        {
        }
    }
}

StoragePtr TableFunctionURL::getStorage(
    const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr context,
    const std::string & table_name, const String & compression_method_, bool is_insert_query) const
{
    const auto & settings = context->getSettingsRef();
    const auto is_secondary_query = context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY;
    const auto parallel_replicas_cluster_name = settings[Setting::cluster_for_parallel_replicas].toString();
    const bool can_use_parallel_replicas = !parallel_replicas_cluster_name.empty()
        && settings[Setting::parallel_replicas_for_cluster_engines]
        && context->canUseTaskBasedParallelReplicas()
        && !context->isDistributed()
        && !is_secondary_query
        && !is_insert_query;

    const auto & client_info = context->getClientInfo();
    bool can_use_distributed_iterator =
        client_info.collaborate_with_initiator &&
        context->hasClusterFunctionReadTaskCallback();

    if (can_use_parallel_replicas)
    {
        return std::make_shared<StorageURLCluster>(
            context,
            parallel_replicas_cluster_name,
            source,
            format_,
            compression_method_,
            StorageID(getDatabaseName(), table_name),
            getActualTableStructure(context, true),
            ConstraintsDescription{},
            configuration);
    }

    if (!is_insert_query && configuration.http_method.empty() && urlPathHasListableGlobs(source))
    {
        checkExperimentalURLWildcardFromIndexPages(context);
        auto object_storage_configuration = std::make_shared<StorageWebConfiguration>();

        auto engine_args = makeWebObjectStorageEngineArgs(source, format_, structure, compression_method_, configuration.headers);
        StorageObjectStorageConfiguration::initialize(*object_storage_configuration, engine_args, context, /* with_table_structure */ true);

        ObjectStoragePtr object_storage = object_storage_configuration->createObjectStorage(context, /* is_readonly */ true, std::nullopt);

        return std::make_shared<StorageObjectStorage>(
            object_storage_configuration,
            object_storage,
            context,
            StorageID(getDatabaseName(), table_name),
            columns,
            ConstraintsDescription{},
            String{},
            std::nullopt,
            LoadingStrictnessLevel::CREATE,
            /* catalog */ nullptr,
            /* if_not_exists */ false,
            /* is_datalake_query */ false,
            /* distributed_processing */ can_use_distributed_iterator,
            /* partition_by */ nullptr,
            /* order_by */ nullptr,
            /* is_table_function */ true,
            /* lazy_init */ false);
    }

    return std::make_shared<StorageURL>(
        source,
        StorageID(getDatabaseName(), table_name),
        format_,
        std::nullopt /*format settings*/,
        columns,
        ConstraintsDescription{},
        String{},
        context,
        compression_method_,
        configuration.headers,
        configuration.http_method,
        nullptr,
        /*distributed_processing=*/can_use_distributed_iterator);
}

ColumnsDescription TableFunctionURL::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    if (structure == "auto")
    {
        ColumnsDescription columns;
        String sample_path = filename;

        if (configuration.http_method.empty() && urlPathHasListableGlobs(filename))
        {
            checkExperimentalURLWildcardFromIndexPages(context);

            auto object_storage_configuration = std::make_shared<StorageWebConfiguration>();
            auto engine_args = makeWebObjectStorageEngineArgs(filename, format, structure, compression_method, configuration.headers);
            StorageObjectStorageConfiguration::initialize(*object_storage_configuration, engine_args, context, /* with_table_structure */ true);

            auto object_storage = object_storage_configuration->createObjectStorage(context, /* is_readonly */ true, std::nullopt);
            if (format == "auto")
            {
                auto schema_and_format = StorageObjectStorage::resolveSchemaAndFormatFromData(
                    object_storage, object_storage_configuration, std::nullopt, sample_path, context);
                columns = std::move(schema_and_format.first);
            }
            else
            {
                columns = StorageObjectStorage::resolveSchemaFromData(
                    object_storage, object_storage_configuration, std::nullopt, sample_path, context);
            }
        }
        else if (format == "auto")
        {
            columns = StorageURL::getTableStructureAndFormatFromData(
                filename,
                chooseCompressionMethod(Poco::URI(filename).getPath(), compression_method),
                configuration.headers,
                std::nullopt,
                context).first;
        }
        else
        {
            columns = StorageURL::getTableStructureFromData(format,
                filename,
                chooseCompressionMethod(Poco::URI(filename).getPath(), compression_method),
                configuration.headers,
                std::nullopt,
                context);
        }

        HivePartitioningUtils::setupHivePartitioningForFileURLLikeStorage(
            columns,
            sample_path,
            /* inferred_schema */ true,
            /* format_settings */ std::nullopt,
            context);

        return columns;
    }

    return parseColumnsListFromString(structure, context);
}

std::optional<String> TableFunctionURL::tryGetFormatFromFirstArgument()
{
    return FormatFactory::instance().tryGetFormatFromFileName(Poco::URI(filename).getPath());
}

void registerTableFunctionURL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionURL>({});
}
}
