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
#include <Parsers/ASTExpressionList.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageURLCluster.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromVector.h>
#include <Storages/HivePartitioningUtils.h>


namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsBool parallel_replicas_for_cluster_engines;
    extern const SettingsString cluster_for_parallel_replicas;
    extern const SettingsParallelReplicasMode parallel_replicas_mode;
    extern const SettingsString url_base;
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

    /// Dispatch to another backend based on the URL scheme (file://, s3://, az://, hdfs://, ...).
    /// http(s) and unrecognized schemes keep the plain StorageURL behavior below.
    if (const auto target = classifyURLScheme(filename); target != URLSchemeTarget::URL)
    {
        buildDelegate(target, context);
        return;
    }

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

void TableFunctionURL::buildDelegate(URLSchemeTarget target, const ContextPtr & context)
{
    delegate_engine_name = storageEngineNameForURLScheme(target);

    auto args_list = make_intrusive<ASTExpressionList>();

    if (target == URLSchemeTarget::Azure)
    {
        auto parts = parseAzureURL(filename);
        args_list->children.push_back(make_intrusive<ASTLiteral>(parts.account_url));
        args_list->children.push_back(make_intrusive<ASTLiteral>(parts.container));
        args_list->children.push_back(make_intrusive<ASTLiteral>(parts.blob_path));

        /// `azureBlobStorage` positional order after the source triple is (format, compression, structure).
        const bool need_structure = structure != "auto";
        const bool need_compression = compression_method != "auto";
        const bool need_format = format != "auto" || need_compression || need_structure;
        if (need_format)
            args_list->children.push_back(make_intrusive<ASTLiteral>(format));
        if (need_compression || need_structure)
            args_list->children.push_back(make_intrusive<ASTLiteral>(need_compression ? compression_method : String("auto")));
        if (need_structure)
            args_list->children.push_back(make_intrusive<ASTLiteral>(structure));
    }
    else
    {
        const String source = (target == URLSchemeTarget::File) ? getLocalPathFromFileURL(filename) : filename;
        args_list->children.push_back(make_intrusive<ASTLiteral>(source));

        /// `file`, `s3` and `hdfs` share the (source, format, structure, compression) positional order.
        const bool need_compression = compression_method != "auto";
        const bool need_structure = structure != "auto" || need_compression;
        const bool need_format = format != "auto" || need_structure;
        if (need_format)
            args_list->children.push_back(make_intrusive<ASTLiteral>(format));
        if (need_structure)
            args_list->children.push_back(make_intrusive<ASTLiteral>(structure));
        if (need_compression)
            args_list->children.push_back(make_intrusive<ASTLiteral>(compression_method));
    }

    auto delegate_ast = make_intrusive<ASTFunction>();
    delegate_ast->name = tableFunctionNameForURLScheme(target);
    delegate_ast->arguments = args_list;
    delegate_ast->children.push_back(args_list);

    delegate = TableFunctionFactory::instance().get(delegate_ast, context);

    if (!structure_hint.empty())
        delegate->setStructureHint(structure_hint);
}

StoragePtr TableFunctionURL::executeImpl(
    const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const
{
    if (delegate)
        return delegate->execute(ast_function, context, table_name, std::move(cached_columns), /*use_global_context=*/false, is_insert_query);

    return ITableFunctionFileLike::executeImpl(ast_function, context, table_name, std::move(cached_columns), is_insert_query);
}

bool TableFunctionURL::needStructureHint() const
{
    return delegate ? delegate->needStructureHint() : ITableFunctionFileLike::needStructureHint();
}

void TableFunctionURL::setStructureHint(const ColumnsDescription & structure_hint_)
{
    ITableFunctionFileLike::setStructureHint(structure_hint_);
    if (delegate)
        delegate->setStructureHint(structure_hint_);
}

bool TableFunctionURL::supportsReadingSubsetOfColumns(const ContextPtr & context)
{
    return delegate ? delegate->supportsReadingSubsetOfColumns(context) : ITableFunctionFileLike::supportsReadingSubsetOfColumns(context);
}

NameSet TableFunctionURL::getVirtualsToCheckBeforeUsingStructureHint() const
{
    return delegate ? delegate->getVirtualsToCheckBeforeUsingStructureHint() : ITableFunctionFileLike::getVirtualsToCheckBeforeUsingStructureHint();
}

bool TableFunctionURL::hasStaticStructure() const
{
    /// ITableFunctionFileLike::hasStaticStructure() is private; replicate its logic for the URL path.
    return delegate ? delegate->hasStaticStructure() : (structure != "auto");
}

void TableFunctionURL::setPartitionBy(const ASTPtr & partition_by_)
{
    if (delegate)
        delegate->setPartitionBy(partition_by_);
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

ColumnsDescription TableFunctionURL::getActualTableStructure(ContextPtr context, bool is_insert_query) const
{
    if (delegate)
        return delegate->getActualTableStructure(context, is_insert_query);

    if (structure == "auto")
    {
        ColumnsDescription columns;

        if (format == "auto")
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
            filename,
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
