#include <TableFunctions/TableFunctionURL.h>

#include "registerTableFunctions.h"
#include <Access/Common/AccessFlags.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageURLCluster.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromVector.h>


namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsBool parallel_replicas_for_cluster_engines;
    extern const SettingsString cluster_for_parallel_replicas;
    extern const SettingsParallelReplicasMode parallel_replicas_mode;
}

std::vector<size_t> TableFunctionURL::skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr) const
{
    auto & table_function_node = query_node_table_function->as<TableFunctionNode &>();
    auto & table_function_arguments_nodes = table_function_node.getArguments().getNodes();
    size_t table_function_arguments_size = table_function_arguments_nodes.size();

    std::vector<size_t> result;

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
        if (format == "auto")
            format = FormatFactory::instance().tryGetFormatFromFileName(Poco::URI(filename).getPath()).value_or("auto");

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
}

void TableFunctionURL::updateStructureAndFormatArgumentsIfNeeded(ASTs & args, const String & structure_, const String & format_, const ContextPtr & context)
{
    if (auto collection = tryGetNamedCollectionWithOverrides(args, context))
    {
        /// In case of named collection, just add key-value pairs "format='...', structure='...'"
        /// at the end of arguments to override existed format and structure with "auto" values.
        if (collection->getOrDefault<String>("format", "auto") == "auto")
        {
            ASTs format_equal_func_args = {std::make_shared<ASTIdentifier>("format"), std::make_shared<ASTLiteral>(format_)};
            auto format_equal_func = makeASTFunction("equals", std::move(format_equal_func_args));
            args.push_back(format_equal_func);
        }
        if (collection->getOrDefault<String>("structure", "auto") == "auto")
        {
            ASTs structure_equal_func_args = {std::make_shared<ASTIdentifier>("structure"), std::make_shared<ASTLiteral>(structure_)};
            auto structure_equal_func = makeASTFunction("equals", std::move(structure_equal_func_args));
            args.push_back(structure_equal_func);
        }
    }
    else
    {
        /// If arguments contain headers, just remove it and add to the end of arguments later.
        HTTPHeaderEntries tmp_headers;
        size_t count = StorageURL::evalArgsAndCollectHeaders(args, tmp_headers, context);
        ASTPtr headers_ast;
        if (count != args.size())
        {
            chassert(count + 1 == args.size());
            headers_ast = args.back();
            args.pop_back();
        }

        ITableFunctionFileLike::updateStructureAndFormatArgumentsIfNeeded(args, structure_, format_, context);

        if (headers_ast)
            args.push_back(headers_ast);
    }
}

StoragePtr TableFunctionURL::getStorage(
    const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr global_context,
    const std::string & table_name, const String & compression_method_) const
{
    const auto & settings = global_context->getSettingsRef();
    auto parallel_replicas_cluster_name = settings[Setting::cluster_for_parallel_replicas].toString();
    auto can_use_parallel_replicas = settings[Setting::allow_experimental_parallel_reading_from_replicas] > 0
        && settings[Setting::parallel_replicas_for_cluster_engines]
        && settings[Setting::parallel_replicas_mode] == ParallelReplicasMode::READ_TASKS
        && !parallel_replicas_cluster_name.empty()
        && !global_context->isDistributed();

    if (can_use_parallel_replicas)
    {
        return std::make_shared<StorageURLCluster>(
            global_context,
            settings[Setting::cluster_for_parallel_replicas],
            filename,
            format,
            compression_method,
            StorageID(getDatabaseName(), table_name),
            getActualTableStructure(global_context, /* is_insert_query */ true),
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
        global_context,
        compression_method_,
        configuration.headers,
        configuration.http_method);
}

ColumnsDescription TableFunctionURL::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    if (structure == "auto")
    {
        context->checkAccess(getSourceAccessType());
        if (format == "auto")
            return StorageURL::getTableStructureAndFormatFromData(
                       filename,
                       chooseCompressionMethod(Poco::URI(filename).getPath(), compression_method),
                       configuration.headers,
                       std::nullopt,
                       context).first;

        return StorageURL::getTableStructureFromData(format,
            filename,
            chooseCompressionMethod(Poco::URI(filename).getPath(), compression_method),
            configuration.headers,
            std::nullopt,
            context);
    }

    return parseColumnsListFromString(structure, context);
}

std::optional<String> TableFunctionURL::tryGetFormatFromFirstArgument()
{
    return FormatFactory::instance().tryGetFormatFromFileName(Poco::URI(filename).getPath());
}

void registerTableFunctionURL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionURL>();
}
}
