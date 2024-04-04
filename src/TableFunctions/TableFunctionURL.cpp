#include <TableFunctions/TableFunctionURL.h>

#include "registerTableFunctions.h"
#include <Access/Common/AccessFlags.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageExternalDistributed.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Formats/FormatFactory.h>

#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromVector.h>
namespace DB
{

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
            format = FormatFactory::instance().getFormatFromFileName(Poco::URI(filename).getPath(), true);

        StorageURL::collectHeaders(args, configuration.headers, context);
    }
    else
    {
        auto * headers_it = StorageURL::collectHeaders(args, configuration.headers, context);
        /// ITableFunctionFileLike cannot parse headers argument, so remove it.
        if (headers_it != args.end())
            args.erase(headers_it);

        ITableFunctionFileLike::parseArgumentsImpl(args, context);
    }
}

void TableFunctionURL::addColumnsStructureToArguments(ASTs & args, const String & desired_structure, const ContextPtr & context)
{
    if (tryGetNamedCollectionWithOverrides(args, context))
    {
        /// In case of named collection, just add key-value pair "structure='...'"
        /// at the end of arguments to override existed structure.
        ASTs equal_func_args = {std::make_shared<ASTIdentifier>("structure"), std::make_shared<ASTLiteral>(desired_structure)};
        auto equal_func = makeASTFunction("equals", std::move(equal_func_args));
        args.push_back(equal_func);
    }
    else
    {
        /// If arguments contain headers, just remove it and add to the end of arguments later
        /// (header argument can be at any position).
        HTTPHeaderEntries tmp_headers;
        auto * headers_it = StorageURL::collectHeaders(args, tmp_headers, context);
        ASTPtr headers_ast;
        if (headers_it != args.end())
        {
            headers_ast = *headers_it;
            args.erase(headers_it);
        }

        ITableFunctionFileLike::addColumnsStructureToArguments(args, desired_structure, context);

        if (headers_ast)
            args.push_back(headers_ast);
    }
}

StoragePtr TableFunctionURL::getStorage(
    const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr global_context,
    const std::string & table_name, const String & compression_method_) const
{
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
        return StorageURL::getTableStructureFromData(format,
            filename,
            chooseCompressionMethod(Poco::URI(filename).getPath(), compression_method),
            configuration.headers,
            std::nullopt,
            context);
    }

    return parseColumnsListFromString(structure, context);
}

String TableFunctionURL::getFormatFromFirstArgument()
{
    return FormatFactory::instance().getFormatFromFileName(Poco::URI(filename).getPath(), true);
}

void registerTableFunctionURL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionURL>();
}
}
