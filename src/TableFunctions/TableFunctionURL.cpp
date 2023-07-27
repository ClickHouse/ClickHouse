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
#include <Interpreters/Context.h>
#include <Formats/FormatFactory.h>


namespace DB
{
static const String bad_arguments_error_message = "Table function URL can have the following arguments: "
    "url, name of used format (taken from file extension by default), "
    "optional table structure, optional compression method, "
    "optional headers (specified as `headers('name'='value', 'name2'='value2')`)";

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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
    const auto & ast_function = assert_cast<const ASTFunction *>(ast.get());

    const auto & args = ast_function->children;
    if (args.empty())
        throw Exception::createDeprecated(bad_arguments_error_message, ErrorCodes::BAD_ARGUMENTS);

    auto & url_function_args = assert_cast<ASTExpressionList *>(args[0].get())->children;

    if (auto named_collection = tryGetNamedCollectionWithOverrides(url_function_args, context))
    {
        StorageURL::processNamedCollectionResult(configuration, *named_collection);

        filename = configuration.url;
        structure = configuration.structure;
        compression_method = configuration.compression_method;

        format = configuration.format;
        if (format == "auto")
            format = FormatFactory::instance().getFormatFromFileName(Poco::URI(filename).getPath(), true);

        StorageURL::collectHeaders(url_function_args, configuration.headers, context);
    }
    else
    {
        auto * headers_it = StorageURL::collectHeaders(url_function_args, configuration.headers, context);
        /// ITableFunctionFileLike cannot parse headers argument, so remove it.
        if (headers_it != url_function_args.end())
            url_function_args.erase(headers_it);

        ITableFunctionFileLike::parseArguments(ast, context);
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

ColumnsDescription TableFunctionURL::getActualTableStructure(ContextPtr context) const
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
