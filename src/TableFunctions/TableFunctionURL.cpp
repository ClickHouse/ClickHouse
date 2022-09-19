#include <TableFunctions/TableFunctionURL.h>

#include "registerTableFunctions.h"
#include <Access/Common/AccessFlags.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageURL.h>
#include <Storages/StorageExternalDistributed.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Formats/FormatFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void TableFunctionURL::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();
    if (!func_args.arguments)
        throw Exception("Table function 'URL' must have arguments.", ErrorCodes::BAD_ARGUMENTS);

    if (auto with_named_collection = getURLBasedDataSourceConfiguration(func_args.arguments->children, context))
    {
        auto [common_configuration, storage_specific_args] = with_named_collection.value();
        configuration.set(common_configuration);

        if (!configuration.http_method.empty()
            && configuration.http_method != Poco::Net::HTTPRequest::HTTP_POST
            && configuration.http_method != Poco::Net::HTTPRequest::HTTP_PUT)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Method can be POST or PUT (current: {}). For insert default is POST, for select GET",
                            configuration.http_method);

        if (!storage_specific_args.empty())
        {
            String illegal_args;
            for (const auto & arg : storage_specific_args)
            {
                if (!illegal_args.empty())
                    illegal_args += ", ";
                illegal_args += arg.first;
            }
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown argument `{}` for table function URL", illegal_args);
        }

        filename = configuration.url;
        format = configuration.format;
        if (format == "auto")
            format = FormatFactory::instance().getFormatFromFileName(filename, true);
        structure = configuration.structure;
        compression_method = configuration.compression_method;
    }
    else
    {
        String bad_arguments_error_message = "Table function URL can have the following arguments: "
            "url, name of used format (taken from file extension by default), "
            "optional table structure, optional compression method, optional headers (specified as `headers('name'='value', 'name2'='value2')`)";

        auto & args = ast_function->children;
        if (args.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, bad_arguments_error_message);

        auto * url_function_args_expr = assert_cast<ASTExpressionList *>(args[0].get());
        auto & url_function_args = url_function_args_expr->children;
        auto headers_it = StorageURL::collectHeaders(url_function_args, configuration, context);
        /// ITableFunctionFileLike cannot parse headers argument, so remove it.
        if (headers_it != url_function_args.end())
            url_function_args.erase(headers_it);

        ITableFunctionFileLike::parseArguments(ast_function, context);
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
        getHeaders(),
        configuration.http_method);
}

ReadWriteBufferFromHTTP::HTTPHeaderEntries TableFunctionURL::getHeaders() const
{
    ReadWriteBufferFromHTTP::HTTPHeaderEntries headers;
    for (const auto & [header, value] : configuration.headers)
    {
        auto value_literal = value.safeGet<String>();
        if (header == "Range")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Range headers are not allowed");
        headers.emplace_back(std::make_pair(header, value_literal));
    }
    return headers;
}

ColumnsDescription TableFunctionURL::getActualTableStructure(ContextPtr context) const
{
    if (structure == "auto")
        return StorageURL::getTableStructureFromData(format, filename, compression_method, getHeaders(), std::nullopt, context);

    return parseColumnsListFromString(structure, context);
}

void registerTableFunctionURL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionURL>();
}
}
