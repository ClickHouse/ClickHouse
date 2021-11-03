#include <TableFunctions/TableFunctionURL.h>

#include "registerTableFunctions.h"
#include <Access/Common/AccessFlags.h>
#include <Poco/URI.h>
#include <Parsers/ASTFunction.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageURL.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Storages/StorageExternalDistributed.h>


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
        structure = configuration.structure;
        compression_method = configuration.compression_method;
    }
    else
    {
        ITableFunctionFileLike::parseArguments(ast_function, context);
    }
}


StoragePtr TableFunctionURL::getStorage(
    const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr global_context,
    const std::string & table_name, const String & compression_method_) const
{
    ReadWriteBufferFromHTTP::HTTPHeaderEntries headers;
    for (const auto & [header, value] : configuration.headers)
    {
        auto value_literal = value.safeGet<String>();
        headers.emplace_back(std::make_pair(header, value_literal));
    }

    return StorageURL::create(
        source,
        StorageID(getDatabaseName(), table_name),
        format_,
        std::nullopt /*format settings*/,
        columns,
        ConstraintsDescription{},
        String{},
        global_context,
        compression_method_,
        headers,
        configuration.http_method);
}

void registerTableFunctionURL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionURL>();
}
}
