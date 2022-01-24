#include <type_traits>
#include <base/scope_guard.h>

#include <Core/Defines.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/ConnectionTimeoutsContext.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/parseQuery.h>
#include <Storages/StorageXDBC.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/ITableFunctionXDBC.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Poco/Net/HTTPRequest.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Poco/NumberFormatter.h>
#include "registerTableFunctions.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

void ITableFunctionXDBC::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & args_func = ast_function->as<ASTFunction &>();

    if (!args_func.arguments)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::LOGICAL_ERROR);

    ASTs & args = args_func.arguments->children;
    if (args.size() != 2 && args.size() != 3)
        throw Exception("Table function '" + getName() + "' requires 2 or 3 arguments: " + getName() + "('DSN', table) or " + getName()
                + "('DSN', schema, table)",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    if (args.size() == 3)
    {
        connection_string = args[0]->as<ASTLiteral &>().value.safeGet<String>();
        schema_name = args[1]->as<ASTLiteral &>().value.safeGet<String>();
        remote_table_name = args[2]->as<ASTLiteral &>().value.safeGet<String>();
    }
    else if (args.size() == 2)
    {
        connection_string = args[0]->as<ASTLiteral &>().value.safeGet<String>();
        remote_table_name = args[1]->as<ASTLiteral &>().value.safeGet<String>();
    }
}

void ITableFunctionXDBC::startBridgeIfNot(ContextPtr context) const
{
    if (!helper)
    {
        /// Have to const_cast, because bridges store their commands inside context
        helper = createBridgeHelper(context, context->getSettingsRef().http_receive_timeout.value, connection_string);
        helper->startBridgeSync();
    }
}

ColumnsDescription ITableFunctionXDBC::getActualTableStructure(ContextPtr context) const
{
    startBridgeIfNot(context);

    /* Infer external table structure */
    Poco::URI columns_info_uri = helper->getColumnsInfoURI();
    columns_info_uri.addQueryParameter("connection_string", connection_string);
    if (!schema_name.empty())
        columns_info_uri.addQueryParameter("schema", schema_name);
    columns_info_uri.addQueryParameter("table", remote_table_name);

    const auto use_nulls = context->getSettingsRef().external_table_functions_use_nulls;
    columns_info_uri.addQueryParameter("external_table_functions_use_nulls",
                                       Poco::NumberFormatter::format(use_nulls));

    Poco::Net::HTTPBasicCredentials credentials{};
    ReadWriteBufferFromHTTP buf(columns_info_uri, Poco::Net::HTTPRequest::HTTP_POST, {}, ConnectionTimeouts::getHTTPTimeouts(context), credentials);

    std::string columns_info;
    readStringBinary(columns_info, buf);
    NamesAndTypesList columns = NamesAndTypesList::parse(columns_info);

    return ColumnsDescription{columns};
}

StoragePtr ITableFunctionXDBC::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    startBridgeIfNot(context);
    auto columns = getActualTableStructure(context);
    auto result = std::make_shared<StorageXDBC>(
        StorageID(getDatabaseName(), table_name), schema_name, remote_table_name, columns, String{}, context, helper);
    result->startup();
    return result;
}

void registerTableFunctionJDBC(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionJDBC>();
}

void registerTableFunctionODBC(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionODBC>();
}
}
