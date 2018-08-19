#include <TableFunctions/TableFunctionODBC.h>
#include <type_traits>
#include <ext/scope_guard.h>

#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Poco/Net/HTTPRequest.h>
#include <Storages/StorageODBC.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Common/ODBCBridgeHelper.h>
#include <Core/Defines.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

StoragePtr TableFunctionODBC::executeImpl(const ASTPtr & ast_function, const Context & context) const
{
    const ASTFunction & args_func = typeid_cast<const ASTFunction &>(*ast_function);

    if (!args_func.arguments)
        throw Exception("Table function 'odbc' must have arguments.", ErrorCodes::LOGICAL_ERROR);

    ASTs & args = typeid_cast<ASTExpressionList &>(*args_func.arguments).children;

    if (args.size() != 2)
        throw Exception("Table function 'odbc' requires exactly 2 arguments: ODBC connection string and table name.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (int i = 0; i < 2; ++i)
        args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], context);

    std::string connection_string = static_cast<const ASTLiteral &>(*args[0]).value.safeGet<String>();
    std::string table_name = static_cast<const ASTLiteral &>(*args[1]).value.safeGet<String>();
    const auto & config = context.getConfigRef();
    ODBCBridgeHelper helper(config, context.getSettingsRef().http_receive_timeout.value, connection_string);
    helper.startODBCBridgeSync();

    Poco::URI columns_info_uri = helper.getColumnsInfoURI();
    columns_info_uri.addQueryParameter("connection_string", connection_string);
    columns_info_uri.addQueryParameter("table", table_name);

    ReadWriteBufferFromHTTP buf(columns_info_uri, Poco::Net::HTTPRequest::HTTP_POST, nullptr);

    std::string columns_info;
    readStringBinary(columns_info, buf);
    NamesAndTypesList columns = NamesAndTypesList::parse(columns_info);

    auto result = StorageODBC::create(table_name, connection_string, "", table_name, ColumnsDescription{columns}, context);
    result->startup();
    return result;
}


void registerTableFunctionODBC(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionODBC>();
}
}
