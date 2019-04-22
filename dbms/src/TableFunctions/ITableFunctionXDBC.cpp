#include <type_traits>
#include <ext/scope_guard.h>

#include <Core/Defines.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
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


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_EXCEPTION;
    extern const int LOGICAL_ERROR;
}

StoragePtr ITableFunctionXDBC::executeImpl(const ASTPtr & ast_function, const Context & context) const
{
    const auto & args_func = ast_function->as<ASTFunction &>();

    if (!args_func.arguments)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::LOGICAL_ERROR);

    ASTs & args = args_func.arguments->children;
    if (args.size() != 2 && args.size() != 3)
        throw Exception("Table function '" + getName() + "' requires 2 or 3 arguments: " + getName() + "('DSN', table) or " + getName()
                + "('DSN', schema, table)",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (auto i = 0u; i < args.size(); ++i)
        args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], context);

    std::string connection_string;
    std::string schema_name;
    std::string table_name;

    if (args.size() == 3)
    {
        connection_string = args[0]->as<ASTLiteral &>().value.safeGet<String>();
        schema_name = args[1]->as<ASTLiteral &>().value.safeGet<String>();
        table_name = args[2]->as<ASTLiteral &>().value.safeGet<String>();
    }
    else if (args.size() == 2)
    {
        connection_string = args[0]->as<ASTLiteral &>().value.safeGet<String>();
        table_name = args[1]->as<ASTLiteral &>().value.safeGet<String>();
    }

    /* Infer external table structure */
    /// Have to const_cast, because bridges store their commands inside context
    BridgeHelperPtr helper = createBridgeHelper(const_cast<Context &>(context), context.getSettingsRef().http_receive_timeout.value, connection_string);
    helper->startBridgeSync();

    Poco::URI columns_info_uri = helper->getColumnsInfoURI();
    columns_info_uri.addQueryParameter("connection_string", connection_string);
    if (!schema_name.empty())
        columns_info_uri.addQueryParameter("schema", schema_name);
    columns_info_uri.addQueryParameter("table", table_name);

    ReadWriteBufferFromHTTP buf(columns_info_uri, Poco::Net::HTTPRequest::HTTP_POST, nullptr);

    std::string columns_info;
    readStringBinary(columns_info, buf);
    NamesAndTypesList columns = NamesAndTypesList::parse(columns_info);

    auto result = std::make_shared<StorageXDBC>(table_name, schema_name, table_name, ColumnsDescription{columns}, context, helper);

    if (!result)
        throw Exception("Failed to instantiate storage from table function " + getName(), ErrorCodes::UNKNOWN_EXCEPTION);

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
