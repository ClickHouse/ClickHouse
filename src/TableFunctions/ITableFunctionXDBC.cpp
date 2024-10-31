#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/parseQuery.h>
#include <Storages/StorageXDBC.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Poco/Net/HTTPRequest.h>
#include <Common/Exception.h>
#include "registerTableFunctions.h"

#include <Poco/Util/AbstractConfiguration.h>
#include <BridgeHelper/XDBCBridgeHelper.h>

#include "config.h"


namespace DB
{
namespace Setting
{
    extern const SettingsBool external_table_functions_use_nulls;
    extern const SettingsSeconds http_receive_timeout;
    extern const SettingsBool odbc_bridge_use_connection_pooling;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

namespace
{

/**
 * Base class for table functions, that works over external bridge
 * Xdbc (Xdbc connect string, table) - creates a temporary StorageXDBC.
 */
class ITableFunctionXDBC : public ITableFunction
{
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    /* A factory method to create bridge helper, that will assist in remote interaction */
    virtual BridgeHelperPtr createBridgeHelper(ContextPtr context,
        Poco::Timespan http_timeout_,
        const std::string & connection_string_,
        bool use_connection_pooling_) const = 0;

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    void startBridgeIfNot(ContextPtr context) const;

    String connection_string;
    String schema_name;
    String remote_table_name;
    mutable BridgeHelperPtr helper;
};

class TableFunctionJDBC : public ITableFunctionXDBC
{
public:
    static constexpr auto name = "jdbc";
    std::string getName() const override
    {
        return name;
    }

private:
    BridgeHelperPtr createBridgeHelper(ContextPtr context,
        Poco::Timespan http_timeout_,
        const std::string & connection_string_,
        bool use_connection_pooling_) const override
    {
        return std::make_shared<XDBCBridgeHelper<JDBCBridgeMixin>>(context, http_timeout_, connection_string_, use_connection_pooling_);
    }

    const char * getStorageTypeName() const override { return "JDBC"; }
};

class TableFunctionODBC : public ITableFunctionXDBC
{
public:
    static constexpr auto name = "odbc";
    std::string getName() const override
    {
        return name;
    }

private:
    BridgeHelperPtr createBridgeHelper(ContextPtr context,
        Poco::Timespan http_timeout_,
        const std::string & connection_string_,
        bool use_connection_pooling_) const override
    {
        return std::make_shared<XDBCBridgeHelper<ODBCBridgeMixin>>(context, http_timeout_, connection_string_, use_connection_pooling_);
    }

    const char * getStorageTypeName() const override { return "ODBC"; }
};


void ITableFunctionXDBC::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & args_func = ast_function->as<ASTFunction &>();

    if (!args_func.arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments.", getName());

    ASTs & args = args_func.arguments->children;
    if (args.size() != 2 && args.size() != 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{0}' requires 2 or 3 arguments: {0}('DSN', table) or {0}('DSN', schema, table)", getName());

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
        helper = createBridgeHelper(
            context,
            context->getSettingsRef()[Setting::http_receive_timeout].value,
            connection_string,
            context->getSettingsRef()[Setting::odbc_bridge_use_connection_pooling].value);
        helper->startBridgeSync();
    }
}

ColumnsDescription ITableFunctionXDBC::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    startBridgeIfNot(context);

    /// Infer external table structure.
    Poco::URI columns_info_uri = helper->getColumnsInfoURI();
    columns_info_uri.addQueryParameter("connection_string", connection_string);
    if (!schema_name.empty())
        columns_info_uri.addQueryParameter("schema", schema_name);
    columns_info_uri.addQueryParameter("table", remote_table_name);

    bool use_nulls = context->getSettingsRef()[Setting::external_table_functions_use_nulls];
    columns_info_uri.addQueryParameter("external_table_functions_use_nulls", toString(use_nulls));

    Poco::Net::HTTPBasicCredentials credentials{};
    auto buf = BuilderRWBufferFromHTTP(columns_info_uri)
                   .withConnectionGroup(HTTPConnectionGroupType::STORAGE)
                   .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
                   .withTimeouts(ConnectionTimeouts::getHTTPTimeouts(
                        context->getSettingsRef(),
                        context->getServerSettings()))
                   .create(credentials);

    std::string columns_info;
    readStringBinary(columns_info, *buf);
    NamesAndTypesList columns = NamesAndTypesList::parse(columns_info);

    return ColumnsDescription{columns};
}

StoragePtr ITableFunctionXDBC::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/, bool is_insert_query) const
{
    startBridgeIfNot(context);
    auto columns = getActualTableStructure(context, is_insert_query);
    auto result = std::make_shared<StorageXDBC>(
        StorageID(getDatabaseName(), table_name), schema_name, remote_table_name, columns, ConstraintsDescription{}, String{}, context, helper);
    result->startup();
    return result;
}

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
