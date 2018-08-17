#include <Dictionaries/ODBCBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageODBC.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Poco/Ext/SessionPoolHelpers.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <common/logger_useful.h>

#include <IO/ReadHelpers.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Poco/File.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Path.h>
#include <Common/ShellCommand.h>
#include <ext/range.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int EXTERNAL_EXECUTABLE_NOT_FOUND;
    extern const int EXTERNAL_SERVER_IS_NOT_RESPONDING;
}


StorageODBC::StorageODBC(const std::string & table_name_,
    const std::string & connection_string,
    const std::string & remote_database_name_,
    const std::string & remote_table_name_,
    const ColumnsDescription & columns_,
    const Context & context_)
    : IStorageURLBase(Poco::URI(), context_, table_name_, ODBCBridgeHelper::DEFAULT_FORMAT, columns_)
    , odbc_bridge_helper(context_global.getConfigRef(), context_global.getSettingsRef().http_receive_timeout.value, connection_string)
    , remote_database_name(remote_database_name_)
    , remote_table_name(remote_table_name_)
    , log(&Poco::Logger::get("StorageODBC"))
{
    const auto & config = context_global.getConfigRef();
    size_t bridge_port = config.getUInt("odbc_bridge.port", ODBCBridgeHelper::DEFAULT_PORT);
    std::string bridge_host = config.getString("odbc_bridge.host", ODBCBridgeHelper::DEFAULT_HOST);

    uri.setHost(bridge_host);
    uri.setPort(bridge_port);
    uri.setScheme("http");
}

std::string StorageODBC::getReadMethod() const
{
    return Poco::Net::HTTPRequest::HTTP_POST;
}

std::vector<std::pair<std::string, std::string>> StorageODBC::getReadURIParams(const Names & column_names,
    const SelectQueryInfo & /*query_info*/,
    const Context & /*context*/,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t max_block_size) const
{
    NamesAndTypesList cols;
    for (const String & name : column_names)
    {
        auto column_data = getColumn(name);
        cols.emplace_back(column_data.name, column_data.type);
    }
    return odbc_bridge_helper.getURLParams(cols.toString(), max_block_size);
}

std::function<void(std::ostream &)> StorageODBC::getReadPOSTDataCallback(const Names & /*column_names*/,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t /*max_block_size*/) const
{
    String query = transformQueryForExternalDatabase(
        *query_info.query, getColumns().ordinary, IdentifierQuotingStyle::DoubleQuotes, remote_database_name, remote_table_name, context);

    return [query](std::ostream & os) { os << "query=" << query; };
}

BlockInputStreams StorageODBC::read(const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{

    odbc_bridge_helper.startODBCBridgeSync();
    return IStorageURLBase::read(column_names, query_info, context, processed_stage, max_block_size, num_streams);
}


void registerStorageODBC(StorageFactory & factory)
{
    factory.registerStorage("ODBC", [](const StorageFactory::Arguments & args) {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 3)
            throw Exception(
                "Storage ODBC requires exactly 3 parameters: ODBC('DSN', database, table).", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (size_t i = 0; i < 2; ++i)
            engine_args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[i], args.local_context);

        return StorageODBC::create(args.table_name,
            static_cast<const ASTLiteral &>(*engine_args[0]).value.safeGet<String>(),
            static_cast<const ASTLiteral &>(*engine_args[1]).value.safeGet<String>(),
            static_cast<const ASTLiteral &>(*engine_args[2]).value.safeGet<String>(),
            args.columns,
            args.context);
    });
}
}
