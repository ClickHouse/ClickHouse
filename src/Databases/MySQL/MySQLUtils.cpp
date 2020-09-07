#include <Databases/MySQL/MySQLUtils.h>

#include <memory>
#include <sstream>
#include <unordered_map>

#include <Columns/IColumn.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/quoteString.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/MySQLBlockInputStream.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/executeQuery.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB {

String checkVariableAndGetVersion(const mysqlxx::Pool::Entry & connection)
{
    Block variables_header{
        {std::make_shared<DataTypeString>(), "Variable_name"},
        {std::make_shared<DataTypeString>(), "Value"}
    };

    const String & check_query = "SHOW VARIABLES WHERE "
         "(Variable_name = 'log_bin' AND upper(Value) = 'ON') "
         "OR (Variable_name = 'binlog_format' AND upper(Value) = 'ROW') "
         "OR (Variable_name = 'binlog_row_image' AND upper(Value) = 'FULL') "
         "OR (Variable_name = 'default_authentication_plugin' AND upper(Value) = 'MYSQL_NATIVE_PASSWORD');";

    MySQLBlockInputStream variables_input(connection, check_query, variables_header, DEFAULT_BLOCK_SIZE);

    Block variables_block = variables_input.read();
    if (!variables_block || variables_block.rows() != 4)
    {
        std::unordered_map<String, String> variables_error_message{
            {"log_bin", "log_bin = 'ON'"},
            {"binlog_format", "binlog_format='ROW'"},
            {"binlog_row_image", "binlog_row_image='FULL'"},
            {"default_authentication_plugin", "default_authentication_plugin='mysql_native_password'"}
        };
        ColumnPtr variable_name_column = variables_block.getByName("Variable_name").column;

        for (size_t index = 0; index < variables_block.rows(); ++index)
        {
            const auto & error_message_it = variables_error_message.find(variable_name_column->getDataAt(index).toString());

            if (error_message_it != variables_error_message.end())
                variables_error_message.erase(error_message_it);
        }

        bool first = true;
        std::stringstream error_message;
        error_message << "Illegal MySQL variables, the MaterializeMySQL engine requires ";
        for (const auto & [variable_name, variable_error_message] : variables_error_message)
        {
            error_message << (first ? "" : ", ") << variable_error_message;

            if (first)
                first = false;
        }

        throw Exception(error_message.str(), ErrorCodes::ILLEGAL_MYSQL_VARIABLE);
    }

    Block version_header{{std::make_shared<DataTypeString>(), "version"}};
    MySQLBlockInputStream version_input(connection, "SELECT version() AS version;", version_header, DEFAULT_BLOCK_SIZE);

    Block version_block = version_input.read();
    if (!version_block || version_block.rows() != 1)
        throw Exception("LOGICAL ERROR: cannot get mysql version.", ErrorCodes::LOGICAL_ERROR);

    return version_block.getByPosition(0).column->getDataAt(0).toString();
}

Context createQueryContext(const Context & global_context)
{
    Settings new_query_settings = global_context.getSettings();
    new_query_settings.insert_allow_materialized_columns = true;

    Context query_context(global_context);
    query_context.setSettings(new_query_settings);
    CurrentThread::QueryScope query_scope(query_context);

    query_context.getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
    query_context.setCurrentQueryId(""); // generate random query_id
    return query_context;
}

BlockIO tryToExecuteQuery(const String & query_to_execute, Context & query_context, const String & database, const String & comment)
{
    try
    {
        if (!database.empty())
            query_context.setCurrentDatabase(database);

        return executeQuery("/*" + comment + "*/ " + query_to_execute, query_context, true);
    }
    catch (...)
    {
        tryLogCurrentException(
            &Poco::Logger::get("MaterializeMySQLSyncThread(" + database + ")"),
            "Query " + query_to_execute + " wasn't finished successfully");
        throw;
    }
}

DatabaseMaterializeMySQL & getDatabase(const String & database_name)
{
    DatabasePtr database = DatabaseCatalog::instance().getDatabase(database_name);

    if (DatabaseMaterializeMySQL * database_materialize = typeid_cast<DatabaseMaterializeMySQL *>(database.get()))
        return *database_materialize;

    throw Exception("LOGICAL_ERROR: cannot cast to DatabaseMaterializeMySQL, it is a bug.", ErrorCodes::LOGICAL_ERROR);
}

BlockOutputStreamPtr getTableOutput(const String & database_name, const String & table_name, Context & query_context, bool insert_materialized)
{
    const StoragePtr & storage = DatabaseCatalog::instance().getTable(StorageID(database_name, table_name), query_context);

    std::stringstream insert_columns_str;
    const StorageInMemoryMetadata & storage_metadata = storage->getInMemoryMetadata();
    const ColumnsDescription & storage_columns = storage_metadata.getColumns();
    const NamesAndTypesList & insert_columns_names = insert_materialized ? storage_columns.getAllPhysical() : storage_columns.getOrdinary();


    for (auto iterator = insert_columns_names.begin(); iterator != insert_columns_names.end(); ++iterator)
    {
        if (iterator != insert_columns_names.begin())
            insert_columns_str << ", ";

        insert_columns_str << backQuoteIfNeed(iterator->name);
    }


    String comment = "Materialize MySQL step 1: execute dump data";
    BlockIO res = tryToExecuteQuery("INSERT INTO " + backQuoteIfNeed(table_name) + "(" + insert_columns_str.str() + ")" + " VALUES",
        query_context, database_name, comment);

    if (!res.out)
        throw Exception("LOGICAL ERROR: It is a bug.", ErrorCodes::LOGICAL_ERROR);

    return res.out;
}

}
