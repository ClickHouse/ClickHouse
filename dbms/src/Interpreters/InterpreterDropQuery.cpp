#include <Poco/File.h>

#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Storages/IStorage.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TABLE_WAS_NOT_DROPPED;
    extern const int DATABASE_NOT_EMPTY;
    extern const int UNKNOWN_DATABASE;
    extern const int READONLY;
}


InterpreterDropQuery::InterpreterDropQuery(const ASTPtr & query_ptr_, Context & context_) : query_ptr(query_ptr_), context(context_) {}


BlockIO InterpreterDropQuery::execute()
{
    ASTDropQuery & drop = typeid_cast<ASTDropQuery &>(*query_ptr);

    checkAccess(drop);

    if (!drop.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, context);

    String path = context.getPath();
    String current_database = context.getCurrentDatabase();

    bool drop_database = drop.table.empty() && !drop.database.empty();

    if (drop_database && drop.detach)
    {
        context.detachDatabase(drop.database);
        return {};
    }

    /// Drop temporary table.
    if (drop.database.empty() || drop.temporary)
    {
        StoragePtr table = (context.hasSessionContext() ? context.getSessionContext() : context).tryRemoveExternalTable(drop.table);
        if (table)
        {
            if (drop.database.empty() && !drop.temporary)
            {
                LOG_WARNING((&Logger::get("InterpreterDropQuery")),
                            "It is recommended to use `DROP TEMPORARY TABLE` to delete temporary tables");
            }
            table->shutdown();
            /// If table was already dropped by anyone, an exception will be thrown
            auto table_lock = table->lockForAlter(__PRETTY_FUNCTION__);
            /// Delete table data
            table->drop();
            table->is_dropped = true;
            return {};
        }
    }

    String database_name = drop.database.empty() ? current_database : drop.database;
    String database_name_escaped = escapeForFileName(database_name);

    String metadata_path = path + "metadata/" + database_name_escaped + "/";
    String database_metadata_path = path + "metadata/" + database_name_escaped + ".sql";

    auto database = context.tryGetDatabase(database_name);
    if (!database && !drop.if_exists)
        throw Exception("Database " + database_name + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);

    std::vector<std::pair<StoragePtr, std::unique_ptr<DDLGuard>>> tables_to_drop;

    if (!drop_database)
    {
        StoragePtr table;

        if (drop.if_exists)
            table = context.tryGetTable(database_name, drop.table);
        else
            table = context.getTable(database_name, drop.table);

        if (table)
            tables_to_drop.emplace_back(table,
                context.getDDLGuard(
                    database_name, drop.table, "Table " + database_name + "." + drop.table + " is dropping or detaching right now"));
        else
            return {};
    }
    else
    {
        if (!database)
        {
            if (!drop.if_exists)
                throw Exception("Database " + database_name + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
            return {};
        }

        for (auto iterator = database->getIterator(context); iterator->isValid(); iterator->next())
            tables_to_drop.emplace_back(iterator->table(),
                context.getDDLGuard(database_name,
                    iterator->name(),
                    "Table " + database_name + "." + iterator->name() + " is dropping or detaching right now"));
    }

    for (auto & table : tables_to_drop)
    {
        if (!drop.detach)
        {
            if (!table.first->checkTableCanBeDropped())
                throw Exception("Table " + database_name + "." + table.first->getTableName() + " couldn't be dropped due to failed pre-drop check",
                    ErrorCodes::TABLE_WAS_NOT_DROPPED);
        }

        table.first->shutdown();

        /// If table was already dropped by anyone, an exception will be thrown
        auto table_lock = table.first->lockForAlter(__PRETTY_FUNCTION__);

        String current_table_name = table.first->getTableName();

        if (drop.detach)
        {
            /// Drop table from memory, don't touch data and metadata
            database->detachTable(current_table_name);
        }
        else
        {
            /// Delete table metdata and table itself from memory
            database->removeTable(context, current_table_name);
            /// Delete table data
            table.first->drop();

            table.first->is_dropped = true;

            String database_data_path = database->getDataPath();

            /// If it is not virtual database like Dictionary then drop remaining data dir
            if (!database_data_path.empty())
            {
                String table_data_path = database_data_path + "/" + escapeForFileName(current_table_name);

                if (Poco::File(table_data_path).exists())
                    Poco::File(table_data_path).remove(true);
            }
        }
    }

    if (drop_database)
    {
        /// Delete the database. The tables in it have already been deleted.

        auto lock = context.getLock();

        /// Someone could have time to delete the database before us.
        context.assertDatabaseExists(database_name);

        /// Someone could have time to create a table in the database to be deleted while we deleted the tables without the context lock.
        if (!context.getDatabase(database_name)->empty(context))
            throw Exception("New table appeared in database being dropped. Try dropping it again.", ErrorCodes::DATABASE_NOT_EMPTY);

        /// Delete database information from the RAM
        auto database = context.detachDatabase(database_name);

        /// Delete the database.
        database->drop();

        /// Remove data directory if it is not virtual database. TODO: should IDatabase::drop() do that?
        String database_data_path = database->getDataPath();
        if (!database_data_path.empty())
            Poco::File(database_data_path).remove(false);

        Poco::File(metadata_path).remove(false);

        /// Old ClickHouse versions did not store database.sql files
        Poco::File database_metadata_file(database_metadata_path);
        if (database_metadata_file.exists())
            database_metadata_file.remove(false);
    }

    return {};
}


void InterpreterDropQuery::checkAccess(const ASTDropQuery & drop)
{
    const Settings & settings = context.getSettingsRef();
    auto readonly = settings.limits.readonly;

    /// It's allowed to drop temporary tables.
    if (!readonly || (drop.database.empty() && context.tryGetExternalTable(drop.table) && readonly >= 2))
    {
        return;
    }

    throw Exception("Cannot drop table in readonly mode", ErrorCodes::READONLY);
}

}
