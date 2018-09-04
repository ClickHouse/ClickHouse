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
    extern const int LOGICAL_ERROR;
    extern const int SYNTAX_ERROR;
    extern const int UNKNOWN_TABLE;
}


InterpreterDropQuery::InterpreterDropQuery(const ASTPtr & query_ptr_, Context & context_) : query_ptr(query_ptr_), context(context_) {}


BlockIO InterpreterDropQuery::execute()
{
    ASTDropQuery & drop = typeid_cast<ASTDropQuery &>(*query_ptr);

    checkAccess(drop);

    if (!drop.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, context, {drop.database});

    if (!drop.table.empty())
        return executeToTable(drop.database, drop.table, drop.kind, drop.if_exists, drop.temporary);
    else if (!drop.database.empty())
        return executeToDatabase(drop.database, drop.kind, drop.if_exists);
    else
        throw Exception("Database and table names is empty.", ErrorCodes::LOGICAL_ERROR);
}


BlockIO InterpreterDropQuery::executeToTable(String & database_name_, String & table_name, ASTDropQuery::Kind kind, bool if_exists, bool if_temporary)
{
    if (if_temporary || database_name_.empty())
    {
        auto & session_context = context.hasSessionContext() ? context.getSessionContext() : context;

        if (session_context.isExternalTableExist(table_name))
            return executeToTemporaryTable(table_name, kind);
    }

    String database_name = database_name_.empty() ? context.getCurrentDatabase() : database_name_;

    DatabaseAndTable database_and_table = tryGetDatabaseAndTable(database_name, table_name, if_exists);

    if (database_and_table.first && database_and_table.second)
    {
        auto ddl_guard = context.getDDLGuard(
            database_name, table_name, "Table " + database_name + "." + table_name + " is dropping or detaching right now");

        if (kind == ASTDropQuery::Kind::Detach)
        {
            database_and_table.second->shutdown();
            /// If table was already dropped by anyone, an exception will be thrown
            auto table_lock = database_and_table.second->lockForAlter(__PRETTY_FUNCTION__);
            /// Drop table from memory, don't touch data and metadata
            database_and_table.first->detachTable(database_and_table.second->getTableName());
        }
        else if (kind == ASTDropQuery::Kind::Truncate)
        {
            database_and_table.second->checkTableCanBeDropped();

            /// If table was already dropped by anyone, an exception will be thrown
            auto table_lock = database_and_table.second->lockForAlter(__PRETTY_FUNCTION__);
            /// Drop table data, don't touch metadata
            database_and_table.second->truncate(query_ptr);
        }
        else if (kind == ASTDropQuery::Kind::Drop)
        {
            database_and_table.second->checkTableCanBeDropped();

            database_and_table.second->shutdown();
            /// If table was already dropped by anyone, an exception will be thrown
            auto table_lock = database_and_table.second->lockForAlter(__PRETTY_FUNCTION__);
            /// Delete table metdata and table itself from memory
            database_and_table.first->removeTable(context, database_and_table.second->getTableName());
            /// Delete table data
            database_and_table.second->drop();
            database_and_table.second->is_dropped = true;

            String database_data_path = database_and_table.first->getDataPath();

            /// If it is not virtual database like Dictionary then drop remaining data dir
            if (!database_data_path.empty())
            {
                String table_data_path = database_data_path + "/" + escapeForFileName(database_and_table.second->getTableName());

                if (Poco::File(table_data_path).exists())
                    Poco::File(table_data_path).remove(true);
            }
        }
    }

    return {};
}

BlockIO InterpreterDropQuery::executeToTemporaryTable(String & table_name, ASTDropQuery::Kind kind)
{
    if (kind == ASTDropQuery::Kind::Detach)
        throw Exception("Unable to detach temporary table.", ErrorCodes::SYNTAX_ERROR);
    else
    {
        auto & context_handle = context.hasSessionContext() ? context.getSessionContext() : context;
        StoragePtr table = context_handle.tryGetExternalTable(table_name);
        if (table)
        {
            if (kind == ASTDropQuery::Kind::Truncate)
            {
                /// If table was already dropped by anyone, an exception will be thrown
                auto table_lock = table->lockForAlter(__PRETTY_FUNCTION__);
                /// Drop table data, don't touch metadata
                table->truncate(query_ptr);
            }
            else if (kind == ASTDropQuery::Kind::Drop)
            {
                context_handle.tryRemoveExternalTable(table_name);
                table->shutdown();
                /// If table was already dropped by anyone, an exception will be thrown
                auto table_lock = table->lockForAlter(__PRETTY_FUNCTION__);
                /// Delete table data
                table->drop();
                table->is_dropped = true;
            }
        }
    }

    return {};
}

BlockIO InterpreterDropQuery::executeToDatabase(String & database_name, ASTDropQuery::Kind kind, bool if_exists)
{
    if (auto database = tryGetDatabase(database_name, if_exists))
    {
        if (kind == ASTDropQuery::Kind::Truncate)
        {
            throw Exception("Unable to truncate database.", ErrorCodes::SYNTAX_ERROR);
        }
        else if (kind == ASTDropQuery::Kind::Detach)
        {
            context.detachDatabase(database_name);
            database->shutdown();
        }
        else if (kind == ASTDropQuery::Kind::Drop)
        {
            for (auto iterator = database->getIterator(context); iterator->isValid(); iterator->next())
            {
                String current_table_name = iterator->table()->getTableName();
                executeToTable(database_name, current_table_name, kind, false, false);
            }

            auto context_lock = context.getLock();

            /// Someone could have time to delete the database before us.
            context.assertDatabaseExists(database_name);

            /// Someone could have time to create a table in the database to be deleted while we deleted the tables without the context lock.
            if (!context.getDatabase(database_name)->empty(context))
                throw Exception("New table appeared in database being dropped. Try dropping it again.", ErrorCodes::DATABASE_NOT_EMPTY);

            /// Delete database information from the RAM
            context.detachDatabase(database_name);

            database->shutdown();

            /// Delete the database.
            database->drop();

            /// Old ClickHouse versions did not store database.sql files
            Poco::File database_metadata_file(context.getPath() + "metadata/" + escapeForFileName(database_name) + ".sql");
            if (database_metadata_file.exists())
                database_metadata_file.remove(false);
        }
    }

    return {};
}

DatabasePtr InterpreterDropQuery::tryGetDatabase(String & database_name, bool if_exists)
{
    return if_exists ? context.tryGetDatabase(database_name) : context.getDatabase(database_name);
}

DatabaseAndTable InterpreterDropQuery::tryGetDatabaseAndTable(String & database_name, String & table_name, bool if_exists)
{
    DatabasePtr database = tryGetDatabase(database_name, if_exists);

    if (database)
    {
        StoragePtr table = database->tryGetTable(context, table_name);
        if (!table && !if_exists)
            throw Exception("Table " + backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(table_name) + " doesn't exist.",
                            ErrorCodes::UNKNOWN_TABLE);

        return std::make_pair<DatabasePtr, StoragePtr>(std::move(database), std::move(table));
    }
    return {};
}

void InterpreterDropQuery::checkAccess(const ASTDropQuery & drop)
{
    const Settings & settings = context.getSettingsRef();
    auto readonly = settings.readonly;

    /// It's allowed to drop temporary tables.
    if (!readonly || (drop.database.empty() && context.tryGetExternalTable(drop.table) && readonly >= 2))
    {
        return;
    }

    throw Exception("Cannot drop table in readonly mode", ErrorCodes::READONLY);
}

}
