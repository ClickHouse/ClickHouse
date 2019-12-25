#include <Poco/File.h>

#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Parsers/ASTDropQuery.h>
#include <Storages/IStorage.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
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
    extern const int QUERY_IS_PROHIBITED;
    extern const int UNKNOWN_DICTIONARY;
}


InterpreterDropQuery::InterpreterDropQuery(const ASTPtr & query_ptr_, Context & context_) : query_ptr(query_ptr_), context(context_) {}


BlockIO InterpreterDropQuery::execute()
{
    auto & drop = query_ptr->as<ASTDropQuery &>();

    checkAccess(drop);

    if (!drop.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, context, {drop.database});

    if (!drop.table.empty())
    {
        if (!drop.is_dictionary)
            return executeToTable(drop.database, drop.table, drop.kind, drop.if_exists, drop.temporary, drop.no_ddl_lock);
        else
            return executeToDictionary(drop.database, drop.table, drop.kind, drop.if_exists, drop.temporary, drop.no_ddl_lock);
    }
    else if (!drop.database.empty())
        return executeToDatabase(drop.database, drop.kind, drop.if_exists);
    else
        throw Exception("Nothing to drop, both names are empty.", ErrorCodes::LOGICAL_ERROR);
}


BlockIO InterpreterDropQuery::executeToTable(
    String & database_name_,
    String & table_name,
    ASTDropQuery::Kind kind,
    bool if_exists,
    bool if_temporary,
    bool no_ddl_lock)
{
    if (if_temporary || database_name_.empty())
    {
        auto & session_context = context.hasSessionContext() ? context.getSessionContext() : context;

        if (session_context.isExternalTableExist(table_name))
            return executeToTemporaryTable(table_name, kind);
    }

    String database_name = database_name_.empty() ? context.getCurrentDatabase() : database_name_;

    auto ddl_guard = (!no_ddl_lock ? context.getDDLGuard(database_name, table_name) : nullptr);

    DatabaseAndTable database_and_table = tryGetDatabaseAndTable(database_name, table_name, if_exists);

    if (database_and_table.first && database_and_table.second)
    {
        if (kind == ASTDropQuery::Kind::Detach)
        {
            database_and_table.second->shutdown();
            /// If table was already dropped by anyone, an exception will be thrown
            auto table_lock = database_and_table.second->lockExclusively(context.getCurrentQueryId());
            /// Drop table from memory, don't touch data and metadata
            database_and_table.first->detachTable(database_and_table.second->getTableName());
        }
        else if (kind == ASTDropQuery::Kind::Truncate)
        {
            database_and_table.second->checkTableCanBeDropped();

            /// If table was already dropped by anyone, an exception will be thrown
            auto table_lock = database_and_table.second->lockExclusively(context.getCurrentQueryId());
            /// Drop table data, don't touch metadata
            database_and_table.second->truncate(query_ptr, context, table_lock);
        }
        else if (kind == ASTDropQuery::Kind::Drop)
        {
            database_and_table.second->checkTableCanBeDropped();

            database_and_table.second->shutdown();
            /// If table was already dropped by anyone, an exception will be thrown

            auto table_lock = database_and_table.second->lockExclusively(context.getCurrentQueryId());

            const std::string metadata_file_without_extension =
                database_and_table.first->getMetadataPath()
                + escapeForFileName(database_and_table.second->getTableName());

            const auto prev_metadata_name = metadata_file_without_extension + ".sql";
            const auto drop_metadata_name = metadata_file_without_extension + ".sql.tmp_drop";

            /// Try to rename metadata file and delete the data
            try
            {
                /// There some kind of tables that have no metadata - ignore renaming
                if (Poco::File(prev_metadata_name).exists())
                    Poco::File(prev_metadata_name).renameTo(drop_metadata_name);
                /// Delete table data
                database_and_table.second->drop(table_lock);
            }
            catch (...)
            {
                if (Poco::File(drop_metadata_name).exists())
                    Poco::File(drop_metadata_name).renameTo(prev_metadata_name);
                throw;
            }

            /// Delete table metadata and table itself from memory
            database_and_table.first->removeTable(context, database_and_table.second->getTableName());
            database_and_table.second->is_dropped = true;

            String database_data_path = database_and_table.first->getDataPath();

            /// If it is not virtual database like Dictionary then drop remaining data dir
            if (!database_data_path.empty())
            {
                String table_data_path = context.getPath() + database_data_path + "/" + escapeForFileName(table_name);

                if (Poco::File(table_data_path).exists())
                    Poco::File(table_data_path).remove(true);
            }
        }
    }

    return {};
}


BlockIO InterpreterDropQuery::executeToDictionary(
    String & database_name_,
    String & dictionary_name,
    ASTDropQuery::Kind kind,
    bool if_exists,
    bool is_temporary,
    bool no_ddl_lock)
{
    if (is_temporary)
        throw Exception("Temporary dictionaries are not possible.", ErrorCodes::SYNTAX_ERROR);

    String database_name = database_name_.empty() ? context.getCurrentDatabase() : database_name_;

    auto ddl_guard = (!no_ddl_lock ? context.getDDLGuard(database_name, dictionary_name) : nullptr);

    DatabasePtr database = tryGetDatabase(database_name, if_exists);

    if (!database || !database->isDictionaryExist(context, dictionary_name))
    {
        if (!if_exists)
            throw Exception(
                "Dictionary " + backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(dictionary_name) + " doesn't exist.",
                ErrorCodes::UNKNOWN_DICTIONARY);
        else
            return {};
    }

    if (kind == ASTDropQuery::Kind::Detach)
    {
        /// Drop dictionary from memory, don't touch data and metadata
        database->detachDictionary(dictionary_name, context);
    }
    else if (kind == ASTDropQuery::Kind::Truncate)
    {
        throw Exception("Cannot TRUNCATE dictionary", ErrorCodes::SYNTAX_ERROR);
    }
    else if (kind == ASTDropQuery::Kind::Drop)
    {
        database->removeDictionary(context, dictionary_name);
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
                auto table_lock = table->lockExclusively(context.getCurrentQueryId());
                /// Drop table data, don't touch metadata
                table->truncate(query_ptr, context, table_lock);
            }
            else if (kind == ASTDropQuery::Kind::Drop)
            {
                context_handle.tryRemoveExternalTable(table_name);
                table->shutdown();
                /// If table was already dropped by anyone, an exception will be thrown
                auto table_lock = table->lockExclusively(context.getCurrentQueryId());
                /// Delete table data
                table->drop(table_lock);
                table->is_dropped = true;
            }
        }
    }

    return {};
}

BlockIO InterpreterDropQuery::executeToDatabase(String & database_name, ASTDropQuery::Kind kind, bool if_exists)
{
    auto ddl_guard = context.getDDLGuard(database_name, "");

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
            for (auto iterator = database->getTablesIterator(context); iterator->isValid(); iterator->next())
            {
                String current_table_name = iterator->name();
                executeToTable(database_name, current_table_name, kind, false, false, false);
            }

            for (auto iterator = database->getDictionariesIterator(context); iterator->isValid(); iterator->next())
            {
                String current_dictionary = iterator->name();
                executeToDictionary(database_name, current_dictionary, kind, false, false, false);
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
            database->drop(context);

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

        return {std::move(database), std::move(table)};
    }
    return {};
}

void InterpreterDropQuery::checkAccess(const ASTDropQuery & drop)
{
    const Settings & settings = context.getSettingsRef();
    auto readonly = settings.readonly;
    bool allow_ddl = settings.allow_ddl;

    /// It's allowed to drop temporary tables.
    if ((!readonly && allow_ddl) || (drop.database.empty() && context.tryGetExternalTable(drop.table) && readonly >= 2))
        return;

    if (readonly)
        throw Exception("Cannot drop table in readonly mode", ErrorCodes::READONLY);

    throw Exception("Cannot drop table. DDL queries are prohibited for the user", ErrorCodes::QUERY_IS_PROHIBITED);
}

}
