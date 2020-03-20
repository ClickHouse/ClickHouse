#include <Poco/File.h>

#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Access/AccessRightsElement.h>
#include <Parsers/ASTDropQuery.h>
#include <Storages/IStorage.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SYNTAX_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int UNKNOWN_DICTIONARY;
}


static DatabasePtr tryGetDatabase(const String & database_name, bool if_exists)
{
    return if_exists ? DatabaseCatalog::instance().tryGetDatabase(database_name) : DatabaseCatalog::instance().getDatabase(database_name);
}


InterpreterDropQuery::InterpreterDropQuery(ASTPtr query_ptr_, Context & context_) : query_ptr(std::move(query_ptr_)), context(context_) {}


BlockIO InterpreterDropQuery::execute()
{
    auto & drop = query_ptr->as<ASTDropQuery &>();
    if (!drop.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, context, getRequiredAccessForDDLOnCluster());

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
    const String & database_name_,
    const String & table_name,
    ASTDropQuery::Kind kind,
    bool if_exists,
    bool is_temporary,
    bool no_ddl_lock)
{
    if (is_temporary || database_name_.empty())
    {
        if (context.tryResolveStorageID({"", table_name}, Context::ResolveExternal))
            return executeToTemporaryTable(table_name, kind);
    }

    if (is_temporary)
    {
        if (if_exists)
            return {};
        throw Exception("Temporary table " + backQuoteIfNeed(table_name) + " doesn't exist.",
                        ErrorCodes::UNKNOWN_TABLE);
    }

    String database_name = context.resolveDatabase(database_name_);

    auto ddl_guard = (!no_ddl_lock ? DatabaseCatalog::instance().getDDLGuard(database_name, table_name) : nullptr);

    auto [database, table] = tryGetDatabaseAndTable(database_name, table_name, if_exists);

    if (database && table)
    {
        auto table_id = table->getStorageID();
        if (kind == ASTDropQuery::Kind::Detach)
        {
            context.checkAccess(table->isView() ? AccessType::DROP_VIEW : AccessType::DROP_TABLE, table_id);
            table->shutdown();
            /// If table was already dropped by anyone, an exception will be thrown
            auto table_lock = table->lockExclusively(context.getCurrentQueryId());
            /// Drop table from memory, don't touch data and metadata
            database->detachTable(table_name);
        }
        else if (kind == ASTDropQuery::Kind::Truncate)
        {
            context.checkAccess(table->isView() ? AccessType::TRUNCATE_VIEW : AccessType::TRUNCATE_TABLE, table_id);
            table->checkTableCanBeDropped();

            /// If table was already dropped by anyone, an exception will be thrown
            auto table_lock = table->lockExclusively(context.getCurrentQueryId());
            /// Drop table data, don't touch metadata
            table->truncate(query_ptr, context, table_lock);
        }
        else if (kind == ASTDropQuery::Kind::Drop)
        {
            context.checkAccess(table->isView() ? AccessType::DROP_VIEW : AccessType::DROP_TABLE, table_id);
            table->checkTableCanBeDropped();

            table->shutdown();
            /// If table was already dropped by anyone, an exception will be thrown

            auto table_lock = table->lockExclusively(context.getCurrentQueryId());

            const std::string metadata_file_without_extension = database->getMetadataPath() + escapeForFileName(table_id.table_name);
            const auto prev_metadata_name = metadata_file_without_extension + ".sql";
            const auto drop_metadata_name = metadata_file_without_extension + ".sql.tmp_drop";

            /// Try to rename metadata file and delete the data
            try
            {
                /// There some kind of tables that have no metadata - ignore renaming
                if (Poco::File(prev_metadata_name).exists())
                    Poco::File(prev_metadata_name).renameTo(drop_metadata_name);
                /// Delete table data
                table->drop(table_lock);
            }
            catch (...)
            {
                if (Poco::File(drop_metadata_name).exists())
                    Poco::File(drop_metadata_name).renameTo(prev_metadata_name);
                throw;
            }

            String table_data_path_relative = database->getTableDataPath(table_name);

            /// Delete table metadata and table itself from memory
            database->removeTable(context, table_name);
            table->is_dropped = true;

            /// If it is not virtual database like Dictionary then drop remaining data dir
            if (!table_data_path_relative.empty())
            {
                String table_data_path = context.getPath() + table_data_path_relative;
                if (Poco::File(table_data_path).exists())
                    Poco::File(table_data_path).remove(true);
            }
        }
    }

    return {};
}


BlockIO InterpreterDropQuery::executeToDictionary(
    const String & database_name_,
    const String & dictionary_name,
    ASTDropQuery::Kind kind,
    bool if_exists,
    bool is_temporary,
    bool no_ddl_lock)
{
    if (is_temporary)
        throw Exception("Temporary dictionaries are not possible.", ErrorCodes::SYNTAX_ERROR);

    String database_name = context.resolveDatabase(database_name_);

    auto ddl_guard = (!no_ddl_lock ? DatabaseCatalog::instance().getDDLGuard(database_name, dictionary_name) : nullptr);

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
        context.checkAccess(AccessType::DROP_DICTIONARY, database_name, dictionary_name);
        database->detachDictionary(dictionary_name, context);
    }
    else if (kind == ASTDropQuery::Kind::Truncate)
    {
        throw Exception("Cannot TRUNCATE dictionary", ErrorCodes::SYNTAX_ERROR);
    }
    else if (kind == ASTDropQuery::Kind::Drop)
    {
        context.checkAccess(AccessType::DROP_DICTIONARY, database_name, dictionary_name);
        database->removeDictionary(context, dictionary_name);
    }
    return {};
}

BlockIO InterpreterDropQuery::executeToTemporaryTable(const String & table_name, ASTDropQuery::Kind kind)
{
    if (kind == ASTDropQuery::Kind::Detach)
        throw Exception("Unable to detach temporary table.", ErrorCodes::SYNTAX_ERROR);
    else
    {
        auto & context_handle = context.hasSessionContext() ? context.getSessionContext() : context;
        auto resolved_id = context_handle.tryResolveStorageID(StorageID("", table_name), Context::ResolveExternal);
        if (resolved_id)
        {
            StoragePtr table = DatabaseCatalog::instance().getTable(resolved_id);
            if (kind == ASTDropQuery::Kind::Truncate)
            {
                /// If table was already dropped by anyone, an exception will be thrown
                auto table_lock = table->lockExclusively(context.getCurrentQueryId());
                /// Drop table data, don't touch metadata
                table->truncate(query_ptr, context, table_lock);
            }
            else if (kind == ASTDropQuery::Kind::Drop)
            {
                context_handle.removeExternalTable(table_name);
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


BlockIO InterpreterDropQuery::executeToDatabase(const String & database_name, ASTDropQuery::Kind kind, bool if_exists)
{
    auto ddl_guard = DatabaseCatalog::instance().getDDLGuard(database_name, "");

    if (auto database = tryGetDatabase(database_name, if_exists))
    {
        if (kind == ASTDropQuery::Kind::Truncate)
        {
            throw Exception("Unable to truncate database.", ErrorCodes::SYNTAX_ERROR);
        }
        else if (kind == ASTDropQuery::Kind::Detach || kind == ASTDropQuery::Kind::Drop)
        {
            bool drop = kind == ASTDropQuery::Kind::Drop;
            context.checkAccess(AccessType::DROP_DATABASE, database_name);

            /// DETACH or DROP all tables and dictionaries inside database
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

            /// DETACH or DROP database itself
            DatabaseCatalog::instance().detachDatabase(database_name, drop);
        }
    }

    return {};
}


DatabaseAndTable InterpreterDropQuery::tryGetDatabaseAndTable(const String & database_name, const String & table_name, bool if_exists)
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


AccessRightsElements InterpreterDropQuery::getRequiredAccessForDDLOnCluster() const
{
    AccessRightsElements required_access;
    const auto & drop = query_ptr->as<const ASTDropQuery &>();

    if (drop.table.empty())
    {
        if (drop.kind == ASTDropQuery::Kind::Detach)
            required_access.emplace_back(AccessType::DROP_DATABASE, drop.database);
        else if (drop.kind == ASTDropQuery::Kind::Drop)
            required_access.emplace_back(AccessType::DROP_DATABASE, drop.database);
    }
    else if (drop.is_dictionary)
    {
        if (drop.kind == ASTDropQuery::Kind::Detach)
            required_access.emplace_back(AccessType::DROP_DICTIONARY, drop.database, drop.table);
        else if (drop.kind == ASTDropQuery::Kind::Drop)
            required_access.emplace_back(AccessType::DROP_DICTIONARY, drop.database, drop.table);
    }
    else if (!drop.temporary)
    {
        /// It can be view or table.
        if (drop.kind == ASTDropQuery::Kind::Drop)
            required_access.emplace_back(AccessType::DROP_TABLE | AccessType::DROP_VIEW, drop.database, drop.table);
        else if (drop.kind == ASTDropQuery::Kind::Truncate)
            required_access.emplace_back(AccessType::TRUNCATE_TABLE | AccessType::TRUNCATE_VIEW, drop.database, drop.table);
        else if (drop.kind == ASTDropQuery::Kind::Detach)
            required_access.emplace_back(AccessType::DROP_TABLE | AccessType::DROP_VIEW, drop.database, drop.table);
    }

    return required_access;
}

}
