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
#include <Databases/DatabaseAtomic.h>


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


InterpreterDropQuery::InterpreterDropQuery(const ASTPtr & query_ptr_, Context & context_) : query_ptr(query_ptr_), context(context_) {}


BlockIO InterpreterDropQuery::execute()
{
    auto & drop = query_ptr->as<ASTDropQuery &>();
    if (!drop.cluster.empty())
        return executeDDLQueryOnCluster(query_ptr, context, getRequiredAccessForDDLOnCluster());

    if (!drop.table.empty())
    {
        if (!drop.is_dictionary)
            return executeToTable({drop.database, drop.table, drop.uuid}, drop);
        else
            return executeToDictionary(drop.database, drop.table, drop.kind, drop.if_exists, drop.temporary, drop.no_ddl_lock);
    }
    else if (!drop.database.empty())
        return executeToDatabase(drop.database, drop.kind, drop.if_exists);
    else
        throw Exception("Nothing to drop, both names are empty", ErrorCodes::LOGICAL_ERROR);
}


BlockIO InterpreterDropQuery::executeToTable(
    const StorageID & table_id_,
    const ASTDropQuery & query)
{
    if (query.temporary || table_id_.database_name.empty())
    {
        if (context.tryResolveStorageID(table_id_, Context::ResolveExternal))
            return executeToTemporaryTable(table_id_.getTableName(), query.kind);
    }

    if (query.temporary)
    {
        if (query.if_exists)
            return {};
        throw Exception("Temporary table " + backQuoteIfNeed(table_id_.table_name) + " doesn't exist",
                        ErrorCodes::UNKNOWN_TABLE);
    }

    auto table_id = query.if_exists ? context.tryResolveStorageID(table_id_, Context::ResolveOrdinary)
                                    : context.resolveStorageID(table_id_, Context::ResolveOrdinary);
    if (!table_id)
        return {};

    auto ddl_guard = (!query.no_ddl_lock ? DatabaseCatalog::instance().getDDLGuard(table_id.database_name, table_id.table_name) : nullptr);

    /// If table was already dropped by anyone, an exception will be thrown
    auto [database, table] = query.if_exists ? DatabaseCatalog::instance().tryGetDatabaseAndTable(table_id, context)
                                             : DatabaseCatalog::instance().getDatabaseAndTable(table_id, context);

    if (database && table)
    {
        if (query_ptr->as<ASTDropQuery &>().is_view && !table->isView())
            throw Exception("Table " + table_id.getNameForLogs() + " is not a View", ErrorCodes::LOGICAL_ERROR);

        if (query.kind == ASTDropQuery::Kind::Detach)
        {
            context.checkAccess(table->isView() ? AccessType::DROP_VIEW : AccessType::DROP_TABLE, table_id);
            table->shutdown();
            TableExclusiveLockHolder table_lock;
            if (database->getEngineName() != "Atomic")
                table_lock = table->lockExclusively(context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);
            /// Drop table from memory, don't touch data and metadata
            database->detachTable(table_id.table_name);
        }
        else if (query.kind == ASTDropQuery::Kind::Truncate)
        {
            context.checkAccess(AccessType::TRUNCATE, table_id);
            table->checkTableCanBeDropped();

            auto table_lock = table->lockExclusively(context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);
            auto metadata_snapshot = table->getInMemoryMetadataPtr();
            /// Drop table data, don't touch metadata
            table->truncate(query_ptr, metadata_snapshot, context, table_lock);
        }
        else if (query.kind == ASTDropQuery::Kind::Drop)
        {
            context.checkAccess(table->isView() ? AccessType::DROP_VIEW : AccessType::DROP_TABLE, table_id);
            table->checkTableCanBeDropped();

            table->shutdown();

            TableExclusiveLockHolder table_lock;
            if (database->getEngineName() != "Atomic")
                table_lock = table->lockExclusively(context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);

            database->dropTable(context, table_id.table_name, query.no_delay);
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

    if (!database || !database->isDictionaryExist(dictionary_name))
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
        database->detachDictionary(dictionary_name);
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
            StoragePtr table = DatabaseCatalog::instance().getTable(resolved_id, context);
            if (kind == ASTDropQuery::Kind::Truncate)
            {
                auto table_lock = table->lockExclusively(context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);
                /// Drop table data, don't touch metadata
                auto metadata_snapshot = table->getInMemoryMetadataPtr();
                table->truncate(query_ptr, metadata_snapshot, context, table_lock);
            }
            else if (kind == ASTDropQuery::Kind::Drop)
            {
                context_handle.removeExternalTable(table_name);
                table->shutdown();
                auto table_lock = table->lockExclusively(context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);
                /// Delete table data
                table->drop();
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
            throw Exception("Unable to truncate database", ErrorCodes::SYNTAX_ERROR);
        }
        else if (kind == ASTDropQuery::Kind::Detach || kind == ASTDropQuery::Kind::Drop)
        {
            bool drop = kind == ASTDropQuery::Kind::Drop;
            context.checkAccess(AccessType::DROP_DATABASE, database_name);

            if (database->shouldBeEmptyOnDetach())
            {
                /// DETACH or DROP all tables and dictionaries inside database.
                /// First we should DETACH or DROP dictionaries because StorageDictionary
                /// must be detached only by detaching corresponding dictionary.
                for (auto iterator = database->getDictionariesIterator(); iterator->isValid(); iterator->next())
                {
                    String current_dictionary = iterator->name();
                    executeToDictionary(database_name, current_dictionary, kind, false, false, false);
                }

                ASTDropQuery query;
                query.kind = kind;
                query.database = database_name;
                for (auto iterator = database->getTablesIterator(context); iterator->isValid(); iterator->next())
                {
                    query.table = iterator->name();
                    executeToTable({query.database, query.table}, query);
                }
            }

            auto * database_atomic = typeid_cast<DatabaseAtomic *>(database.get());
            if (!drop && database_atomic)
                database_atomic->assertCanBeDetached(true);

            /// DETACH or DROP database itself
            DatabaseCatalog::instance().detachDatabase(database_name, drop, database->shouldBeEmptyOnDetach());
        }
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
            required_access.emplace_back(AccessType::TRUNCATE, drop.database, drop.table);
        else if (drop.kind == ASTDropQuery::Kind::Detach)
            required_access.emplace_back(AccessType::DROP_TABLE | AccessType::DROP_VIEW, drop.database, drop.table);
    }

    return required_access;
}

}
