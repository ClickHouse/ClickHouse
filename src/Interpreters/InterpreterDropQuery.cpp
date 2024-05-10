#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/QueryLog.h>
#include <Access/Common/AccessRightsElement.h>
#include <Parsers/ASTDropQuery.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Databases/DatabaseReplicated.h>

#include "config.h"

#if USE_MYSQL
#   include <Databases/MySQL/DatabaseMaterializedMySQL.h>
#endif

#if USE_LIBPQXX
#   include <Databases/PostgreSQL/DatabaseMaterializedPostgreSQL.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SYNTAX_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_QUERY;
    extern const int TABLE_IS_READ_ONLY;
}

namespace ActionLocks
{
    extern const StorageActionBlockType PartsMerge;
}

static DatabasePtr tryGetDatabase(const String & database_name, bool if_exists)
{
    return if_exists ? DatabaseCatalog::instance().tryGetDatabase(database_name) : DatabaseCatalog::instance().getDatabase(database_name);
}


InterpreterDropQuery::InterpreterDropQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_)
{
}


BlockIO InterpreterDropQuery::execute()
{
    auto & drop = query_ptr->as<ASTDropQuery &>();
    if (!drop.cluster.empty() && !maybeRemoveOnCluster(query_ptr, getContext()))
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = getRequiredAccessForDDLOnCluster();
        return executeDDLQueryOnCluster(query_ptr, getContext(), params);
    }

    if (getContext()->getSettingsRef().database_atomic_wait_for_drop_and_detach_synchronously)
        drop.sync = true;

    if (drop.table)
        return executeToTable(drop);
    else if (drop.database)
        return executeToDatabase(drop);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Nothing to drop, both names are empty");
}


void InterpreterDropQuery::waitForTableToBeActuallyDroppedOrDetached(const ASTDropQuery & query, const DatabasePtr & db, const UUID & uuid_to_wait)
{
    if (uuid_to_wait == UUIDHelpers::Nil)
        return;

    if (query.kind == ASTDropQuery::Kind::Drop)
        DatabaseCatalog::instance().waitTableFinallyDropped(uuid_to_wait);
    else if (query.kind == ASTDropQuery::Kind::Detach)
        db->waitDetachedTableNotInUse(uuid_to_wait);
}

BlockIO InterpreterDropQuery::executeToTable(ASTDropQuery & query)
{
    DatabasePtr database;
    UUID table_to_wait_on = UUIDHelpers::Nil;
    auto res = executeToTableImpl(getContext(), query, database, table_to_wait_on);
    if (query.sync)
        waitForTableToBeActuallyDroppedOrDetached(query, database, table_to_wait_on);
    return res;
}

BlockIO InterpreterDropQuery::executeToTableImpl(ContextPtr context_, ASTDropQuery & query, DatabasePtr & db, UUID & uuid_to_wait)
{
    /// NOTE: it does not contain UUID, we will resolve it with locked DDLGuard
    auto table_id = StorageID(query);
    if (query.temporary || table_id.database_name.empty())
    {
        if (context_->tryResolveStorageID(table_id, Context::ResolveExternal))
            return executeToTemporaryTable(table_id.getTableName(), query.kind);
        else
            query.setDatabase(table_id.database_name = context_->getCurrentDatabase());
    }

    if (query.temporary)
    {
        if (query.if_exists)
            return {};
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Temporary table {} doesn't exist", backQuoteIfNeed(table_id.table_name));
    }

    auto ddl_guard = (!query.no_ddl_lock ? DatabaseCatalog::instance().getDDLGuard(table_id.database_name, table_id.table_name) : nullptr);

    /// If table was already dropped by anyone, an exception will be thrown
    auto [database, table] = query.if_exists ? DatabaseCatalog::instance().tryGetDatabaseAndTable(table_id, context_)
                                             : DatabaseCatalog::instance().getDatabaseAndTable(table_id, context_);

    if (database && table)
    {
        checkStorageSupportsTransactionsIfNeeded(table, context_);

        auto & ast_drop_query = query.as<ASTDropQuery &>();

        if (ast_drop_query.is_view && !table->isView())
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Table {} is not a View",
                table_id.getNameForLogs());

        if (ast_drop_query.is_dictionary && !table->isDictionary())
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Table {} is not a Dictionary",
                table_id.getNameForLogs());

        /// Now get UUID, so we can wait for table data to be finally dropped
        table_id.uuid = database->tryGetTableUUID(table_id.table_name);

        /// Prevents recursive drop from drop database query. The original query must specify a table.
        bool is_drop_or_detach_database = !query_ptr->as<ASTDropQuery>()->table;

        AccessFlags drop_storage;

        if (table->isView())
            drop_storage = AccessType::DROP_VIEW;
        else if (table->isDictionary())
            drop_storage = AccessType::DROP_DICTIONARY;
        else
            drop_storage = AccessType::DROP_TABLE;

        if (database->shouldReplicateQuery(getContext(), query_ptr))
        {
            if (query.kind == ASTDropQuery::Kind::Detach)
                context_->checkAccess(drop_storage, table_id);
            else if (query.kind == ASTDropQuery::Kind::Truncate)
                context_->checkAccess(AccessType::TRUNCATE, table_id);
            else if (query.kind == ASTDropQuery::Kind::Drop)
                context_->checkAccess(drop_storage, table_id);

            ddl_guard->releaseTableLock();
            table.reset();
            return database->tryEnqueueReplicatedDDL(query.clone(), context_);
        }

        if (query.kind == ASTDropQuery::Kind::Detach)
        {
            context_->checkAccess(drop_storage, table_id);

            if (table->isDictionary())
            {
                /// If DROP DICTIONARY query is not used, check if Dictionary can be dropped with DROP TABLE query
                if (!query.is_dictionary)
                    table->checkTableCanBeDetached();
            }
            else
                table->checkTableCanBeDetached();

            table->flushAndShutdown();
            TableExclusiveLockHolder table_lock;

            if (database->getUUID() == UUIDHelpers::Nil)
                table_lock = table->lockExclusively(context_->getCurrentQueryId(), context_->getSettingsRef().lock_acquire_timeout);

            if (query.permanently)
            {
                /// Server may fail to restart of DETACH PERMANENTLY if table has dependent ones
                bool check_ref_deps = getContext()->getSettingsRef().check_referential_table_dependencies;
                bool check_loading_deps = !check_ref_deps && getContext()->getSettingsRef().check_table_dependencies;
                DatabaseCatalog::instance().removeDependencies(table_id, check_ref_deps, check_loading_deps, is_drop_or_detach_database);
                /// Drop table from memory, don't touch data, metadata file renamed and will be skipped during server restart
                database->detachTablePermanently(context_, table_id.table_name);
            }
            else
            {
                /// Drop table from memory, don't touch data and metadata
                database->detachTable(context_, table_id.table_name);
            }
        }
        else if (query.kind == ASTDropQuery::Kind::Truncate)
        {
            if (table->isDictionary())
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Cannot TRUNCATE dictionary");

            context_->checkAccess(AccessType::TRUNCATE, table_id);
            if (table->isStaticStorage())
                throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Table is read-only");

            table->checkTableCanBeDropped(context_);

            TableExclusiveLockHolder table_excl_lock;
            /// We don't need any lock for ReplicatedMergeTree and for simple MergeTree
            /// For the rest of tables types exclusive lock is needed
            if (!std::dynamic_pointer_cast<MergeTreeData>(table))
                table_excl_lock = table->lockExclusively(context_->getCurrentQueryId(), context_->getSettingsRef().lock_acquire_timeout);

            auto metadata_snapshot = table->getInMemoryMetadataPtr();
            /// Drop table data, don't touch metadata
            table->truncate(query_ptr, metadata_snapshot, context_, table_excl_lock);
        }
        else if (query.kind == ASTDropQuery::Kind::Drop)
        {
            context_->checkAccess(drop_storage, table_id);

            if (table->isDictionary())
            {
                /// If DROP DICTIONARY query is not used, check if Dictionary can be dropped with DROP TABLE query
                if (!query.is_dictionary)
                    table->checkTableCanBeDropped(context_);
            }
            else
                table->checkTableCanBeDropped(context_);

            /// Check dependencies before shutting table down
            bool check_ref_deps = getContext()->getSettingsRef().check_referential_table_dependencies;
            bool check_loading_deps = !check_ref_deps && getContext()->getSettingsRef().check_table_dependencies;
            DatabaseCatalog::instance().checkTableCanBeRemovedOrRenamed(table_id, check_ref_deps, check_loading_deps, is_drop_or_detach_database);

            table->flushAndShutdown();

            TableExclusiveLockHolder table_lock;
            if (database->getUUID() == UUIDHelpers::Nil)
                table_lock = table->lockExclusively(context_->getCurrentQueryId(), context_->getSettingsRef().lock_acquire_timeout);

            DatabaseCatalog::instance().removeDependencies(table_id, check_ref_deps, check_loading_deps, is_drop_or_detach_database);
            database->dropTable(context_, table_id.table_name, query.sync);

            /// We have to clear mmapio cache when dropping table from Ordinary database
            /// to avoid reading old data if new table with the same name is created
            if (database->getUUID() == UUIDHelpers::Nil)
                context_->clearMMappedFileCache();
        }

        db = database;
        uuid_to_wait = table_id.uuid;
    }

    return {};
}

BlockIO InterpreterDropQuery::executeToTemporaryTable(const String & table_name, ASTDropQuery::Kind kind)
{
    if (kind == ASTDropQuery::Kind::Detach)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Unable to detach temporary table.");
    else
    {
        auto context_handle = getContext()->hasSessionContext() ? getContext()->getSessionContext() : getContext();
        auto resolved_id = context_handle->tryResolveStorageID(StorageID("", table_name), Context::ResolveExternal);
        if (resolved_id)
        {
            StoragePtr table = DatabaseCatalog::instance().getTable(resolved_id, getContext());
            if (kind == ASTDropQuery::Kind::Truncate)
            {
                auto table_lock
                    = table->lockExclusively(getContext()->getCurrentQueryId(), getContext()->getSettingsRef().lock_acquire_timeout);
                /// Drop table data, don't touch metadata
                auto metadata_snapshot = table->getInMemoryMetadataPtr();
                table->truncate(query_ptr, metadata_snapshot, getContext(), table_lock);
            }
            else if (kind == ASTDropQuery::Kind::Drop)
            {
                context_handle->removeExternalTable(table_name);
            }
            else if (kind == ASTDropQuery::Kind::Detach)
            {
                table->is_detached = true;
            }
        }
    }

    return {};
}


BlockIO InterpreterDropQuery::executeToDatabase(const ASTDropQuery & query)
{
    DatabasePtr database;
    std::vector<UUID> tables_to_wait;
    BlockIO res;
    try
    {
        res = executeToDatabaseImpl(query, database, tables_to_wait);
    }
    catch (...)
    {
        if (query.sync)
        {
            for (const auto & table_uuid : tables_to_wait)
                waitForTableToBeActuallyDroppedOrDetached(query, database, table_uuid);
        }
        throw;
    }

    if (query.sync)
    {
        for (const auto & table_uuid : tables_to_wait)
            waitForTableToBeActuallyDroppedOrDetached(query, database, table_uuid);
    }
    return res;
}

BlockIO InterpreterDropQuery::executeToDatabaseImpl(const ASTDropQuery & query, DatabasePtr & database, std::vector<UUID> & uuids_to_wait)
{
    const auto & database_name = query.getDatabase();
    auto ddl_guard = DatabaseCatalog::instance().getDDLGuard(database_name, "");

    database = tryGetDatabase(database_name, query.if_exists);
    if (database)
    {
        if (query.kind == ASTDropQuery::Kind::Detach || query.kind == ASTDropQuery::Kind::Drop
            || query.kind == ASTDropQuery::Kind::Truncate)
        {
            bool drop = query.kind == ASTDropQuery::Kind::Drop;
            bool truncate = query.kind == ASTDropQuery::Kind::Truncate;

            getContext()->checkAccess(AccessType::DROP_DATABASE, database_name);

            if (query.kind == ASTDropQuery::Kind::Detach && query.permanently)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DETACH PERMANENTLY is not implemented for databases");

            if (database->hasReplicationThread())
                database->stopReplication();

            if (database->shouldBeEmptyOnDetach())
            {
                ASTDropQuery query_for_table;
                query_for_table.kind = query.kind;
                // For truncate operation on database, drop the tables
                if (truncate)
                    query_for_table.kind = ASTDropQuery::Kind::Drop;
                query_for_table.if_exists = true;
                query_for_table.setDatabase(database_name);
                query_for_table.sync = query.sync;

                /// Flush should not be done if shouldBeEmptyOnDetach() == false,
                /// since in this case getTablesIterator() may do some additional work,
                /// see DatabaseMaterializedMySQL::getTablesIterator()
                auto table_context = Context::createCopy(getContext());
                table_context->setInternalQuery(true);
                /// Do not hold extra shared pointers to tables
                std::vector<std::pair<String, bool>> tables_to_drop;
                for (auto iterator = database->getTablesIterator(table_context); iterator->isValid(); iterator->next())
                {
                    iterator->table()->flushAndPrepareForShutdown();
                    tables_to_drop.push_back({iterator->name(), iterator->table()->isDictionary()});
                }

                for (const auto & table : tables_to_drop)
                {
                    query_for_table.setTable(table.first);
                    query_for_table.is_dictionary = table.second;
                    DatabasePtr db;
                    UUID table_to_wait = UUIDHelpers::Nil;
                    executeToTableImpl(table_context, query_for_table, db, table_to_wait);
                    uuids_to_wait.push_back(table_to_wait);
                }
            }
           // only if operation is DETACH
            if ((!drop || !truncate) && query.sync)
            {
                /// Avoid "some tables are still in use" when sync mode is enabled
                for (const auto & table_uuid : uuids_to_wait)
                    database->waitDetachedTableNotInUse(table_uuid);
            }

            /// Protects from concurrent CREATE TABLE queries
            auto db_guard = DatabaseCatalog::instance().getExclusiveDDLGuardForDatabase(database_name);
            // only if operation is DETACH
            if (!drop || !truncate)
                database->assertCanBeDetached(true);

            /// DETACH or DROP database itself. If TRUNCATE skip dropping/erasing the database.
            if (!truncate)
                DatabaseCatalog::instance().detachDatabase(getContext(), database_name, drop, database->shouldBeEmptyOnDetach());
        }
    }

    return {};
}


AccessRightsElements InterpreterDropQuery::getRequiredAccessForDDLOnCluster() const
{
    AccessRightsElements required_access;
    const auto & drop = query_ptr->as<const ASTDropQuery &>();

    if (!drop.table)
    {
        if (drop.kind == ASTDropQuery::Kind::Detach)
            required_access.emplace_back(AccessType::DROP_DATABASE, drop.getDatabase());
        else if (drop.kind == ASTDropQuery::Kind::Drop)
            required_access.emplace_back(AccessType::DROP_DATABASE, drop.getDatabase());
    }
    else if (drop.is_dictionary)
    {
        if (drop.kind == ASTDropQuery::Kind::Detach)
            required_access.emplace_back(AccessType::DROP_DICTIONARY, drop.getDatabase(), drop.getTable());
        else if (drop.kind == ASTDropQuery::Kind::Drop)
            required_access.emplace_back(AccessType::DROP_DICTIONARY, drop.getDatabase(), drop.getTable());
    }
    else if (!drop.temporary)
    {
        /// It can be view or table.
        if (drop.kind == ASTDropQuery::Kind::Drop)
            required_access.emplace_back(AccessType::DROP_TABLE | AccessType::DROP_VIEW, drop.getDatabase(), drop.getTable());
        else if (drop.kind == ASTDropQuery::Kind::Truncate)
            required_access.emplace_back(AccessType::TRUNCATE, drop.getDatabase(), drop.getTable());
        else if (drop.kind == ASTDropQuery::Kind::Detach)
            required_access.emplace_back(AccessType::DROP_TABLE | AccessType::DROP_VIEW, drop.getDatabase(), drop.getTable());
    }

    return required_access;
}

void InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind kind, ContextPtr global_context, ContextPtr current_context,
                                            const StorageID & target_table_id, bool sync, bool ignore_sync_setting, bool need_ddl_guard)
{
    auto ddl_guard = (need_ddl_guard ? DatabaseCatalog::instance().getDDLGuard(target_table_id.database_name, target_table_id.table_name) : nullptr);
    if (DatabaseCatalog::instance().tryGetTable(target_table_id, current_context))
    {
        /// We create and execute `drop` query for internal table.
        auto drop_query = std::make_shared<ASTDropQuery>();
        drop_query->setDatabase(target_table_id.database_name);
        drop_query->setTable(target_table_id.table_name);
        drop_query->kind = kind;
        drop_query->sync = sync;
        drop_query->if_exists = true;
        ASTPtr ast_drop_query = drop_query;
        /// FIXME We have to use global context to execute DROP query for inner table
        /// to avoid "Not enough privileges" error if current user has only DROP VIEW ON mat_view_name privilege
        /// and not allowed to drop inner table explicitly. Allowing to drop inner table without explicit grant
        /// looks like expected behaviour and we have tests for it.
        auto drop_context = Context::createCopy(global_context);
        if (ignore_sync_setting)
            drop_context->setSetting("database_atomic_wait_for_drop_and_detach_synchronously", false);
        drop_context->setQueryKind(ClientInfo::QueryKind::SECONDARY_QUERY);
        if (auto txn = current_context->getZooKeeperMetadataTransaction())
        {
            /// For Replicated database
            drop_context->setQueryKindReplicatedDatabaseInternal();
            drop_context->setQueryContext(std::const_pointer_cast<Context>(current_context));
            drop_context->initZooKeeperMetadataTransaction(txn, true);
        }
        InterpreterDropQuery drop_interpreter(ast_drop_query, drop_context);
        drop_interpreter.execute();
    }
}

bool InterpreterDropQuery::supportsTransactions() const
{
    /// Enable only for truncate table with MergeTreeData engine

    auto & drop = query_ptr->as<ASTDropQuery &>();

    return drop.cluster.empty()
            && !drop.temporary
            && drop.kind == ASTDropQuery::Kind::Truncate
            && drop.table;
}

}
