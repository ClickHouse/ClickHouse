#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/QueryLog.h>
#include <IO/SharedThreadPools.h>
#include <Access/Common/AccessRightsElement.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>
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
namespace Setting
{
    extern const SettingsBool check_referential_table_dependencies;
    extern const SettingsBool check_table_dependencies;
    extern const SettingsBool database_atomic_wait_for_drop_and_detach_synchronously;
    extern const SettingsFloat ignore_drop_queries_probability;
    extern const SettingsSeconds lock_acquire_timeout;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SYNTAX_ERROR;
    extern const int UNKNOWN_TABLE;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_QUERY;
    extern const int TABLE_IS_READ_ONLY;
    extern const int TABLE_NOT_EMPTY;
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
    BlockIO res;
    auto & drop = query_ptr->as<ASTDropQuery &>();
    ASTs drops = drop.getRewrittenASTsOfSingleTable();
    for (const auto & drop_query_ptr : drops)
    {
        current_query_ptr = drop_query_ptr;
        res = executeSingleDropQuery(drop_query_ptr);
    }
    return res;
}

BlockIO InterpreterDropQuery::executeSingleDropQuery(const ASTPtr & drop_query_ptr)
{
    auto & drop = drop_query_ptr->as<ASTDropQuery &>();
    if (!drop.cluster.empty() && drop.table && !drop.if_empty && !maybeRemoveOnCluster(current_query_ptr, getContext()))
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = getRequiredAccessForDDLOnCluster();
        return executeDDLQueryOnCluster(current_query_ptr, getContext(), params);
    }

    if (getContext()->getSettingsRef()[Setting::database_atomic_wait_for_drop_and_detach_synchronously])
        drop.sync = true;

    if (drop.table)
        return executeToTable(drop);
    else if (drop.database && !drop.cluster.empty() && !maybeRemoveOnCluster(current_query_ptr, getContext()))
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = getRequiredAccessForDDLOnCluster();
        return executeDDLQueryOnCluster(current_query_ptr, getContext(), params);
    }
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

BlockIO InterpreterDropQuery::executeToTableImpl(const ContextPtr & context_, ASTDropQuery & query, DatabasePtr & db, UUID & uuid_to_wait)
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
        const auto & settings = getContext()->getSettingsRef();
        if (query.if_empty)
        {
            if (auto rows = table->totalRows(settings); rows > 0)
                throw Exception(ErrorCodes::TABLE_NOT_EMPTY, "Table {} is not empty", backQuoteIfNeed(table_id.table_name));
        }
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

        if (settings[Setting::ignore_drop_queries_probability] != 0 && ast_drop_query.kind == ASTDropQuery::Kind::Drop
            && std::uniform_real_distribution<>(0.0, 1.0)(thread_local_rng) <= settings[Setting::ignore_drop_queries_probability])
        {
            ast_drop_query.sync = false;
            if (table->storesDataOnDisk())
            {
                LOG_TEST(getLogger("InterpreterDropQuery"), "Ignore DROP TABLE query for table {}.{}", table_id.database_name, table_id.table_name);
                return {};
            }

            LOG_TEST(getLogger("InterpreterDropQuery"), "Replace DROP TABLE query to TRUNCATE TABLE for table {}.{}", table_id.database_name, table_id.table_name);
            ast_drop_query.kind = ASTDropQuery::Truncate;
        }

        /// Now get UUID, so we can wait for table data to be finally dropped
        table_id.uuid = database->tryGetTableUUID(table_id.table_name);

        /// Prevents recursive drop from drop database query. The original query must specify a table.
        bool is_drop_or_detach_database = !current_query_ptr->as<ASTDropQuery>()->table;

        AccessFlags drop_storage;

        if (table->isView())
            drop_storage = AccessType::DROP_VIEW;
        else if (table->isDictionary())
            drop_storage = AccessType::DROP_DICTIONARY;
        else
            drop_storage = AccessType::DROP_TABLE;

        auto new_query_ptr = query.clone();
        auto & query_to_send = new_query_ptr->as<ASTDropQuery &>();

        if (!query.cluster.empty() && !maybeRemoveOnCluster(new_query_ptr, getContext()))
        {
            query_to_send.if_empty = false;

            DDLQueryOnClusterParams params;
            params.access_to_check = getRequiredAccessForDDLOnCluster();
            return executeDDLQueryOnCluster(new_query_ptr, getContext(), params);
        }

        if (database->shouldReplicateQuery(getContext(), current_query_ptr))
        {
            if (query.kind == ASTDropQuery::Kind::Detach)
                context_->checkAccess(drop_storage, table_id);
            else if (query.kind == ASTDropQuery::Kind::Truncate)
                context_->checkAccess(AccessType::TRUNCATE, table_id);
            else if (query.kind == ASTDropQuery::Kind::Drop)
                context_->checkAccess(drop_storage, table_id);

            ddl_guard->releaseTableLock();
            table.reset();

            query_to_send.if_empty = false;

            return database->tryEnqueueReplicatedDDL(new_query_ptr, context_);
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
                table_lock = table->lockExclusively(context_->getCurrentQueryId(), context_->getSettingsRef()[Setting::lock_acquire_timeout]);

            if (query.permanently)
            {
                /// Server may fail to restart of DETACH PERMANENTLY if table has dependent ones
                bool check_ref_deps = getContext()->getSettingsRef()[Setting::check_referential_table_dependencies];
                bool check_loading_deps = !check_ref_deps && getContext()->getSettingsRef()[Setting::check_table_dependencies];
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
                table_excl_lock = table->lockExclusively(context_->getCurrentQueryId(), context_->getSettingsRef()[Setting::lock_acquire_timeout]);

            auto metadata_snapshot = table->getInMemoryMetadataPtr();
            /// Drop table data, don't touch metadata
            table->truncate(current_query_ptr, metadata_snapshot, context_, table_excl_lock);
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
            bool check_ref_deps = getContext()->getSettingsRef()[Setting::check_referential_table_dependencies];
            bool check_loading_deps = !check_ref_deps && getContext()->getSettingsRef()[Setting::check_table_dependencies];
            DatabaseCatalog::instance().checkTableCanBeRemovedOrRenamed(table_id, check_ref_deps, check_loading_deps, is_drop_or_detach_database);

            table->flushAndShutdown(true);

            TableExclusiveLockHolder table_lock;
            if (database->getUUID() == UUIDHelpers::Nil)
                table_lock = table->lockExclusively(context_->getCurrentQueryId(), context_->getSettingsRef()[Setting::lock_acquire_timeout]);

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
                    = table->lockExclusively(getContext()->getCurrentQueryId(), getContext()->getSettingsRef()[Setting::lock_acquire_timeout]);
                /// Drop table data, don't touch metadata
                auto metadata_snapshot = table->getInMemoryMetadataPtr();
                table->truncate(current_query_ptr, metadata_snapshot, getContext(), table_lock);
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
    if (query.kind != ASTDropQuery::Kind::Detach && query.kind != ASTDropQuery::Kind::Drop && query.kind != ASTDropQuery::Kind::Truncate)
        return {};

    const auto & database_name = query.getDatabase();
    auto ddl_guard = DatabaseCatalog::instance().getDDLGuard(database_name, "");

    database = tryGetDatabase(database_name, query.if_exists);
    if (!database)
        return {};

    bool drop = query.kind == ASTDropQuery::Kind::Drop;
    bool truncate = query.kind == ASTDropQuery::Kind::Truncate;

    getContext()->checkAccess(AccessType::DROP_DATABASE, database_name);

    if (query.kind == ASTDropQuery::Kind::Detach && query.permanently)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DETACH PERMANENTLY is not implemented for databases");

    if (query.if_empty)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DROP IF EMPTY is not implemented for databases");

    if (!truncate && database->hasReplicationThread())
        database->stopReplication();

    if (database->shouldBeEmptyOnDetach())
    {
        /// Cancel restarting replicas in that database, wait for remaining RESTART queries to finish.
        /// So it will not startup tables concurrently with the flushAndPrepareForShutdown call below.
        auto restart_replica_lock = DatabaseCatalog::instance().getLockForDropDatabase(database_name);

        ASTDropQuery query_for_table;
        query_for_table.kind = query.kind;
        // For truncate operation on database, drop the tables
        if (truncate)
            query_for_table.kind = query.has_all_tables ? ASTDropQuery::Kind::Truncate : ASTDropQuery::Kind::Drop;
        query_for_table.if_exists = true;
        query_for_table.if_empty = false;
        query_for_table.setDatabase(database_name);
        query_for_table.sync = query.sync;

        /// Flush should not be done if shouldBeEmptyOnDetach() == false,
        /// since in this case getTablesIterator() may do some additional work,
        /// see DatabaseMaterializedMySQL::getTablesIterator()
        auto table_context = Context::createCopy(getContext());
        table_context->setInternalQuery(true);
        /// Do not hold extra shared pointers to tables
        std::vector<std::pair<StorageID, bool>> tables_to_drop;
        // NOTE: This means we wait for all tables to be loaded inside getTablesIterator() call in case of `async_load_databases = true`.
        for (auto iterator = database->getTablesIterator(table_context); iterator->isValid(); iterator->next())
        {
            auto table_ptr = iterator->table();
            tables_to_drop.push_back({table_ptr->getStorageID(), table_ptr->isDictionary()});
        }

        /// Prepare tables for shutdown in parallel.
        ThreadPoolCallbackRunnerLocal<void> runner(getDatabaseCatalogDropTablesThreadPool().get(), "DropTables");
        for (const auto & [name, _] : tables_to_drop)
        {
            auto table_ptr = DatabaseCatalog::instance().getTable(name, table_context);
            runner([my_table_ptr = std::move(table_ptr)]()
            {
                my_table_ptr->flushAndPrepareForShutdown();
            });
        }
        runner.waitForAllToFinishAndRethrowFirstError();

        for (const auto & table : tables_to_drop)
        {
            query_for_table.setTable(table.first.getTableName());
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

    return {};
}

void InterpreterDropQuery::extendQueryLogElemImpl(DB::QueryLogElement & elem, const DB::ASTPtr & ast, DB::ContextPtr context_) const
{
    auto & drop = ast->as<ASTDropQuery &>();
    if (drop.database_and_tables)
    {
        auto & list = drop.database_and_tables->as<ASTExpressionList &>();
        for (auto & child : list.children)
        {
            auto identifier = dynamic_pointer_cast<ASTTableIdentifier>(child);
            if (!identifier)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Unexpected type for list of table names.");

            String query_database = identifier->getDatabaseName();
            String query_table = identifier->shortName();
            if (!query_database.empty() && query_table.empty())
            {
                elem.query_databases.insert(backQuoteIfNeed(query_database));
            }
            else if (!query_table.empty())
            {
                auto quoted_database = query_database.empty() ? backQuoteIfNeed(context_->getCurrentDatabase())
                                                              : backQuoteIfNeed(query_database);
                elem.query_databases.insert(quoted_database);
                elem.query_tables.insert(quoted_database + "." + backQuoteIfNeed(query_table));
            }
        }
    }
}

AccessRightsElements InterpreterDropQuery::getRequiredAccessForDDLOnCluster() const
{
    AccessRightsElements required_access;
    const auto & drop = current_query_ptr->as<const ASTDropQuery &>();

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

void registerInterpreterDropQuery(InterpreterFactory & factory)
{
    auto create_fn = [] (const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropQuery>(args.query, args.context);
    };
    factory.registerInterpreter("InterpreterDropQuery", create_fn);
}
}
