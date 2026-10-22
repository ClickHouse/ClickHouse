#include <Interpreters/SystemCommands/SystemCommandHelpers.h>
#include <Interpreters/SystemCommands/SystemCommandsReplication.h>

#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <Databases/DatabaseReplicated.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/TransactionManager.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageProxy.h>
#include <Storages/StorageQueryRunner.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <base/scope_guard.h>
#include <base/sleep.h>
#include <Common/CurrentMetrics.h>
#include <Common/FailPoint.h>
#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperRetries.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>


namespace CurrentMetrics
{
    extern const Metric RestartReplicaThreads;
    extern const Metric RestartReplicaThreadsActive;
    extern const Metric RestartReplicaThreadsScheduled;
}

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 keeper_max_retries;
    extern const SettingsUInt64 keeper_retry_initial_backoff_ms;
    extern const SettingsUInt64 keeper_retry_max_backoff_ms;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsSeconds receive_timeout;
}

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int BAD_ARGUMENTS;
    extern const int FAULT_INJECTED;
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
    extern const int TOO_DEEP_RECURSION;
}

namespace FailPoints
{
    extern const char restart_replica_fail_after_detach[];
}

namespace
{

/// Replicated tables and Replicated databases.
class ReplicationCommands : public SystemCommandContext
{
public:
    explicit ReplicationCommands(const SystemCommandContext & command_context) : SystemCommandContext(command_context) {}

    /// Tries to get a replicated table and restart it
    /// Returns pointer to a newly created table if the restart was successful
    ///
    /// @param throw_on_error - set to true for SYSTEM RESTART REPLICA, and false for SYSTEM RESTART REPLICAS
    StoragePtr doRestartReplica(const StorageID & replica, bool throw_on_error);

    void restartReplica(const StorageID & replica);
    void restartReplicas();
    void syncReplica(ASTSystemQuery & query);
    void waitLoadingParts();
    void waitQueryRunner();
    void syncReplicatedDatabase(ASTSystemQuery & query);
    void syncTransactionLog();
    void restoreReplica();
    void restoreDatabaseReplica(ASTSystemQuery & query);
};

void ReplicationCommands::restoreReplica()
{
    getContext()->checkAccess(AccessType::SYSTEM_RESTORE_REPLICA, table_id);

    const StoragePtr table_ptr = DatabaseCatalog::instance().getTable(table_id, getContext());

    auto * const table_replicated_ptr = castStorage<StorageReplicatedMergeTree>(table_ptr, DeferredTable::Load).get();

    if (table_replicated_ptr == nullptr)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, table_is_not_replicated.data(), table_id.getNameForLogs());

    const auto & settings = getContext()->getSettingsRef();

    getContext()->onQueryExecutionStarted();
    table_replicated_ptr->restoreMetadataInZooKeeper(
        ZooKeeperRetriesInfo{
            settings[Setting::keeper_max_retries],
            settings[Setting::keeper_retry_initial_backoff_ms],
            settings[Setting::keeper_retry_max_backoff_ms],
            getContext()->getProcessListElementSafe()},
        false);
}

void ReplicationCommands::restoreDatabaseReplica(ASTSystemQuery & query)
{
    const String database_name = query.getDatabase();
    getContext()->checkAccess(AccessType::SYSTEM_RESTORE_DATABASE_REPLICA, database_name);

    const auto db_ptr = DatabaseCatalog::instance().getDatabase(database_name);

    auto* replicated_db = dynamic_cast<DatabaseReplicated*>(db_ptr.get());
    if (!replicated_db)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database {} is not Replicated", database_name);
    }

    getContext()->onQueryExecutionStarted();
    replicated_db->restoreDatabaseInKeeper(getContext());

    LOG_TRACE(log, "Replicated database {} was restored.", database_name);
}

StoragePtr ReplicationCommands::doRestartReplica(const StorageID & replica, bool throw_on_error)
{
    LOG_TRACE(log, "Restarting replica {}", replica);
    auto table_ddl_guard = DatabaseCatalog::instance().getDDLGuard(replica.getDatabaseName(), replica.getTableName(), nullptr);

    auto restart_replica_lock = DatabaseCatalog::instance().tryGetLockForRestartReplica(replica.getDatabaseName());
    if (!restart_replica_lock)
        throw Exception(ErrorCodes::ABORTED, "Database {} is being dropped or detached, will not restart replica {}",
                        backQuoteIfNeed(replica.getDatabaseName()), replica.getNameForLogs());

    std::optional<Exception> exception;
    auto [database, table] = DatabaseCatalog::instance().getTableImpl(replica, getContext(), &exception);
    ASTPtr create_ast;

    if (!table)
    {
        if (throw_on_error)
            throw Exception(*exception);
        LOG_WARNING(getLogger("InterpreterSystemQuery"), "Cannot RESTART REPLICA {}: {}", replica.getNameForLogs(), exception->message());
        return nullptr;
    }
    const StorageID replica_table_id = table->getStorageID();

    /// The `replica` StorageID may contain a UUID resolved before the DDL guard was acquired.
    /// If a concurrent RENAME moved the table to a different name, UUID-based resolution in
    /// `getTableImpl` finds the table at its new name, while the DDL guard only protects
    /// `replica.table_name`. Verify that the resolved table is actually at the guarded name;
    /// otherwise we would detach/attach the wrong table.
    if (replica_table_id.table_name != replica.getTableName())
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Table {} was concurrently renamed to {} during RESTART REPLICA",
                replica.getNameForLogs(), replica_table_id.getNameForLogs());
        LOG_WARNING(getLogger("InterpreterSystemQuery"), "Cannot RESTART REPLICA {}: table was concurrently renamed to {}",
            replica.getNameForLogs(), replica_table_id.getNameForLogs());
        return nullptr;
    }

    /// The resolved pointer must not outlive this check. `waitDetachedTableNotInUse` below waits
    /// for the last reference to the detached table to be released.
    if (!castStorage<StorageReplicatedMergeTree>(table, DeferredTable::Load))
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, table_is_not_replicated.data(), replica.getNameForLogs());
        LOG_WARNING(getLogger("InterpreterSystemQuery"), "Cannot RESTART REPLICA {}: not a ReplicatedMergeTree anymore", replica.getNameForLogs());
        return nullptr;
    }

    SCOPE_EXIT({
        if (table)
            table->is_being_restarted = false;
    });
    table->is_being_restarted = true;
    getContext()->onQueryExecutionStarted();
    table->flushAndShutdown();

    /// The definition re-attached below was read back from this server's own metadata, so it must be
    /// accepted as it is. `system_context` is shared by every branch of `execute`, hence the copy.
    auto attach_context = Context::createCopy(system_context);
    attach_context->setRecoveryFromStoredMetadata(true);

    /// For DatabaseReplicated, suppress digest checks while the table is temporarily detached.
    /// The table is removed from the in-memory tables map between detach and attach, making it
    /// inconsistent with tables_metadata_digest (which stays correct and is not modified).
    std::optional<DatabaseReplicated::RestartReplicaGuard> restart_guard;
    if (auto * replicated_db = typeid_cast<DatabaseReplicated *>(database.get()))
        restart_guard.emplace(*replicated_db);

    {
        /// If table was already dropped by anyone, an exception will be thrown
        auto table_lock = table->lockExclusively(getContext()->getCurrentQueryId(), getContext()->getSettingsRef()[Setting::lock_acquire_timeout]);
        create_ast = database->getCreateTableQuery(replica_table_id.table_name, getContext());

        database->detachTable(system_context, replica_table_id.table_name);
    }
    table.reset();
    {
        QueryStatusPtr query_status = getContext()->getProcessListElementSafe();
        database->waitDetachedTableNotInUse(replica_table_id.uuid, [&]()
        {
            if (query_status)
                query_status->throwIfKilled();
        });
    }

    /// Attach actions
    /// getCreateTableQuery must return canonical CREATE query representation, there are no need for AST postprocessing
    auto & create = create_ast->as<ASTCreateQuery &>();
    create.attach = true;
    /// The definition comes from metadata stored on this server, not from a user, so it must load the
    /// same way a short `ATTACH TABLE t` does: storage creators read this flag to skip the validation
    /// that only a freshly introduced definition needs.
    create.attach_short_syntax = true;

    auto columns = InterpreterCreateQuery::getColumnsDescription(*create.columns_list->columns, system_context, LoadingStrictnessLevel::ATTACH);
    auto constraints = InterpreterCreateQuery::getConstraintsDescription(create.columns_list->constraints, columns, system_context);
    auto data_path = database->getTableDataPath(create);

    /// After the table is detached, we must re-create and re-attach it.
    /// If table creation fails (e.g. due to memory allocation failure from fault injection),
    /// the table is left permanently detached from the database while still existing in ZooKeeper
    /// metadata, which causes metadata digest mismatches in DatabaseReplicated.
    /// We retry on all exceptions, not just ZooKeeper ones, because re-creating from valid metadata
    /// should always eventually succeed for transient errors.
    /// If all retries fail (or the query is cancelled), we adjust the in-memory metadata digest
    /// to reflect the table's absence, preventing false "Digest does not match" assertions.

    StoragePtr new_table;
    try
    {
        size_t non_zk_retries = 0;
        constexpr size_t max_non_zk_retries = 10;
        while (true)
        {
            try
            {
                fiu_do_on(FailPoints::restart_replica_fail_after_detach,
                {
                    throw Exception(ErrorCodes::FAULT_INJECTED,
                        "Injected failure by failpoint restart_replica_fail_after_detach");
                });

                new_table = StorageFactory::instance().get(create,
                    data_path,
                    attach_context,
                    system_context->getGlobalContext(),
                    columns,
                    constraints,
                    LoadingStrictnessLevel::ATTACH);

                break;
            }
            catch (const Coordination::Exception & e)
            {
                /// Only retry on transient ZooKeeper errors (connection loss, session expired, etc.)
                if (!Coordination::isHardwareError(e.code))
                    throw;

                tryLogCurrentException(
                    getLogger("InterpreterSystemQuery"),
                    fmt::format("Failed to restart replica {}, will retry", replica_table_id.getNameForLogs()));

                /// Check if the query was cancelled (e.g. server is shutting down)
                if (auto process_list_element = getContext()->getProcessListElementSafe())
                    process_list_element->checkTimeLimit();

                sleepForSeconds(1);
            }
            catch (...)
            {
                if (++non_zk_retries > max_non_zk_retries)
                    throw;

                tryLogCurrentException(
                    getLogger("InterpreterSystemQuery"),
                    fmt::format("Failed to restart replica {} (attempt {}/{}), will retry",
                        replica_table_id.getNameForLogs(), non_zk_retries, max_non_zk_retries));

                if (auto process_list_element = getContext()->getProcessListElementSafe())
                    process_list_element->checkTimeLimit();

                sleepForSeconds(1);
            }
        }
    }
    catch (...)
    {
        /// The table is left permanently detached from the in-memory tables map.
        /// Adjust the metadata digest so it stays consistent with the tables map,
        /// preventing false "Digest does not match" LOGICAL_ERROR exceptions.
        /// The table's metadata file still exists on disk, so it will be restored
        /// on server restart or after DatabaseReplicated recovery.
        if (auto * replicated_db = typeid_cast<DatabaseReplicated *>(database.get()))
        {
            try
            {
                replicated_db->adjustDigestOnTableLostFromRestart(replica_table_id.table_name);
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to adjust digest after failed SYSTEM RESTART REPLICA for table " + replica_table_id.table_name + "; digest mismatch will persist until server restart or DatabaseReplicated recovery");
            }
        }
        throw;
    }

    database->attachTable(system_context, replica_table_id.table_name, new_table, data_path);
    restart_guard.reset();
    if (new_table->getStorageID().uuid != replica_table_id.uuid)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Tables UUID does not match after RESTART REPLICA (old: {}, new: {})", replica_table_id.uuid, new_table->getStorageID().uuid);

    new_table->startup();
    LOG_TRACE(log, "Restarted replica {}", new_table->getStorageID().getNameForLogs());
    return new_table;
}

void ReplicationCommands::restartReplica(const StorageID & replica)
{
    getContext()->checkAccess(AccessType::SYSTEM_RESTART_REPLICA, replica);
    doRestartReplica(replica, /*throw_on_error=*/ true);
}

void ReplicationCommands::restartReplicas()
{
    std::vector<StorageID> replica_names;
    auto & catalog = DatabaseCatalog::instance();

    auto access = getContext()->getAccess();
    bool access_is_granted_globally = access->isGranted(AccessType::SYSTEM_RESTART_REPLICA);
    bool show_tables_is_granted_globally = access->isGranted(AccessType::SHOW_TABLES);

    for (auto & elem : catalog.getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false}))
    {
        if (elem.second->isExternal())
            continue;

        if (!access_is_granted_globally && !show_tables_is_granted_globally && !access->isGranted(AccessType::SHOW_TABLES, elem.first))
        {
            LOG_INFO(log, "Access {} denied, skipping {}", "SHOW TABLES", elem.first);
            continue;
        }

        for (auto it = elem.second->getTablesIterator(getContext()); it->isValid(); it->next())
        {
            if (castStorage<StorageReplicatedMergeTree>(it->table(), DeferredTable::Skip))
            {
                if (!access_is_granted_globally && !access->isGranted(AccessType::SYSTEM_RESTART_REPLICA, elem.first, it->name()))
                {
                    LOG_INFO(log, "Access {} denied, skipping {}.{}", "SYSTEM RESTART REPLICA", elem.first, it->name());
                    continue;
                }
                replica_names.emplace_back(it->databaseName(), it->name());
            }
        }
    }

    if (replica_names.empty())
        return;

    size_t threads = std::min(static_cast<size_t>(getNumberOfCPUCoresToUse()), replica_names.size());
    LOG_DEBUG(log, "Will restart {} replicas using {} threads", replica_names.size(), threads);
    ThreadPool pool(CurrentMetrics::RestartReplicaThreads, CurrentMetrics::RestartReplicaThreadsActive, CurrentMetrics::RestartReplicaThreadsScheduled, threads);

    for (auto & replica : replica_names)
    {
        pool.scheduleOrThrowOnError([&]() { doRestartReplica(replica, /*throw_on_error=*/ false); });
    }
    pool.wait();
}

void ReplicationCommands::syncReplica(ASTSystemQuery & query)
{
    getContext()->checkAccess(AccessType::SYSTEM_SYNC_REPLICA, table_id);
    StoragePtr table;

    if (query.if_exists)
    {
        table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
        if (!table)
            return;
    }
    else
    {
        table = DatabaseCatalog::instance().getTable(table_id, getContext());
    }

    std::unordered_set<std::string> replicas(query.src_replicas.begin(), query.src_replicas.end());
    if (!trySyncReplica(table, query.sync_replica_mode, replicas, getContext()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, table_is_not_replicated.data(), table_id.getNameForLogs());
}

void ReplicationCommands::waitLoadingParts()
{
    getContext()->checkAccess(AccessType::SYSTEM_WAIT_LOADING_PARTS, table_id);
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());

    if (auto * merge_tree = castStorage<MergeTreeData>(table, DeferredTable::Load).get())
    {
        LOG_TRACE(log, "Waiting for loading of parts of table {}", table_id.getFullTableName());
        getContext()->onQueryExecutionStarted();
        merge_tree->waitForOutdatedPartsToBeLoaded();
        LOG_TRACE(log, "Finished waiting for loading of parts of table {}", table_id.getFullTableName());
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Command WAIT LOADING PARTS is supported only for MergeTree table, but got: {}", table->getName());
    }
}

void ReplicationCommands::waitQueryRunner()
{
    getContext()->checkAccess(AccessType::SYSTEM_WAIT_QUERY_RUNNER, table_id);
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());

    if (auto * query_runner = dynamic_cast<StorageQueryRunner *>(table.get()))
    {
        getContext()->onQueryExecutionStarted();
        query_runner->waitForQueriesToFinish(getContext()->getProcessListElement());
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Command WAIT QUERY RUNNER is supported only for QueryRunner table, but got: {}", table->getName());
}

void ReplicationCommands::syncReplicatedDatabase(ASTSystemQuery & query)
{
    const auto database_name = query.getDatabase();
    auto guard = DatabaseCatalog::instance().getDDLGuard(database_name, "", nullptr);
    auto database = DatabaseCatalog::instance().getDatabase(database_name);

    if (query.sync_replica_mode != SyncReplicaMode::DEFAULT && query.sync_replica_mode != SyncReplicaMode::STRICT)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SYSTEM SYNC DATABASE REPLICA supports only DEFAULT and STRICT modes");

    if (auto * ptr = typeid_cast<DatabaseReplicated *>(database.get()))
    {
        LOG_TRACE(log, "Synchronizing entries in the database replica's (name: {}) queue with the log", database_name);
        getContext()->onQueryExecutionStarted();
        if (!ptr->waitForReplicaToProcessAllEntries(getContext()->getSettingsRef()[Setting::receive_timeout].totalMilliseconds(), query.sync_replica_mode))
        {
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "SYNC DATABASE REPLICA {}: database is readonly or command timed out. " \
                    "See the 'receive_timeout' setting", database_name);
        }
        LOG_TRACE(log, "SYNC DATABASE REPLICA {}: OK", database_name);
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SYSTEM SYNC DATABASE REPLICA query is intended to work only with Replicated engine");
}


void ReplicationCommands::syncTransactionLog()
{
    getContext()->checkAccess(AccessType::SYSTEM_SYNC_TRANSACTION_LOG);
    getContext()->checkTransactionsAreAllowed(/* explicit_tcl_query */ true);
    getContext()->onQueryExecutionStarted();
    TransactionManager::instance().sync();
}

}

bool trySyncReplica(StoragePtr table, SyncReplicaMode sync_replica_mode, const std::unordered_set<String> & src_replicas, ContextPtr context_)
{
    auto table_id_ = table->getStorageID();

    /// If materialized view, sync its target table.
    for (int i = 0;; ++i)
    {
        if (i >= 100)
            throw Exception(ErrorCodes::TOO_DEEP_RECURSION, "Materialized view targets form a cycle or a very long chain");

        if (auto * storage_mv = dynamic_cast<StorageMaterializedView *>(table.get()))
            table = storage_mv->getTargetTable();
        else
            break;
    }


    if (auto * storage_replicated = castStorage<StorageReplicatedMergeTree>(table, DeferredTable::Load).get())
    {
        auto log = getLogger("InterpreterSystemQuery");
        LOG_TRACE(log, "Synchronizing entries in replica's queue with table's log and waiting for current last entry to be processed");
        auto sync_timeout = context_->getSettingsRef()[Setting::receive_timeout].totalMilliseconds();
        context_->onQueryExecutionStarted();
        if (!storage_replicated->waitForProcessingQueue(sync_timeout, sync_replica_mode, src_replicas))
        {
            LOG_ERROR(log, "SYNC REPLICA {}: Timed out.", table_id_.getNameForLogs());
            throw Exception(
                ErrorCodes::TIMEOUT_EXCEEDED,
                "SYNC REPLICA {}: command timed out. "
                "See the 'receive_timeout' setting",
                table_id_.getNameForLogs());
        }
        LOG_TRACE(log, "SYNC REPLICA {}: OK", table_id_.getNameForLogs());
    }
    else
        return false;

    return true;
}

void registerSystemCommandsReplication(SystemCommandFactory & factory)
{
    using Type = ASTSystemQuery::Type;

    auto register_command = [&](Type type, SystemCommandFactory::AccessOnClusterFn access_on_cluster, std::function<void(ASTSystemQuery &, ReplicationCommands &)> action)
    {
        factory.registerCommand(
            type,
            std::move(access_on_cluster),
            [action_fn = std::move(action)](ASTSystemQuery & query, SystemCommandContext & context)
            {
                ReplicationCommands commands(context);
                action_fn(query, commands);
                return BlockIO{};
            });
    };

    register_command(Type::SYNC_REPLICA, tableAccessOnCluster(AccessType::SYSTEM_SYNC_REPLICA), [](ASTSystemQuery & query, ReplicationCommands & commands)
    {
        commands.syncReplica(query);
    });

    /// TODO: `ON CLUSTER` requires `SYSTEM_SYNC_DATABASE_REPLICA`, but the local execution checks no privileges.
    register_command(Type::SYNC_DATABASE_REPLICA, databaseAccessOnCluster(AccessType::SYSTEM_SYNC_DATABASE_REPLICA), [](ASTSystemQuery & query, ReplicationCommands & commands)
    {
        commands.syncReplicatedDatabase(query);
    });

    register_command(Type::SYNC_TRANSACTION_LOG, globalAccessOnCluster(AccessType::SYSTEM_SYNC_TRANSACTION_LOG), [](ASTSystemQuery &, ReplicationCommands & commands)
    {
        commands.syncTransactionLog();
    });

    register_command(Type::RESTART_REPLICAS, globalAccessOnCluster(AccessType::SYSTEM_RESTART_REPLICA), [](ASTSystemQuery &, ReplicationCommands & commands)
    {
        commands.restartReplicas();
    });

    register_command(Type::RESTART_REPLICA, tableAccessOnCluster(AccessType::SYSTEM_RESTART_REPLICA), [](ASTSystemQuery &, ReplicationCommands & commands)
    {
        commands.restartReplica(commands.table_id);
    });

    register_command(Type::RESTORE_REPLICA, tableAccessOnCluster(AccessType::SYSTEM_RESTORE_REPLICA), [](ASTSystemQuery &, ReplicationCommands & commands)
    {
        commands.restoreReplica();
    });

    register_command(Type::RESTORE_DATABASE_REPLICA, databaseAccessOnCluster(AccessType::SYSTEM_RESTORE_DATABASE_REPLICA), [](ASTSystemQuery & query, ReplicationCommands & commands)
    {
        commands.restoreDatabaseReplica(query);
    });

    register_command(Type::WAIT_LOADING_PARTS, tableAccessOnCluster(AccessType::SYSTEM_WAIT_LOADING_PARTS), [](ASTSystemQuery &, ReplicationCommands & commands)
    {
        commands.waitLoadingParts();
    });

    register_command(Type::WAIT_QUERY_RUNNER, tableAccessOnCluster(AccessType::SYSTEM_WAIT_QUERY_RUNNER), [](ASTSystemQuery &, ReplicationCommands & commands)
    {
        commands.waitQueryRunner();
    });
}

}
