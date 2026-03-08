#include <Databases/DatabaseReplicatedWorker.h>
#include <base/sleep.h>

#include <filesystem>
#include <Core/ServerUUID.h>
#include <Core/Settings.h>
#include <Databases/DatabaseReplicated.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLReplicatorSettings.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/StorageMaterializedView.h>
#include <Common/FailPoint.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/thread_local_rng.h>
#include <Parsers/ASTRenameQuery.h>

namespace fs = std::filesystem;

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 database_replicated_initial_query_timeout_sec;
}

namespace DatabaseReplicatedSetting
{
    extern const DatabaseReplicatedSettingsBool allow_skipping_old_temporary_tables_ddls_of_refreshable_materialized_views;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int DATABASE_REPLICATION_FAILED;
    extern const int NOT_A_LEADER;
    extern const int QUERY_WAS_CANCELLED;
    extern const int TABLE_IS_DROPPED;
    extern const int UNFINISHED;
}

namespace FailPoints
{
    extern const char database_replicated_delay_recovery[];
    extern const char database_replicated_delay_entry_execution[];
    extern const char database_replicated_stop_entry_execution[];
}


DatabaseReplicatedDDLWorker::DatabaseReplicatedDDLWorker(DatabaseReplicated * db, ContextPtr context_)
    : DDLReplicateWorker(db, context_, fmt::format("DDLWorker({})", db->getDatabaseName()))
    , database(db)
{
    LOG_INFO(
        log,
        "allow_skipping_old_temporary_tables_ddls_of_refreshable_materialized_views {}",
        database->db_settings[DatabaseReplicatedSetting::allow_skipping_old_temporary_tables_ddls_of_refreshable_materialized_views].value);
    /// Pool size must be 1 to avoid reordering of log entries.
    /// TODO Make a dependency graph of DDL queries. It will allow to execute independent entries in parallel.
    /// We also need similar graph to load tables on server startup in order of topsort.
}

bool DatabaseReplicatedDDLWorker::beforeReplication()
{
    if (database->is_readonly)
        database->tryConnectToZooKeeperAndInitDatabase(LoadingStrictnessLevel::ATTACH);
    if (database->is_probably_dropped)
    {
        /// The flag was set in tryConnectToZooKeeperAndInitDatabase
        LOG_WARNING(log, "Exiting main thread, because the database was probably dropped");
        /// NOTE It will not stop cleanup thread until DDLWorker::shutdown() call (cleanup thread will just do nothing)
        return false;
    }
    return true;
}

void DatabaseReplicatedDDLWorker::scheduleTasks(bool reinitialized)
{
    DDLReplicateWorker::scheduleTasks(reinitialized);
    if (need_update_cached_cluster)
    {
        database->setCluster(database->getClusterImpl());
        if (!database->replica_group_name.empty())
            database->setCluster(database->getClusterImpl(/*all_groups*/ true), /*all_groups*/ true);
        need_update_cached_cluster = false;
    }
}

String DatabaseReplicatedDDLWorker::tryEnqueueAndExecuteEntry(DDLLogEntry & entry, ContextPtr query_context, bool internal_query)
{
    OpenTelemetry::SpanHolder span(__FUNCTION__);
    span.addAttribute("clickhouse.cluster", database->getDatabaseName());
    entry.tracing_context = OpenTelemetry::CurrentContext();
    return DDLReplicateWorker::tryEnqueueAndExecuteEntry(entry, query_context, internal_query);
}

static bool getRMVCoordinationInfo(
    LoggerPtr log,
    const ZooKeeperPtr & zookeeper,
    UUID parent_uuid,
    Coordination::Stat & stats,
    RefreshTask::CoordinationZnode & coordination_znode)
{
    if (parent_uuid == UUIDHelpers::Nil)
        return false;

    const auto storage = DatabaseCatalog::instance().tryGetByUUID(parent_uuid).second;
    if (!storage)
        return false;
    auto in_memory_metadata = storage->getInMemoryMetadataPtr();
    const auto * refresh = in_memory_metadata->refresh->as<ASTRefreshStrategy>();
    if (!refresh || refresh->append)
        return false;
    const auto * mv = dynamic_cast<const StorageMaterializedView *>(storage.get());
    if (!mv)
        return false;

    const auto coordination_path = mv->getCoordinationPath();

    if (!coordination_path.has_value() || (*coordination_path).empty())
        return false;
    try
    {
        String data;
        if (!zookeeper->tryGet(*coordination_path, data, &stats))
            return false;
        coordination_znode.parse(data);
        return true;
    }
    catch (...)
    {
        tryLogCurrentException(log, "Unable to get coordination information: " + *coordination_path);
        return false;
    }
}

bool DatabaseReplicatedDDLWorker::shouldSkipCreatingRMVTempTable(
    const ZooKeeperPtr & zookeeper, UUID parent_uuid, UUID create_uuid, int64_t ddl_log_ctime)
{
    if (create_uuid == UUIDHelpers::Nil)
        return false;

    Coordination::Stat stats;
    RefreshTask::CoordinationZnode coordination_znode;

    if (!getRMVCoordinationInfo(log, zookeeper, parent_uuid, stats, coordination_znode))
        return false;

    LOG_TEST(log, "MV {}, coordination info: {}", parent_uuid, coordination_znode.toString());
    if (coordination_znode.last_success_table_uuid == create_uuid)
        return false;

    LOG_TEST(log, "ddl_log_ctime {}, stats.mtime {}", ddl_log_ctime, stats.mtime);
    // It is possible the the temporary table is created and replicated before the coordiation info is updated.
    // So if ddl_log_ctime >= stats.mtime, the table is new and should not be skip.
    return ddl_log_ctime < stats.mtime;
}

bool DatabaseReplicatedDDLWorker::shouldSkipRenamingRMVTempTable(
    const ZooKeeperPtr & zookeeper, UUID parent_uuid, const QualifiedTableName & rename_from_table)
{
    Coordination::Stat stats;
    RefreshTask::CoordinationZnode coordination_znode;

    if (!getRMVCoordinationInfo(log, zookeeper, parent_uuid, stats, coordination_znode))
        return false;

    StorageID storage_id{rename_from_table};
    // If the temporary table creating DDL is skipped, there is no table, we skip renaming
    return !DatabaseCatalog::instance().isTableExist(storage_id, context);
}

DDLTaskPtr DatabaseReplicatedDDLWorker::initAndCheckTask(const String & entry_name, String & out_reason, const ZooKeeperPtr & zookeeper, bool dry_run)
{
    Coordination::Stat stats;
    auto task = DDLReplicateWorker::initAndCheckTaskImpl(entry_name, out_reason, zookeeper, dry_run, &stats);
    if (!task)
    {
        /// Some replica is added or removed, let's update cached cluster
        if (out_reason.ends_with("is a dummy task"))
            need_update_cached_cluster = true;
        return {};
    }

    if (task->entry.parent_table_uuid.has_value() && !checkParentTableExists(task->entry.parent_table_uuid.value()))
    {
        out_reason = fmt::format("Parent table {} doesn't exist", task->entry.parent_table_uuid.value());
        return {};
    }
    if (database->db_settings[DatabaseReplicatedSetting::allow_skipping_old_temporary_tables_ddls_of_refreshable_materialized_views]
        && task->entry.parent_table_uuid)
    {
        // Refreshing in non-append Refreshable Materialized Views:
        // 1. Create a temporary table: .tmp_inner.<uuid of the parent MV>
        // 2. Fill in the data from the SELECT clause
        // 3. Rename (with exchange) the temporary table to the target table name. The target table name can be the inner table of the view, or the table specified in the view create query.
        // 4. Drop the temporary table after exchanging. Note that the temporary table after exchanging is not the table created in step 1. It is the one exchanged in step 3.
        // 5. Update the coordination info in Keeper

        // Queries created in steps 1, 3, and 4 are replicated through the DDL logs.
        // If a replica is woken up after a long sleep, there are lots of creating, renaming and dropping DDLs generated by the RMVs.
        // To speed up the DDL processing, these old create DDLs are skipped.
        // Once the creating DDL is skipped, there is no table to rename in the renaming DDL, so the renaming DDL is skipped if the table name does not exist.
        // For the dropping DDLs, as the query contains `IF EXISTS`, we don't need to skip.
        const auto & parent_table_uuid = *task->entry.parent_table_uuid;
        LOG_DEBUG(log, "Entry {}, parent_uuid {}", entry_name, parent_table_uuid);
        if (const auto * create_query = task->query->as<ASTCreateQuery>())
        {
            if (create_query->uuid != UUIDHelpers::Nil
                && shouldSkipCreatingRMVTempTable(zookeeper, *task->entry.parent_table_uuid, create_query->uuid, stats.ctime))
            {
                LOG_INFO(
                    log,
                    "Skip DDL {} as creating the old temp table of RMV '{}', query {}",
                    entry_name,
                    parent_table_uuid,
                    create_query->formatForLogging());
                out_reason = fmt::format(
                    "Creating the old temp table '{}' of Refreshable Materialized View '{}'", create_query->uuid, parent_table_uuid);
                return {};
            }
        }
        else if (const auto * rename_query = task->query->as<ASTRenameQuery>())
        {
            if (rename_query->getElements().size() == 1)
            {
                const auto & element = rename_query->getElements().front();
                QualifiedTableName from_table{.database = element.from.getDatabase(), .table = element.from.getTable()};
                if (shouldSkipRenamingRMVTempTable(zookeeper, *task->entry.parent_table_uuid, from_table))
                {
                    LOG_INFO(
                        log,
                        "Skip DDL {} as renaming the old temp table of RMV '{}', query {}",
                        entry_name,
                        parent_table_uuid,
                        rename_query->formatForLogging());

                    out_reason = fmt::format(
                        "Renaming the old temp table '{}' of Refreshable Materialized View '{}'",
                        from_table.getFullName(),
                        parent_table_uuid);
                    return {};
                }
            }
        }
    }

    return task;
}

bool DatabaseReplicatedDDLWorker::checkParentTableExists(const UUID & uuid) const
{
    auto [db, table] = DatabaseCatalog::instance().tryGetByUUID(uuid);
    return db.get() == database && table != nullptr && !table->is_dropped.load() && !table->is_detached.load();
}

DDLReplicateTaskPtr DatabaseReplicatedDDLWorker::createTask(const String & name, const String & path) const
{
    return std::make_unique<DatabaseReplicatedTask>(name, path, database);
}

void DatabaseReplicatedDDLWorker::checkBeforeProcessEntry(DDLLogEntry & entry)
{
    if (entry.parent_table_uuid.has_value() && !checkParentTableExists(entry.parent_table_uuid.value()))
        throw Exception(ErrorCodes::TABLE_IS_DROPPED, "Parent table doesn't exist");
}

}
