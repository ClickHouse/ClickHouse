#include <Interpreters/SystemCommands/SystemCommandHelpers.h>

#include <Core/Settings.h>
#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Interpreters/AsynchronousInsertQueue.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>
#include <Interpreters/SystemLog.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/Freeze.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/ManualMergeSelector.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/ObjectStorageQueue/StorageObjectStorageQueue.h>
#include <Storages/StorageDistributed.h>
#include <Storages/StorageProxy.h>
#include <Common/CurrentThread.h>
#include <Common/DynamicDelay.h>
#include <Common/ThreadStatus.h>
#include <Common/logger_useful.h>

#include <chrono>
#include <thread>

#include "config.h"

#if USE_FILELOG
#include <Storages/FileLog/StorageFileLog.h>
#endif


namespace DB
{

namespace Setting
{
    extern const SettingsSeconds max_execution_time;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsMergeSelectorAlgorithm merge_selector_algorithm;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int QUERY_WAS_CANCELLED;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TIMEOUT_EXCEEDED;
}

namespace
{

MergeTreeData & getMergeTreeWithManualSelector(const StoragePtr & table, const StorageID & table_id, const char * action)
{
    auto resolved = resolveStorageProxyLoading(table);
    auto * merge_tree = castStorage<MergeTreeData>(resolved, DeferredTable::Load).get();
    if (!merge_tree)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Command {} is supported only for MergeTree-family tables, but got: {}",
            action, table->getName());

    const auto algorithm = (*merge_tree->getSettings())[MergeTreeSetting::merge_selector_algorithm].value;
    if (algorithm != MergeSelectorAlgorithm::MANUAL)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Command {} requires merge_selector_algorithm = 'manual' on table {}",
            action, table_id.getNameForLogs());

    return *merge_tree;
}

/// Commands that work with tables, disks and logs.
class StorageCommands : public SystemCommandContext
{
public:
    explicit StorageCommands(const SystemCommandContext & command_context) : SystemCommandContext(command_context) {}

    void restartDisk(const String & disk_name);
    void scheduleMerge(ASTSystemQuery & query);
    void syncMerges();
    void loadPrimaryKeys();
    void unloadPrimaryKeys();
    void loadOrUnloadPrimaryKeysImpl(bool load);
    void flushDistributed(ASTSystemQuery & query);
    void flushObjectStorageQueue(ASTSystemQuery & query);
    void resetFileLog(ASTSystemQuery & query);
};

void StorageCommands::restartDisk(const String & disk_name)
{
    getContext()->checkAccess(AccessType::SYSTEM_RESTART_DISK);

    auto disk = getContext()->getDisk(disk_name);

    /// A loaded `MergeTree` caches its set of active parts and never re-scans it on its own, so
    /// reloading the disk alone would not surface new parts until the table is re-attached. For
    /// every already-loaded table that uses this disk and whose data is entirely on readonly disks
    /// (i.e. a readonly replica of data written elsewhere), re-scan the data directory -- the same
    /// work the background `refresh_parts_interval` task performs. `refreshDataPartsOnce` reloads
    /// the disk metadata first (for a `plain_rewritable` object-storage disk this re-reads the path
    /// map, so directories written by another writer process become visible), passing 0 to bypass
    /// the time-based refresh throttle. Tables with a writable disk own their part set and must not
    /// have it rebuilt here.
    bool disk_refreshed = false;
    for (auto & elem : DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false}))
    {
        /// skip_not_loaded: act only on already-loaded tables, do not block on async loading.
        for (auto it = elem.second->getTablesIterator(getContext(), {}, /*skip_not_loaded=*/ true); it->isValid(); it->next())
        {
            auto * merge_tree = castStorage<MergeTreeData>(it->table(), DeferredTable::Skip).get();
            if (!merge_tree)
                continue;

            auto policy = merge_tree->getStoragePolicy();
            if (!policy->tryGetDiskByName(disk_name))
                continue;

            bool all_disks_are_readonly = true;
            for (const auto & table_disk : policy->getDisks())
            {
                if (!table_disk->isReadOnly())
                {
                    all_disks_are_readonly = false;
                    break;
                }
            }
            if (!all_disks_are_readonly)
                continue;

            merge_tree->refreshDataPartsOnce(/*interval_milliseconds=*/ 0);
            disk_refreshed = true;
        }
    }

    /// No readonly table re-scanned this disk above, but the user still asked to restart it, so
    /// reload its in-memory metadata directly. Passing 0 bypasses the time-based refresh throttle.
    if (!disk_refreshed)
        disk->refresh(/*not_sooner_than_milliseconds=*/ 0);
}

void StorageCommands::scheduleMerge(ASTSystemQuery & query)
{
    getContext()->checkAccess(AccessType::SYSTEM_MERGES, table_id);
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());
    auto & merge_tree = getMergeTreeWithManualSelector(table, table_id, "SCHEDULE MERGE");

    if (!query.scheduled_merge_parts || query.scheduled_merge_parts->children.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SCHEDULE MERGE requires at least one part name");

    Names parts_to_merge;
    parts_to_merge.reserve(query.scheduled_merge_parts->children.size());
    for (const auto & child : query.scheduled_merge_parts->children)
        parts_to_merge.emplace_back(child->as<ASTLiteral &>().value.safeGet<String>());

    ManualMergeSelector::push(table_id, parts_to_merge);
    merge_tree.triggerBackgroundOperations();
}

void StorageCommands::syncMerges()
{
    getContext()->checkAccess(AccessType::SYSTEM_MERGES, table_id);
    StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());
    auto & merge_tree = getMergeTreeWithManualSelector(table, table_id, "SYNC MERGES");

    getContext()->onQueryExecutionStarted();
    DynamicDelay poll_delay;
    poll_delay.setConfiguration(/*min_delay_=*/50, /*max_delay_=*/500, /*factor_up_=*/2.0, /*factor_lower_=*/1.0);

    const auto start = std::chrono::steady_clock::now();
    const auto max_execution_time_us = getContext()->getSettingsRef()[Setting::max_execution_time].totalMicroseconds();
    /// Compare the *elapsed* time against the timeout instead of building an absolute deadline:
    /// `now + max_execution_time` is not representable for the largest values `max_execution_time`
    /// accepts, and both capping the deadline at the end of the clock's range and clamping the
    /// timeout with `saturatedMilliseconds` (a one-year bound meant for a `wait_for` slice) would
    /// time the command out long before the configured limit.
    while (true)
    {
        if (max_execution_time_us != 0
            && std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count()
                >= max_execution_time_us)
            break;

        if (CurrentThread::isInitialized() && CurrentThread::get().isQueryCanceled())
            throw DB::Exception(DB::ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled");

        merge_tree.triggerBackgroundOperations();

        ActiveDataPartSet active_set;
        for (const auto & part : merge_tree.getDataPartsVectorForInternalUsage())
            if (part->version->isVisibleByLatestSnapshot())
                active_set.add(part->info, part->name);

        if (ManualMergeSelector::isAllScheduledPartsCovered(table_id, active_set))
            return;

        std::this_thread::sleep_for(std::chrono::milliseconds(poll_delay.getCurrentDelay()));
        poll_delay.up();
    }

    throw DB::Exception(DB::ErrorCodes::TIMEOUT_EXCEEDED, "SYNC MERGES {}: command timed out. See the 'max_execution_time' setting", table_id.getNameForLogs());
}

void StorageCommands::loadPrimaryKeys()
{
    loadOrUnloadPrimaryKeysImpl(true);
}

void StorageCommands::unloadPrimaryKeys()
{
    loadOrUnloadPrimaryKeysImpl(false);
}

void StorageCommands::loadOrUnloadPrimaryKeysImpl(bool load)
{
    if (!table_id.empty())
    {
        getContext()->checkAccess(load ? AccessType::SYSTEM_LOAD_PRIMARY_KEY : AccessType::SYSTEM_UNLOAD_PRIMARY_KEY, table_id.database_name, table_id.table_name);
        StoragePtr table = DatabaseCatalog::instance().getTable(table_id, getContext());

        if (auto * merge_tree = castStorage<MergeTreeData>(table, DeferredTable::Load).get())
        {
            LOG_TRACE(log, "{} primary keys for table {}", load ? "Loading" : "Unloading", table_id.getFullTableName());
            getContext()->onQueryExecutionStarted();
            load ? merge_tree->loadPrimaryKeys() : merge_tree->unloadPrimaryKeys();
        }
        else
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Command {} PRIMARY KEY is supported only for MergeTree tables, but got: {}", load ? "LOAD" : "UNLOAD", table->getName());
        }
    }
    else
    {
        getContext()->checkAccess(load ? AccessType::SYSTEM_LOAD_PRIMARY_KEY : AccessType::SYSTEM_UNLOAD_PRIMARY_KEY);
        LOG_TRACE(log, "{} primary keys for all tables", load ? "Loading" : "Unloading");

        getContext()->onQueryExecutionStarted();
        for (auto & database : DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false}))
        {
            for (auto it = database.second->getTablesIterator(getContext()); it->isValid(); it->next())
            {
                if (auto * merge_tree = castStorage<MergeTreeData>(it->table(), DeferredTable::Skip).get())
                {
                    load ? merge_tree->loadPrimaryKeys() : merge_tree->unloadPrimaryKeys();
                }
            }
        }
    }
}

void StorageCommands::flushDistributed(ASTSystemQuery & query)
{
    getContext()->checkAccess(AccessType::SYSTEM_FLUSH_DISTRIBUTED, table_id);

    SettingsChanges settings_changes;
    if (query.query_settings)
        settings_changes = query.query_settings->as<ASTSetQuery>()->changes;

    /// Keep the StoragePtr alive for the whole flush: the table holds no other owning
    /// reference here (DROP on an Atomic database does not take the exclusive drop_lock,
    /// and the flush does not hold an async-insert lock), so a concurrent DROP could
    /// otherwise destroy the table while flushClusterNodesAllData is still running.
    auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
    if (auto * storage_distributed = dynamic_cast<StorageDistributed *>(table.get()))
    {
        getContext()->onQueryExecutionStarted();
        storage_distributed->flushClusterNodesAllData(getContext(), settings_changes);
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table {} is not distributed", table_id.getNameForLogs());
}

void StorageCommands::flushObjectStorageQueue(ASTSystemQuery & query)
{
    auto context = getContext();
    context->checkAccess(AccessType::SYSTEM_FLUSH_OBJECT_STORAGE_QUEUE, table_id);

    if (query.queue_path.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PATH must be specified for SYSTEM FLUSH OBJECT STORAGE QUEUE");

    auto table = DatabaseCatalog::instance().getTable(table_id, context);
    auto queue = castStorage<StorageObjectStorageQueue>(table, DeferredTable::Load);
    if (!queue)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Table {} is not an S3Queue or AzureQueue table", table_id.getNameForLogs());

    context->onQueryExecutionStarted();
    queue->waitForPathToBeProcessed(query.queue_path, context);
}

void StorageCommands::resetFileLog([[maybe_unused]] ASTSystemQuery & query)
{
    getContext()->checkAccess(AccessType::SYSTEM_RESET_FILELOG, table_id);
#if USE_FILELOG
    auto file_log = castStorage<StorageFileLog>(DatabaseCatalog::instance().getTable(table_id, getContext()), DeferredTable::Load);
    if (!file_log)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table {} is not a FileLog table", table_id.getNameForLogs());
    std::optional<UInt64> offset = 0;
    if (query.filelog_file)
    {
        if (query.filelog_offset)
            offset = query.filelog_offset;
        else if (query.filelog_to_end)
            offset = std::nullopt;
    }
    file_log->resetReadPosition(query.filelog_file, offset);
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "The server was compiled without FileLog support");
#endif
}

}

void registerSystemCommandsStorage(SystemCommandFactory & factory)
{
    using Type = ASTSystemQuery::Type;

    auto register_command = [&](Type type, SystemCommandFactory::AccessOnClusterFn access_on_cluster, std::function<void(ASTSystemQuery &, StorageCommands &)> action)
    {
        factory.registerCommand(
            type,
            std::move(access_on_cluster),
            [action_fn = std::move(action)](ASTSystemQuery & query, SystemCommandContext & context)
            {
                StorageCommands commands(context);
                action_fn(query, commands);
                return BlockIO{};
            });
    };

    register_command(Type::SCHEDULE_MERGE, tableOrGlobalAccessOnCluster(AccessType::SYSTEM_MERGES), [](ASTSystemQuery & query, StorageCommands & commands)
    {
        commands.scheduleMerge(query);
    });

    register_command(Type::SYNC_MERGES, tableOrGlobalAccessOnCluster(AccessType::SYSTEM_MERGES), [](ASTSystemQuery &, StorageCommands & commands)
    {
        commands.syncMerges();
    });

    register_command(Type::LOAD_PRIMARY_KEY, tableOrGlobalAccessOnCluster(AccessType::SYSTEM_LOAD_PRIMARY_KEY), [](ASTSystemQuery &, StorageCommands & commands)
    {
        commands.loadPrimaryKeys();
    });

    register_command(Type::UNLOAD_PRIMARY_KEY, tableOrGlobalAccessOnCluster(AccessType::SYSTEM_UNLOAD_PRIMARY_KEY), [](ASTSystemQuery &, StorageCommands & commands)
    {
        commands.unloadPrimaryKeys();
    });

    register_command(Type::FLUSH_DISTRIBUTED, tableAccessOnCluster(AccessType::SYSTEM_FLUSH_DISTRIBUTED), [](ASTSystemQuery & query, StorageCommands & commands)
    {
        commands.flushDistributed(query);
    });

    register_command(Type::FLUSH_OBJECT_STORAGE_QUEUE, tableAccessOnCluster(AccessType::SYSTEM_FLUSH_OBJECT_STORAGE_QUEUE), [](ASTSystemQuery & query, StorageCommands & commands)
    {
        commands.flushObjectStorageQueue(query);
    });

    register_command(Type::RESET_FILELOG, tableAccessOnCluster(AccessType::SYSTEM_RESET_FILELOG), [](ASTSystemQuery & query, StorageCommands & commands)
    {
        commands.resetFileLog(query);
    });

    register_command(Type::RESTART_DISK, globalAccessOnCluster(AccessType::SYSTEM_RESTART_DISK), [](ASTSystemQuery & query, StorageCommands & commands)
    {
        commands.restartDisk(query.disk);
    });

    factory.registerSimpleCommand(Type::WAIT_BLOBS_CLEANUP, AccessType::SYSTEM_WAIT_BLOBS_CLEANUP, [](ASTSystemQuery & query, SystemCommandContext & context)
    {
        auto disk_ptr = context.getContext()->getDisk(query.disk);
        auto * object_disk = dynamic_cast<DiskObjectStorage *>(disk_ptr.get());
        if (!object_disk)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk '{}' is not an object storage disk", query.disk);

        /// We wait 2 times here because a background blob cleanup round may already be running
        /// and this query must guarantee that after it returns, all expected blobs have been cleaned up.
        context.getContext()->onQueryExecutionStarted();
        object_disk->waitBlobsCleanup();
        object_disk->waitBlobsCleanup();
    });

    factory.registerSimpleCommand(Type::FLUSH_LOGS, AccessType::SYSTEM_FLUSH_LOGS, [](ASTSystemQuery & query, SystemCommandContext & context)
    {
        auto system_logs = context.getContext()->getSystemLogs();
        context.getContext()->onQueryExecutionStarted();
        system_logs.flush(query.tables);
    });

    factory.registerSimpleCommand(Type::FLUSH_ASYNC_INSERT_QUEUE, AccessType::SYSTEM_FLUSH_ASYNC_INSERT_QUEUE, [](ASTSystemQuery & query, SystemCommandContext & context)
    {
        auto * queue = context.getContext()->tryGetAsynchronousInsertQueue();
        if (!queue)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot flush asynchronous insert queue because it is not initialized");

        std::vector<StorageID> tables;
        for (const auto & [database, table]: query.tables)
        {
            tables.push_back(context.getContext()->resolveStorageID({database, table}, Context::ResolveOrdinary));
            context.getContext()->getQueryContext()->addQueryAccessInfo(tables.back(), {});
        }

        context.getContext()->onQueryExecutionStarted();
        queue->flush(tables);
    });

    factory.registerCommand(
        Type::UNFREEZE,
        globalAccessOnCluster(AccessType::SYSTEM_UNFREEZE),
        [](ASTSystemQuery & query, SystemCommandContext & context)
        {
            context.getContext()->checkAccess(AccessType::SYSTEM_UNFREEZE);
            context.getContext()->onQueryExecutionStarted();
            /// The result contains information about deleted parts as a table. It is for compatibility with ALTER TABLE UNFREEZE query.
            return Unfreezer(context.getContext()).systemUnfreeze(query.backup_name);
        });
}

}
