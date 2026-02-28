#include <Storages/StorageMergeTree.h>

#include <optional>
#include <ranges>

#include <Backups/BackupEntriesCollector.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Names.h>
#include <Core/QueryProcessingStage.h>
#include <Core/Settings.h>
#include <Databases/IDatabase.h>
#include <Disks/supportWritingWithAppend.h>
#include <IO/SharedThreadPools.h>
#include <IO/copyData.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/TransactionLog.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTPartition.h>
#include <Planner/Utils.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/AlterCommands.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/Compaction/CompactionStatistics.h>
#include <Storages/MergeTree/Compaction/ConstructFuturePart.h>
#include <Storages/MergeTree/Compaction/MergePredicates/MergeTreeMergePredicate.h>
#include <Storages/MergeTree/Compaction/MergeSelectorApplier.h>
#include <Storages/MergeTree/Compaction/PartsCollectors/MergeTreePartsCollector.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MergePlainMergeTreeTask.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeMutationStatus.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeSink.h>
#include <Storages/MergeTree/MergeTreeSinkPatch.h>
#include <Storages/MergeTree/PatchParts/PatchPartsUtils.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/PartitionCommands.h>
#include <Storages/buildQueryTreeForShard.h>
#include <base/sleep.h>
#include <fmt/core.h>
#include <Common/CurrentThread.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/MemoryTracker.h>
#include <Common/ProfileEventsScope.h>
#include <Common/escapeForFileName.h>


namespace ProfileEvents
{
    extern const Event PatchesAcquireLockTries;
    extern const Event PatchesAcquireLockMicroseconds;
    extern const Event MergesRejectedByMemoryLimit;
}

namespace DB
{

namespace FailPoints
{
    extern const char storage_merge_tree_background_clear_old_parts_pause[];
    extern const char mt_merge_selecting_task_pause_when_scheduled[];
    extern const char mt_select_parts_to_mutate_no_free_threads[];
    extern const char mt_select_parts_to_mutate_max_part_size[];
    extern const char storage_shared_merge_tree_mutate_pause_before_wait[];
}

namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool allow_suspicious_primary_key;
    extern const SettingsUInt64 alter_sync;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsBool materialize_ttl_after_modify;
    extern const SettingsUInt64 max_expanded_ast_elements;
    extern const SettingsUInt64 max_partitions_per_insert_block;
    extern const SettingsUInt64 mutations_sync;
    extern const SettingsBool optimize_skip_merged_partitions;
    extern const SettingsBool optimize_throw_if_noop;
    extern const SettingsBool parallel_replicas_for_non_replicated_merge_tree;
    extern const SettingsBool throw_on_unsupported_query_inside_transaction;
    extern const SettingsUInt64 max_parts_to_move;
    extern const SettingsUpdateParallelMode update_parallel_mode;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool allow_experimental_replacing_merge_with_cleanup;
    extern const MergeTreeSettingsBool always_use_copy_instead_of_hardlinks;
    extern const MergeTreeSettingsBool assign_part_uuids;
    extern const MergeTreeSettingsDeduplicateMergeProjectionMode deduplicate_merge_projection_mode;
    extern const MergeTreeSettingsBool enable_replacing_merge_with_cleanup_for_min_age_to_force_merge;
    extern const MergeTreeSettingsUInt64 finished_mutations_to_keep;
    extern const MergeTreeSettingsSeconds lock_acquire_timeout_for_background_operations;
    extern const MergeTreeSettingsBool min_age_to_force_merge_on_partition_only;
    extern const MergeTreeSettingsUInt64 min_age_to_force_merge_seconds;
    extern const MergeTreeSettingsUInt64 max_number_of_merges_with_ttl_in_pool;
    extern const MergeTreeSettingsUInt64 max_postpone_time_for_failed_mutations_ms;
    extern const MergeTreeSettingsUInt64 merge_tree_clear_old_parts_interval_seconds;
    extern const MergeTreeSettingsUInt64 merge_tree_clear_old_temporary_directories_interval_seconds;
    extern const MergeTreeSettingsUInt64 non_replicated_deduplication_window;
    extern const MergeTreeSettingsSeconds temporary_directories_lifetime;
    extern const MergeTreeSettingsString auto_statistics_types;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int NOT_ENOUGH_SPACE;
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int CANNOT_ASSIGN_OPTIMIZE;
    extern const int TIMEOUT_EXCEEDED;
    extern const int UNKNOWN_POLICY;
    extern const int NO_SUCH_DATA_PART;
    extern const int ABORTED;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TABLE_IS_READ_ONLY;
    extern const int TOO_MANY_PARTS;
    extern const int PART_IS_LOCKED;
    extern const int PART_IS_TEMPORARILY_LOCKED;
}

namespace ActionLocks
{
    extern const StorageActionBlockType PartsMerge;
    extern const StorageActionBlockType PartsTTLMerge;
    extern const StorageActionBlockType PartsMove;
}

static MergeTreeTransactionPtr tryGetTransactionForMutation(const MergeTreeMutationEntry & mutation, LoggerPtr log = nullptr)
{
    assert(!mutation.tid.isEmpty());
    if (mutation.tid.isPrehistoric())
        return {};

    auto txn = TransactionLog::instance().tryGetRunningTransaction(mutation.tid.getHash());
    if (txn)
        return txn;

    if (log)
        LOG_WARNING(log, "Cannot find transaction {} which had started mutation {}, probably it finished", mutation.tid, mutation.file_name);

    return {};
}

static bool supportTransaction(const Disks & disks, LoggerPtr log)
{
    for (const auto & disk : disks)
    {
        if (!supportWritingWithAppend(disk))
        {
            LOG_DEBUG(log, "Disk {} does not support writing with append", disk->getName());
            return false;
        }
    }
    return true;
}

StorageMergeTree::StorageMergeTree(
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    LoadingStrictnessLevel mode,
    ContextMutablePtr context_,
    const String & date_column_name,
    const MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> storage_settings_)
    : MergeTreeData(
          table_id_,
          metadata_,
          context_,
          date_column_name,
          merging_params_,
          std::move(storage_settings_),
          false, /// require_part_metadata
          mode)
    , writer(*this)
    , merger_mutator(*this)
    , support_transaction(supportTransaction(getDisks(), log.load()))
{
    initializeDirectoriesAndFormatVersion(relative_data_path_, LoadingStrictnessLevel::ATTACH <= mode, date_column_name);

    loadDataParts(LoadingStrictnessLevel::FORCE_RESTORE <= mode, std::nullopt);

    if (mode < LoadingStrictnessLevel::ATTACH && !getDataPartsForInternalUsage().empty() && !isStaticStorage())
        throw Exception(ErrorCodes::INCORRECT_DATA,
                        "Data directory for table already containing data parts - probably "
                        "it was unclean DROP table or manual intervention. "
                        "You must either clear directory by hand or use ATTACH TABLE instead "
                        "of CREATE TABLE if you need to use those parts");

    increment.set(getMaxBlockNumber());

    loadMutations();
    loadDeduplicationLog();
    prewarmCaches(getActivePartsLoadingThreadPool().get(), getCachesToPrewarm(0));
}


void StorageMergeTree::startup()
{
    /// Do not schedule any background jobs if current storage has static data files.
    if (isStaticStorage())
        return;

    clearEmptyParts();

    /// Temporary directories contain incomplete results of merges (after forced restart)
    ///  and don't allow to reinitialize them, so delete each of them immediately
    clearOldTemporaryDirectories(0, {"tmp_", "delete_tmp_", "tmp-fetch_"});

    /// NOTE background task will also do the above cleanups periodically.
    time_after_previous_cleanup_parts.restart();
    time_after_previous_cleanup_temporary_directories.restart();

    try
    {
        background_operations_assignee.start();
        startBackgroundMovesIfNeeded();
        startOutdatedAndUnexpectedDataPartsLoadingTask();
        startStatisticsCache();
    }
    catch (...)
    {
        /// Exception safety: failed "startup" does not require a call to "shutdown" from the caller.
        /// And it should be able to safely destroy table after exception in "startup" method.
        /// It means that failed "startup" must not create any background tasks that we will have to wait.
        try
        {
            shutdown(false);
        }
        catch (...)
        {
            std::terminate();
        }

        /// Note: after failed "startup", the table will be in a state that only allows to destroy the object.
        throw;
    }
}

void StorageMergeTree::flushAndPrepareForShutdown()
{
    LOG_TRACE(log, "Start preparing for shutdown");

    if (flush_called.exchange(true))
        return;

    merger_mutator.merges_blocker.cancelForever();
    parts_mover.moves_blocker.cancelForever();

    background_operations_assignee.finish();
    background_moves_assignee.finish();

    LOG_TRACE(log, "Finished preparing for shutdown");
}

void StorageMergeTree::shutdown(bool)
{
    if (shutdown_called.exchange(true))
        return;

    if (refresh_parts_task)
        refresh_parts_task->deactivate();

    if (refresh_stats_task)
        refresh_stats_task->deactivate();

    stopOutdatedAndUnexpectedDataPartsLoadingTask();

    /// Unlock all waiting mutations
    {
        std::lock_guard lock(mutation_wait_mutex);
        mutation_wait_event.notify_all();
    }

    flushAndPrepareForShutdown();

    if (deduplication_log)
        deduplication_log->shutdown();
}


StorageMergeTree::~StorageMergeTree()
{
    shutdown(false);
}

void StorageMergeTree::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    const auto & settings = local_context->getSettingsRef();
    /// reading step for parallel replicas with the analyzer is built in Planner, so don't do it here
    if (local_context->canUseParallelReplicasOnInitiator() && settings[Setting::parallel_replicas_for_non_replicated_merge_tree]
        && !settings[Setting::allow_experimental_analyzer])
    {
        ClusterProxy::executeQueryWithParallelReplicas(
            query_plan, getStorageID(), processed_stage, query_info.query, local_context, query_info.storage_limits);
        return;
    }

    if (local_context->canUseParallelReplicasCustomKey() && settings[Setting::parallel_replicas_for_non_replicated_merge_tree]
        && !settings[Setting::allow_experimental_analyzer] && local_context->getClientInfo().distributed_depth == 0)
    {
        auto cluster = local_context->getClusterForParallelReplicas();
        if (local_context->canUseParallelReplicasCustomKeyForCluster(*cluster))
        {
            auto modified_query_info = query_info;
            modified_query_info.cluster = std::move(cluster);
            ClusterProxy::executeQueryWithParallelReplicasCustomKey(
                query_plan,
                getStorageID(),
                std::move(modified_query_info),
                getInMemoryMetadataPtr()->getColumns(),
                storage_snapshot,
                processed_stage,
                query_info.query,
                local_context);
            return;
        }
        LOG_WARNING(
            log,
            "Parallel replicas with custom key will not be used because cluster defined by 'cluster_for_parallel_replicas' ('{}') has "
            "multiple shards",
            cluster->getName());
    }

    const bool enable_parallel_reading = local_context->canUseParallelReplicasOnFollower()
        && local_context->getSettingsRef()[Setting::parallel_replicas_for_non_replicated_merge_tree];

    auto plan = MergeTreeDataSelectExecutor(*this).read(
        column_names,
        storage_snapshot,
        query_info,
        local_context,
        max_block_size,
        num_streams,
        local_context->getPartitionIdToMaxBlock(getStorageID().uuid),
        enable_parallel_reading);

    if (plan)
        query_plan = std::move(*plan);
}

std::optional<UInt64> StorageMergeTree::totalRows(ContextPtr) const
{
    return getTotalActiveSizeInRows();
}

std::optional<UInt64> StorageMergeTree::totalRowsByPartitionPredicate(const ActionsDAG & filter_actions_dag, ContextPtr local_context) const
{
    auto parts = getVisibleDataPartsVector(local_context);
    return totalRowsByPartitionPredicateImpl(filter_actions_dag, local_context, RangesInDataParts(parts));
}

std::optional<UInt64> StorageMergeTree::totalBytes(ContextPtr) const
{
    return getTotalActiveSizeInBytes();
}

std::optional<UInt64> StorageMergeTree::totalBytesUncompressed(const Settings &) const
{
    UInt64 res = 0;
    auto parts = getDataPartsForInternalUsage();
    for (const auto & part : parts)
        res += part->getBytesUncompressedOnDisk();
    return res;
}

SinkToStoragePtr
StorageMergeTree::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool /*async_insert*/)
{
    assertNotReadonly();

    const auto & settings = local_context->getSettingsRef();
    return std::make_shared<MergeTreeSink>(*this, metadata_snapshot, settings[Setting::max_partitions_per_insert_block], local_context);
}

void StorageMergeTree::drop()
{
    shutdown(true);
    dropAllData();
}

void StorageMergeTree::alter(
    const AlterCommands & commands,
    ContextPtr local_context,
    AlterLockHolder & table_lock_holder)
{
    assertNotReadonly();

    if (local_context->getCurrentTransaction() && local_context->getSettingsRef()[Setting::throw_on_unsupported_query_inside_transaction])
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ALTER METADATA is not supported inside transactions");

    auto table_id = getStorageID();
    auto old_storage_settings = getSettings();
    const auto & query_settings = local_context->getSettingsRef();

    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    StorageInMemoryMetadata old_metadata = getInMemoryMetadata();

    auto maybe_mutation_commands = commands.getMutationCommands(new_metadata, query_settings[Setting::materialize_ttl_after_modify], local_context);
    if (!maybe_mutation_commands.empty())
        delayMutationOrThrowIfNeeded(nullptr, local_context);

    Int64 mutation_version = -1;

    removeImplicitStatistics(new_metadata.columns);
    commands.apply(new_metadata, local_context);

    auto [auto_statistics_types, statistics_changed] = MergeTreeData::getNewImplicitStatisticsTypes(new_metadata, *old_storage_settings);
    addImplicitStatistics(new_metadata.columns, auto_statistics_types);

    if (!query_settings[Setting::allow_suspicious_primary_key])
        MergeTreeData::verifySortingKey(new_metadata.sorting_key);

    /// This alter can be performed at new_metadata level only
    if (commands.isSettingsAlter())
    {
        changeSettings(new_metadata.settings_changes, table_lock_holder);

        if (statistics_changed)
            setInMemoryMetadata(new_metadata);

        /// It is safe to ignore exceptions here as only settings are changed, which is not validated in `alterTable`
        DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(local_context, table_id, new_metadata, /*validate_new_create_query=*/true);
    }
    else if (commands.isCommentAlter())
    {
        setInMemoryMetadata(new_metadata);
        /// It is safe to ignore exceptions here as only the comment changed, which is not validated in `alterTable`
        DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(local_context, table_id, new_metadata, /*validate_new_create_query=*/true);
    }
    else
    {
        if (!maybe_mutation_commands.empty() && maybe_mutation_commands.containBarrierCommand())
        {
            int64_t prev_mutation = 0;
            {
                std::lock_guard lock(currently_processing_in_background_mutex);
                auto it = current_mutations_by_version.rbegin();
                if (it != current_mutations_by_version.rend())
                    prev_mutation = it->first;
            }

            /// Always wait previous mutations synchronously, because alters
            /// should be executed in sequential order.
            if (prev_mutation != 0)
            {
                LOG_DEBUG(log, "Cannot change metadata with barrier alter query, will wait for mutation {}", prev_mutation);
                waitForMutation(prev_mutation, /* from_another_mutation */ true);
                LOG_DEBUG(log, "Mutation {} finished", prev_mutation);
            }
        }

        {
            changeSettings(new_metadata.settings_changes, table_lock_holder);
            checkTTLExpressions(new_metadata, old_metadata);

            /// Reinitialize primary key because primary key column types might have changed.
            setProperties(new_metadata, old_metadata, false, local_context);

            try
            {
                DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(local_context, table_id, new_metadata, /*validate_new_create_query=*/true);
            }
            catch (...)
            {
                LOG_ERROR(log, "Failed to alter table in database, reverting changes");
                changeSettings(old_metadata.settings_changes, table_lock_holder);
                setProperties(old_metadata, new_metadata, false, local_context);
                throw;
            }

            {
                auto parts_lock = lockParts();
                resetSerializationHints(parts_lock);
            }

            if (!maybe_mutation_commands.empty())
                mutation_version = startMutation(maybe_mutation_commands, local_context);
        }

        if (!maybe_mutation_commands.empty() && query_settings[Setting::alter_sync] > 0)
            waitForMutation(mutation_version, false);
    }

    {
        /// Some additional changes in settings
        auto new_storage_settings = getSettings();

        if ((*old_storage_settings)[MergeTreeSetting::non_replicated_deduplication_window] != (*new_storage_settings)[MergeTreeSetting::non_replicated_deduplication_window])
        {
            /// We cannot place this check into settings sanityCheck because it depends on format_version.
            /// sanityCheck must work event without storage.
            if ((*new_storage_settings)[MergeTreeSetting::non_replicated_deduplication_window] != 0 && format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deduplication for non-replicated MergeTree in old syntax is not supported");

            deduplication_log->setDeduplicationWindowSize((*new_storage_settings)[MergeTreeSetting::non_replicated_deduplication_window]);
        }
    }
}


/// While exists, marks parts as 'currently_merging_mutating_parts' and reserves free space on filesystem.
CurrentlyMergingPartsTagger::CurrentlyMergingPartsTagger(
    FutureMergedMutatedPartPtr future_part_,
    size_t total_size,
    StorageMergeTree & storage_,
    const StorageMetadataPtr & metadata_snapshot,
    bool is_mutation)
    : future_part(future_part_), storage(storage_)
{
    /// Assume mutex is already locked, because this method is called from mergeTask.

    /// if we mutate part, than we should reserve space on the same disk, because mutations possible can create hardlinks
    if (is_mutation)
    {
        reserved_space = StorageMergeTree::tryReserveSpace(total_size, future_part->parts[0]->getDataPartStorage());
    }
    else
    {
        IMergeTreeDataPart::TTLInfos ttl_infos;
        size_t max_volume_index = 0;
        for (auto & part_ptr : future_part->parts)
        {
            ttl_infos.update(part_ptr->ttl_infos);
            auto disk_name = part_ptr->getDataPartStorage().getDiskName();
            size_t volume_index = storage.getStoragePolicy()->getVolumeIndexByDiskName(disk_name);
            max_volume_index = std::max(max_volume_index, volume_index);
        }

        reserved_space = storage.balancedReservation(
            metadata_snapshot,
            total_size,
            max_volume_index,
            future_part->name,
            future_part->part_info,
            future_part->parts,
            &tagger,
            &ttl_infos);

        if (!reserved_space)
            reserved_space
                = storage.tryReserveSpacePreferringTTLRules(metadata_snapshot, total_size, ttl_infos, time(nullptr), max_volume_index);
    }

    if (!reserved_space)
    {
        if (is_mutation)
            throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "Not enough space for mutating part '{}'", future_part->parts[0]->name);
        throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "Not enough space for merging parts");
    }

    future_part->updatePath(storage, reserved_space.get());

    for (const auto & part : future_part->parts)
    {
        if (storage.currently_merging_mutating_parts.contains(part))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Tagging already tagged part {}. This is a bug.", part->name);
    }
    storage.currently_merging_mutating_parts.insert(future_part->parts.begin(), future_part->parts.end());

    if (is_mutation)
        storage.currently_mutating_part_future_versions[future_part->parts[0]] = future_part->part_info.mutation;
}


void CurrentlyMergingPartsTagger::finalize()
{
    std::lock_guard lock(storage.currently_processing_in_background_mutex);
    finalized = true;

    for (const auto & part : future_part->parts)
    {
        if (!storage.currently_merging_mutating_parts.contains(part))
            std::terminate();
        storage.currently_merging_mutating_parts.erase(part);
        storage.currently_mutating_part_future_versions.erase(part);
    }

    storage.currently_processing_in_background_condition.notify_all();
}

CurrentlyMergingPartsTagger::~CurrentlyMergingPartsTagger()
{
    if (!finalized)
    {
        LOG_WARNING(getLogger("CurrentlyMergingPartsTagger"), "CurrentlyMergingPartsTagger was not finalized, the finalization will happen in the destructor");
        finalize();
    }

}

void MergeMutateSelectedEntry::finalize()
{
    finalized = true;
    tagger->finalize();
}

MergeMutateSelectedEntry::~MergeMutateSelectedEntry()
{
    if (!finalized)
    {
        LOG_WARNING(getLogger("MergeMutateSelectedEntry"), "MergeMutateSelectedEntry was not finalized, the finalization will happen in the destructor");
        finalize();
    }
}

Int64 StorageMergeTree::startMutation(const MutationCommands & commands, ContextPtr query_context)
{
    /// Choose any disk, because when we load mutations we search them at each disk
    /// where storage can be placed. See loadMutations().
    auto disk = getStoragePolicy()->getAnyDisk();
    TransactionID current_tid = Tx::PrehistoricTID;
    String additional_info;
    auto txn = query_context->getCurrentTransaction();
    if (txn)
    {
        current_tid = txn->tid;
        additional_info = fmt::format(" (TID: {}; TIDH: {})", current_tid, current_tid.getHash());
    }

    MergeTreeMutationEntry entry(commands, disk, relative_data_path, insert_increment.get(), current_tid, getContext()->getWriteSettings());
    auto block_holder = allocateBlockNumber(CommittingBlock::Op::Mutation);

    Int64 version = block_holder->block.number;
    entry.commit(version);
    String mutation_id = entry.file_name;
    if (txn)
        txn->addMutation(shared_from_this(), mutation_id);

    {
        std::lock_guard lock(currently_processing_in_background_mutex);

        auto [it, inserted] = current_mutations_by_version.try_emplace(version, std::move(entry));
        if (!inserted)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Mutation {} already exists, it's a bug", version);

        incrementMutationsCounters(mutation_counters, *it->second.commands);
    }

    LOG_INFO(log, "Added mutation: {}{}", mutation_id, additional_info);
    background_operations_assignee.trigger();
    return version;
}


void StorageMergeTree::updateMutationEntriesErrors(FutureMergedMutatedPartPtr result_part, bool is_successful, const String & exception_message, const String & error_code_name)
{
    /// Update the information about failed parts in the system.mutations table.

    Int64 sources_data_version = result_part->parts.at(0)->info.getDataVersion();
    Int64 result_data_version = result_part->part_info.getDataVersion();
    auto & failed_part = result_part->parts.at(0);

    if (sources_data_version != result_data_version)
    {
        std::lock_guard lock(currently_processing_in_background_mutex);
        auto mutations_begin_it = current_mutations_by_version.upper_bound(sources_data_version);
        auto mutations_end_it = current_mutations_by_version.upper_bound(result_data_version);

        for (auto it = mutations_begin_it; it != mutations_end_it; ++it)
        {
            MergeTreeMutationEntry & entry = it->second;
            if (is_successful)
            {
                if (!entry.latest_failed_part.empty() && result_part->part_info.contains(entry.latest_failed_part_info))
                {
                    entry.latest_failed_part.clear();
                    entry.latest_failed_part_info = MergeTreePartInfo();
                    entry.latest_fail_time = 0;
                    entry.latest_fail_reason.clear();
                    entry.latest_fail_error_code_name.clear();
                    if (static_cast<UInt64>(result_part->part_info.mutation) == it->first)
                        mutation_backoff_policy.removePartFromFailed(failed_part->name);
                }
            }
            else
            {
                entry.latest_failed_part = failed_part->name;
                entry.latest_failed_part_info = failed_part->info;
                entry.latest_fail_time = time(nullptr);
                entry.latest_fail_reason = exception_message;
                entry.latest_fail_error_code_name = error_code_name;

                if (static_cast<UInt64>(result_part->part_info.mutation) == it->first)
                {
                    mutation_backoff_policy.addPartMutationFailure(failed_part->name, (*getSettings())[MergeTreeSetting::max_postpone_time_for_failed_mutations_ms]);
                }
            }
        }
    }

    std::unique_lock lock(mutation_wait_mutex);
    mutation_wait_event.notify_all();
}

void StorageMergeTree::waitForMutation(Int64 version, bool wait_for_another_mutation)
{
    String mutation_id = MergeTreeMutationEntry::versionToFileName(version);
    waitForMutation(version, mutation_id, wait_for_another_mutation);
}

void StorageMergeTree::waitForMutation(const String & mutation_id, bool wait_for_another_mutation)
{
    Int64 version = MergeTreeMutationEntry::parseFileName(mutation_id);
    waitForMutation(version, mutation_id, wait_for_another_mutation);
}

void StorageMergeTree::waitForMutation(Int64 version, const String & mutation_id, bool wait_for_another_mutation)
{
    LOG_INFO(log, "Waiting mutation: {}", mutation_id);
    {
        auto check = [version, wait_for_another_mutation, this]()
        {
            if (shutdown_called)
                return true;
            auto mutation_status = getIncompleteMutationsStatus(version, nullptr, wait_for_another_mutation);
            return !mutation_status || mutation_status->is_done || !mutation_status->latest_fail_reason.empty();
        };

        /// Get the process list element to check for query cancellation while waiting.
        QueryStatusPtr process_list_element;
        if (CurrentThread::isInitialized())
        {
            auto query_context = CurrentThread::get().getQueryContext();
            if (query_context)
                process_list_element = query_context->getProcessListElement();
        }

        std::unique_lock lock(mutation_wait_mutex);
        while (!check())
        {
            mutation_wait_event.wait_for(lock, std::chrono::seconds(1));

            /// Check if the query was cancelled while we were waiting.
            if (process_list_element)
                process_list_element->checkTimeLimit();
        }
    }

    /// At least we have our current mutation
    std::set<String> mutation_ids;
    mutation_ids.insert(mutation_id);

    auto mutation_status = getIncompleteMutationsStatus(version, &mutation_ids, wait_for_another_mutation);
    checkMutationStatus(mutation_status, mutation_ids);

    LOG_INFO(log, "Mutation {} done", mutation_id);
}

void StorageMergeTree::setMutationCSN(const String & mutation_id, CSN csn)
{
    LOG_INFO(log, "Writing CSN {} for mutation {}", csn, mutation_id);
    UInt64 version = MergeTreeMutationEntry::parseFileName(mutation_id);

    std::lock_guard lock(currently_processing_in_background_mutex);
    auto it = current_mutations_by_version.find(version);
    if (it == current_mutations_by_version.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find mutation {}", mutation_id);
    it->second.writeCSN(csn);
}

void StorageMergeTree::mutate(const MutationCommands & commands, ContextPtr query_context)
{
    assertNotReadonly();

    delayMutationOrThrowIfNeeded(nullptr, query_context);

    /// Validate partition IDs (if any) before starting mutation
    getPartitionIdsAffectedByCommands(commands, query_context);

    Int64 version;
    {
        /// It's important to serialize order of mutations with alter queries because
        /// they can depend on each other.
        if (auto alter_lock = tryLockForAlter(query_context->getSettingsRef()[Setting::lock_acquire_timeout]); alter_lock == std::nullopt)
        {
            throw Exception(
                ErrorCodes::TIMEOUT_EXCEEDED,
                "Cannot start mutation in {}ms because some metadata-changing ALTER (MODIFY|RENAME|ADD|DROP) is currently executing. "
                "You can change this timeout with `lock_acquire_timeout` setting",
                query_context->getSettingsRef()[Setting::lock_acquire_timeout].totalMilliseconds());
        }
        version = startMutation(commands, query_context);
    }

    if (query_context->getSettingsRef()[Setting::mutations_sync] > 0 || query_context->getCurrentTransaction())
    {
        FailPointInjection::pauseFailPoint(FailPoints::storage_shared_merge_tree_mutate_pause_before_wait);
        waitForMutation(version, false);
    }
}

std::unique_ptr<PlainLightweightUpdateLock> StorageMergeTree::getLockForLightweightUpdate(const MutationCommands & commands, const ContextPtr & local_context)
{
    auto update_lock = std::make_unique<PlainLightweightUpdateLock>();
    auto parallel_mode = local_context->getSettingsRef()[Setting::update_parallel_mode];
    auto timeout_ms = local_context->getSettingsRef()[Setting::lock_acquire_timeout].totalMilliseconds();

    if (parallel_mode == UpdateParallelMode::SYNC)
    {
        ProfileEvents::increment(ProfileEvents::PatchesAcquireLockTries);
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::PatchesAcquireLockMicroseconds);

        update_lock->sync_lock = std::unique_lock(lightweight_updates_sync.sync_mutex, std::defer_lock);
        bool res = update_lock->sync_lock.try_lock_for(std::chrono::milliseconds(timeout_ms));

        if (!res)
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Failed to get lock in {} ms for lightwegiht update with sync mode", timeout_ms);

        LOG_TRACE(log, "Got lock for lightweight update in sync mode");
    }
    else if (parallel_mode == UpdateParallelMode::AUTO)
    {
        ProfileEvents::increment(ProfileEvents::PatchesAcquireLockTries);
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::PatchesAcquireLockMicroseconds);

        auto affected_columns = getUpdateAffectedColumns(commands, local_context);
        lightweight_updates_sync.lockColumns(affected_columns, timeout_ms);

        update_lock->affected_columns = std::move(affected_columns);
        update_lock->lightweight_updates_sync = &lightweight_updates_sync;

        LOG_TRACE(log, "Got lock for lightweight update in auto mode");
    }

    return update_lock;
}

QueryPipeline StorageMergeTree::updateLightweight(const MutationCommands & commands, ContextPtr query_context)
{
    assertNotReadonly();
    auto context_copy = Context::createCopy(query_context);

    PlainLightweightUpdateHolder update_holder;
    update_holder.update_lock = getLockForLightweightUpdate(commands, context_copy);
    update_holder.block_holder = allocateBlockNumber(CommittingBlock::Op::Update);

    auto all_partitions = getAllPartitionIds();
    auto partition_id_to_max_block = std::make_shared<PartitionIdToMaxBlock>();
    UInt64 block_number = update_holder.block_holder->block.number;

    size_t timeout_ms = context_copy->getSettingsRef()[Setting::lock_acquire_timeout].totalMilliseconds();
    waitForCommittingInsertsAndMutations(block_number, timeout_ms);

    for (const auto & partition_id : all_partitions)
    {
        if (!partition_id.starts_with(MergeTreePartInfo::PATCH_PART_PREFIX))
            partition_id_to_max_block->emplace(partition_id, block_number);
    }

    context_copy->setPartitionIdToMaxBlock(getStorageID().uuid, std::move(partition_id_to_max_block));
    /// Updates currently don't work with parallel replicas.
    context_copy->setSetting("max_parallel_replicas", Field(1));

    auto pipeline = updateLightweightImpl(commands, context_copy);
    auto patch_metadata = DB::getPatchPartMetadata(pipeline.getHeader(), context_copy);
    auto sink = std::make_shared<MergeTreeSinkPatch>(*this, std::move(patch_metadata), std::move(update_holder), context_copy);

    chassert(!pipeline.completed());
    pipeline.complete(std::move(sink));
    return pipeline;
}

bool StorageMergeTree::hasLightweightDeletedMask() const
{
    return has_lightweight_delete_parts.load(std::memory_order_relaxed);
}

namespace
{

struct PartVersionWithName
{
    Int64 version;
    String name;
};

bool comparator(const PartVersionWithName & f, const PartVersionWithName & s)
{
    return f.version < s.version;
}

}

std::optional<MergeTreeMutationStatus> StorageMergeTree::getIncompleteMutationsStatus(
    Int64 mutation_version, std::set<String> * mutation_ids, bool from_another_mutation) const
{
    std::unique_lock lock(currently_processing_in_background_mutex);
    return getIncompleteMutationsStatusUnlocked(mutation_version, lock, mutation_ids, from_another_mutation);
}

std::optional<MergeTreeMutationStatus> StorageMergeTree::getIncompleteMutationsStatusUnlocked(
    Int64 mutation_version, std::unique_lock<std::mutex> & /*lock*/, std::set<String> * mutation_ids, bool from_another_mutation) const
{
    auto current_mutation_it = current_mutations_by_version.find(mutation_version);
    /// Killed
    if (current_mutation_it == current_mutations_by_version.end())
        return {};

    MergeTreeMutationStatus result{.is_done = false};

    const auto & mutation_entry = current_mutation_it->second;

    auto txn = tryGetTransactionForMutation(mutation_entry, log.load());
    /// There's no way a transaction may finish before a mutation that was started by the transaction.
    /// But sometimes we need to check status of an unrelated mutation, in this case we don't care about transactions.
    assert(txn || mutation_entry.tid.isPrehistoric() || from_another_mutation);

    /// Check deadlock: if this mutation belongs to a transaction, check if there are
    /// intermediate mutations between it and an earlier mutation from the same transaction
    /// Scenario:
    /// 1. mutation_1 (txn 1)  is submitted
    /// 2. mutation_2 (no txn) is submitted - will wait for mutation_1 (txn 1) to commit or rollback
    /// 3. mutation_3 (txn 1)  is submitted - will wait for mutation_2 to finish: Deadlock!
    if (txn && !from_another_mutation)
    {
        /// Scan backwards to find the most recent mutation from the same transaction
        for (auto it = current_mutation_it; it != current_mutations_by_version.begin();)
        {
            --it;

            const auto & earlier_mutation = it->second;
            if (earlier_mutation.tid == mutation_entry.tid)
            {
                if (++it != current_mutation_it)
                {
                    result.latest_failed_part = "";
                    result.latest_fail_reason = fmt::format(
                        "Deadlock detected: mutation {} in transaction {} depends on earlier mutation {} "
                        "from the same transaction with intermediate mutations in between. ",
                        mutation_entry.file_name, mutation_entry.tid, earlier_mutation.file_name);
                    result.latest_fail_error_code_name = ErrorCodes::getName(ErrorCodes::LOGICAL_ERROR);
                    result.latest_fail_time = time(nullptr);
                    return result;
                }

                break;
            }
        }
    }

    auto data_parts = getVisibleDataPartsVector(txn);
    for (const auto & data_part : data_parts)
    {
        Int64 data_version = data_part->info.getDataVersion();
        if (data_version < mutation_version)
        {
            if (!mutation_entry.latest_fail_reason.empty())
            {
                result.latest_failed_part = mutation_entry.latest_failed_part;
                result.latest_fail_reason = mutation_entry.latest_fail_reason;
                result.latest_fail_error_code_name = mutation_entry.latest_fail_error_code_name;
                result.latest_fail_time = mutation_entry.latest_fail_time;

                /// Fill all mutations which failed with the same error
                /// (we can execute several mutations together)
                if (mutation_ids)
                {
                    auto mutations_begin_it = current_mutations_by_version.upper_bound(data_version);

                    for (auto it = mutations_begin_it; it != current_mutations_by_version.end(); ++it)
                        /// All mutations with the same failure
                        if (it->second.latest_fail_reason == result.latest_fail_reason)
                            mutation_ids->insert(it->second.file_name);
                }
            }
            else if (txn && !from_another_mutation)
            {
                /// Part is locked by concurrent transaction, most likely it will never be mutated
                TIDHash part_locked = data_part->version.removal_tid_lock.load();
                if (part_locked && part_locked != mutation_entry.tid.getHash())
                {
                    result.latest_failed_part = data_part->name;
                    result.latest_fail_reason = fmt::format("Serialization error: part {} is locked by transaction {}", data_part->name, part_locked);
                    result.latest_fail_error_code_name = ErrorCodes::getName(ErrorCodes::PART_IS_LOCKED);
                    result.latest_fail_time = time(nullptr);
                }
            }

            return result;
        }
    }

    result.is_done = true;
    return result;
}

std::map<std::string, MutationCommands> StorageMergeTree::getUnfinishedMutationCommands() const
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    std::vector<PartVersionWithName> part_versions_with_names;
    auto data_parts = getDataPartsVectorForInternalUsage();
    part_versions_with_names.reserve(data_parts.size());
    for (const auto & part : data_parts)
        part_versions_with_names.emplace_back(PartVersionWithName{part->info.getDataVersion(), part->name});
    std::sort(part_versions_with_names.begin(), part_versions_with_names.end(), comparator);

    std::map<std::string, MutationCommands> result;

    for (const auto & [mutation_version, entry] : current_mutations_by_version)
    {
        const PartVersionWithName needle{static_cast<Int64>(mutation_version), ""};
        auto versions_it = std::lower_bound(
            part_versions_with_names.begin(), part_versions_with_names.end(), needle, comparator);

        size_t parts_to_do = versions_it - part_versions_with_names.begin();
        if (parts_to_do > 0)
            result.emplace(entry.file_name, *entry.commands);
    }
    return result;
}

std::vector<MergeTreeMutationStatus> StorageMergeTree::getMutationsStatus() const
{
    std::lock_guard lock(currently_processing_in_background_mutex);

    std::vector<PartVersionWithName> part_versions_with_names;
    auto data_parts = getDataPartsVectorForInternalUsage();
    part_versions_with_names.reserve(data_parts.size());
    for (const auto & part : data_parts)
        part_versions_with_names.emplace_back(PartVersionWithName{part->info.getDataVersion(), part->name});
    std::sort(part_versions_with_names.begin(), part_versions_with_names.end(), comparator);

    std::vector<MergeTreeMutationStatus> result;
    for (const auto & kv : current_mutations_by_version)
    {
        Int64 mutation_version = kv.first;
        const MergeTreeMutationEntry & entry = kv.second;
        const PartVersionWithName needle{mutation_version, ""};
        auto versions_it = std::lower_bound(
            part_versions_with_names.begin(), part_versions_with_names.end(), needle, comparator);

        size_t parts_to_do = versions_it - part_versions_with_names.begin();
        Names parts_to_do_names;
        parts_to_do_names.reserve(parts_to_do);
        for (size_t i = 0; i < parts_to_do; ++i)
            parts_to_do_names.push_back(part_versions_with_names[i].name);

        std::map<String, Int64> block_numbers_map({{"", entry.block_number}});

        Names parts_in_progress_names;
        for (const auto &[part, future_version] : currently_mutating_part_future_versions)
        {
            if (part->info.getDataVersion() < mutation_version && future_version >= mutation_version)
                parts_in_progress_names.push_back(part->name);
        }

        std::map<String, String> parts_postpone_reasons_map;
        if (!parts_to_do_names.empty())
        {
            for (const auto &[part_name, postpone_reason] : current_parts_postpone_reasons)
            {
                if (part_name == PostponeReasons::ALL_PARTS_KEY)
                {
                    parts_postpone_reasons_map[part_name] = postpone_reason;
                    chassert(current_parts_postpone_reasons.size() == 1);
                }
                else
                {
                    auto part_info = MergeTreePartInfo::fromPartName(part_name, format_version);
                    if (part_info.getDataVersion() < mutation_version)
                        parts_postpone_reasons_map[part_name] = postpone_reason;
                }
            }
        }

        for (const MutationCommand & command : *entry.commands)
        {
            result.push_back(MergeTreeMutationStatus
            {
                entry.file_name,
                command.ast->formatWithSecretsOneLine(),
                entry.create_time,
                block_numbers_map,
                parts_in_progress_names,
                parts_to_do_names,
                parts_postpone_reasons_map,
                /* is_done = */parts_to_do_names.empty(),
                entry.latest_failed_part,
                entry.latest_fail_time,
                entry.latest_fail_reason,
                entry.latest_fail_error_code_name,
            });
        }
    }

    return result;
}

CancellationCode StorageMergeTree::killMutation(const String & mutation_id)
{
    assertNotReadonly();

    LOG_TRACE(log, "Killing mutation {}", mutation_id);
    UInt64 mutation_version = MergeTreeMutationEntry::tryParseFileName(mutation_id);
    if (!mutation_version)
        return CancellationCode::NotFound;

    std::optional<MergeTreeMutationEntry> to_kill;
    {
        std::lock_guard lock(currently_processing_in_background_mutex);

        auto it = current_mutations_by_version.find(mutation_version);
        if (it != current_mutations_by_version.end())
        {
            if (!it->second.is_done)
                decrementMutationsCounters(mutation_counters, *it->second.commands);

            to_kill.emplace(std::move(it->second));
            current_mutations_by_version.erase(it);
        }
    }

    mutation_backoff_policy.resetMutationFailures();

    if (!to_kill)
        return CancellationCode::NotFound;

    if (auto txn = tryGetTransactionForMutation(*to_kill, log.load()))
    {
        LOG_TRACE(log, "Cancelling transaction {} which had started mutation {}", to_kill->tid, mutation_id);
        TransactionLog::instance().rollbackTransaction(txn);
    }

    getContext()->getMergeList().cancelPartMutations(getStorageID(), {}, to_kill->block_number);
    to_kill->removeFile();
    LOG_TRACE(log, "Cancelled part mutations and removed mutation file {}", mutation_id);
    {
        std::lock_guard lock(mutation_wait_mutex);
        mutation_wait_event.notify_all();
    }

    /// Maybe there is another mutation that was blocked by the killed one. Try to execute it immediately.
    background_operations_assignee.trigger();

    return CancellationCode::CancelSent;
}

void StorageMergeTree::loadDeduplicationLog()
{
    auto settings = getSettings();
    if ((*settings)[MergeTreeSetting::non_replicated_deduplication_window] != 0 && format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deduplication for non-replicated MergeTree in old syntax is not supported");

    auto disk = getDisks()[0];
    std::string path = fs::path(relative_data_path) / "deduplication_logs";

    /// Deduplication log only matters on INSERTs.
    if (!disk->isReadOnly())
    {
        deduplication_log = std::make_unique<MergeTreeDeduplicationLog>(path, (*settings)[MergeTreeSetting::non_replicated_deduplication_window], format_version, disk);
        deduplication_log->load();
    }
}

void StorageMergeTree::loadMutations()
{
    std::lock_guard lock(currently_processing_in_background_mutex);

    for (const auto & disk : getDisks())
    {
        for (auto it = disk->iterateDirectory(relative_data_path); it->isValid(); it->next())
        {
            if (startsWith(it->name(), "mutation_"))
            {
                MergeTreeMutationEntry entry(disk, relative_data_path, it->name());
                UInt64 block_number = entry.block_number;
                LOG_DEBUG(log, "Loading mutation: {} entry, commands size: {}", it->name(), entry.commands->size());

                if (!entry.tid.isPrehistoric() && !entry.csn)
                {
                    if (auto csn = TransactionLog::getCSN(entry.tid))
                    {
                        /// Transaction is committed => mutation is finished, but let's load it anyway (so it will be shown in system.mutations)
                        entry.writeCSN(csn);
                    }
                    else
                    {
                        /// Transaction is not committed. The TID may be outdated if the transaction log entry
                        /// was garbage-collected (e.g. after upgrade from a version that advanced tail_ptr).
                        /// In either case the mutation was not committed and should be removed.
                        LOG_DEBUG(log, "Mutation entry {} was created by transaction {}, but it was not committed. Removing mutation entry",
                                  it->name(), entry.tid);
                        disk->removeFile(it->path());
                        continue;
                    }
                }

                auto [entry_it, inserted] = current_mutations_by_version.try_emplace(block_number, std::move(entry));
                if (!inserted)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Mutation {} already exists, it's a bug", block_number);

                incrementMutationsCounters(mutation_counters, *entry_it->second.commands);
            }
            else if (startsWith(it->name(), "tmp_mutation_"))
            {
                disk->removeFile(it->path());
            }
        }
    }

    if (!current_mutations_by_version.empty())
        increment.value = std::max(increment.value.load(), current_mutations_by_version.rbegin()->first);
}

std::expected<MergeMutateSelectedEntryPtr, SelectMergeFailure> StorageMergeTree::selectPartsToMerge(
    const StorageMetadataPtr & metadata_snapshot,
    bool aggressive,
    const String & partition_id,
    bool final,
    TableLockHolder & /* table_lock_holder */,
    std::unique_lock<std::mutex> & lock,
    const MergeTreeTransactionPtr & txn,
    bool optimize_skip_merged_partitions)
{
    auto merge_predicate = std::make_shared<MergeTreeMergePredicate>(*this, lock);
    auto parts_collector = std::make_shared<MergeTreePartsCollector>(*this, txn, merge_predicate);

    const auto is_background_memory_usage_ok = []() -> std::expected<void, PreformattedMessage>
    {
        if (canEnqueueBackgroundTask())
            return {};

        ProfileEvents::increment(ProfileEvents::MergesRejectedByMemoryLimit);
        return std::unexpected(PreformattedMessage::create("Current background tasks memory usage ({}) is more than the limit ({})",
                formatReadableSizeWithBinarySuffix(background_memory_tracker.get()),
                formatReadableSizeWithBinarySuffix(background_memory_tracker.getSoftLimit())));
    };

    const auto construct_future_part = [&](MergeSelectorChoices choices) -> std::expected<FutureMergedMutatedPartPtr, SelectMergeFailure>
    {
        chassert(choices.size() == 1);
        MergeSelectorChoice choice = std::move(choices[0]);

        auto future_part = [&]()
        {
            if (txn != nullptr)
                return constructFuturePart(*this, choice, {MergeTreeDataPartState::Active, MergeTreeDataPartState::Outdated});

            return constructFuturePart(*this, choice, {MergeTreeDataPartState::Active});
        }();

        if (!future_part)
        {
            return std::unexpected(SelectMergeFailure{
                .reason = SelectMergeFailure::Reason::CANNOT_SELECT,
                .explanation = PreformattedMessage::create("Can't construct future part from source parts. Probably there was a drop part/partition user query."),
            });
        }

        if ((*getSettings())[MergeTreeSetting::assign_part_uuids])
            future_part->uuid = UUIDHelpers::generateV4();

        return future_part;
    };

    const auto select_without_hint = [&]() -> std::expected<FutureMergedMutatedPartPtr, SelectMergeFailure>
    {
        if (auto check_memory_result = is_background_memory_usage_ok(); !check_memory_result.has_value())
            return std::unexpected(SelectMergeFailure{
                .reason = SelectMergeFailure::Reason::CANNOT_SELECT,
                .explanation = std::move(check_memory_result.error()),
            });

        UInt64 max_source_parts_bytes_for_merge = CompactionStatistics::getMaxSourcePartsBytesForMerge(*this);
        UInt64 max_result_part_rows = CompactionStatistics::getMaxResultPartRowsCount(*this);
        bool merge_with_ttl_allowed = getTotalMergesWithTTLInMergeList() < (*getSettings())[MergeTreeSetting::max_number_of_merges_with_ttl_in_pool];

        /// TTL requirements is much more strict than for regular merge, so
        /// if regular not possible, than merge with ttl is also not possible.
        if (max_source_parts_bytes_for_merge == 0)
        {
            return std::unexpected(SelectMergeFailure{
                .reason = SelectMergeFailure::Reason::CANNOT_SELECT,
                .explanation = PreformattedMessage::create("Current value of max_source_parts_bytes is zero"),
            });
        }

        auto select_result = merger_mutator.selectPartsToMerge(
            parts_collector,
            merge_predicate,
            MergeSelectorApplier(
                /*merge_constraints=*/{{max_source_parts_bytes_for_merge, max_result_part_rows}},
                /*merge_with_ttl_allowed=*/merge_with_ttl_allowed,
                /*aggressive=*/aggressive,
                /*range_filter_=*/nullptr
            ),
            /*partitions_hint=*/std::nullopt);

        return select_result.and_then(construct_future_part);
    };

    const auto select_in_partition = [&]() -> std::expected<FutureMergedMutatedPartPtr, SelectMergeFailure>
    {
        while (true)
        {
            auto timeout_ms = (*getSettings())[MergeTreeSetting::lock_acquire_timeout_for_background_operations].totalMilliseconds();
            auto timeout = std::chrono::milliseconds(timeout_ms);

            if (auto memory_check = is_background_memory_usage_ok(); !memory_check.has_value())
            {
                constexpr auto poll_interval = std::chrono::seconds(1);
                Int64 attempts = timeout / poll_interval;
                bool ok = false;
                for (Int64 i = 0; i < attempts; ++i)
                {
                    std::this_thread::sleep_for(poll_interval);
                    if (memory_check = is_background_memory_usage_ok(); memory_check.has_value())
                    {
                        ok = true;
                        break;
                    }
                }
                if (!ok)
                    return std::unexpected(SelectMergeFailure{
                        .reason = SelectMergeFailure::Reason::CANNOT_SELECT,
                        .explanation = std::move(memory_check.error()),
                    });
            }

            auto select_result = merger_mutator.selectAllPartsToMergeWithinPartition(
                metadata_snapshot,
                parts_collector,
                merge_predicate,
                partition_id,
                final,
                optimize_skip_merged_partitions);

            if (!select_result.has_value())
            {
                /// If final - we will wait for currently processing merges to finish and continue.
                if (final && !currently_merging_mutating_parts.empty())
                {
                    LOG_DEBUG(log, "Waiting for currently running merges ({} parts are merging right now) to perform OPTIMIZE FINAL",
                        currently_merging_mutating_parts.size());

                    if (std::cv_status::timeout == currently_processing_in_background_condition.wait_for(lock, timeout))
                        return std::unexpected(SelectMergeFailure{
                            .reason = SelectMergeFailure::Reason::CANNOT_SELECT,
                            .explanation = PreformattedMessage::create("Timeout ({} ms) while waiting for already running merges before running OPTIMIZE with FINAL.", timeout_ms),
                        });

                    continue;
                }
                else
                    return std::unexpected(select_result.error());
            }

            return select_result.and_then(construct_future_part);
        }
    };

    const auto construct_merge_select_entry = [&](FutureMergedMutatedPartPtr future_part) -> std::expected<MergeMutateSelectedEntryPtr, SelectMergeFailure>
    {
        /// Account TTL merge here to avoid exceeding the max_number_of_merges_with_ttl_in_pool limit
        if (isTTLMergeType(future_part->merge_type))
            getContext()->getMergeList().bookMergeWithTTL();

        try
        {
            uint64_t needed_disk_space = CompactionStatistics::estimateNeededDiskSpace(future_part->parts, true);
            auto tagger = std::make_unique<CurrentlyMergingPartsTagger>(future_part, needed_disk_space, *this, metadata_snapshot, false);

            return std::make_shared<MergeMutateSelectedEntry>(future_part, std::move(tagger), std::make_shared<MutationCommands>());
        }
        catch (...)
        {
            if (isTTLMergeType(future_part->merge_type))
                getContext()->getMergeList().cancelMergeWithTTL();

            throw;
        }
    };

    if (partition_id.empty())
        return select_without_hint().and_then(construct_merge_select_entry);
    else
        return select_in_partition().and_then(construct_merge_select_entry);
}

bool StorageMergeTree::merge(
    bool aggressive,
    const String & partition_id,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    bool cleanup,
    const MergeTreeTransactionPtr & txn,
    PreformattedMessage & out_disable_reason,
    bool optimize_skip_merged_partitions)
{
    auto table_lock_holder = lockForShare(RWLockImpl::NO_QUERY, (*getSettings())[MergeTreeSetting::lock_acquire_timeout_for_background_operations]);
    auto metadata_snapshot = getInMemoryMetadataPtr();

    auto merge_select_result = [&]()
    {
        std::unique_lock lock(currently_processing_in_background_mutex);
        if (merger_mutator.merges_blocker.isCancelledForPartition(partition_id))
            throw Exception(ErrorCodes::ABORTED, "Cancelled merging parts");

        return selectPartsToMerge(
            metadata_snapshot,
            aggressive,
            partition_id,
            final,
            table_lock_holder,
            lock,
            txn,
            optimize_skip_merged_partitions);
    }();

    if (merge_select_result.has_value())
    {
        /// Copying a vector of columns `deduplicate by columns.
        IExecutableTask::TaskResultCallback f = [](bool) {};
        auto task = std::make_shared<MergePlainMergeTreeTask>(
            *this, metadata_snapshot, deduplicate, deduplicate_by_columns, cleanup, merge_select_result.value(), table_lock_holder, f);

        task->setCurrentTransaction(MergeTreeTransactionHolder{}, MergeTreeTransactionPtr{txn});

        executeHere(task);
        return true;
    }

    auto error = std::move(merge_select_result.error());
    out_disable_reason = std::move(error.explanation);

    /// If there is nothing to merge then we treat this merge as successful (needed for optimize final optimization)
    if (error.reason == SelectMergeFailure::Reason::NOTHING_TO_MERGE)
        return true;

    return false;
}


bool StorageMergeTree::partIsAssignedToBackgroundOperation(const DataPartPtr & part) const
{
    std::lock_guard background_processing_lock(currently_processing_in_background_mutex);
    return currently_merging_mutating_parts.contains(part);
}

MergeMutateSelectedEntryPtr StorageMergeTree::selectPartsToMutate(
    const StorageMetadataPtr & metadata_snapshot, PreformattedMessage & /* disable_reason */, TableLockHolder & /* table_lock_holder */,
    std::unique_lock<std::mutex> & /*currently_processing_in_background_mutex_lock*/)
{
    current_parts_postpone_reasons.clear();

    if (current_mutations_by_version.empty())
        return {};

    size_t max_source_part_size = CompactionStatistics::getMaxSourcePartBytesForMutation(*this);
    fiu_do_on(FailPoints::mt_select_parts_to_mutate_no_free_threads, { max_source_part_size = 0; });
    if (max_source_part_size == 0)
    {
        LOG_DEBUG(
            log,
            "Not enough idle threads to apply mutations at the moment. See settings 'number_of_free_entries_in_pool_to_execute_mutation' "
            "and 'background_pool_size'");
        current_parts_postpone_reasons[PostponeReasons::ALL_PARTS_KEY] = PostponeReasons::NO_FREE_THREADS;
        return {};
    }

    size_t max_ast_elements = getContext()->getSettingsRef()[Setting::max_expanded_ast_elements];

    auto future_part = std::make_shared<FutureMergedMutatedPart>();
    if ((*storage_settings.get())[MergeTreeSetting::assign_part_uuids])
        future_part->uuid = UUIDHelpers::generateV4();

    CurrentlyMergingPartsTaggerPtr tagger;

    auto mutations_end_it = current_mutations_by_version.end();
    for (const auto & part : getDataPartsVectorForInternalUsage())
    {
        if (currently_merging_mutating_parts.contains(part))
            continue;

        /// Skip parts in partitions where merges/mutations are blocked (e.g. by REPLACE PARTITION).
        /// This check must be inside selectPartsToMutate (under currently_processing_in_background_mutex)
        /// to prevent a race where a mutation is selected after the partition blocker is set
        /// but before stopMergesAndWaitForPartition finishes waiting.
        if (merger_mutator.merges_blocker.isCancelledForPartition(part->info.getPartitionId()))
            continue;

        auto mutations_begin_it = current_mutations_by_version.upper_bound(part->info.getDataVersion());
        if (mutations_begin_it == mutations_end_it)
            continue;

        fiu_do_on(FailPoints::mt_select_parts_to_mutate_max_part_size, { max_source_part_size = 1; });
        if (max_source_part_size < part->getBytesOnDisk())
        {
            LOG_DEBUG(
                log,
                "Current max source part size for mutation is {} but part size {}. Will not mutate part {} yet",
                max_source_part_size,
                part->getBytesOnDisk(),
                part->name);
            current_parts_postpone_reasons[part->name] = PostponeReasons::EXCEED_MAX_PART_SIZE;
            continue;
        }

        TransactionID first_mutation_tid = mutations_begin_it->second.tid;
        MergeTreeTransactionPtr txn;

        if (!mutation_backoff_policy.partCanBeMutated(part->name))
        {
            LOG_DEBUG(log, "According to exponential backoff policy, do not perform mutations for the part {} yet. Put it aside.", part->name);
            current_parts_postpone_reasons[part->name] = PostponeReasons::HIT_MUTATION_BACKOFF;
            continue;
        }

        if (!first_mutation_tid.isPrehistoric())
        {

            /// Mutate visible parts only
            /// NOTE Do not mutate visible parts in Outdated state, because it does not make sense:
            /// mutation will fail anyway due to serialization error.

            /// It's possible that both mutation and transaction are already finished,
            /// because that part should not be mutated because it was not visible for that transaction.
            if (!part->version.isVisible(first_mutation_tid.start_csn, first_mutation_tid))
            {
                current_parts_postpone_reasons[part->name] = PostponeReasons::VERSION_NOT_VISIBLE;
                continue;
            }

            txn = tryGetTransactionForMutation(mutations_begin_it->second, log.load());
            if (!txn)
            {
                CSN mutation_csn = mutations_begin_it->second.csn;
                if (mutation_csn == Tx::RolledBackCSN)
                {
                    /// Transaction was rolled back, mutation should be removed soon, skip it for now
                    LOG_DEBUG(log, "Mutation {} was started by transaction {} that was rolled back, skipping part {}",
                              mutations_begin_it->second.file_name, first_mutation_tid, part->name);
                    continue;
                }
                if (mutation_csn == Tx::UnknownCSN)
                {
                    /// Transaction is not running but hasn't committed yet - this shouldn't happen
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find transaction {} that has started mutation {} "
                                    "that is going to be applied to part {}, and mutation CSN is still unknown",
                                    first_mutation_tid, mutations_begin_it->second.file_name, part->name);
                }
                /// Transaction has committed, mutation can proceed without the transaction pointer
                /// (txn is already null, which is fine for MergeMutateSelectedEntry)
            }
        }
        else
        {
            /// Mutate visible parts only (similar to mutation with transaction)
            /// NOTE Do not mutate parts in an active transaction.
            /// Mutation without transaction should wait for the transaction to commit or rollback.
            if (!part->version.isVisible(Tx::MaxCommittedCSN, Tx::EmptyTID))
            {
                LOG_DEBUG(log, "Cannot mutate part {} because it's not visible (outdated, being created or removed "
                          "in an active transaction) to mutation {}.",
                          part->name, mutations_begin_it->second.file_name);
                current_parts_postpone_reasons[part->name] = PostponeReasons::VERSION_NOT_VISIBLE;
                continue;
            }
        }

        auto commands = std::make_shared<MutationCommands>();
        Strings mutation_ids;
        size_t current_ast_elements = 0;
        auto last_mutation_to_apply = mutations_end_it;
        for (auto it = mutations_begin_it; it != mutations_end_it; ++it)
        {
            /// Do not squash mutations from different transactions to be able to commit/rollback them independently.
            if (first_mutation_tid != it->second.tid)
                break;

            size_t commands_size = 0;
            MutationCommands commands_for_size_validation;
            for (const auto & command : *it->second.commands)
            {
                if (command.type != MutationCommand::Type::DROP_COLUMN
                    && command.type != MutationCommand::Type::DROP_INDEX
                    && command.type != MutationCommand::Type::DROP_PROJECTION
                    && command.type != MutationCommand::Type::DROP_STATISTICS
                    && command.type != MutationCommand::Type::RENAME_COLUMN)
                {
                    commands_for_size_validation.push_back(command);
                }
                else
                {
                    commands_size += command.ast->size();
                }
            }

            if (!commands_for_size_validation.empty())
            {
                try
                {
                    auto fake_query_context = Context::createCopy(getContext());
                    fake_query_context->makeQueryContext();
                    fake_query_context->setCurrentQueryId("");
                    MutationsInterpreter::Settings settings(false);
                    MutationsInterpreter interpreter(
                        shared_from_this(), metadata_snapshot, commands_for_size_validation, fake_query_context, settings);
                    commands_size += interpreter.evaluateCommandsSize();
                }
                catch (...)
                {
                    tryLogCurrentException(log);
                    MergeTreeMutationEntry & entry = it->second;
                    entry.latest_fail_time = time(nullptr);
                    entry.latest_fail_reason = getCurrentExceptionMessage(false);
                    entry.latest_fail_error_code_name = ErrorCodes::getName(getCurrentExceptionCode());
                    /// NOTE we should not skip mutations, because exception may be retryable (e.g. MEMORY_LIMIT_EXCEEDED)
                    break;
                }
            }

            if (current_ast_elements + commands_size >= max_ast_elements)
                break;

            const auto & single_mutation_commands = it->second.commands;

            if (single_mutation_commands->containBarrierCommand())
            {
                if (commands->empty())
                {
                    commands->insert(commands->end(), single_mutation_commands->begin(), single_mutation_commands->end());
                    mutation_ids.push_back(it->second.file_name);
                    last_mutation_to_apply = it;
                }
                break;
            }

            current_ast_elements += commands_size;
            commands->insert(commands->end(), single_mutation_commands->begin(), single_mutation_commands->end());
            mutation_ids.push_back(it->second.file_name);
            last_mutation_to_apply = it;
        }

        assert(commands->empty() == (last_mutation_to_apply == mutations_end_it));
        if (!commands->empty())
        {
            auto new_part_info = part->info;
            new_part_info.mutation = last_mutation_to_apply->first;

            future_part->parts.push_back(part);
            future_part->part_info = new_part_info;
            future_part->name = part->getNewName(new_part_info);
            future_part->part_format = part->getFormat();

            tagger = std::make_unique<CurrentlyMergingPartsTagger>(future_part, CompactionStatistics::estimateNeededDiskSpace({part}, false), *this, metadata_snapshot, true);
            return std::make_shared<MergeMutateSelectedEntry>(future_part, std::move(tagger), commands, txn, mutation_ids);
        }
    }

    return {};
}

UInt32 StorageMergeTree::getMaxLevelInBetween(const PartProperties & left, const PartProperties & right) const
{
    auto parts_lock = readLockParts();

    auto begin = data_parts_by_info.find(left.info);
    if (begin == data_parts_by_info.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "unable to find left part, left part {}. It's a bug", left.name);

    auto end = data_parts_by_info.find(right.info);
    if (end == data_parts_by_info.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "unable to find right part, right part {}. It's a bug", right.name);

    UInt32 level = 0;

    for (auto it = begin++; it != end; ++it)
    {
        if (it == data_parts_by_info.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "left and right parts in the wrong order, left part {}, right part {}. It's a bug", left.name, right.name);

        level = std::max(level, (*it)->info.level);
    }

    return level;
}

bool StorageMergeTree::scheduleDataProcessingJob(BackgroundJobsAssignee & assignee)
{
    if (shutdown_called)
        return false;

    assert(!isStaticStorage());

    FailPointInjection::pauseFailPoint(FailPoints::mt_merge_selecting_task_pause_when_scheduled);

    auto metadata_snapshot = getInMemoryMetadataPtr();
    MergeMutateSelectedEntryPtr merge_entry;
    MergeMutateSelectedEntryPtr mutate_entry;

    auto shared_lock = lockForShare(RWLockImpl::NO_QUERY, (*getSettings())[MergeTreeSetting::lock_acquire_timeout_for_background_operations]);

    MergeTreeTransactionHolder transaction_for_merge;
    MergeTreeTransactionPtr txn;
    if (transactions_enabled.load(std::memory_order_relaxed))
    {
        /// TODO Transactions: avoid beginning transaction if there is nothing to merge.
        txn = TransactionLog::instance().beginTransaction();
        transaction_for_merge = MergeTreeTransactionHolder{txn, /* autocommit = */ false};
    }

    bool has_mutations = false;
    {
        std::unique_lock lock(currently_processing_in_background_mutex);

        if (merger_mutator.merges_blocker.isCancelled())
            return false;

        {
            if (auto merge_select_result = selectPartsToMerge(metadata_snapshot, false, {}, false, shared_lock, lock, txn))
                merge_entry = std::move(merge_select_result.value());
            else
                LOG_TRACE(LogFrequencyLimiter(log.load(), 300), "Didn't start merge: {}", merge_select_result.error().explanation.text);
        }

        if (!merge_entry && !current_mutations_by_version.empty())
        {
            PreformattedMessage out_reason;
            mutate_entry = selectPartsToMutate(metadata_snapshot, out_reason, shared_lock, lock);

            if (!mutate_entry)
                LOG_TRACE(LogFrequencyLimiter(log.load(), 300), "Didn't start mutation: {}", out_reason.text);
        }

        has_mutations = !current_mutations_by_version.empty();
    }

    auto is_cancelled = [&merges_blocker = merger_mutator.merges_blocker](const MergeMutateSelectedEntryPtr & entry)
    {
        if (entry->future_part)
            return merges_blocker.isCancelledForPartition(entry->future_part->part_info.getPartitionId());

        return merges_blocker.isCancelled();
    };

    if (merge_entry)
    {
        if (is_cancelled(merge_entry))
            return false;

        bool cleanup = merge_entry->future_part->final
            && (*getSettings())[MergeTreeSetting::allow_experimental_replacing_merge_with_cleanup]
            && (*getSettings())[MergeTreeSetting::enable_replacing_merge_with_cleanup_for_min_age_to_force_merge]
            && (*getSettings())[MergeTreeSetting::min_age_to_force_merge_seconds]
            && (*getSettings())[MergeTreeSetting::min_age_to_force_merge_on_partition_only];

        auto task = std::make_shared<MergePlainMergeTreeTask>(*this, metadata_snapshot, /* deduplicate */ false, Names{}, cleanup, merge_entry, shared_lock, common_assignee_trigger);
        task->setCurrentTransaction(std::move(transaction_for_merge), std::move(txn));
        bool scheduled = assignee.scheduleMergeMutateTask(task);
        /// The problem that we already booked a slot for TTL merge, but a merge list entry will be created only in a prepare method
        /// in MergePlainMergeTreeTask. So, this slot will never be freed.
        if (!scheduled && isTTLMergeType(merge_entry->future_part->merge_type))
            getContext()->getMergeList().cancelMergeWithTTL();
        return scheduled;
    }
    if (mutate_entry)
    {
        if (is_cancelled(mutate_entry))
            return false;

        /// We take new metadata snapshot here. It's because mutation commands can be executed only with metadata snapshot
        /// which is equal or more fresh than commands themselves. In extremely rare case it can happen that we will have alter
        /// in between we took snapshot above and selected commands. That is why we take new snapshot here.
        auto task = std::make_shared<MutatePlainMergeTreeTask>(*this, getInMemoryMetadataPtr(), mutate_entry, shared_lock, common_assignee_trigger);
        return assignee.scheduleMergeMutateTask(task);
    }
    if (has_mutations)
    {
        /// Notify in case of errors if no mutation was successfully selected.
        /// Otherwise, notification will occur after any of mutations complete.
        std::lock_guard lock(mutation_wait_mutex);
        mutation_wait_event.notify_all();
    }

    bool scheduled = false;
    if (auto lock = time_after_previous_cleanup_temporary_directories.compareAndRestartDeferred(
            static_cast<double>((*getSettings())[MergeTreeSetting::merge_tree_clear_old_temporary_directories_interval_seconds])))
    {
        assignee.scheduleCommonTask(std::make_shared<ExecutableLambdaAdapter>(
            [this, shared_lock] ()
            {
                return clearOldTemporaryDirectories((*getSettings())[MergeTreeSetting::temporary_directories_lifetime].totalSeconds());
            }, common_assignee_trigger, getStorageID()), /* need_trigger */ false);
        scheduled = true;
    }

    if (auto lock = time_after_previous_cleanup_parts.compareAndRestartDeferred(
            static_cast<double>((*getSettings())[MergeTreeSetting::merge_tree_clear_old_parts_interval_seconds])))
    {
        assignee.scheduleCommonTask(std::make_shared<ExecutableLambdaAdapter>(
            [this, shared_lock] ()
            {
                /// All use relative_data_path which changes during rename
                /// so execute under share lock.
                size_t cleared_count = 0;
                cleared_count += clearOldPartsFromFilesystem(/* force */ false, /* with_pause_point */true);
                cleared_count += clearOldMutations();
                cleared_count += clearEmptyParts();
                cleared_count += clearUnusedPatchParts();
                cleared_count += unloadPrimaryKeysAndClearCachesOfOutdatedParts();
                return cleared_count;
                /// TODO maybe take into account number of cleared objects when calculating backoff
            }, common_assignee_trigger, getStorageID()), /* need_trigger */ false);
        scheduled = true;
    }

    return scheduled;
}

UInt64 StorageMergeTree::getCurrentMutationVersion(UInt64 data_version, std::unique_lock<std::mutex> & /*currently_processing_in_background_mutex_lock*/) const
{
    auto it = current_mutations_by_version.upper_bound(data_version);
    if (it == current_mutations_by_version.begin())
        return 0;
    --it;
    return it->first;
}

UInt64 StorageMergeTree::getNextMutationVersion(UInt64 data_version, std::unique_lock<std::mutex> & /* currently_processing_in_background_mutex_lock */) const
{
    auto it = current_mutations_by_version.upper_bound(data_version);
    if (it == current_mutations_by_version.end())
        return 0;
    return it->first;
}

size_t StorageMergeTree::clearOldMutations(bool truncate)
{
    size_t finished_mutations_to_keep = (*getSettings())[MergeTreeSetting::finished_mutations_to_keep];
    if (!truncate && !finished_mutations_to_keep)
        return 0;

    finished_mutations_to_keep = truncate ? 0 : finished_mutations_to_keep;
    std::vector<MergeTreeMutationEntry> mutations_to_delete;
    {
        std::lock_guard lock(currently_processing_in_background_mutex);

        auto end_it = current_mutations_by_version.end();
        auto begin_it = current_mutations_by_version.begin();

        if (std::optional<Int64> min_version = getMinPartDataVersion())
            end_it = current_mutations_by_version.upper_bound(*min_version);

        size_t done_count = 0;
        for (auto it = begin_it; it != end_it; ++it)
        {
            auto & entry = it->second;

            if (!entry.tid.isPrehistoric())
            {
                end_it = it;
                break;
            }

            if (!entry.is_done)
            {
                entry.is_done = true;
                decrementMutationsCounters(mutation_counters, *entry.commands);
            }

            ++done_count;
        }

        if (done_count <= finished_mutations_to_keep)
            return 0;

        size_t to_delete_count = done_count - finished_mutations_to_keep;

        auto it = begin_it;
        for (size_t i = 0; i < to_delete_count; ++i)
        {
            const auto & tid = it->second.tid;
            if (!tid.isPrehistoric() && !TransactionLog::getCSN(tid))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot remove mutation {}, because transaction {} is not committed. It's a bug",
                                it->first, tid);

            mutations_to_delete.push_back(std::move(it->second));
            it = current_mutations_by_version.erase(it);
        }
    }

    for (auto & mutation : mutations_to_delete)
    {
        LOG_TRACE(log, "Removing mutation: {}", mutation.file_name);
        mutation.removeFile();
    }

    return mutations_to_delete.size();
}

size_t StorageMergeTree::clearOldPartsFromFilesystem(bool force, bool with_pause_fail_point)
{
    DataPartsVector parts_to_remove = grabOldParts(force);
    if (parts_to_remove.empty())
        return 0;

    if (with_pause_fail_point)
    {
        // storage_merge_tree_background_clear_old_parts_pause is set after grabOldParts intentionally
        // It allows the use case
        // - firstly SYSTEM ENABLE FAILPOINT storage_merge_tree_background_clear_old_parts_pause
        // - after do operation like merge / optimize final (operations like drop part / drop partition / truncate do not fit here, they remove old parts synchronously without timeout)
        // All parts which are dropped in that operations are not removed until failpoint is released
        // If we would set this failpoint before grabOldParts, it leads us to a case when
        // background thread already passed the failpoint but did not reach grabOldParts yet
        // if failpoint is enabled at that time, background thread could grab parts from those operations and remove them regardless enabled failpoint
        FailPointInjection::pauseFailPoint(FailPoints::storage_merge_tree_background_clear_old_parts_pause);
    }

    clearPartsFromFilesystemAndRollbackIfError(parts_to_remove, "old");

    /// This is needed to close files to avoid they reside on disk after being deleted.
    /// NOTE: we can drop files from cache more selectively but this is good enough.
    getContext()->clearMMappedFileCache();

    return parts_to_remove.size();
}

bool StorageMergeTree::optimize(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    bool cleanup,
    ContextPtr local_context)
{
    assertNotReadonly();

    const auto mode = (*getSettings())[MergeTreeSetting::deduplicate_merge_projection_mode];
    if (deduplicate && getInMemoryMetadataPtr()->hasProjections()
        && (mode == DeduplicateMergeProjectionMode::THROW || mode == DeduplicateMergeProjectionMode::IGNORE))
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                    "OPTIMIZE DEDUPLICATE query is not supported for table {} as it has projections. "
                    "Please drop all projections manually before running the query, "
                    "or set setting 'deduplicate_merge_projection_mode' to 'drop' or 'rebuild'",
                    getStorageID().getTableName());

    if (deduplicate)
    {
        if (deduplicate_by_columns.empty())
            LOG_DEBUG(log, "DEDUPLICATE BY all columns");
        else
            LOG_DEBUG(log, "DEDUPLICATE BY ('{}')", fmt::join(deduplicate_by_columns, "', '"));
    }

    auto txn = local_context->getCurrentTransaction();

    PreformattedMessage disable_reason;
    if (!partition && final)
    {
        if (cleanup && this->merging_params.mode != MergingParams::Mode::Replacing)
            throw Exception(ErrorCodes::CANNOT_ASSIGN_OPTIMIZE, "Cannot OPTIMIZE with CLEANUP table: only ReplacingMergeTree can be CLEANUP");

        if (cleanup && !(*getSettings())[MergeTreeSetting::allow_experimental_replacing_merge_with_cleanup])
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Experimental merges with CLEANUP are not allowed");

        DataPartsVector data_parts = getVisibleDataPartsVector(local_context);
        std::unordered_set<String> partition_ids;

        for (const DataPartPtr & part : data_parts)
            partition_ids.emplace(part->info.getPartitionId());

        for (const String & partition_id : partition_ids)
        {
            if (!merge(
                    true,
                    partition_id,
                    true,
                    deduplicate,
                    deduplicate_by_columns,
                    cleanup,
                    txn,
                    disable_reason,
                    local_context->getSettingsRef()[Setting::optimize_skip_merged_partitions]))
            {
                constexpr auto message = "Cannot OPTIMIZE table: {}";
                LOG_INFO(log, message, disable_reason.text);

                if (local_context->getSettingsRef()[Setting::optimize_throw_if_noop])
                    throw Exception(ErrorCodes::CANNOT_ASSIGN_OPTIMIZE, message, disable_reason.text);

                return false;
            }
        }
    }
    else
    {
        String partition_id;
        if (partition)
            partition_id = getPartitionIDFromQuery(partition, local_context);

        if (!merge(
                true,
                partition_id,
                final,
                deduplicate,
                deduplicate_by_columns,
                cleanup,
                txn,
                disable_reason,
                local_context->getSettingsRef()[Setting::optimize_skip_merged_partitions]))
        {
            constexpr auto message = "Cannot OPTIMIZE table: {}";
            LOG_INFO(log, message, disable_reason.text);

            if (local_context->getSettingsRef()[Setting::optimize_throw_if_noop])
                throw Exception(ErrorCodes::CANNOT_ASSIGN_OPTIMIZE, message, disable_reason.text);

            return false;
        }
    }

    return true;
}

namespace
{

size_t countOccurrences(const StorageMergeTree::DataParts & haystack, const DataPartsVector & needle)
{
    size_t total = 0;
    for (const auto & n : needle)
        total += haystack.count(n);

    return total;
}

auto getNameWithState(const auto & parts)
{
    return std::views::transform(parts, [](const auto & p)
    {
        return p->getNameWithState();
    });
}

}

// Same as stopMergesAndWait, but waits only for merges on parts belonging to a certain partition.
ActionLock StorageMergeTree::stopMergesAndWaitForPartition(String partition_id)
{
    LOG_DEBUG(log, "StorageMergeTree::stopMergesAndWaitForPartition partition_id: `{}`", partition_id);
    /// Stop all merges and prevent new from starting, BUT unlike stopMergesAndWait(), only wait for the merges on small set of parts to finish.

    std::unique_lock lock(currently_processing_in_background_mutex);

    /// Asks to complete merges and does not allow them to start.
    /// This protects against "revival" of data for a removed partition after completion of merge.
    auto merge_blocker = merger_mutator.merges_blocker.cancelForPartition(partition_id);

    DataPartsVector parts_to_wait;
    {
        auto parts_lock = readLockParts();
        parts_to_wait = getDataPartsVectorInPartitionForInternalUsage(MergeTreeDataPartState::Active, partition_id, parts_lock);
    }
    LOG_TRACE(log, "StorageMergeTree::stopMergesAndWaitForPartition parts to wait: {} ({} items)",
        fmt::join(getNameWithState(parts_to_wait), ", "), parts_to_wait.size());

    LOG_DEBUG(log, "StorageMergeTree::stopMergesAndWaitForPartition all mutating parts: {} ({} items)",
        fmt::join(getNameWithState(currently_merging_mutating_parts), ", "), currently_merging_mutating_parts.size());

    // TODO allow to stop merges in specific partition only (like it's done in ReplicatedMergeTree)

    while (size_t still_merging = countOccurrences(currently_merging_mutating_parts, parts_to_wait))
    {
        LOG_DEBUG(log, "StorageMergeTree::stopMergesAndWaitForPartition Waiting for currently running merges ({} {} parts are merging right now)",
            fmt::join(getNameWithState(currently_merging_mutating_parts), ", "), still_merging);

        if (std::cv_status::timeout == currently_processing_in_background_condition.wait_for(
            lock, std::chrono::seconds(DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC)))
        {
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Timeout while waiting for already running merges");
        }
    }

    LOG_DEBUG(log, "StorageMergeTree::stopMergesAndWaitForPartition done waiting, still merging {} ({} items)",
              fmt::join(getNameWithState(currently_merging_mutating_parts), ", "), currently_merging_mutating_parts.size());
    return merge_blocker;
}

ActionLock StorageMergeTree::stopMergesAndWait()
{
    /// TODO allow to stop merges in specific partition only (like it's done in ReplicatedMergeTree)
    std::unique_lock lock(currently_processing_in_background_mutex);

    /// Asks to complete merges and does not allow them to start.
    /// This protects against "revival" of data for a removed partition after completion of merge.
    auto merge_blocker = merger_mutator.merges_blocker.cancel();

    while (!currently_merging_mutating_parts.empty())
    {
        LOG_DEBUG(log, "Waiting for currently running merges ({} parts are merging right now)",
            currently_merging_mutating_parts.size());

        if (std::cv_status::timeout == currently_processing_in_background_condition.wait_for(
            lock, std::chrono::seconds(DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC)))
        {
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Timeout while waiting for already running merges");
        }
    }

    return merge_blocker;
}

MergeTreeDataPartPtr StorageMergeTree::outdatePart(MergeTreeTransaction * txn, const String & part_name, bool force, bool clear_without_timeout)
{
    if (force)
    {
        /// Forcefully stop merges and make part outdated
        auto merge_blocker = stopMergesAndWait();
        auto parts_lock = lockParts();
        auto part = getPartIfExistsUnlocked(part_name, {MergeTreeDataPartState::Active}, parts_lock);
        if (!part)
            throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "Part {} not found, won't try to drop it.", part_name);

        removePartsFromWorkingSet(txn, {part}, clear_without_timeout, parts_lock);
        return part;
    }

    /// Wait merges selector
    std::unique_lock lock(currently_processing_in_background_mutex);
    auto parts_lock = lockParts();

    auto part = getPartIfExistsUnlocked(part_name, {MergeTreeDataPartState::Active}, parts_lock);
    /// It's okay, part was already removed
    if (!part)
        return nullptr;

    /// Part will be "removed" by merge or mutation, it's OK in case of some
    /// background cleanup processes like removing of empty parts.
    if (currently_merging_mutating_parts.contains(part))
        return nullptr;

    removePartsFromWorkingSet(txn, {part}, clear_without_timeout, parts_lock);
    return part;
}

void StorageMergeTree::dropPartNoWaitNoThrow(const String & part_name)
{
    if (auto part = outdatePart(NO_TRANSACTION_RAW, part_name, /*force=*/ false, /*clear_without_timeout=*/ false))
    {
        if (deduplication_log)
        {
            deduplication_log->dropPart(part->info);
        }

        /// Need to destroy part objects before clearing them from filesystem.
        part.reset();

        clearOldPartsFromFilesystem();
    }

    /// Else nothing to do, part was removed in some different way
}

struct FutureNewEmptyPart
{
    MergeTreePartInfo part_info;
    MergeTreePartition partition;
    std::string part_name;

    StorageMergeTree::MutableDataPartPtr data_part;
};

using FutureNewEmptyParts = std::vector<FutureNewEmptyPart>;

Strings getPartsNames(const FutureNewEmptyParts & parts)
{
    Strings part_names;
    for (const auto & p : parts)
        part_names.push_back(p.part_name);
    return part_names;
}

FutureNewEmptyParts initCoverageWithNewEmptyParts(const DataPartsVector & old_parts)
{
    FutureNewEmptyParts future_parts;

    for (const auto & old_part : old_parts)
    {
        future_parts.emplace_back();
        auto & new_part = future_parts.back();

        new_part.part_info = old_part->info;
        new_part.part_info.level += 1;
        new_part.partition = old_part->partition;
        new_part.part_name = old_part->getNewName(new_part.part_info);
    }

    return future_parts;
}

std::pair<StorageMergeTree::MutableDataPartsVector, std::vector<scope_guard>> createEmptyDataParts(
    MergeTreeData & data, FutureNewEmptyParts & future_parts, const MergeTreeTransactionPtr & txn)
{
    std::pair<StorageMergeTree::MutableDataPartsVector, std::vector<scope_guard>> data_parts;
    for (auto & part: future_parts)
    {
        auto [new_data_part, tmp_dir_holder] = data.createEmptyPart(part.part_info, part.partition, part.part_name, txn);
        data_parts.first.emplace_back(std::move(new_data_part));
        data_parts.second.emplace_back(std::move(tmp_dir_holder));
    }
    return data_parts;
}


void StorageMergeTree::renameAndCommitEmptyParts(MutableDataPartsVector & new_parts, Transaction & transaction)
{
    DataPartsVector covered_parts;
    size_t next_part_index = 0;

    auto timeout_ms = getContext()->getSettingsRef()[Setting::lock_acquire_timeout].totalMilliseconds();
    Stopwatch watch;
    do
    {
        try
        {
            auto part_lock = lockParts();
            while (next_part_index < new_parts.size())
            {
                auto & part = new_parts.at(next_part_index);
                DataPartsVector covered_parts_by_one_part
                    = renameTempPartAndReplaceUnlocked(part, part_lock, transaction, /*rename_in_transaction=*/true);

                if (covered_parts_by_one_part.size() > 1)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Part {} expected to cover not more than 1 part. "
                        "{} covered parts have been found. This is a bug.",
                        part->name,
                        covered_parts_by_one_part.size());

                std::move(covered_parts_by_one_part.begin(), covered_parts_by_one_part.end(), std::back_inserter(covered_parts));
                ++next_part_index;
            }
            break;
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::PART_IS_TEMPORARILY_LOCKED)
                throw;

            if (Int64(watch.elapsedMilliseconds()) >= timeout_ms)
                throw;

            auto & part = new_parts[next_part_index];
            LOG_INFO(log, "Part {} is temporarily locked, will clear old parts and retry.", part->name);
        }

        clearOldPartsFromFilesystem();
        sleepForMilliseconds(200);
    } while (true);

    LOG_INFO(log, "Remove {} parts by covering them with empty {} parts. With txn {}.",
             covered_parts.size(), new_parts.size(), transaction.getTID());

    transaction.renameParts();
    transaction.commit();

    /// Remove covered parts without waiting for old_parts_lifetime seconds.
    for (auto & part: covered_parts)
        part->remove_time.store(0, std::memory_order_relaxed);

    if (deduplication_log)
        for (const auto & part : covered_parts)
            deduplication_log->dropPart(part->info);
}

void StorageMergeTree::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr query_context, TableExclusiveLockHolder &)
{
    assertNotReadonly();

    {
        /// Asks to complete merges and does not allow them to start.
        /// This protects against "revival" of data for a removed partition after completion of merge.
        waitForOutdatedPartsToBeLoaded();
        auto merge_blocker = stopMergesAndWait();

        Stopwatch watch;
        ProfileEventsScope profile_events_scope;

        auto txn = query_context->getCurrentTransaction();
        if (txn)
        {
            auto data_parts_lock = lockParts();
            auto parts_to_remove = getVisibleDataPartsVectorUnlocked(query_context, data_parts_lock);
            removePartsFromWorkingSet(txn.get(), parts_to_remove, true, data_parts_lock);
            LOG_INFO(log, "Removed {} parts: [{}]", parts_to_remove.size(), fmt::join(getPartsNames(parts_to_remove), ", "));
        }
        else
        {
            MergeTreeData::Transaction transaction(*this, txn.get());

            auto operation_data_parts_lock = lockOperationsWithParts();

            auto parts = getVisibleDataPartsVector(query_context);

            auto future_parts = initCoverageWithNewEmptyParts(parts);

            LOG_TEST(log, "Made {} empty parts in order to cover {} parts. Empty parts: {}, covered parts: {}. With txn {}",
                     future_parts.size(), parts.size(),
                     fmt::join(getPartsNames(future_parts), ", "), fmt::join(getPartsNames(parts), ", "),
                     transaction.getTID());

            auto [new_data_parts, tmp_dir_holders] = createEmptyDataParts(*this, future_parts, txn);
            renameAndCommitEmptyParts(new_data_parts, transaction);

            PartLog::addNewParts(query_context, PartLog::createPartLogEntries(new_data_parts, watch.elapsed(), profile_events_scope.getSnapshot()));

            LOG_INFO(log, "Truncated table with {} parts by replacing them with new empty {} parts. With txn {}",
                     parts.size(), future_parts.size(),
                     transaction.getTID());
        }
    }

    /// Old parts are needed to be destroyed before clearing them from filesystem.
    clearOldMutations(true);
    clearOldPartsFromFilesystem();
    clearEmptyParts();
}

void StorageMergeTree::dropPart(const String & part_name, bool detach, ContextPtr query_context)
{
    {
        /// Asks to complete merges and does not allow them to start.
        /// This protects against "revival" of data for a removed partition after completion of merge.
        auto merge_blocker = stopMergesAndWait();

        Stopwatch watch;
        ProfileEventsScope profile_events_scope;

        /// It's important to create it outside of lock scope because
        /// otherwise it can lock parts in destructor and deadlock is possible.
        auto txn = query_context->getCurrentTransaction();
        if (txn)
        {
            if (auto part = outdatePart(txn.get(), part_name, /*force=*/ true))
                dropPartsImpl({part}, detach);
        }
        else
        {
            MergeTreeData::Transaction transaction(*this, txn.get());

            auto operation_data_parts_lock = lockOperationsWithParts();

            auto part = getPartIfExists(part_name, {MergeTreeDataPartState::Active});
            if (!part)
                throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "Part {} not found, won't try to drop it.", part_name);

            if (detach)
            {
                auto metadata_snapshot = getInMemoryMetadataPtr();
                String part_dir = part->getDataPartStorage().getPartDirectory();
                LOG_INFO(log, "Detaching {}", part_dir);
                auto holder = getTemporaryPartDirectoryHolder(String(DETACHED_DIR_NAME) + "/" + part_dir);
                part->makeCloneInDetached("", metadata_snapshot, /*disk_transaction*/ {});
            }

            {
                auto future_parts = initCoverageWithNewEmptyParts({part});

                LOG_TEST(log, "Made {} empty parts in order to cover {} part. With txn {}",
                         fmt::join(getPartsNames(future_parts), ", "), fmt::join(getPartsNames({part}), ", "),
                         transaction.getTID());

                auto [new_data_parts, tmp_dir_holders] = createEmptyDataParts(*this, future_parts, txn);
                renameAndCommitEmptyParts(new_data_parts, transaction);

                PartLog::addNewParts(query_context, PartLog::createPartLogEntries(new_data_parts, watch.elapsed(), profile_events_scope.getSnapshot()));

                const auto * op = detach ? "Detached" : "Dropped";
                LOG_INFO(log, "{} {} part by replacing it with new empty {} part. With txn {}",
                         op, part->name, future_parts[0].part_name,
                         transaction.getTID());
            }
        }
    }

    clearOldPartsFromFilesystem();
    clearEmptyParts();
}

void StorageMergeTree::dropPartition(const ASTPtr & partition, bool detach, ContextPtr query_context)
{
    {
        const auto * partition_ast = partition->as<ASTPartition>();

        /// Asks to complete merges and does not allow them to start.
        /// This protects against "revival" of data for a removed partition after completion of merge.
        auto merge_blocker = stopMergesAndWait();

        Stopwatch watch;
        ProfileEventsScope profile_events_scope;

        /// It's important to create it outside of lock scope because
        /// otherwise it can lock parts in destructor and deadlock is possible.
        auto txn = query_context->getCurrentTransaction();

        if (txn)
        {
            DataPartsVector parts_to_remove;
            {
                auto data_parts_lock = lockParts();
                if (partition_ast && partition_ast->all)
                    parts_to_remove = getVisibleDataPartsVectorUnlocked(query_context, data_parts_lock);
                else
                {
                    String partition_id = getPartitionIDFromQuery(partition, query_context, &data_parts_lock);
                    parts_to_remove = getVisibleDataPartsVectorInPartition(query_context, partition_id, data_parts_lock);
                }
                removePartsFromWorkingSet(txn.get(), parts_to_remove, true, data_parts_lock);
            }
            dropPartsImpl(std::move(parts_to_remove), detach);
        }
        else
        {
            MergeTreeData::Transaction transaction(*this, txn.get());

            auto operation_data_parts_lock = lockOperationsWithParts();

            DataPartsVector parts;

            if (partition_ast && partition_ast->all)
            {
                parts = getVisibleDataPartsVector(query_context);
            }
            else
            {
                String partition_id = getPartitionIDFromQuery(partition, query_context);
                parts = getVisibleDataPartsVectorInPartition(query_context, partition_id);
            }

            if (detach)
            {
                for (const auto & part : parts)
                {
                    auto metadata_snapshot = getInMemoryMetadataPtr();
                    String part_dir = part->getDataPartStorage().getPartDirectory();
                    LOG_INFO(log, "Detaching {}", part_dir);
                    auto holder = getTemporaryPartDirectoryHolder(String(DETACHED_DIR_NAME) + "/" + part_dir);
                    part->makeCloneInDetached("", metadata_snapshot, /*disk_transaction*/ {});
                }
            }

            auto future_parts = initCoverageWithNewEmptyParts(parts);

            LOG_TEST(log, "Made {} empty parts in order to cover {} parts. Empty parts: {}, covered parts: {}. With txn {}",
                     future_parts.size(), parts.size(),
                     fmt::join(getPartsNames(future_parts), ", "), fmt::join(getPartsNames(parts), ", "),
                     transaction.getTID());


            auto [new_data_parts, tmp_dir_holders] = createEmptyDataParts(*this, future_parts, txn);
            renameAndCommitEmptyParts(new_data_parts, transaction);

            PartLog::addNewParts(query_context, PartLog::createPartLogEntries(new_data_parts, watch.elapsed(), profile_events_scope.getSnapshot()));

            const auto * op = detach ? "Detached" : "Dropped";
            LOG_INFO(log, "{} partition with {} parts by replacing them with new empty {} parts. With txn {}",
                     op, parts.size(), future_parts.size(),
                     transaction.getTID());
        }
    }

    clearOldPartsFromFilesystem();
    clearEmptyParts();
}

void StorageMergeTree::dropPartsImpl(DataPartsVector && parts_to_remove, bool detach)
{
    auto metadata_snapshot = getInMemoryMetadataPtr();

    if (detach)
    {
        /// If DETACH clone parts to detached/ directory
        /// NOTE: no race with background cleanup until we hold pointers to parts
        for (const auto & part : parts_to_remove)
        {
            String part_dir = part->getDataPartStorage().getPartDirectory();
            LOG_INFO(log, "Detaching {}", part_dir);
            auto holder = getTemporaryPartDirectoryHolder(String(DETACHED_DIR_NAME) + "/" + part_dir);
            part->makeCloneInDetached("", metadata_snapshot, /*disk_transaction*/ {});
        }
    }

    if (deduplication_log)
    {
        for (const auto & part : parts_to_remove)
            deduplication_log->dropPart(part->info);
    }

    if (detach)
        LOG_INFO(log, "Detached {} parts: [{}]", parts_to_remove.size(), fmt::join(getPartsNames(parts_to_remove), ", "));
    else
        LOG_INFO(log, "Removed {} parts: [{}]", parts_to_remove.size(), fmt::join(getPartsNames(parts_to_remove), ", "));
}

PartitionCommandsResultInfo StorageMergeTree::attachPartition(
    const PartitionCommand & command, const StorageMetadataPtr &, ContextPtr local_context)
{
    PartitionCommandsResultInfo results;
    PartsTemporaryRename renamed_parts(*this, DETACHED_DIR_NAME);
    MutableDataPartsVector loaded_parts = tryLoadPartsToAttach(command, local_context, renamed_parts);

    for (size_t i = 0; i < loaded_parts.size(); ++i)
    {
        LOG_INFO(log, "Attaching part {} from {}", loaded_parts[i]->name, renamed_parts.old_and_new_names[i].new_dir);
        /// We should write version metadata on part creation to distinguish it from parts that were created without transaction.
        auto txn = local_context->getCurrentTransaction();
        TransactionID tid = txn ? txn->tid : Tx::PrehistoricTID;
        loaded_parts[i]->version.setCreationTID(tid, nullptr);
        loaded_parts[i]->storeVersionMetadata();

        /// It's important to create it outside of lock scope because
        /// otherwise it can lock parts in destructor and deadlock is possible.
        MergeTreeData::Transaction transaction(*this, local_context->getCurrentTransaction().get());
        {
            auto lock = lockParts();
            auto block_holder = fillNewPartNameAndResetLevel(loaded_parts[i], lock);
            renameTempPartAndAdd(loaded_parts[i], transaction, lock, /*rename_in_transaction=*/ false);
            transaction.commit(lock);
        }

        results.push_back(PartitionCommandResultInfo{
            .command_type = "ATTACH_PART",
            .partition_id = loaded_parts[i]->info.getPartitionId(),
            .part_name = loaded_parts[i]->name,
            .old_part_name = renamed_parts.old_and_new_names[i].old_dir,
        });

        renamed_parts.old_and_new_names[i].old_dir.clear();
        LOG_INFO(log, "Finished attaching part");
    }

    return results;
}

void StorageMergeTree::replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, ContextPtr local_context)
{
    assertNotReadonly();
    LOG_DEBUG(log, "StorageMergeTree::replacePartitionFrom\tsource_table: {}, replace: {}", source_table->getStorageID().getShortName(), replace);

    auto lock1 = lockForShare(local_context->getCurrentQueryId(), local_context->getSettingsRef()[Setting::lock_acquire_timeout]);
    auto lock2 = source_table->lockForShare(local_context->getCurrentQueryId(), local_context->getSettingsRef()[Setting::lock_acquire_timeout]);

    bool is_all = partition->as<ASTPartition>()->all;

    String partition_id;

    if (is_all)
    {
        if (replace)
            throw DB::Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Only support DROP/DETACH/ATTACH PARTITION ALL currently");
    }
    else
    {
        partition_id = getPartitionIDFromQuery(partition, local_context);
    }

    /// We use the global `stopMergesAndWait` (not the per-partition variant) because the
    /// per-partition blocker check in `scheduleDataProcessingJob` happens outside
    /// `currently_processing_in_background_mutex`, creating a race window where a mutation
    /// can be selected inside the mutex, pass the per-partition check outside the mutex,
    /// and get scheduled after the blocker was set but before we call
    /// `removePartsInRangeFromWorkingSet`, "resurrecting" old data.
    /// The global blocker is checked inside the mutex, eliminating this race.
    ActionLock merges_blocker = stopMergesAndWait();

    auto source_metadata_snapshot = source_table->getInMemoryMetadataPtr();
    auto my_metadata_snapshot = getInMemoryMetadataPtr();

    Stopwatch watch;
    ProfileEventsScope profile_events_scope;

    MergeTreeData & src_data = checkStructureAndGetMergeTreeData(source_table, source_metadata_snapshot, my_metadata_snapshot);
    DataPartsVector src_parts;

    if (is_all)
        src_parts = src_data.getVisibleDataPartsVector(local_context);
    else
        src_parts = src_data.getVisibleDataPartsVectorInPartition(local_context, partition_id);

    MutableDataPartsVector dst_parts;
    std::vector<scope_guard> dst_parts_locks;

    static const String TMP_PREFIX = "tmp_replace_from_";

    bool are_policies_partition_op_compatible = getStoragePolicy()->isCompatibleForPartitionOps(source_table->getStoragePolicy());
    for (const DataPartPtr & src_part : src_parts)
    {
        if (is_all)
            partition_id = src_part->partition.getID(src_data);

        if (!canReplacePartition(src_part))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Cannot replace partition '{}' because part '{}' has inconsistent granularity with table",
                            partition_id, src_part->name);

        /// This will generate unique name in scope of current server process.
        Int64 temp_index = insert_increment.get();
        MergeTreePartInfo dst_part_info(partition_id, temp_index, temp_index, src_part->info.level);

        IDataPartStorage::ClonePartParams clone_params{.txn = local_context->getCurrentTransaction()};
        if (replace)
        {
            /// Replace can only work on the same disk
            auto [dst_part, part_lock] = cloneAndLoadDataPart(
                src_part,
                TMP_PREFIX,
                dst_part_info,
                my_metadata_snapshot,
                clone_params,
                local_context->getReadSettings(),
                local_context->getWriteSettings(),
                !are_policies_partition_op_compatible /*must_on_same_disk*/);
            dst_parts.emplace_back(std::move(dst_part));
            dst_parts_locks.emplace_back(std::move(part_lock));
        }
        else
        {
            /// Attach can work on another disk
            auto [dst_part, part_lock] = cloneAndLoadDataPart(
                src_part,
                TMP_PREFIX,
                dst_part_info,
                my_metadata_snapshot,
                clone_params,
                local_context->getReadSettings(),
                local_context->getWriteSettings(),
                false/*must_on_same_disk*/);
            dst_parts.emplace_back(std::move(dst_part));
            dst_parts_locks.emplace_back(std::move(part_lock));
        }
    }

    /// ATTACH empty part set
    if (!replace && dst_parts.empty())
        return;

    MergeTreePartInfo drop_range;
    std::unique_ptr<PlainCommittingBlockHolder> block_holder;

    if (replace)
    {
        block_holder = allocateBlockNumber(CommittingBlock::Op::NewPart);
        drop_range.setPartitionId(partition_id);
        drop_range.min_block = 0;
        drop_range.max_block = block_holder->block.number; // there will be a "hole" in block numbers
        drop_range.level = std::numeric_limits<decltype(drop_range.level)>::max();
    }

    /// Atomically add new parts and remove old ones
    try
    {
        {
            /// Here we use the transaction just like RAII since rare errors in renameTempPartAndReplace() are possible
            ///  and we should be able to rollback already added (Precomitted) parts
            Transaction transaction(*this, local_context->getCurrentTransaction().get());

            auto data_parts_lock = lockParts();
            std::vector<std::unique_ptr<PlainCommittingBlockHolder>> block_holders;

            /** It is important that obtaining new block number and adding that block to parts set is done atomically.
              * Otherwise there is race condition - merge of blocks could happen in interval that doesn't yet contain new part.
              */
            for (auto part : dst_parts)
            {
                block_holders.emplace_back(fillNewPartName(part, data_parts_lock));
                renameTempPartAndReplaceUnlocked(part, transaction, data_parts_lock, /*rename_in_transaction=*/ false);
            }
            /// Populate transaction
            transaction.commit(data_parts_lock);

            /// If it is REPLACE (not ATTACH), remove all parts which max_block_number less then min_block_number of the first new block
            if (replace)
                removePartsInRangeFromWorkingSet(local_context->getCurrentTransaction().get(), drop_range, data_parts_lock);
        }

        /// Note: same elapsed time and profile events for all parts is used
        PartLog::addNewParts(getContext(), PartLog::createPartLogEntries(dst_parts, watch.elapsed(), profile_events_scope.getSnapshot()));
    }
    catch (...)
    {
        PartLog::addNewParts(getContext(), PartLog::createPartLogEntries(dst_parts, watch.elapsed()), ExecutionStatus::fromCurrentException("", true));
        throw;
    }
}

void StorageMergeTree::movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, ContextPtr local_context)
{
    auto dest_table_storage = std::dynamic_pointer_cast<StorageMergeTree>(dest_table);
    if (!dest_table_storage)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Table {} supports movePartitionToTable only for MergeTree family of table engines. Got {}",
                        getStorageID().getNameForLogs(), dest_table->getName());
    bool are_policies_partition_op_compatible = getStoragePolicy()->isCompatibleForPartitionOps(dest_table_storage->getStoragePolicy());

    if (!are_policies_partition_op_compatible)
        throw Exception(
            ErrorCodes::UNKNOWN_POLICY,
            "Destination table {} should have the same storage policy of source table, or the policies must be compatible for partition "
            "operations {}. {}: {}, {}: {}",
            dest_table_storage->getStorageID().getNameForLogs(),
            getStorageID().getNameForLogs(),
            getStorageID().getNameForLogs(),
            this->getStoragePolicy()->getName(),
            dest_table_storage->getStorageID().getNameForLogs(),
            dest_table_storage->getStoragePolicy()->getName());

    // Use the same back-pressure (delay/throw) logic as for INSERTs to be consistent and avoid possibility of exceeding part limits using MOVE PARTITION queries
    dest_table_storage->delayInsertOrThrowIfNeeded(nullptr, local_context, true);
    const auto & settings = local_context->getSettingsRef();
    auto lock1 = lockForShare(local_context->getCurrentQueryId(), settings[Setting::lock_acquire_timeout]);
    auto lock2 = dest_table->lockForShare(local_context->getCurrentQueryId(), settings[Setting::lock_acquire_timeout]);
    auto merges_blocker = stopMergesAndWait();

    /// Lock both mutexes in a deadlock-free order to prevent potential deadlock
    /// when two concurrent MOVE PARTITION operations work with the same pair of tables
    /// in opposite directions (A→B and B→A).
    /// This is equivalent to calling `lockOperationsWithParts` on both tables; see also MergeTreeData.h.
    std::lock(operation_with_data_parts_mutex, dest_table_storage->operation_with_data_parts_mutex);
    OperationDataPartsLock operation_data_parts_lock_src(operation_with_data_parts_mutex, std::adopt_lock);
    OperationDataPartsLock operation_data_parts_lock_dest(dest_table_storage->operation_with_data_parts_mutex, std::adopt_lock);

    auto dest_metadata_snapshot = dest_table->getInMemoryMetadataPtr();
    auto metadata_snapshot = getInMemoryMetadataPtr();
    Stopwatch watch;
    ProfileEventsScope profile_events_scope;

    MergeTreeData & src_data = dest_table_storage->checkStructureAndGetMergeTreeData(*this, metadata_snapshot, dest_metadata_snapshot);
    String partition_id = getPartitionIDFromQuery(partition, local_context);

    DataPartsVector src_parts = src_data.getVisibleDataPartsVectorInPartition(local_context, partition_id);
    if (src_parts.size() > settings[Setting::max_parts_to_move])
    {
        /// Moving a large number of parts at once can take a long time or get stuck in a retry loop in case of an S3 error, for example.
        /// Since merging is blocked, it can lead to a kind of deadlock:
        /// MOVE cannot be done because of the number of parts, and merges are not executed because of the MOVE.
        /// So abort the operation until parts are merged and user should retry
        throw Exception(ErrorCodes::TOO_MANY_PARTS,
                        "Cannot move {} parts at once, the limit is {}. "
                        "Wait until some parts are merged and retry, move smaller partitions, or increase the setting 'max_parts_to_move'.",
                        src_parts.size(), settings[Setting::max_parts_to_move].value);
    }

    MutableDataPartsVector dst_parts;
    std::vector<scope_guard> dst_parts_locks;

    static const String TMP_PREFIX = "tmp_move_from_";

    for (const DataPartPtr & src_part : src_parts)
    {
        if (!dest_table_storage->canReplacePartition(src_part))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Cannot move partition '{}' because part '{}' has inconsistent granularity with table",
                            partition_id, src_part->name);

        /// This will generate unique name in scope of current server process.
        Int64 temp_index = insert_increment.get();
        MergeTreePartInfo dst_part_info(partition_id, temp_index, temp_index, src_part->info.level);

        IDataPartStorage::ClonePartParams clone_params
        {
            .txn = local_context->getCurrentTransaction(),
            .copy_instead_of_hardlink = (*getSettings())[MergeTreeSetting::always_use_copy_instead_of_hardlinks],
        };

        auto [dst_part, part_lock] = dest_table_storage->cloneAndLoadDataPart(
            src_part,
            TMP_PREFIX,
            dst_part_info,
            dest_metadata_snapshot,
            clone_params,
            local_context->getReadSettings(),
            local_context->getWriteSettings(),
            !are_policies_partition_op_compatible /*must_on_same_disk*/
        );

        dst_parts.emplace_back(std::move(dst_part));
        dst_parts_locks.emplace_back(std::move(part_lock));
    }

    /// empty part set
    if (dst_parts.empty())
        return;

    /// Move new parts to the destination table. NOTE It doesn't look atomic.
    try
    {
        auto txn = local_context->getCurrentTransaction();
        auto future_parts = initCoverageWithNewEmptyParts(src_parts);
        auto [new_empty_covering_src_parts, _] = createEmptyDataParts(*this, future_parts, txn);

        Transaction dest_transaction(*dest_table_storage, txn.get());
        std::vector<std::unique_ptr<PlainCommittingBlockHolder>> block_holders;
        {
            auto dest_data_parts_lock = dest_table_storage->lockParts();

            for (auto & part : dst_parts)
            {
                block_holders.push_back(dest_table_storage->fillNewPartName(part, dest_data_parts_lock));
                dest_table_storage->renameTempPartAndReplaceUnlocked(part, dest_transaction, dest_data_parts_lock, /*rename_in_transaction=*/true);
            }
        }

        Transaction src_transaction(*this, txn.get());
        {
            auto src_data_parts_lock = lockParts();

            for (auto & part : new_empty_covering_src_parts)
            {
                renameTempPartAndReplaceUnlocked(part, src_data_parts_lock, src_transaction, /*rename_in_transaction=*/true);
            }
        }

        dest_transaction.renameParts();
        dest_transaction.commit();

        src_transaction.renameParts();
        src_transaction.commit();

        clearOldPartsFromFilesystem();

        /// Note: same elapsed time and profile events for all parts is used
        PartLog::addNewParts(getContext(), PartLog::createPartLogEntries(dst_parts, watch.elapsed(), profile_events_scope.getSnapshot()));
    }
    catch (...)
    {
        PartLog::addNewParts(getContext(), PartLog::createPartLogEntries(dst_parts, watch.elapsed()), ExecutionStatus::fromCurrentException("", true));
        throw;
    }
}

ActionLock StorageMergeTree::getActionLock(StorageActionBlockType action_type)
{
    if (action_type == ActionLocks::PartsMerge)
        return merger_mutator.merges_blocker.cancel();
    if (action_type == ActionLocks::PartsTTLMerge)
        return merger_mutator.ttl_merges_blocker.cancel();
    if (action_type == ActionLocks::PartsMove)
        return parts_mover.moves_blocker.cancel();

    return {};
}

void StorageMergeTree::onActionLockRemove(StorageActionBlockType action_type)
{
    if (action_type == ActionLocks::PartsMerge ||  action_type == ActionLocks::PartsTTLMerge)
        background_operations_assignee.trigger();
    else if (action_type == ActionLocks::PartsMove)
        background_moves_assignee.trigger();
}

IStorage::DataValidationTasksPtr StorageMergeTree::getCheckTaskList(
    const std::variant<std::monostate, ASTPtr, String> & check_task_filter, ContextPtr local_context)
{
    DataPartsVector data_parts;
    if (const auto * partition_opt = std::get_if<ASTPtr>(&check_task_filter))
    {
        const auto & partition = *partition_opt;
        if (!partition->as<ASTPartition>())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected partition, got {}", partition->formatForErrorMessage());
        String partition_id = getPartitionIDFromQuery(partition, local_context);
        data_parts = getVisibleDataPartsVectorInPartition(local_context, partition_id);
    }
    else if (const auto * part_name = std::get_if<String>(&check_task_filter))
    {
        auto part = getPartIfExists(*part_name, {MergeTreeDataPartState::Active, MergeTreeDataPartState::Outdated});
        if (!part)
            throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "No such data part '{}' to check in table '{}'",
                            *part_name, getStorageID().getFullTableName());
        data_parts.emplace_back(std::move(part));
    }
    else
        data_parts = getVisibleDataPartsVector(local_context);

    return std::make_unique<DataValidationTasks>(std::move(data_parts), local_context);
}

std::optional<CheckResult> StorageMergeTree::checkDataNext(DataValidationTasksPtr & check_task_list)
{
    auto * data_validation_tasks = assert_cast<DataValidationTasks *>(check_task_list.get());
    auto local_context = data_validation_tasks->context;
    if (auto part = data_validation_tasks->next())
    {
        /// If the checksums file is not present, calculate the checksums and write them to disk.
        static constexpr auto checksums_path = "checksums.txt";
        bool noop;
        if (!part->getDataPartStorage().existsFile(checksums_path))
        {
            try
            {
                auto calculated_checksums = checkDataPart(part, false, noop, /* is_cancelled */[]{ return false; }, /* throw_on_broken_projection */true);
                calculated_checksums.checkEqual(part->checksums, true, part->name);

                auto & part_mutable = const_cast<IMergeTreeDataPart &>(*part);
                part_mutable.writeChecksums(part->checksums, local_context->getWriteSettings());

                return CheckResult(part->name, true, "Checksums recounted and written to disk.");
            }
            catch (...)
            {
                if (isRetryableException(std::current_exception()))
                    throw;

                tryLogCurrentException(log, __PRETTY_FUNCTION__);
                return CheckResult(part->name, false, "Check of part finished with error: '" + getCurrentExceptionMessage(false) + "'");
            }
        }
        else
        {
            try
            {
                checkDataPart(part, true, noop, /* is_cancelled */[]{ return false; }, /* throw_on_broken_projection */true);
                return CheckResult(part->name, true, "");
            }
            catch (...)
            {
                if (isRetryableException(std::current_exception()))
                    throw;

                return CheckResult(part->name, false, getCurrentExceptionMessage(false));
            }
        }
    }

    return {};
}


void StorageMergeTree::backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & partitions)
{
    const auto & backup_settings = backup_entries_collector.getBackupSettings();
    auto local_context = backup_entries_collector.getContext();

    DataPartsVector data_parts;
    if (partitions)
        data_parts = getVisibleDataPartsVectorInPartitions(local_context, getPartitionIDsFromQuery(*partitions, local_context));
    else
        data_parts = getVisibleDataPartsVector(local_context);

    Int64 min_data_version = std::numeric_limits<Int64>::max();
    for (const auto & data_part : data_parts)
        min_data_version = std::min(min_data_version, data_part->info.getDataVersion() + 1);

    auto parts_backup_entries = backupParts(data_parts, data_path_in_backup, backup_settings, local_context);
    for (auto & part_backup_entries : parts_backup_entries)
        backup_entries_collector.addBackupEntries(std::move(part_backup_entries.backup_entries));

    backup_entries_collector.addBackupEntries(backupMutations(min_data_version, data_path_in_backup));
}


BackupEntries StorageMergeTree::backupMutations(UInt64 version, const String & data_path_in_backup) const
{
    std::lock_guard lock(currently_processing_in_background_mutex);

    fs::path mutations_path_in_backup = fs::path{data_path_in_backup} / "mutations";
    BackupEntries backup_entries;
    for (auto it = current_mutations_by_version.lower_bound(version); it != current_mutations_by_version.end(); ++it)
        backup_entries.emplace_back(mutations_path_in_backup / fmt::format("{:010}.txt", it->first), it->second.backup());
    return backup_entries;
}


void StorageMergeTree::attachRestoredParts(MutableDataPartsVector && parts)
{
    for (auto part : parts)
    {
        /// It's important to create it outside of lock scope because
        /// otherwise it can lock parts in destructor and deadlock is possible.
        MergeTreeData::Transaction transaction(*this, NO_TRANSACTION_RAW);
        {
            auto lock = lockParts();
            auto block_holder = fillNewPartName(part, lock);
            renameTempPartAndAdd(part, transaction, lock, /*rename_in_transaction=*/ false);
            transaction.commit(lock);
        }
    }
}

StorageMergeTree::MutationsSnapshot::MutationsSnapshot(Params params_, MutationCounters counters_, MutationsByVersion mutations_snapshot, DataPartsVector patches_)
    : MutationsSnapshotBase(std::move(params_), std::move(counters_), std::move(patches_))
    , mutations_by_version(std::move(mutations_snapshot))
{
}

MutationCommands StorageMergeTree::MutationsSnapshot::getOnFlyMutationCommandsForPart(const DataPartPtr & part) const
{
    MutationCommands result;
    UInt64 part_data_version = part->info.getDataVersion();

    for (const auto & [mutation_version, commands] : mutations_by_version | std::views::reverse)
    {
        if (mutation_version <= part_data_version)
            break;

        addSupportedCommands(*commands, mutation_version, result);
    }

    std::reverse(result.begin(), result.end());
    return result;
}

NameSet StorageMergeTree::MutationsSnapshot::getAllUpdatedColumns() const
{
    NameSet res = getColumnsUpdatedInPatches();
    if (!hasDataMutations())
        return res;

    for (const auto & [version, commands] : mutations_by_version)
    {
        auto names = commands->getAllUpdatedColumns();
        std::move(names.begin(), names.end(), std::inserter(res, res.end()));
    }
    return res;
}

MergeTreeData::MutationsSnapshotPtr StorageMergeTree::getMutationsSnapshot(const IMutationsSnapshot::Params & params) const
{
    DataPartsVector patch_parts;
    MutationCounters mutations_snapshot_counters;
    MutationsSnapshot::MutationsByVersion mutations_snapshot;

    if (params.need_patch_parts)
        patch_parts = getPatchPartsVectorForInternalUsage();

    std::lock_guard lock(currently_processing_in_background_mutex);
    if (!params.need_data_mutations && !params.need_alter_mutations && mutation_counters.num_metadata <= 0)
        return std::make_shared<MutationsSnapshot>(params, std::move(mutations_snapshot_counters), std::move(mutations_snapshot), std::move(patch_parts));

    UInt64 max_mutation_version = std::numeric_limits<UInt64>::max();
    if (params.max_mutation_versions && !params.max_mutation_versions->empty())
        max_mutation_version = params.max_mutation_versions->begin()->second;

    for (const auto & [version, entry] : current_mutations_by_version)
    {
        /// Copy a pointer to all commands to avoid extracting and copying them.
        /// Required commands will be copied later only for specific parts.
        if (version <= max_mutation_version && MergeTreeData::IMutationsSnapshot::needIncludeMutationToSnapshot(params, *entry.commands))
        {
            mutations_snapshot.emplace(version, entry.commands);
            incrementMutationsCounters(mutations_snapshot_counters, *entry.commands);
        }
    }

    return std::make_shared<MutationsSnapshot>(params, std::move(mutations_snapshot_counters), std::move(mutations_snapshot), std::move(patch_parts));
}

MutationCounters StorageMergeTree::getMutationCounters() const
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    return mutation_counters;
}

void StorageMergeTree::startBackgroundMovesIfNeeded()
{
    if (areBackgroundMovesNeeded())
        background_moves_assignee.start();
}

std::unique_ptr<MergeTreeSettings> StorageMergeTree::getDefaultSettings() const
{
    return std::make_unique<MergeTreeSettings>(getContext()->getMergeTreeSettings());
}

PreparedSetsCachePtr StorageMergeTree::getPreparedSetsCache(Int64 mutation_id)
{
    auto l = std::lock_guard(mutation_prepared_sets_cache_mutex);

    /// Cleanup stale entries where the shared_ptr is expired.
    while (!mutation_prepared_sets_cache.empty())
    {
        auto it = mutation_prepared_sets_cache.begin();
        if (it->second.lock())
            break;
        mutation_prepared_sets_cache.erase(it);
    }

    /// Look up an existing entry.
    auto it = mutation_prepared_sets_cache.find(mutation_id);
    if (it != mutation_prepared_sets_cache.end())
    {
        /// If the entry is still alive, return it.
        auto existing_set_cache = it->second.lock();
        if (existing_set_cache)
            return existing_set_cache;
    }

    /// Create new entry.
    auto cache = std::make_shared<PreparedSetsCache>();
    mutation_prepared_sets_cache[mutation_id] = cache;
    return cache;
}

void StorageMergeTree::assertNotReadonly() const
{
    if (isStaticStorage())
        throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "Table is in readonly mode due to static storage");
}

std::unique_ptr<PlainCommittingBlockHolder> StorageMergeTree::fillNewPartName(MutableDataPartPtr & part, DataPartsLock &)
{
    auto block_holder = allocateBlockNumber(CommittingBlock::Op::NewPart);

    part->info.min_block = block_holder->block.number;
    part->info.max_block = block_holder->block.number;
    part->setName(part->getNewName(part->info));

    return block_holder;
}

std::unique_ptr<PlainCommittingBlockHolder> StorageMergeTree::fillNewPartNameAndResetLevel(MutableDataPartPtr & part, DataPartsLock &)
{
    auto block_holder = allocateBlockNumber(CommittingBlock::Op::NewPart);

    part->info.min_block = block_holder->block.number;
    part->info.max_block = block_holder->block.number;
    part->info.mutation = 0;

    bool keep_non_zero_level = merging_params.mode != MergeTreeData::MergingParams::Ordinary;
    part->info.level = (keep_non_zero_level && part->info.level > 0) ? 1 : 0;
    part->setName(part->getNewName(part->info));

    return block_holder;
}

void StorageMergeTree::removeCommittingBlock(CommittingBlock block)
{
    std::lock_guard lock(committing_blocks_mutex);
    committing_blocks.erase(block);
    committing_blocks_cv.notify_one();
}

std::unique_ptr<PlainCommittingBlockHolder> StorageMergeTree::allocateBlockNumber(CommittingBlock::Op op)
{
    std::lock_guard lock(committing_blocks_mutex);

    CommittingBlock block(op, increment.get());
    auto block_holder = std::make_unique<PlainCommittingBlockHolder>(std::move(block), *this);
    committing_blocks.insert(block_holder->block);

    LOG_DEBUG(log, "Allocated block number {}", block_holder->block.number);
    return block_holder;
}

void StorageMergeTree::waitForCommittingInsertsAndMutations(Int64 max_block_number, size_t timeout_ms) const
{
    auto all_committed = [&]
    {
        for (const auto & block : committing_blocks)
        {
            if (block.number >= max_block_number)
                break;

            if (block.op != CommittingBlock::Op::Update)
                return false;
        }

        return true;
    };

    std::unique_lock lock(committing_blocks_mutex);
    bool res = committing_blocks_cv.wait_for(lock, std::chrono::milliseconds(timeout_ms), all_committed);

    if (!res)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Failed to wait ({} ms) for inserts and mutations to commit up to block number {}", timeout_ms, max_block_number);
}

CommittingBlocksSet StorageMergeTree::getCommittingBlocks() const
{
    std::lock_guard lock(committing_blocks_mutex);
    return committing_blocks;
}
}
