#include "StorageMergeTree.h"
#include "Core/QueryProcessingStage.h"
#include "Storages/MergeTree/IMergeTreeDataPart.h"

#include <optional>
#include <ranges>

#include <base/sort.h>
#include <Backups/BackupEntriesCollector.h>
#include <Databases/IDatabase.h>
#include <Common/MemoryTracker.h>
#include <Common/escapeForFileName.h>
#include <Common/ProfileEventsScope.h>
#include <Common/typeid_cast.h>
#include <Common/ThreadPool.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/Context.h>
#include <Interpreters/TransactionLog.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <IO/copyData.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/queryToString.h>
#include <Parsers/formatAST.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/AlterCommands.h>
#include <Storages/PartitionCommands.h>
#include <Storages/MergeTree/MergeTreeSink.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/MergeTree/MergePlainMergeTreeTask.h>
#include <Storages/MergeTree/PartitionPruner.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <fmt/core.h>


namespace DB
{


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
}

namespace ActionLocks
{
    extern const StorageActionBlockType PartsMerge;
    extern const StorageActionBlockType PartsTTLMerge;
    extern const StorageActionBlockType PartsMove;
}

static MergeTreeTransactionPtr tryGetTransactionForMutation(const MergeTreeMutationEntry & mutation, Poco::Logger * log = nullptr)
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


StorageMergeTree::StorageMergeTree(
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    bool attach,
    ContextMutablePtr context_,
    const String & date_column_name,
    const MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> storage_settings_,
    bool has_force_restore_data_flag)
    : MergeTreeData(
        table_id_,
        metadata_,
        context_,
        date_column_name,
        merging_params_,
        std::move(storage_settings_),
        false,      /// require_part_metadata
        attach)
    , reader(*this)
    , writer(*this)
    , merger_mutator(*this)
{
    initializeDirectoriesAndFormatVersion(relative_data_path_, attach, date_column_name);

    loadDataParts(has_force_restore_data_flag);

    if (!attach && !getDataPartsForInternalUsage().empty() && !isStaticStorage())
        throw Exception(ErrorCodes::INCORRECT_DATA,
                        "Data directory for table already containing data parts - probably "
                        "it was unclean DROP table or manual intervention. "
                        "You must either clear directory by hand or use ATTACH TABLE instead "
                        "of CREATE TABLE if you need to use that parts.");

    increment.set(getMaxBlockNumber());

    loadMutations();
    loadDeduplicationLog();
}


void StorageMergeTree::startup()
{
    clearOldWriteAheadLogs();
    clearEmptyParts();

    /// Temporary directories contain incomplete results of merges (after forced restart)
    ///  and don't allow to reinitialize them, so delete each of them immediately
    clearOldTemporaryDirectories(0, {"tmp_", "delete_tmp_", "tmp-fetch_"});

    /// NOTE background task will also do the above cleanups periodically.
    time_after_previous_cleanup_parts.restart();
    time_after_previous_cleanup_temporary_directories.restart();

    /// Do not schedule any background jobs if current storage has static data files.
    if (isStaticStorage())
        return;

    try
    {
        background_operations_assignee.start();
        startBackgroundMovesIfNeeded();
        startOutdatedDataPartsLoadingTask();
    }
    catch (...)
    {
        /// Exception safety: failed "startup" does not require a call to "shutdown" from the caller.
        /// And it should be able to safely destroy table after exception in "startup" method.
        /// It means that failed "startup" must not create any background tasks that we will have to wait.
        try
        {
            shutdown();
        }
        catch (...)
        {
            std::terminate();
        }

        /// Note: after failed "startup", the table will be in a state that only allows to destroy the object.
        throw;
    }
}

void StorageMergeTree::shutdown()
{
    if (shutdown_called.exchange(true))
        return;

    stopOutdatedDataPartsLoadingTask();

    /// Unlock all waiting mutations
    {
        std::lock_guard lock(mutation_wait_mutex);
        mutation_wait_event.notify_all();
    }

    merger_mutator.merges_blocker.cancelForever();
    parts_mover.moves_blocker.cancelForever();

    background_operations_assignee.finish();
    background_moves_assignee.finish();

    if (deduplication_log)
        deduplication_log->shutdown();
}


StorageMergeTree::~StorageMergeTree()
{
    shutdown();
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
    if (!query_info.parallel_replicas_disabled &&
        local_context->canUseParallelReplicasOnInitiator() &&
        local_context->getSettingsRef().parallel_replicas_for_non_replicated_merge_tree)
    {
        auto table_id = getStorageID();

        const auto & modified_query_ast =  ClusterProxy::rewriteSelectQuery(
            local_context, query_info.query,
            table_id.database_name, table_id.table_name, /*remote_table_function_ptr*/nullptr);

        String cluster_for_parallel_replicas = local_context->getSettingsRef().cluster_for_parallel_replicas;
        auto cluster = local_context->getCluster(cluster_for_parallel_replicas);

        Block header;

        if (local_context->getSettingsRef().allow_experimental_analyzer)
            header = InterpreterSelectQueryAnalyzer::getSampleBlock(modified_query_ast, local_context, SelectQueryOptions(processed_stage).analyze());
        else
            header = InterpreterSelectQuery(modified_query_ast, local_context, SelectQueryOptions(processed_stage).analyze()).getSampleBlock();

        ClusterProxy::SelectStreamFactory select_stream_factory =
            ClusterProxy::SelectStreamFactory(
                header,
                {},
                storage_snapshot,
                processed_stage);

        ClusterProxy::executeQueryWithParallelReplicas(
            query_plan, getStorageID(), /*remove_table_function_ptr*/ nullptr,
            select_stream_factory, modified_query_ast,
            local_context, query_info, cluster);
    }
    else
    {
        const bool enable_parallel_reading =
            !query_info.parallel_replicas_disabled &&
            local_context->canUseParallelReplicasOnFollower() &&
            local_context->getSettingsRef().parallel_replicas_for_non_replicated_merge_tree;

        if (auto plan = reader.read(
            column_names, storage_snapshot, query_info,
            local_context, max_block_size, num_streams,
            processed_stage, nullptr, enable_parallel_reading))
            query_plan = std::move(*plan);
    }

    /// Now, copy of parts that is required for the query, stored in the processors,
    /// while snapshot_data.parts includes all parts, even one that had been filtered out with partition pruning,
    /// reset them to avoid holding them.
    auto & snapshot_data = assert_cast<MergeTreeData::SnapshotData &>(*storage_snapshot->data);
    snapshot_data.parts = {};
    snapshot_data.alter_conversions = {};
}

std::optional<UInt64> StorageMergeTree::totalRows(const Settings &) const
{
    return getTotalActiveSizeInRows();
}

std::optional<UInt64> StorageMergeTree::totalRowsByPartitionPredicate(const SelectQueryInfo & query_info, ContextPtr local_context) const
{
    auto parts = getVisibleDataPartsVector(local_context);
    return totalRowsByPartitionPredicateImpl(query_info, local_context, parts);
}

std::optional<UInt64> StorageMergeTree::totalBytes(const Settings &) const
{
    return getTotalActiveSizeInBytes();
}

SinkToStoragePtr
StorageMergeTree::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool /*async_insert*/)
{
    const auto & settings = local_context->getSettingsRef();
    return std::make_shared<MergeTreeSink>(
        *this, metadata_snapshot, settings.max_partitions_per_insert_block, local_context);
}

void StorageMergeTree::checkTableCanBeDropped([[ maybe_unused ]] ContextPtr query_context) const
{
    if (!supportsReplication() && isStaticStorage())
        return;

    auto table_id = getStorageID();
    getContext()->checkTableCanBeDropped(table_id.database_name, table_id.table_name, getTotalActiveSizeInBytes());
}

void StorageMergeTree::drop()
{
    shutdown();
    /// In case there is read-only disk we cannot allow to call dropAllData(), but dropping tables is allowed.
    if (isStaticStorage())
        return;
    dropAllData();
}

void StorageMergeTree::alter(
    const AlterCommands & commands,
    ContextPtr local_context,
    AlterLockHolder & table_lock_holder)
{
    if (local_context->getCurrentTransaction() && local_context->getSettingsRef().throw_on_unsupported_query_inside_transaction)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ALTER METADATA is not supported inside transactions");

    auto table_id = getStorageID();
    auto old_storage_settings = getSettings();

    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    StorageInMemoryMetadata old_metadata = getInMemoryMetadata();

    auto maybe_mutation_commands = commands.getMutationCommands(new_metadata, local_context->getSettingsRef().materialize_ttl_after_modify, local_context);
    if (!maybe_mutation_commands.empty())
        delayMutationOrThrowIfNeeded(nullptr, local_context);

    Int64 mutation_version = -1;
    commands.apply(new_metadata, local_context);

    /// This alter can be performed at new_metadata level only
    if (commands.isSettingsAlter())
    {
        changeSettings(new_metadata.settings_changes, table_lock_holder);
        DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(local_context, table_id, new_metadata);
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

            DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(local_context, table_id, new_metadata);

            if (!maybe_mutation_commands.empty())
                mutation_version = startMutation(maybe_mutation_commands, local_context);
        }

        {
            /// Reset Object columns, because column of type
            /// Object may be added or dropped by alter.
            auto parts_lock = lockParts();
            resetObjectColumnsFromActiveParts(parts_lock);
        }

        /// Always execute required mutations synchronously, because alters
        /// should be executed in sequential order.
        if (!maybe_mutation_commands.empty())
            waitForMutation(mutation_version, false);
    }

    {
        /// Some additional changes in settings
        auto new_storage_settings = getSettings();

        if (old_storage_settings->non_replicated_deduplication_window != new_storage_settings->non_replicated_deduplication_window)
        {
            /// We cannot place this check into settings sanityCheck because it depends on format_version.
            /// sanityCheck must work event without storage.
            if (new_storage_settings->non_replicated_deduplication_window != 0 && format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deduplication for non-replicated MergeTree in old syntax is not supported");

            deduplication_log->setDeduplicationWindowSize(new_storage_settings->non_replicated_deduplication_window);
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
        reserved_space = storage.tryReserveSpace(total_size, future_part->parts[0]->getDataPartStorage());
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
        else
            throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "Not enough space for merging parts");
    }

    future_part->updatePath(storage, reserved_space.get());

    for (const auto & part : future_part->parts)
    {
        if (storage.currently_merging_mutating_parts.contains(part))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Tagging already tagged part {}. This is a bug.", part->name);
    }
    storage.currently_merging_mutating_parts.insert(future_part->parts.begin(), future_part->parts.end());
}

CurrentlyMergingPartsTagger::~CurrentlyMergingPartsTagger()
{
    std::lock_guard lock(storage.currently_processing_in_background_mutex);

    for (const auto & part : future_part->parts)
    {
        if (!storage.currently_merging_mutating_parts.contains(part))
            std::terminate();
        storage.currently_merging_mutating_parts.erase(part);
    }

    storage.currently_processing_in_background_condition.notify_all();
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

    Int64 version;
    {
        std::lock_guard lock(currently_processing_in_background_mutex);

        MergeTreeMutationEntry entry(commands, disk, relative_data_path, insert_increment.get(), current_tid, getContext()->getWriteSettings());
        version = increment.get();
        entry.commit(version);
        String mutation_id = entry.file_name;
        if (txn)
            txn->addMutation(shared_from_this(), mutation_id);
        bool inserted = current_mutations_by_version.try_emplace(version, std::move(entry)).second;
        if (!inserted)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Mutation {} already exists, it's a bug", version);

        LOG_INFO(log, "Added mutation: {}{}", mutation_id, additional_info);
    }
    background_operations_assignee.trigger();
    return version;
}


void StorageMergeTree::updateMutationEntriesErrors(FutureMergedMutatedPartPtr result_part, bool is_successful, const String & exception_message)
{
    /// Update the information about failed parts in the system.mutations table.

    Int64 sources_data_version = result_part->parts.at(0)->info.getDataVersion();
    Int64 result_data_version = result_part->part_info.getDataVersion();
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
                }
            }
            else
            {
                entry.latest_failed_part = result_part->parts.at(0)->name;
                entry.latest_failed_part_info = result_part->parts.at(0)->info;
                entry.latest_fail_time = time(nullptr);
                entry.latest_fail_reason = exception_message;
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

        std::unique_lock lock(mutation_wait_mutex);
        mutation_wait_event.wait(lock, check);
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
    delayMutationOrThrowIfNeeded(nullptr, query_context);

    /// Validate partition IDs (if any) before starting mutation
    getPartitionIdsAffectedByCommands(commands, query_context);

    Int64 version;
    {
        /// It's important to serialize order of mutations with alter queries because
        /// they can depend on each other.
        if (auto alter_lock = tryLockForAlter(query_context->getSettings().lock_acquire_timeout); alter_lock == std::nullopt)
        {
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED,
                            "Cannot start mutation in {}ms because some metadata-changing ALTER (MODIFY|RENAME|ADD|DROP) is currently executing. "
                            "You can change this timeout with `lock_acquire_timeout` setting",
                            query_context->getSettings().lock_acquire_timeout.totalMilliseconds());
        }
        version = startMutation(commands, query_context);
    }

    if (query_context->getSettingsRef().mutations_sync > 0 || query_context->getCurrentTransaction())
        waitForMutation(version, false);
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

    auto txn = tryGetTransactionForMutation(mutation_entry, log);
    /// There's no way a transaction may finish before a mutation that was started by the transaction.
    /// But sometimes we need to check status of an unrelated mutation, in this case we don't care about transactions.
    assert(txn || mutation_entry.tid.isPrehistoric() || from_another_mutation);
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
                    result.latest_fail_time = time(nullptr);
                }
            }

            return result;
        }
    }

    result.is_done = true;
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

        for (const MutationCommand & command : entry.commands)
        {
            WriteBufferFromOwnString buf;
            formatAST(*command.ast, buf, false, true);
            result.push_back(MergeTreeMutationStatus
            {
                entry.file_name,
                buf.str(),
                entry.create_time,
                block_numbers_map,
                parts_to_do_names,
                /* is_done = */parts_to_do_names.empty(),
                entry.latest_failed_part,
                entry.latest_fail_time,
                entry.latest_fail_reason,
            });
        }
    }

    return result;
}

CancellationCode StorageMergeTree::killMutation(const String & mutation_id)
{
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
            to_kill.emplace(std::move(it->second));
            current_mutations_by_version.erase(it);
        }
    }

    if (!to_kill)
        return CancellationCode::NotFound;

    if (auto txn = tryGetTransactionForMutation(*to_kill, log))
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
    if (settings->non_replicated_deduplication_window != 0 && format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Deduplication for non-replicated MergeTree in old syntax is not supported");

    auto disk = getDisks()[0];
    std::string path = fs::path(relative_data_path) / "deduplication_logs";
    deduplication_log = std::make_unique<MergeTreeDeduplicationLog>(path, settings->non_replicated_deduplication_window, format_version, disk);
    deduplication_log->load();
}

void StorageMergeTree::loadMutations()
{
    for (const auto & disk : getDisks())
    {
        for (auto it = disk->iterateDirectory(relative_data_path); it->isValid(); it->next())
        {
            if (startsWith(it->name(), "mutation_"))
            {
                MergeTreeMutationEntry entry(disk, relative_data_path, it->name());
                UInt64 block_number = entry.block_number;
                LOG_DEBUG(log, "Loading mutation: {} entry, commands size: {}", it->name(), entry.commands.size());

                if (!entry.tid.isPrehistoric() && !entry.csn)
                {
                    if (auto csn = TransactionLog::getCSN(entry.tid))
                    {
                        /// Transaction is committed => mutation is finished, but let's load it anyway (so it will be shown in system.mutations)
                        entry.writeCSN(csn);
                    }
                    else
                    {
                        TransactionLog::assertTIDIsNotOutdated(entry.tid);
                        LOG_DEBUG(log, "Mutation entry {} was created by transaction {}, but it was not committed. Removing mutation entry",
                                  it->name(), entry.tid);
                        disk->removeFile(it->path());
                        continue;
                    }
                }

                auto inserted = current_mutations_by_version.try_emplace(block_number, std::move(entry)).second;
                if (!inserted)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Mutation {} already exists, it's a bug", block_number);
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

MergeMutateSelectedEntryPtr StorageMergeTree::selectPartsToMerge(
    const StorageMetadataPtr & metadata_snapshot,
    bool aggressive,
    const String & partition_id,
    bool final,
    String & out_disable_reason,
    TableLockHolder & /* table_lock_holder */,
    std::unique_lock<std::mutex> & lock,
    const MergeTreeTransactionPtr & txn,
    bool optimize_skip_merged_partitions,
    SelectPartsDecision * select_decision_out)
{
    auto data_settings = getSettings();

    auto future_part = std::make_shared<FutureMergedMutatedPart>();

    if (storage_settings.get()->assign_part_uuids)
        future_part->uuid = UUIDHelpers::generateV4();

    /// You must call destructor with unlocked `currently_processing_in_background_mutex`.
    CurrentlyMergingPartsTaggerPtr merging_tagger;
    MergeList::EntryPtr merge_entry;

    auto can_merge = [this, &lock](const DataPartPtr & left, const DataPartPtr & right, const MergeTreeTransaction * tx, String & disable_reason) -> bool
    {
        if (tx)
        {
            /// Cannot merge parts if some of them are not visible in current snapshot
            /// TODO Transactions: We can use simplified visibility rules (without CSN lookup) here
            if ((left && !left->version.isVisible(tx->getSnapshot(), Tx::EmptyTID))
                    || (right && !right->version.isVisible(tx->getSnapshot(), Tx::EmptyTID)))
            {
                disable_reason = "Some part is not visible in transaction";
                return false;
            }

            /// Do not try to merge parts that are locked for removal (merge will probably fail)
            if ((left && left->version.isRemovalTIDLocked())
                    || (right && right->version.isRemovalTIDLocked()))
            {
                disable_reason = "Some part is locked for removal in another cuncurrent transaction";
                return false;
            }
        }

        /// This predicate is checked for the first part of each range.
        /// (left = nullptr, right = "first part of partition")
        if (!left)
        {
            if (currently_merging_mutating_parts.contains(right))
            {
                disable_reason = "Some part currently in a merging or mutating process";
                return false;
            }
            else
                return true;
        }

        if (currently_merging_mutating_parts.contains(left) || currently_merging_mutating_parts.contains(right))
        {
            disable_reason = "Some part currently in a merging or mutating process";
            return false;
        }

        if (getCurrentMutationVersion(left, lock) != getCurrentMutationVersion(right, lock))
        {
            disable_reason = "Some parts have different mutation version";
            return false;
        }

        if (!partsContainSameProjections(left, right))
        {
            disable_reason = "Some parts contains differ projections";
            return false;
        }

        auto max_possible_level = getMaxLevelInBetween(left, right);
        if (max_possible_level > std::max(left->info.level, right->info.level))
        {
            disable_reason = fmt::format("There is an outdated part in a gap between two active parts ({}, {}) with merge level {} higher than these active parts have", left->name, right->name, max_possible_level);
            return false;
        }

        return true;
    };

    SelectPartsDecision select_decision = SelectPartsDecision::CANNOT_SELECT;

    auto is_background_memory_usage_ok = [](String & disable_reason) -> bool
    {
        if (canEnqueueBackgroundTask())
            return true;
        disable_reason = fmt::format("Current background tasks memory usage ({}) is more than the limit ({})",
            formatReadableSizeWithBinarySuffix(background_memory_tracker.get()),
            formatReadableSizeWithBinarySuffix(background_memory_tracker.getSoftLimit()));
        return false;
    };

    if (partition_id.empty())
    {
        if (is_background_memory_usage_ok(out_disable_reason))
        {
            UInt64 max_source_parts_size = merger_mutator.getMaxSourcePartsSizeForMerge();
            bool merge_with_ttl_allowed = getTotalMergesWithTTLInMergeList() < data_settings->max_number_of_merges_with_ttl_in_pool;

            /// TTL requirements is much more strict than for regular merge, so
            /// if regular not possible, than merge with ttl is not also not
            /// possible.
            if (max_source_parts_size > 0)
            {
                select_decision = merger_mutator.selectPartsToMerge(
                    future_part,
                    aggressive,
                    max_source_parts_size,
                    can_merge,
                    merge_with_ttl_allowed,
                    txn,
                    out_disable_reason);
            }
            else
                out_disable_reason = "Current value of max_source_parts_size is zero";
        }
    }
    else
    {
        while (true)
        {
            auto timeout_ms = getSettings()->lock_acquire_timeout_for_background_operations.totalMilliseconds();
            auto timeout = std::chrono::milliseconds(timeout_ms);

            if (!is_background_memory_usage_ok(out_disable_reason))
            {
                constexpr auto poll_interval = std::chrono::seconds(1);
                Int64 attempts = timeout / poll_interval;
                bool ok = false;
                for (Int64 i = 0; i < attempts; ++i)
                {
                    std::this_thread::sleep_for(poll_interval);
                    if (is_background_memory_usage_ok(out_disable_reason))
                    {
                        ok = true;
                        break;
                    }
                }
                if (!ok)
                    break;
            }

            select_decision = merger_mutator.selectAllPartsToMergeWithinPartition(
                future_part, can_merge, partition_id, final, metadata_snapshot, txn, out_disable_reason, optimize_skip_merged_partitions);

            /// If final - we will wait for currently processing merges to finish and continue.
            if (final
                && select_decision != SelectPartsDecision::SELECTED
                && !currently_merging_mutating_parts.empty())
            {
                LOG_DEBUG(log, "Waiting for currently running merges ({} parts are merging right now) to perform OPTIMIZE FINAL",
                    currently_merging_mutating_parts.size());

                if (std::cv_status::timeout == currently_processing_in_background_condition.wait_for(lock, timeout))
                {
                    out_disable_reason = fmt::format("Timeout ({} ms) while waiting for already running merges before running OPTIMIZE with FINAL", timeout_ms);
                    break;
                }
            }
            else
                break;
        }
    }

    /// In case of final we need to know the decision of select in StorageMergeTree::merge
    /// to treat NOTHING_TO_MERGE as successful merge (otherwise optimize final will be uncompleted)
    if (select_decision_out)
        *select_decision_out = select_decision;

    if (select_decision != SelectPartsDecision::SELECTED)
    {
        if (!out_disable_reason.empty())
            out_disable_reason += ". ";
        out_disable_reason += "Cannot select parts for optimization";

        return {};
    }

    /// Account TTL merge here to avoid exceeding the max_number_of_merges_with_ttl_in_pool limit
    if (isTTLMergeType(future_part->merge_type))
        getContext()->getMergeList().bookMergeWithTTL();

    merging_tagger = std::make_unique<CurrentlyMergingPartsTagger>(future_part, MergeTreeDataMergerMutator::estimateNeededDiskSpace(future_part->parts), *this, metadata_snapshot, false);
    return std::make_shared<MergeMutateSelectedEntry>(future_part, std::move(merging_tagger), std::make_shared<MutationCommands>());
}

bool StorageMergeTree::merge(
    bool aggressive,
    const String & partition_id,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    bool cleanup,
    const MergeTreeTransactionPtr & txn,
    String & out_disable_reason,
    bool optimize_skip_merged_partitions)
{
    auto table_lock_holder = lockForShare(RWLockImpl::NO_QUERY, getSettings()->lock_acquire_timeout_for_background_operations);
    auto metadata_snapshot = getInMemoryMetadataPtr();

    SelectPartsDecision select_decision;

    MergeMutateSelectedEntryPtr merge_mutate_entry;

    {
        std::unique_lock lock(currently_processing_in_background_mutex);
        if (merger_mutator.merges_blocker.isCancelled())
            throw Exception(ErrorCodes::ABORTED, "Cancelled merging parts");

        merge_mutate_entry = selectPartsToMerge(
            metadata_snapshot,
            aggressive,
            partition_id,
            final,
            out_disable_reason,
            table_lock_holder,
            lock,
            txn,
            optimize_skip_merged_partitions,
            &select_decision);
    }

    /// If there is nothing to merge then we treat this merge as successful (needed for optimize final optimization)
    if (select_decision == SelectPartsDecision::NOTHING_TO_MERGE)
        return true;

    if (!merge_mutate_entry)
        return false;

    /// Copying a vector of columns `deduplicate by columns.
    IExecutableTask::TaskResultCallback f = [](bool) {};
    auto task = std::make_shared<MergePlainMergeTreeTask>(
        *this, metadata_snapshot, deduplicate, deduplicate_by_columns, cleanup, merge_mutate_entry, table_lock_holder, f);

    task->setCurrentTransaction(MergeTreeTransactionHolder{}, MergeTreeTransactionPtr{txn});

    executeHere(task);

    return true;
}


bool StorageMergeTree::partIsAssignedToBackgroundOperation(const DataPartPtr & part) const
{
    std::lock_guard background_processing_lock(currently_processing_in_background_mutex);
    return currently_merging_mutating_parts.contains(part);
}

MergeMutateSelectedEntryPtr StorageMergeTree::selectPartsToMutate(
    const StorageMetadataPtr & metadata_snapshot, String & /* disable_reason */, TableLockHolder & /* table_lock_holder */,
    std::unique_lock<std::mutex> & /*currently_processing_in_background_mutex_lock*/)
{
    if (current_mutations_by_version.empty())
        return {};

    size_t max_source_part_size = merger_mutator.getMaxSourcePartSizeForMutation();
    if (max_source_part_size == 0)
    {
        LOG_DEBUG(
            log,
            "Not enough idle threads to apply mutations at the moment. See settings 'number_of_free_entries_in_pool_to_execute_mutation' "
            "and 'background_pool_size'");
        return {};
    }

    size_t max_ast_elements = getContext()->getSettingsRef().max_expanded_ast_elements;

    auto future_part = std::make_shared<FutureMergedMutatedPart>();
    if (storage_settings.get()->assign_part_uuids)
        future_part->uuid = UUIDHelpers::generateV4();

    CurrentlyMergingPartsTaggerPtr tagger;

    auto mutations_end_it = current_mutations_by_version.end();
    for (const auto & part : getDataPartsVectorForInternalUsage())
    {
        if (currently_merging_mutating_parts.contains(part))
            continue;

        auto mutations_begin_it = current_mutations_by_version.upper_bound(part->info.getDataVersion());
        if (mutations_begin_it == mutations_end_it)
            continue;

        if (max_source_part_size < part->getBytesOnDisk())
        {
            LOG_DEBUG(
                log,
                "Current max source part size for mutation is {} but part size {}. Will not mutate part {} yet",
                max_source_part_size,
                part->getBytesOnDisk(),
                part->name);
            continue;
        }

        TransactionID first_mutation_tid = mutations_begin_it->second.tid;
        MergeTreeTransactionPtr txn;

        if (!first_mutation_tid.isPrehistoric())
        {

            /// Mutate visible parts only
            /// NOTE Do not mutate visible parts in Outdated state, because it does not make sense:
            /// mutation will fail anyway due to serialization error.

            /// It's possible that both mutation and transaction are already finished,
            /// because that part should not be mutated because it was not visible for that transaction.
            if (!part->version.isVisible(first_mutation_tid.start_csn, first_mutation_tid))
                continue;

            txn = tryGetTransactionForMutation(mutations_begin_it->second, log);
            if (!txn)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find transaction {} that has started mutation {} "
                                "that is going to be applied to part {}",
                                first_mutation_tid, mutations_begin_it->second.file_name, part->name);
        }

        auto commands = std::make_shared<MutationCommands>();
        size_t current_ast_elements = 0;
        auto last_mutation_to_apply = mutations_end_it;
        for (auto it = mutations_begin_it; it != mutations_end_it; ++it)
        {
            /// Do not squash mutations from different transactions to be able to commit/rollback them independently.
            if (first_mutation_tid != it->second.tid)
                break;

            size_t commands_size = 0;
            MutationCommands commands_for_size_validation;
            for (const auto & command : it->second.commands)
            {
                if (command.type != MutationCommand::Type::DROP_COLUMN
                    && command.type != MutationCommand::Type::DROP_INDEX
                    && command.type != MutationCommand::Type::DROP_PROJECTION
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
                    /// NOTE we should not skip mutations, because exception may be retryable (e.g. MEMORY_LIMIT_EXCEEDED)
                    break;
                }
            }

            if (current_ast_elements + commands_size >= max_ast_elements)
                break;

            const auto & single_mutation_commands = it->second.commands;

            if (single_mutation_commands.containBarrierCommand())
            {
                if (commands->empty())
                {
                    commands->insert(commands->end(), single_mutation_commands.begin(), single_mutation_commands.end());
                    last_mutation_to_apply = it;
                }
                break;
            }
            else
            {
                current_ast_elements += commands_size;
                commands->insert(commands->end(), single_mutation_commands.begin(), single_mutation_commands.end());
                last_mutation_to_apply = it;
            }

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

            tagger = std::make_unique<CurrentlyMergingPartsTagger>(future_part, MergeTreeDataMergerMutator::estimateNeededDiskSpace({part}), *this, metadata_snapshot, true);
            return std::make_shared<MergeMutateSelectedEntry>(future_part, std::move(tagger), commands, txn);
        }
    }

    return {};
}

UInt32 StorageMergeTree::getMaxLevelInBetween(const DataPartPtr & left, const DataPartPtr & right) const
{
    auto parts_lock = lockParts();

    auto begin = data_parts_by_info.find(left->info);
    if (begin == data_parts_by_info.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "unable to find left part, left part {}. It's a bug", left->name);

    auto end = data_parts_by_info.find(right->info);
    if (end == data_parts_by_info.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "unable to find right part, right part {}. It's a bug", right->name);

    UInt32 level = 0;

    for (auto it = begin++; it != end; ++it)
    {
        if (it == data_parts_by_info.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "left and right parts in the wrong order, left part {}, right part {}. It's a bug", left->name, right->name);

        level = std::max(level, (*it)->info.level);
    }

    return level;
}

bool StorageMergeTree::scheduleDataProcessingJob(BackgroundJobsAssignee & assignee)
{
    if (shutdown_called)
        return false;

    assert(!isStaticStorage());

    auto metadata_snapshot = getInMemoryMetadataPtr();
    MergeMutateSelectedEntryPtr merge_entry, mutate_entry;

    auto shared_lock = lockForShare(RWLockImpl::NO_QUERY, getSettings()->lock_acquire_timeout_for_background_operations);

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

        String out_reason;
        merge_entry = selectPartsToMerge(metadata_snapshot, false, {}, false, out_reason, shared_lock, lock, txn);

        if (!merge_entry && !current_mutations_by_version.empty())
            mutate_entry = selectPartsToMutate(metadata_snapshot, out_reason, shared_lock, lock);

        has_mutations = !current_mutations_by_version.empty();
    }

    if (merge_entry)
    {
        auto task = std::make_shared<MergePlainMergeTreeTask>(*this, metadata_snapshot, /* deduplicate */ false, Names{}, /* cleanup */ false, merge_entry, shared_lock, common_assignee_trigger);
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
            getSettings()->merge_tree_clear_old_temporary_directories_interval_seconds))
    {
        assignee.scheduleCommonTask(std::make_shared<ExecutableLambdaAdapter>(
            [this, shared_lock] ()
            {
                return clearOldTemporaryDirectories(getSettings()->temporary_directories_lifetime.totalSeconds());
            }, common_assignee_trigger, getStorageID()), /* need_trigger */ false);
        scheduled = true;
    }

    if (auto lock = time_after_previous_cleanup_parts.compareAndRestartDeferred(
            getSettings()->merge_tree_clear_old_parts_interval_seconds))
    {
        assignee.scheduleCommonTask(std::make_shared<ExecutableLambdaAdapter>(
            [this, shared_lock] ()
            {
                /// All use relative_data_path which changes during rename
                /// so execute under share lock.
                size_t cleared_count = 0;
                cleared_count += clearOldPartsFromFilesystem();
                cleared_count += clearOldWriteAheadLogs();
                cleared_count += clearOldMutations();
                cleared_count += clearEmptyParts();
                cleared_count += clearOldBrokenPartsFromDetachedDirectory();
                return cleared_count;
                /// TODO maybe take into account number of cleared objects when calculating backoff
            }, common_assignee_trigger, getStorageID()), /* need_trigger */ false);
        scheduled = true;
    }


    return scheduled;
}

size_t StorageMergeTree::getNumberOfUnfinishedMutations() const
{
    std::unique_lock lock(currently_processing_in_background_mutex);

    size_t count = 0;
    for (const auto & [version, _] : current_mutations_by_version | std::views::reverse)
    {
        auto status = getIncompleteMutationsStatusUnlocked(version, lock, nullptr, true);
        if (!status)
            continue;

        if (status->is_done)
            break;

        ++count;
    }

    return count;
}

UInt64 StorageMergeTree::getCurrentMutationVersion(
    const DataPartPtr & part,
    std::unique_lock<std::mutex> & /*currently_processing_in_background_mutex_lock*/) const
{
    auto it = current_mutations_by_version.upper_bound(part->info.getDataVersion());
    if (it == current_mutations_by_version.begin())
        return 0;
    --it;
    return it->first;
}

size_t StorageMergeTree::clearOldMutations(bool truncate)
{
    size_t finished_mutations_to_keep = truncate ? 0 : getSettings()->finished_mutations_to_keep;

    std::vector<MergeTreeMutationEntry> mutations_to_delete;
    {
        std::lock_guard lock(currently_processing_in_background_mutex);

        if (current_mutations_by_version.size() <= finished_mutations_to_keep)
            return 0;

        auto end_it = current_mutations_by_version.end();
        auto begin_it = current_mutations_by_version.begin();

        if (std::optional<Int64> min_version = getMinPartDataVersion())
            end_it = current_mutations_by_version.upper_bound(*min_version);

        size_t done_count = std::distance(begin_it, end_it);

        if (done_count <= finished_mutations_to_keep)
            return 0;

        for (auto it = begin_it; it != end_it; ++it)
        {
            if (!it->second.tid.isPrehistoric())
            {
                done_count = std::distance(begin_it, it);
                break;
            }
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
    if (deduplicate)
    {
        if (deduplicate_by_columns.empty())
            LOG_DEBUG(log, "DEDUPLICATE BY all columns");
        else
            LOG_DEBUG(log, "DEDUPLICATE BY ('{}')", fmt::join(deduplicate_by_columns, "', '"));
    }

    auto txn = local_context->getCurrentTransaction();

    String disable_reason;
    if (!partition && final)
    {
        if (cleanup && this->merging_params.mode != MergingParams::Mode::Replacing)
        {
            constexpr const char * message = "Cannot OPTIMIZE with CLEANUP table: {}";
            disable_reason = "only ReplacingMergeTree can be CLEANUP";
            throw Exception(ErrorCodes::CANNOT_ASSIGN_OPTIMIZE, message, disable_reason);
        }

        DataPartsVector data_parts = getVisibleDataPartsVector(local_context);
        std::unordered_set<String> partition_ids;

        for (const DataPartPtr & part : data_parts)
            partition_ids.emplace(part->info.partition_id);

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
                    local_context->getSettingsRef().optimize_skip_merged_partitions))
            {
                constexpr auto message = "Cannot OPTIMIZE table: {}";
                if (disable_reason.empty())
                    disable_reason = "unknown reason";
                LOG_INFO(log, message, disable_reason);

                if (local_context->getSettingsRef().optimize_throw_if_noop)
                    throw Exception(ErrorCodes::CANNOT_ASSIGN_OPTIMIZE, message, disable_reason);
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
                local_context->getSettingsRef().optimize_skip_merged_partitions))
        {
            constexpr auto message = "Cannot OPTIMIZE table: {}";
            if (disable_reason.empty())
                disable_reason = "unknown reason";
            LOG_INFO(log, message, disable_reason);

            if (local_context->getSettingsRef().optimize_throw_if_noop)
                throw Exception(ErrorCodes::CANNOT_ASSIGN_OPTIMIZE, message, disable_reason);
            return false;
        }
    }

    return true;
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

        removePartsFromWorkingSet(txn, {part}, clear_without_timeout, &parts_lock);
        return part;
    }
    else
    {
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

        removePartsFromWorkingSet(txn, {part}, clear_without_timeout, &parts_lock);
        return part;
    }
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

    for (auto & part: new_parts)
    {
        DataPartsVector covered_parts_by_one_part = renameTempPartAndReplace(part, transaction);

        if (covered_parts_by_one_part.size() > 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Part {} expected to cover not more then 1 part. "
                            "{} covered parts have been found. This is a bug.",
                            part->name, covered_parts_by_one_part.size());

        std::move(covered_parts_by_one_part.begin(), covered_parts_by_one_part.end(), std::back_inserter(covered_parts));
    }

    LOG_INFO(log, "Remove {} parts by covering them with empty {} parts. With txn {}.",
             covered_parts.size(), new_parts.size(), transaction.getTID());

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

    /// Old part objects is needed to be destroyed before clearing them from filesystem.
    clearOldMutations(true);
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
            {
                if (partition_ast && partition_ast->all)
                    parts = getVisibleDataPartsVector(query_context);
                else
                {
                    String partition_id = getPartitionIDFromQuery(partition, query_context);
                    parts = getVisibleDataPartsVectorInPartition(query_context, partition_id);
                }
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

    /// Old parts are needed to be destroyed before clearing them from filesystem.
    clearOldMutations(true);
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
    const ASTPtr & partition, const StorageMetadataPtr & /* metadata_snapshot */,
    bool attach_part, ContextPtr local_context)
{
    PartitionCommandsResultInfo results;
    PartsTemporaryRename renamed_parts(*this, "detached/");
    MutableDataPartsVector loaded_parts = tryLoadPartsToAttach(partition, attach_part, local_context, renamed_parts);

    for (size_t i = 0; i < loaded_parts.size(); ++i)
    {
        LOG_INFO(log, "Attaching part {} from {}", loaded_parts[i]->name, renamed_parts.old_and_new_names[i].new_name);
        /// We should write version metadata on part creation to distinguish it from parts that were created without transaction.
        auto txn = local_context->getCurrentTransaction();
        TransactionID tid = txn ? txn->tid : Tx::PrehistoricTID;
        loaded_parts[i]->version.setCreationTID(tid, nullptr);
        loaded_parts[i]->storeVersionMetadata();

        String old_name = renamed_parts.old_and_new_names[i].old_name;
        /// It's important to create it outside of lock scope because
        /// otherwise it can lock parts in destructor and deadlock is possible.
        MergeTreeData::Transaction transaction(*this, local_context->getCurrentTransaction().get());
        {
            auto lock = lockParts();
            fillNewPartName(loaded_parts[i], lock);
            renameTempPartAndAdd(loaded_parts[i], transaction, lock);
            transaction.commit(&lock);
        }

        renamed_parts.old_and_new_names[i].old_name.clear();

        results.push_back(PartitionCommandResultInfo{
            .command_type = "ATTACH_PART",
            .partition_id = loaded_parts[i]->info.partition_id,
            .part_name = loaded_parts[i]->name,
            .old_part_name = old_name,
        });

        LOG_INFO(log, "Finished attaching part");
    }

    /// New parts with other data may appear in place of deleted parts.
    local_context->clearCaches();
    return results;
}

void StorageMergeTree::replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, ContextPtr local_context)
{
    auto lock1 = lockForShare(local_context->getCurrentQueryId(), local_context->getSettingsRef().lock_acquire_timeout);
    auto lock2 = source_table->lockForShare(local_context->getCurrentQueryId(), local_context->getSettingsRef().lock_acquire_timeout);
    auto merges_blocker = stopMergesAndWait();
    auto source_metadata_snapshot = source_table->getInMemoryMetadataPtr();
    auto my_metadata_snapshot = getInMemoryMetadataPtr();

    Stopwatch watch;
    ProfileEventsScope profile_events_scope;

    MergeTreeData & src_data = checkStructureAndGetMergeTreeData(source_table, source_metadata_snapshot, my_metadata_snapshot);
    String partition_id = getPartitionIDFromQuery(partition, local_context);

    DataPartsVector src_parts = src_data.getVisibleDataPartsVectorInPartition(local_context, partition_id);
    MutableDataPartsVector dst_parts;
    std::vector<scope_guard> dst_parts_locks;

    static const String TMP_PREFIX = "tmp_replace_from_";

    for (const DataPartPtr & src_part : src_parts)
    {
        if (!canReplacePartition(src_part))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Cannot replace partition '{}' because part '{}' has inconsistent granularity with table",
                            partition_id, src_part->name);

        /// This will generate unique name in scope of current server process.
        Int64 temp_index = insert_increment.get();
        MergeTreePartInfo dst_part_info(partition_id, temp_index, temp_index, src_part->info.level);

        IDataPartStorage::ClonePartParams clone_params{.txn = local_context->getCurrentTransaction()};
        auto [dst_part, part_lock] = cloneAndLoadDataPartOnSameDisk(src_part, TMP_PREFIX, dst_part_info, my_metadata_snapshot, clone_params);
        dst_parts.emplace_back(std::move(dst_part));
        dst_parts_locks.emplace_back(std::move(part_lock));
    }

    /// ATTACH empty part set
    if (!replace && dst_parts.empty())
        return;

    MergeTreePartInfo drop_range;
    if (replace)
    {
        drop_range.partition_id = partition_id;
        drop_range.min_block = 0;
        drop_range.max_block = increment.get(); // there will be a "hole" in block numbers
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

            /** It is important that obtaining new block number and adding that block to parts set is done atomically.
              * Otherwise there is race condition - merge of blocks could happen in interval that doesn't yet contain new part.
              */
            for (auto part : dst_parts)
            {
                fillNewPartName(part, data_parts_lock);
                renameTempPartAndReplaceUnlocked(part, transaction, data_parts_lock);
            }
            /// Populate transaction
            transaction.commit(&data_parts_lock);

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
    auto lock1 = lockForShare(local_context->getCurrentQueryId(), local_context->getSettingsRef().lock_acquire_timeout);
    auto lock2 = dest_table->lockForShare(local_context->getCurrentQueryId(), local_context->getSettingsRef().lock_acquire_timeout);
    auto merges_blocker = stopMergesAndWait();

    auto dest_table_storage = std::dynamic_pointer_cast<StorageMergeTree>(dest_table);
    if (!dest_table_storage)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Table {} supports movePartitionToTable only for MergeTree family of table engines. Got {}",
                        getStorageID().getNameForLogs(), dest_table->getName());
    if (dest_table_storage->getStoragePolicy() != this->getStoragePolicy())
        throw Exception(ErrorCodes::UNKNOWN_POLICY,
                        "Destination table {} should have the same storage policy of source table {}. {}: {}, {}: {}",
                        dest_table_storage->getStorageID().getNameForLogs(),
                        getStorageID().getNameForLogs(), getStorageID().getNameForLogs(),
                        this->getStoragePolicy()->getName(), dest_table_storage->getStorageID().getNameForLogs(),
                        dest_table_storage->getStoragePolicy()->getName());

    auto dest_metadata_snapshot = dest_table->getInMemoryMetadataPtr();
    auto metadata_snapshot = getInMemoryMetadataPtr();
    Stopwatch watch;
    ProfileEventsScope profile_events_scope;

    MergeTreeData & src_data = dest_table_storage->checkStructureAndGetMergeTreeData(*this, metadata_snapshot, dest_metadata_snapshot);
    String partition_id = getPartitionIDFromQuery(partition, local_context);

    DataPartsVector src_parts = src_data.getVisibleDataPartsVectorInPartition(local_context, partition_id);
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

        IDataPartStorage::ClonePartParams clone_params{.txn = local_context->getCurrentTransaction()};
        auto [dst_part, part_lock] = dest_table_storage->cloneAndLoadDataPartOnSameDisk(src_part, TMP_PREFIX, dst_part_info, dest_metadata_snapshot, clone_params);
        dst_parts.emplace_back(std::move(dst_part));
        dst_parts_locks.emplace_back(std::move(part_lock));
    }

    /// empty part set
    if (dst_parts.empty())
        return;

    /// Move new parts to the destination table. NOTE It doesn't look atomic.
    try
    {
        {
            Transaction transaction(*dest_table_storage, local_context->getCurrentTransaction().get());

            auto src_data_parts_lock = lockParts();
            auto dest_data_parts_lock = dest_table_storage->lockParts();

            for (auto & part : dst_parts)
            {
                dest_table_storage->fillNewPartName(part, dest_data_parts_lock);
                dest_table_storage->renameTempPartAndReplaceUnlocked(part, transaction, dest_data_parts_lock);
            }


            removePartsFromWorkingSet(local_context->getCurrentTransaction().get(), src_parts, true, src_data_parts_lock);
            transaction.commit(&src_data_parts_lock);
        }

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
    else if (action_type == ActionLocks::PartsTTLMerge)
        return merger_mutator.ttl_merges_blocker.cancel();
    else if (action_type == ActionLocks::PartsMove)
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

CheckResults StorageMergeTree::checkData(const ASTPtr & query, ContextPtr local_context)
{
    CheckResults results;
    DataPartsVector data_parts;
    if (const auto & check_query = query->as<ASTCheckQuery &>(); check_query.partition)
    {
        String partition_id = getPartitionIDFromQuery(check_query.partition, local_context);
        data_parts = getVisibleDataPartsVectorInPartition(local_context, partition_id);
    }
    else
        data_parts = getVisibleDataPartsVector(local_context);

    for (auto & part : data_parts)
    {
        /// If the checksums file is not present, calculate the checksums and write them to disk.
        static constexpr auto checksums_path = "checksums.txt";
        if (part->isStoredOnDisk() && !part->getDataPartStorage().exists(checksums_path))
        {
            try
            {
                auto calculated_checksums = checkDataPart(part, false);
                calculated_checksums.checkEqual(part->checksums, true);

                auto & part_mutable = const_cast<IMergeTreeDataPart &>(*part);
                part_mutable.writeChecksums(part->checksums, local_context->getWriteSettings());

                part->checkMetadata();
                results.emplace_back(part->name, true, "Checksums recounted and written to disk.");
            }
            catch (const Exception & ex)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
                results.emplace_back(part->name, false, "Check of part finished with error: '" + ex.message() + "'");
            }
        }
        else
        {
            try
            {
                checkDataPart(part, true);
                part->checkMetadata();
                results.emplace_back(part->name, true, "");
            }
            catch (const Exception & ex)
            {
                results.emplace_back(part->name, false, ex.message());
            }
        }
    }
    return results;
}


void StorageMergeTree::backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & partitions)
{
    const auto & backup_settings = backup_entries_collector.getBackupSettings();
    const auto & read_settings = backup_entries_collector.getReadSettings();
    auto local_context = backup_entries_collector.getContext();

    DataPartsVector data_parts;
    if (partitions)
        data_parts = getVisibleDataPartsVectorInPartitions(local_context, getPartitionIDsFromQuery(*partitions, local_context));
    else
        data_parts = getVisibleDataPartsVector(local_context);

    Int64 min_data_version = std::numeric_limits<Int64>::max();
    for (const auto & data_part : data_parts)
        min_data_version = std::min(min_data_version, data_part->info.getDataVersion() + 1);

    auto parts_backup_entries = backupParts(data_parts, data_path_in_backup, backup_settings, read_settings, local_context);
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
            fillNewPartName(part, lock);
            renameTempPartAndAdd(part, transaction, lock);
            transaction.commit(&lock);
        }
    }
}


std::map<int64_t, MutationCommands> StorageMergeTree::getAlterMutationCommandsForPart(const DataPartPtr & part) const
{
    std::lock_guard lock(currently_processing_in_background_mutex);

    UInt64 part_data_version = part->info.getDataVersion();
    std::map<int64_t, MutationCommands> result;

    for (const auto & [mutation_version, entry] : current_mutations_by_version | std::views::reverse)
    {
        if (mutation_version > part_data_version)
            result[mutation_version] = entry.commands;
        else
            break;
    }

    return result;
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

void StorageMergeTree::fillNewPartName(MutableDataPartPtr & part, DataPartsLock &)
{
    part->info.min_block = part->info.max_block = increment.get();
    part->info.mutation = 0;
    part->setName(part->getNewName(part->info));
}

}
