#include "StorageUniqueMergeTree.h"

#include <optional>

#include <Backups/BackupEntriesCollector.h>
#include <Databases/IDatabase.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/TransactionLog.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/queryToString.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/AlterCommands.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/MergeTree/MergeTreeSink.h>
#include <Storages/MergeTree/PartitionPruner.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/PartitionCommands.h>
#include <Storages/UniqueMergeTree/MergePlainUniqueMergeTreeTask.h>
#include <Storages/UniqueMergeTree/MutatePlainUniqueMergeTreeTask.h>
#include <Storages/UniqueMergeTree/UniqueMergeTreeSink.h>
#include <base/sort.h>
#include <Common/ThreadPool.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>

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


StorageUniqueMergeTree::StorageUniqueMergeTree(
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
        relative_data_path_,
        metadata_,
        context_,
        date_column_name,
        merging_params_,
        std::move(storage_settings_),
        false, /// require_part_metadata
        attach)
    , reader(*this, this)
    , writer(*this)
    , merger_mutator(*this, getContext()->getMergeMutateExecutor()->getMaxTasksCount())
    , primary_index_cache(*this, getSettings()->unique_merge_tree_max_keeped_primary_index)
{
    loadTableVersion(attach);
    loadDataParts(has_force_restore_data_flag, currentVersion());

    auto active_parts = getDataPartsVectorForInternalUsage();
    for (const auto & part : active_parts)
        part_info_by_min_block.emplace(part->info.min_block, part->info);

    if (!attach && !getDataPartsForInternalUsage().empty())
        throw Exception("Data directory for table already containing data parts - probably it was unclean DROP table or manual intervention. You must either clear directory by hand or use ATTACH TABLE instead of CREATE TABLE if you need to use that parts.", ErrorCodes::INCORRECT_DATA);

    increment.set(getMaxBlockNumber());

    loadMutations();

    loadDeduplicationLog();
}


void StorageUniqueMergeTree::startup()
{
    clearOldPartsFromFilesystem();
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

void StorageUniqueMergeTree::flush()
{
    if (flush_called.exchange(true))
        return;

    flushAllInMemoryPartsIfNeeded();
}

void StorageUniqueMergeTree::shutdown()
{
    if (shutdown_called.exchange(true))
        return;

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

    try
    {
        /// We clear all old parts after stopping all background operations.
        /// It's important, because background operations can produce temporary
        /// parts which will remove themselves in their destructors. If so, we
        /// may have race condition between our remove call and background
        /// process.
        /// Do not clear old parts in case when server is shutting down because it failed to start due to some exception.

        if (Context::getGlobalContextInstance()->getApplicationType() == Context::ApplicationType::SERVER
            && Context::getGlobalContextInstance()->isServerCompletelyStarted())
            clearOldPartsFromFilesystem(true);
    }
    catch (...)
    {
        /// Example: the case of readonly filesystem, we have failure removing old parts.
        /// Should not prevent table shutdown.
        tryLogCurrentException(log);
    }
}


StorageUniqueMergeTree::~StorageUniqueMergeTree()
{
    shutdown();
}

void StorageUniqueMergeTree::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned num_streams)
{
    /// If true, then we will ask initiator if we can read chosen ranges
    bool enable_parallel_reading = local_context->getClientInfo().collaborate_with_initiator;

    if (enable_parallel_reading)
        LOG_TRACE(log, "Parallel reading from replicas enabled {}", enable_parallel_reading);

    if (auto plan = reader.read(
        column_names, storage_snapshot, query_info, local_context, max_block_size, num_streams, processed_stage, nullptr, enable_parallel_reading))
        query_plan = std::move(*plan);

    /// Now, copy of parts that is required for the query, stored in the processors,
    /// while snapshot_data.parts includes all parts, even one that had been filtered out with partition pruning,
    /// reset them to avoid holding them.
    auto & snapshot_data = assert_cast<MergeTreeData::SnapshotData &>(*storage_snapshot->data);
    snapshot_data.parts = {};
}

std::optional<UInt64> StorageUniqueMergeTree::totalRows(const Settings &) const
{
    return getTotalActiveSizeInRows();
}

std::optional<UInt64> StorageUniqueMergeTree::totalRowsByPartitionPredicate(const SelectQueryInfo & query_info, ContextPtr local_context) const
{
    auto parts = getVisibleDataPartsVector(local_context);
    return totalRowsByPartitionPredicateImpl(query_info, local_context, parts);
}

std::optional<UInt64> StorageUniqueMergeTree::totalBytes(const Settings &) const
{
    return getTotalActiveSizeInBytes();
}

SinkToStoragePtr
StorageUniqueMergeTree::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    const auto & settings = local_context->getSettingsRef();
    return std::make_shared<UniqueMergeTreeSink>(*this, metadata_snapshot, settings.max_partitions_per_insert_block, local_context);
}

void StorageUniqueMergeTree::checkTableCanBeDropped() const
{
    auto table_id = getStorageID();
    getContext()->checkTableCanBeDropped(table_id.database_name, table_id.table_name, getTotalActiveSizeInBytes());
}

void StorageUniqueMergeTree::drop()
{
    shutdown();
    /// In case there is read-only disk we cannot allow to call dropAllData(), but dropping tables is allowed.
    if (isStaticStorage())
        return;
    dropAllData();
}

void StorageUniqueMergeTree::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr local_context, TableExclusiveLockHolder &)
{
    {
        /// Asks to complete merges and does not allow them to start.
        /// This protects against "revival" of data for a removed partition after completion of merge.
        std::lock_guard lock(write_merge_lock);
        auto merge_blocker = stopMergesAndWait();

        auto data_parts_lock = lockParts();
        auto parts_to_remove = getVisibleDataPartsVectorUnlocked(local_context, data_parts_lock);

        auto table_version_path = getRelativeDataPath() + TABLE_VERSION_NAME;

        auto new_table_version = std::make_unique<TableVersion>();
        auto disk = getStoragePolicy()->getDisks()[0];
        new_table_version->version = currentVersion()->version + 1;

        removePartsFromWorkingSet(local_context->getCurrentTransaction().get(), parts_to_remove, true, data_parts_lock);

        new_table_version->serialize(table_version_path, disk);

        primary_index_cache.reset();
        delete_bitmap_cache.reset();
        table_version.set(std::move(new_table_version));

        LOG_INFO(log, "Removed {} parts.", parts_to_remove.size());
    }

    clearOldMutations(true);
    clearOldPartsFromFilesystem();
}

void StorageUniqueMergeTree::loadTableVersion(bool attach)
{
    auto disk = getStoragePolicy()->getDisks()[0];
    auto version_path = getRelativeDataPath() + TABLE_VERSION_NAME;
    auto version = std::make_unique<TableVersion>();
    if (attach)
    {
        version->deserialize(version_path, disk);
    }
    else
    {
        version->serialize(version_path, disk);
    }
    table_version.set(std::move(version));
}

void StorageUniqueMergeTree::alter(
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
        {
            changeSettings(new_metadata.settings_changes, table_lock_holder);
            checkTTLExpressions(new_metadata, old_metadata);
            /// Reinitialize primary key because primary key column types might have changed.
            setProperties(new_metadata, old_metadata);

            DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(local_context, table_id, new_metadata);

            if (!maybe_mutation_commands.empty())
                mutation_version = startMutation(maybe_mutation_commands, local_context);
        }

        /// Always execute required mutations synchronously, because alters
        /// should be executed in sequential order.
        if (!maybe_mutation_commands.empty())
            waitForMutation(mutation_version);
    }

    {
        /// Some additional changes in settings
        auto new_storage_settings = getSettings();

        if (old_storage_settings->non_replicated_deduplication_window != new_storage_settings->non_replicated_deduplication_window)
        {
            /// We cannot place this check into settings sanityCheck because it depends on format_version.
            /// sanityCheck must work event without storage.
            if (new_storage_settings->non_replicated_deduplication_window != 0 && format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
                throw Exception("Deduplication for non-replicated MergeTree in old syntax is not supported", ErrorCodes::BAD_ARGUMENTS);

            deduplication_log->setDeduplicationWindowSize(new_storage_settings->non_replicated_deduplication_window);
        }
    }
}

Int64 StorageUniqueMergeTree::startMutation(const MutationCommands & commands, ContextPtr query_context)
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


void StorageUniqueMergeTree::updateMutationEntriesErrors(FutureMergedMutatedPartPtr result_part, bool is_successful, const String & exception_message)
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

void StorageUniqueMergeTree::waitForMutation(Int64 version)
{
    String mutation_id = MergeTreeMutationEntry::versionToFileName(version);
    waitForMutation(version, mutation_id);
}

void StorageUniqueMergeTree::waitForMutation(const String & mutation_id)
{
    Int64 version = MergeTreeMutationEntry::parseFileName(mutation_id);
    waitForMutation(version, mutation_id);
}

void StorageUniqueMergeTree::waitForMutation(Int64 version, const String & mutation_id)
{
    LOG_INFO(log, "Waiting mutation: {}", mutation_id);
    {
        auto check = [version, this]()
        {
            if (shutdown_called)
                return true;
            auto mutation_status = getIncompleteMutationsStatus(version);
            return !mutation_status || mutation_status->is_done || !mutation_status->latest_fail_reason.empty();
        };

        std::unique_lock lock(mutation_wait_mutex);
        mutation_wait_event.wait(lock, check);
    }

    /// At least we have our current mutation
    std::set<String> mutation_ids;
    mutation_ids.insert(mutation_id);

    auto mutation_status = getIncompleteMutationsStatus(version, &mutation_ids);
    checkMutationStatus(mutation_status, mutation_ids);

    LOG_INFO(log, "Mutation {} done", mutation_id);
}

void StorageUniqueMergeTree::setMutationCSN(const String & mutation_id, CSN csn)
{
    LOG_INFO(log, "Writing CSN {} for mutation {}", csn, mutation_id);
    UInt64 version = MergeTreeMutationEntry::parseFileName(mutation_id);

    std::lock_guard lock(currently_processing_in_background_mutex);
    auto it = current_mutations_by_version.find(version);
    if (it == current_mutations_by_version.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find mutation {}", mutation_id);
    it->second.writeCSN(csn);
}

void StorageUniqueMergeTree::mutate(const MutationCommands & commands, ContextPtr query_context)
{
    /// Validate partition IDs (if any) before starting mutation
    getPartitionIdsAffectedByCommands(commands, query_context);

    Int64 version = startMutation(commands, query_context);

    if (query_context->getSettingsRef().mutations_sync > 0 || query_context->getCurrentTransaction())
        waitForMutation(version);
}

bool StorageUniqueMergeTree::hasLightweightDeletedMask() const
{
    return has_lightweight_delete_parts.load(std::memory_order_relaxed);
}

std::optional<MergeTreeMutationStatus> StorageUniqueMergeTree::getIncompleteMutationsStatus(Int64 mutation_version, std::set<String> * mutation_ids) const
{
    std::unique_lock lock(currently_processing_in_background_mutex);

    auto current_mutation_it = current_mutations_by_version.find(mutation_version);
    /// Killed
    if (current_mutation_it == current_mutations_by_version.end())
        return {};

    MergeTreeMutationStatus result{.is_done = false};

    const auto & mutation_entry = current_mutation_it->second;

    auto txn = tryGetTransactionForMutation(mutation_entry, log);
    assert(txn || mutation_entry.tid.isPrehistoric());
    auto data_parts = getVisibleDataPartsVector(txn);
    for (const auto & data_part : data_parts)
    {
        Int64 data_version = getUpdatedDataVersion(data_part, lock);
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
            else if (txn)
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

std::vector<MergeTreeMutationStatus> StorageUniqueMergeTree::getMutationsStatus() const
{
    std::unique_lock lock(currently_processing_in_background_mutex);

    auto part_versions_with_names = getSortedPartVersionsWithNames(lock);

    std::vector<MergeTreeMutationStatus> result;
    for (const auto & kv : current_mutations_by_version)
    {
        Int64 mutation_version = kv.first;
        const MergeTreeMutationEntry & entry = kv.second;
        const PartVersionWithName needle{mutation_version, ""};
        auto versions_it = std::lower_bound(
            part_versions_with_names.begin(), part_versions_with_names.end(), needle);

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

CancellationCode StorageUniqueMergeTree::killMutation(const String & mutation_id)
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
        std::lock_guard<std::mutex> lock(mutation_wait_mutex);
        mutation_wait_event.notify_all();
    }

    /// Maybe there is another mutation that was blocked by the killed one. Try to execute it immediately.
    background_operations_assignee.trigger();

    return CancellationCode::CancelSent;
}

void StorageUniqueMergeTree::loadDeduplicationLog()
{
    auto settings = getSettings();
    if (settings->non_replicated_deduplication_window != 0 && format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        throw Exception("Deduplication for non-replicated MergeTree in old syntax is not supported", ErrorCodes::BAD_ARGUMENTS);

    auto disk = getDisks()[0];
    std::string path = fs::path(relative_data_path) / "deduplication_logs";
    deduplication_log = std::make_unique<MergeTreeDeduplicationLog>(path, settings->non_replicated_deduplication_window, format_version, disk);
    deduplication_log->load();
}

void StorageUniqueMergeTree::loadMutations()
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

std::shared_ptr<MergeMutateSelectedEntry> StorageUniqueMergeTree::selectPartsToMerge(
    const StorageMetadataPtr & metadata_snapshot,
    bool aggressive,
    const String & partition_id,
    bool final,
    String * out_disable_reason,
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

    auto can_merge = [this, &lock](const DataPartPtr & left, const DataPartPtr & right, const MergeTreeTransaction * tx, String *) -> bool
    {
        if (tx)
        {
            /// Cannot merge parts if some of them are not visible in current snapshot
            /// TODO Transactions: We can use simplified visibility rules (without CSN lookup) here
            if (left && !left->version.isVisible(tx->getSnapshot(), Tx::EmptyTID))
                return false;
            if (right && !right->version.isVisible(tx->getSnapshot(), Tx::EmptyTID))
                return false;

            /// Do not try to merge parts that are locked for removal (merge will probably fail)
            if (left && left->version.isRemovalTIDLocked())
                return false;
            if (right && right->version.isRemovalTIDLocked())
                return false;
        }

        /// This predicate is checked for the first part of each range.
        /// (left = nullptr, right = "first part of partition")
        if (!left)
            return !currently_merging_mutating_parts.contains(right);
        return !currently_merging_mutating_parts.contains(left) && !currently_merging_mutating_parts.contains(right)
            && getCurrentMutationVersion(left, lock) == getCurrentMutationVersion(right, lock) && partsContainSameProjections(left, right);
    };

    SelectPartsDecision select_decision = SelectPartsDecision::CANNOT_SELECT;

    if (partition_id.empty())
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
        else if (out_disable_reason)
            *out_disable_reason = "Current value of max_source_parts_size is zero";
    }
    else
    {
        while (true)
        {
            select_decision = merger_mutator.selectAllPartsToMergeWithinPartition(
                future_part, can_merge, partition_id, final, metadata_snapshot, txn, out_disable_reason, optimize_skip_merged_partitions);
            auto timeout_ms = getSettings()->lock_acquire_timeout_for_background_operations.totalMilliseconds();
            auto timeout = std::chrono::milliseconds(timeout_ms);

            /// If final - we will wait for currently processing merges to finish and continue.
            if (final
                && select_decision != SelectPartsDecision::SELECTED
                && !currently_merging_mutating_parts.empty()
                && out_disable_reason
                && out_disable_reason->empty())
            {
                LOG_DEBUG(log, "Waiting for currently running merges ({} parts are merging right now) to perform OPTIMIZE FINAL",
                    currently_merging_mutating_parts.size());

                if (std::cv_status::timeout == currently_processing_in_background_condition.wait_for(lock, timeout))
                {
                    *out_disable_reason = fmt::format("Timeout ({} ms) while waiting for already running merges before running OPTIMIZE with FINAL", timeout_ms);
                    break;
                }
            }
            else
                break;
        }
    }

    /// In case of final we need to know the decision of select in StorageUniqueMergeTree::merge
    /// to treat NOTHING_TO_MERGE as successful merge (otherwise optimize final will be uncompleted)
    if (select_decision_out)
        *select_decision_out = select_decision;

    if (select_decision != SelectPartsDecision::SELECTED)
    {
        if (out_disable_reason)
        {
            if (!out_disable_reason->empty())
            {
                *out_disable_reason += ". ";
            }
            *out_disable_reason += "Cannot select parts for optimization";
        }

        return {};
    }

    /// Account TTL merge here to avoid exceeding the max_number_of_merges_with_ttl_in_pool limit
    if (isTTLMergeType(future_part->merge_type))
        getContext()->getMergeList().bookMergeWithTTL();

    merging_tagger = std::make_unique<CurrentlyMergingPartsTagger>(future_part, MergeTreeDataMergerMutator::estimateNeededDiskSpace(future_part->parts), *this, metadata_snapshot, false);
    return std::make_shared<MergeMutateSelectedEntry>(future_part, std::move(merging_tagger), std::make_shared<MutationCommands>());
}

bool StorageUniqueMergeTree::merge(
    bool aggressive,
    const String & partition_id,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    const MergeTreeTransactionPtr & txn,
    String * out_disable_reason,
    bool optimize_skip_merged_partitions)
{
    auto table_lock_holder = lockForShare(RWLockImpl::NO_QUERY, getSettings()->lock_acquire_timeout_for_background_operations);
    auto metadata_snapshot = getInMemoryMetadataPtr();

    SelectPartsDecision select_decision;

    std::shared_ptr<MergeMutateSelectedEntry> merge_mutate_entry;

    {
        std::unique_lock lock(currently_processing_in_background_mutex);
        if (merger_mutator.merges_blocker.isCancelled())
            throw Exception("Cancelled merging parts", ErrorCodes::ABORTED);

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

    /// Copying a vector of columns `deduplicate bu columns.
    IExecutableTask::TaskResultCallback f = [](bool) {};
    auto task = std::make_shared<MergePlainUniqueMergeTreeTask>(
        *this, metadata_snapshot, deduplicate, deduplicate_by_columns, merge_mutate_entry, table_lock_holder, f);

    task->setCurrentTransaction(MergeTreeTransactionHolder{}, MergeTreeTransactionPtr{txn});

    executeHere(task);

    return true;
}


bool StorageUniqueMergeTree::partIsAssignedToBackgroundOperation(const DataPartPtr & part) const
{
    std::lock_guard background_processing_lock(currently_processing_in_background_mutex);
    return currently_merging_mutating_parts.contains(part);
}

std::shared_ptr<MergeMutateSelectedEntry> StorageUniqueMergeTree::selectPartsToMutate(
    const StorageMetadataPtr & metadata_snapshot, String * /* disable_reason */, TableLockHolder & /* table_lock_holder */,
    std::unique_lock<std::mutex> & currently_processing_in_background_mutex_lock,
    bool & were_some_mutations_for_some_parts_skipped)
{
    size_t max_ast_elements = getContext()->getSettingsRef().max_expanded_ast_elements;

    auto future_part = std::make_shared<FutureMergedMutatedPart>();
    if (storage_settings.get()->assign_part_uuids)
        future_part->uuid = UUIDHelpers::generateV4();

    CurrentlyMergingPartsTaggerPtr tagger;

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

    auto mutations_end_it = current_mutations_by_version.end();
    for (const auto & part : getDataPartsVectorForInternalUsage())
    {
        if (currently_merging_mutating_parts.contains(part))
            continue;

        auto mutations_begin_it = current_mutations_by_version.upper_bound(getUpdatedDataVersion(part, currently_processing_in_background_mutex_lock));
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
        MergeTreeTransactionPtr txn = tryGetTransactionForMutation(mutations_begin_it->second, log);
        assert(txn || first_mutation_tid.isPrehistoric());

        if (txn)
        {
            /// Mutate visible parts only
            /// NOTE Do not mutate visible parts in Outdated state, because it does not make sense:
            /// mutation will fail anyway due to serialization error.
            if (!part->version.isVisible(*txn))
                continue;
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
                    MutationsInterpreter interpreter(
                        shared_from_this(), metadata_snapshot, commands_for_size_validation, fake_query_context, false);
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

            current_ast_elements += commands_size;
            commands->insert(commands->end(), it->second.commands.begin(), it->second.commands.end());
            last_mutation_to_apply = it;
        }

        assert(commands->empty() == (last_mutation_to_apply == mutations_end_it));
        if (!commands->empty())
        {
            bool is_partition_affected = false;
            for (const auto & command : *commands)
            {
                if (command.partition == nullptr)
                {
                    is_partition_affected = true;
                    break;
                }

                const String partition_id = part->storage.getPartitionIDFromQuery(command.partition, getContext());
                if (partition_id == part->info.partition_id)
                {
                    is_partition_affected = true;
                    break;
                }
            }

            if (!is_partition_affected)
            {
                /// Shall not create a new part, but will do that later if mutation with higher version appear.
                /// This is needed in order to not produce excessive mutations of non-related parts.
                auto block_range = std::make_pair(part->info.min_block, part->info.max_block);
                updated_version_by_block_range[block_range] = last_mutation_to_apply->first;
                were_some_mutations_for_some_parts_skipped = true;
                continue;
            }

            auto new_part_info = part->info;
            new_part_info.mutation = last_mutation_to_apply->first;

            future_part->parts.push_back(part);
            future_part->part_info = new_part_info;
            future_part->name = part->getNewName(new_part_info);
            future_part->type = part->getType();

            tagger = std::make_unique<CurrentlyMergingPartsTagger>(future_part, MergeTreeDataMergerMutator::estimateNeededDiskSpace({part}), *this, metadata_snapshot, true);
            return std::make_shared<MergeMutateSelectedEntry>(future_part, std::move(tagger), commands, txn);
        }
    }

    return {};
}


bool StorageUniqueMergeTree::scheduleDataProcessingJob(BackgroundJobsAssignee & assignee) //-V657
{
    if (shutdown_called)
        return false;

    assert(!isStaticStorage());

    auto metadata_snapshot = getInMemoryMetadataPtr();
    std::shared_ptr<MergeMutateSelectedEntry> merge_entry, mutate_entry;
    bool were_some_mutations_skipped = false;

    auto share_lock = lockForShare(RWLockImpl::NO_QUERY, getSettings()->lock_acquire_timeout_for_background_operations);

    MergeTreeTransactionHolder transaction_for_merge;
    MergeTreeTransactionPtr txn;
    if (transactions_enabled.load(std::memory_order_relaxed))
    {
        /// TODO Transactions: avoid beginning transaction if there is nothing to merge.
        txn = TransactionLog::instance().beginTransaction();
        transaction_for_merge = MergeTreeTransactionHolder{txn, /* autocommit = */ true};
    }

    bool has_mutations = false;
    {
        std::unique_lock lock(currently_processing_in_background_mutex);
        if (merger_mutator.merges_blocker.isCancelled())
            return false;

        merge_entry = selectPartsToMerge(metadata_snapshot, false, {}, false, nullptr, share_lock, lock, txn);
        if (!merge_entry)
            mutate_entry = selectPartsToMutate(metadata_snapshot, nullptr, share_lock, lock, were_some_mutations_skipped);

        has_mutations = !current_mutations_by_version.empty();
    }

    if ((!mutate_entry && has_mutations) || were_some_mutations_skipped)
    {
        /// Notify in case of errors or if some mutation was skipped (because it has no effect on the part).
        /// TODO @azat: we can also spot some selection errors when `mutate_entry` is true.
        std::lock_guard lock(mutation_wait_mutex);
        mutation_wait_event.notify_all();
    }

    if (merge_entry)
    {
        auto task = std::make_shared<MergePlainUniqueMergeTreeTask>(
            *this, metadata_snapshot, false, Names{}, merge_entry, share_lock, common_assignee_trigger);
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
        /// TODO leefeng, should we implement specific mutate class for UniqueMergeTree
        auto task
            = std::make_shared<MutatePlainUniqueMergeTreeTask>(*this, metadata_snapshot, mutate_entry, share_lock, common_assignee_trigger);
        assignee.scheduleMergeMutateTask(task);
        return true;
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
            [this, share_lock] ()
            {
                return clearOldTemporaryDirectories(getSettings()->temporary_directories_lifetime.totalSeconds());
            }, common_assignee_trigger, getStorageID()), /* need_trigger */ false);
        scheduled = true;
    }

    if (auto lock = time_after_previous_cleanup_parts.compareAndRestartDeferred(
            getSettings()->merge_tree_clear_old_parts_interval_seconds))
    {
        assignee.scheduleCommonTask(std::make_shared<ExecutableLambdaAdapter>(
            [this, share_lock] ()
            {
                /// All use relative_data_path which changes during rename
                /// so execute under share lock.
                size_t cleared_count = 0;
                cleared_count += clearOldPartsFromFilesystem();
                cleared_count += clearOldWriteAheadLogs();
                cleared_count += clearOldMutations();
                cleared_count += clearEmptyParts();
                if (getSettings()->merge_tree_enable_clear_old_broken_detached)
                    cleared_count += clearOldBrokenPartsFromDetachedDirecory();
                return cleared_count;
                /// TODO maybe take into account number of cleared objects when calculating backoff
            }, common_assignee_trigger, getStorageID()), /* need_trigger */ false);
        scheduled = true;
    }

    return scheduled;
}

Int64 StorageUniqueMergeTree::getUpdatedDataVersion(
    const DataPartPtr & part,
    std::unique_lock<std::mutex> & /* currently_processing_in_background_mutex_lock */) const
{
    auto it = updated_version_by_block_range.find(std::make_pair(part->info.min_block, part->info.max_block));
    if (it != updated_version_by_block_range.end())
        return std::max(part->info.getDataVersion(), static_cast<Int64>(it->second));
    else
        return part->info.getDataVersion();
}

UInt64 StorageUniqueMergeTree::getCurrentMutationVersion(
    const DataPartPtr & part,
    std::unique_lock<std::mutex> & currently_processing_in_background_mutex_lock) const
{
    auto it = current_mutations_by_version.upper_bound(getUpdatedDataVersion(part, currently_processing_in_background_mutex_lock));
    if (it == current_mutations_by_version.begin())
        return 0;
    --it;
    return it->first;
}

size_t StorageUniqueMergeTree::clearOldMutations(bool truncate)
{
    size_t finished_mutations_to_keep = truncate ? 0 : getSettings()->finished_mutations_to_keep;

    std::vector<MergeTreeMutationEntry> mutations_to_delete;
    {
        std::unique_lock<std::mutex> lock(currently_processing_in_background_mutex);

        if (current_mutations_by_version.size() <= finished_mutations_to_keep)
            return 0;

        auto end_it = current_mutations_by_version.end();
        auto begin_it = current_mutations_by_version.begin();

        if (std::optional<Int64> min_version = getMinPartDataVersion())
            end_it = current_mutations_by_version.upper_bound(*min_version);

        size_t done_count = std::distance(begin_it, end_it);
        if (done_count <= finished_mutations_to_keep)
            return 0;

        auto part_versions_with_names = getSortedPartVersionsWithNames(lock);

        for (auto it = begin_it; it != end_it; ++it)
        {
            const PartVersionWithName needle{static_cast<Int64>(it->first), ""};
            auto versions_it = std::lower_bound(
                part_versions_with_names.begin(), part_versions_with_names.end(), needle);

            if (versions_it != part_versions_with_names.begin() || !it->second.tid.isPrehistoric())
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

std::vector<StorageUniqueMergeTree::PartVersionWithName>
StorageUniqueMergeTree::getSortedPartVersionsWithNames(std::unique_lock<std::mutex> & currently_processing_in_background_mutex_lock) const
{
    std::vector<PartVersionWithName> part_versions_with_names;
    auto data_parts = getDataPartsVectorForInternalUsage();
    part_versions_with_names.reserve(data_parts.size());
    for (const auto & part : data_parts)
        part_versions_with_names.emplace_back(PartVersionWithName{
            getUpdatedDataVersion(part, currently_processing_in_background_mutex_lock),
            part->name
        });
    ::sort(part_versions_with_names.begin(), part_versions_with_names.end());
    return part_versions_with_names;
}

bool StorageUniqueMergeTree::optimize(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
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
                    txn,
                    &disable_reason,
                    local_context->getSettingsRef().optimize_skip_merged_partitions))
            {
                constexpr const char * message = "Cannot OPTIMIZE table: {}";
                if (disable_reason.empty())
                    disable_reason = "unknown reason";
                LOG_INFO(log, fmt::runtime(message), disable_reason);

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
                txn,
                &disable_reason,
                local_context->getSettingsRef().optimize_skip_merged_partitions))
        {
            constexpr const char * message = "Cannot OPTIMIZE table: {}";
            if (disable_reason.empty())
                disable_reason = "unknown reason";
            LOG_INFO(log, fmt::runtime(message), disable_reason);

            if (local_context->getSettingsRef().optimize_throw_if_noop)
                throw Exception(ErrorCodes::CANNOT_ASSIGN_OPTIMIZE, message, disable_reason);
            return false;
        }
    }

    return true;
}

ActionLock StorageUniqueMergeTree::stopMergesAndWait()
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
            throw Exception("Timeout while waiting for already running merges", ErrorCodes::TIMEOUT_EXCEEDED);
        }
    }

    return merge_blocker;
}


MergeTreeDataPartPtr StorageUniqueMergeTree::outdatePart(MergeTreeTransaction * txn, const String & part_name, bool force)
{
    if (force)
    {
        /// Forcefully stop merges and make part outdated
        auto merge_blocker = stopMergesAndWait();
        auto part = getPartIfExists(part_name, {MergeTreeDataPartState::Active});
        if (!part)
            throw Exception("Part " + part_name + " not found, won't try to drop it.", ErrorCodes::NO_SUCH_DATA_PART);
        removePartsFromWorkingSet(txn, {part}, true);
        return part;
    }
    else
    {
        /// Wait merges selector
        std::unique_lock lock(currently_processing_in_background_mutex);

        auto part = getPartIfExists(part_name, {MergeTreeDataPartState::Active});
        /// It's okay, part was already removed
        if (!part)
            return nullptr;

        /// Part will be "removed" by merge or mutation, it's OK in case of some
        /// background cleanup processes like removing of empty parts.
        if (currently_merging_mutating_parts.contains(part))
            return nullptr;

        removePartsFromWorkingSet(txn, {part}, true);
        return part;
    }
}

void StorageUniqueMergeTree::dropPartNoWaitNoThrow(const String & part_name)
{
    if (auto part = outdatePart(NO_TRANSACTION_RAW, part_name, /*force=*/ false))
        dropPartsImpl({part}, /*detach=*/ false);

    /// Else nothing to do, part was removed in some different way
}

void StorageUniqueMergeTree::dropPart(const String & /* part_name*/, bool /*detach*/, ContextPtr /* query_context */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DROP PART is not supported by storage {}", getName());
}

void StorageUniqueMergeTree::dropPartition(const ASTPtr &, bool, ContextPtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DROP PARTITION is not supported by storage {}", getName());
}

void StorageUniqueMergeTree::dropPartsImpl(DataPartsVector && parts_to_remove, bool detach)
{
    auto metadata_snapshot = getInMemoryMetadataPtr();

    if (detach)
    {
        /// If DETACH clone parts to detached/ directory
        /// NOTE: no race with background cleanup until we hold pointers to parts
        for (const auto & part : parts_to_remove)
        {
            LOG_INFO(log, "Detaching {}", part->data_part_storage->getPartDirectory());
            part->makeCloneInDetached("", metadata_snapshot);
        }
    }

    if (deduplication_log)
    {
        for (const auto & part : parts_to_remove)
            deduplication_log->dropPart(part->info);
    }

    if (detach)
        LOG_INFO(log, "Detached {} parts.", parts_to_remove.size());
    else
        LOG_INFO(log, "Removed {} parts.", parts_to_remove.size());

    /// Need to destroy part objects before clearing them from filesystem.
    parts_to_remove.clear();
    clearOldPartsFromFilesystem();
}


PartitionCommandsResultInfo StorageUniqueMergeTree::attachPartition(
    const ASTPtr &,
    const StorageMetadataPtr & /* metadata_snapshot */
    ,
    bool,
    ContextPtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ATTACH PARTITION is not supported by storage {}", getName());
}

void StorageUniqueMergeTree::replacePartitionFrom(const StoragePtr &, const ASTPtr &, bool, ContextPtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "REPLACE PARTITION FROM TABLE is not supported by storage {}", getName());
}

void StorageUniqueMergeTree::movePartitionToTable(const StoragePtr &, const ASTPtr &, ContextPtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MOVE PARTITION TO TABLE is not supported by storage {}", getName());
}

ActionLock StorageUniqueMergeTree::getActionLock(StorageActionBlockType action_type)
{
    if (action_type == ActionLocks::PartsMerge)
        return merger_mutator.merges_blocker.cancel();
    else if (action_type == ActionLocks::PartsTTLMerge)
        return merger_mutator.ttl_merges_blocker.cancel();
    else if (action_type == ActionLocks::PartsMove)
        return parts_mover.moves_blocker.cancel();

    return {};
}

void StorageUniqueMergeTree::onActionLockRemove(StorageActionBlockType action_type)
{
    if (action_type == ActionLocks::PartsMerge ||  action_type == ActionLocks::PartsTTLMerge)
        background_operations_assignee.trigger();
    else if (action_type == ActionLocks::PartsMove)
        background_moves_assignee.trigger();
}

CheckResults StorageUniqueMergeTree::checkData(const ASTPtr & query, ContextPtr local_context)
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
        String checksums_path = "checksums.txt";
        String tmp_checksums_path = "checksums.txt.tmp";
        if (part->isStoredOnDisk() && !part->data_part_storage->exists(checksums_path))
        {
            try
            {
                auto calculated_checksums = checkDataPart(part, false);
                calculated_checksums.checkEqual(part->checksums, true);

                part->data_part_storage->writeChecksums(part->checksums, local_context->getWriteSettings());

                part->checkMetadata();
                results.emplace_back(part->name, true, "Checksums recounted and written to disk.");
            }
            catch (const Exception & ex)
            {
                results.emplace_back(part->name, false,
                    "Check of part finished with error: '" + ex.message() + "'");
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


void StorageUniqueMergeTree::backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & partitions)
{
    auto local_context = backup_entries_collector.getContext();

    DataPartsVector data_parts;
    if (partitions)
        data_parts = getVisibleDataPartsVectorInPartitions(local_context, getPartitionIDsFromQuery(*partitions, local_context));
    else
        data_parts = getVisibleDataPartsVector(local_context);

    Int64 min_data_version = std::numeric_limits<Int64>::max();
    for (const auto & data_part : data_parts)
        min_data_version = std::min(min_data_version, data_part->info.getDataVersion());

    backup_entries_collector.addBackupEntries(backupParts(data_parts, data_path_in_backup));
    backup_entries_collector.addBackupEntries(backupMutations(min_data_version + 1, data_path_in_backup));
}


BackupEntries StorageUniqueMergeTree::backupMutations(UInt64 version, const String & data_path_in_backup) const
{
    fs::path mutations_path_in_backup = fs::path{data_path_in_backup} / "mutations";
    BackupEntries backup_entries;
    for (auto it = current_mutations_by_version.lower_bound(version); it != current_mutations_by_version.end(); ++it)
        backup_entries.emplace_back(mutations_path_in_backup / fmt::format("{:010}.txt", it->first), it->second.backup());
    return backup_entries;
}


void StorageUniqueMergeTree::attachRestoredParts(MutableDataPartsVector &&)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ATTACH RESTORED PARTS is not supported by storage {}", getName());
}


MutationCommands StorageUniqueMergeTree::getFirstAlterMutationCommandsForPart(const DataPartPtr & part) const
{
    std::unique_lock lock(currently_processing_in_background_mutex);

    auto it = current_mutations_by_version.upper_bound(getUpdatedDataVersion(part, lock));
    if (it == current_mutations_by_version.end())
        return {};
    return it->second.commands;
}

void StorageUniqueMergeTree::startBackgroundMovesIfNeeded()
{
    if (areBackgroundMovesNeeded())
        background_moves_assignee.start();
}

std::unique_ptr<MergeTreeSettings> StorageUniqueMergeTree::getDefaultSettings() const
{
    return std::make_unique<MergeTreeSettings>(getContext()->getMergeTreeSettings());
}

void StorageUniqueMergeTree::fillNewPartName(MutableDataPartPtr & part, DataPartsLock &)
{
    part->info.min_block = part->info.max_block = increment.get();
    part->info.mutation = 0;
    part->name = part->getNewName(part->info);
}

MergeTreeData::DataPartPtr StorageUniqueMergeTree::findPartByInfo(const MergeTreePartInfo & part_info) const
{
    if (auto it = data_parts_by_info.find(part_info); it != data_parts_by_info.end())
    {
        return *it;
    }
    return nullptr;
}

MergeTreePartInfo StorageUniqueMergeTree::findPartInfoByMinBlock(Int64 min_block) const
{
    if (auto it = part_info_by_min_block.find(min_block); it != part_info_by_min_block.end())
        return it->second;
    else
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Can not find part info in part_info_by_block_number, this is a bug, min_block: {}", min_block);
}

bool StorageUniqueMergeTree::updatePrimaryIndexAndDeletes(
    const MergeTreePartition & partition,
    const ColumnPtr & delete_key_column,
    const std::vector<Field> & delete_min_values,
    const std::vector<Field> & delete_max_values,
    ContextPtr local_context)
{
    std::lock_guard<std::mutex> lock(write_merge_lock);
    /// Here we should lock currently_processing_in_background_mutex, then lockParts(), otherwise deadlock
    /// will happen. since in scheduleDataProcessingJob(), it will first lock currently_processing_in_background_mutex,
    /// then lockParts() in selectPartsToMerge().
    std::unique_lock<std::mutex> background_lock(currently_processing_in_background_mutex);
    /// Update primary index
    auto partition_id = partition.getID(*this);

    auto primary_index = primaryIndexCache().getOrCreate(partition_id, partition);
    PrimaryIndex::DeletesMap deletes_map;
    PrimaryIndex::DeletesKeys deletes_keys;

    primary_index->deleteKeys(delete_key_column, delete_min_values, delete_max_values, deletes_map, deletes_keys, local_context);

    auto disk = getStoragePolicy()->getDisks()[0];

    auto new_table_version = std::make_unique<TableVersion>(*currentVersion());
    new_table_version->version++;

    /// Update delete bitmap
    for (const auto & [min_block, row_numbers] : deletes_map)
    {
        auto info = findPartInfoByMinBlock(min_block);
        auto update_part = findPartByInfo(info);

        if (!update_part)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Can not find part in data_parts_by_info, this is a bug, part name: {}", info.getPartName());
        }

        /// The part is do merging, add its deleted keys to delete buffer
        if (currently_merging_mutating_parts.find(update_part) != currently_merging_mutating_parts.end())
        {
            delete_buffer.insertKeysByInfo(info, deletes_keys[min_block]);
            /// Here even if the part is merging, we still should update its delete bitmap,
            /// such that even if the merge failed or abort due to too much deletes, the data
            /// still consistent.
            // continue;
        }

        const auto & part_version = new_table_version->part_versions.find(info);
        if (part_version == new_table_version->part_versions.end())
        {
            throw Exception("Can not find part versions in table version", ErrorCodes::LOGICAL_ERROR);
        }

        auto bitmap = delete_bitmap_cache.getOrCreate(update_part, part_version->second);
        auto new_bitmap = bitmap->addDelsAsNewVersion(new_table_version->version, row_numbers);
        auto bitmap_path = update_part->data_part_storage->getRelativePath() + StorageUniqueMergeTree::DELETE_DIR_NAME;
        new_bitmap->serialize(bitmap_path, update_part->data_part_storage->getDisk());
        delete_bitmap_cache.set({info, new_table_version->version}, new_bitmap);
        new_table_version->part_versions[info] = new_table_version->version;
    }
    /// Now, we should update table version
    auto table_version_path = getRelativeDataPath() + TABLE_VERSION_NAME;

    new_table_version->serialize(table_version_path, disk);
    table_version.set(std::move(new_table_version));
    return true;
}
ASTPtr StorageUniqueMergeTree::getFetchIndexQuery(
    const MergeTreePartition & partition, const std::vector<Field> & min_key_values, const std::vector<Field> & max_key_values)
{
    auto metadata = getInMemoryMetadataPtr();
    auto res_query = std::make_shared<ASTSelectQuery>();

    auto select = std::make_shared<ASTExpressionList>();
    for (const auto & unique_expr : metadata->unique_key.expression_list_ast->children)
    {
        select->children.push_back(unique_expr);
    }
    select->children.push_back(std::make_shared<ASTIdentifier>("_part_min_block"));
    select->children.push_back(std::make_shared<ASTIdentifier>("_part_offset"));
    if (!merging_params.version_column.empty())
        select->children.push_back(std::make_shared<ASTIdentifier>(merging_params.version_column));
    res_query->setExpression(ASTSelectQuery::Expression::SELECT, select);

    res_query->setExpression(ASTSelectQuery::Expression::TABLES, std::make_shared<ASTTablesInSelectQuery>());
    auto tables_elem = std::make_shared<ASTTablesInSelectQueryElement>();
    auto table_expr = std::make_shared<ASTTableExpression>();
    res_query->tables()->children.push_back(tables_elem);
    tables_elem->table_expression = table_expr;
    tables_elem->children.push_back(table_expr);
    table_expr->database_and_table_name = std::make_shared<ASTTableIdentifier>(getStorageID());
    table_expr->children.push_back(table_expr->database_and_table_name);

    /// Now, we should set the PREWHERE expression

    auto func_and = makeASTFunction("and");
    if (partition.getID(*this) != "all")
    {
        const auto & partition_by = metadata->getPartitionKey();
        const auto & partition_by_expr_list = partition_by.expression_list_ast;
        size_t key_size = partition.value.size();
        for (size_t i = 0; i < key_size; ++i)
        {
            if (WhichDataType{partition_by.data_types[i]}.isNativeUInt())
            {
                func_and->arguments->children.push_back(makeASTFunction(
                    "equals",
                    partition_by_expr_list->children[i]->clone(),
                    std::make_shared<ASTLiteral>(partition.value[i].get<UInt64>())));
            }
            else if (WhichDataType{partition_by.data_types[i]}.isNativeInt())
            {
                func_and->arguments->children.push_back(makeASTFunction(
                    "equals", partition_by_expr_list->children[i]->clone(), std::make_shared<ASTLiteral>(partition.value[i].get<Int64>())));
            }
            else if (WhichDataType{partition_by.data_types[i]}.isFloat32())
            {
                func_and->arguments->children.push_back(makeASTFunction(
                    "equals",
                    partition_by_expr_list->children[i]->clone(),
                    std::make_shared<ASTLiteral>(partition.value[i].get<Float32>())));
            }
            else if (WhichDataType{partition_by.data_types[i]}.isFloat64())
            {
                func_and->arguments->children.push_back(makeASTFunction(
                    "equals",
                    partition_by_expr_list->children[i]->clone(),
                    std::make_shared<ASTLiteral>(partition.value[i].get<Float64>())));
            }
            else if (isDate(partition_by.data_types[i]))
            {
                auto func_to_date = makeASTFunction("toDate", std::make_shared<ASTLiteral>(partition.value[i].get<UInt64>()));
                func_and->arguments->children.push_back(
                    makeASTFunction("equals", partition_by_expr_list->children[i]->clone(), func_to_date));
            }
            else if (isDate32(partition_by.data_types[i]))
            {
                auto func_to_date32 = makeASTFunction("toDate32", std::make_shared<ASTLiteral>(partition.value[i].get<UInt64>()));
                func_and->arguments->children.push_back(
                    makeASTFunction("equals", partition_by_expr_list->children[i]->clone(), func_to_date32));
            }
            else if (isDateTime(partition_by.data_types[i]))
            {
                auto func_to_datetime = makeASTFunction("toDateTime", std::make_shared<ASTLiteral>(partition.value[i].get<UInt64>()));
                func_and->arguments->children.push_back(
                    makeASTFunction("equals", partition_by_expr_list->children[i]->clone(), func_to_datetime));
            }
            else if (isStringOrFixedString(partition_by.data_types[i]))
            {
                func_and->arguments->children.push_back(makeASTFunction(
                    "equals",
                    partition_by_expr_list->children[i]->clone(),
                    std::make_shared<ASTLiteral>(partition.value[i].get<String>())));
            }
            else
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Unsupported {} data type of partition by key of StorageUniqueMergeTree",
                    partition_by.data_types[i]->getName());
            }
        }
    }

    const auto & unique_key = metadata->getUniqueKey();
    const auto & unique_key_expr_list = unique_key.expression_list_ast;
    for (size_t i = 0; i < min_key_values.size(); ++i)
    {
        if (WhichDataType{unique_key.data_types[i]}.isNativeUInt())
        {
            func_and->arguments->children.push_back(makeASTFunction(
                "greaterOrEquals",
                unique_key_expr_list->children[i]->clone(),
                std::make_shared<ASTLiteral>(min_key_values[i].get<UInt64>())));
            func_and->arguments->children.push_back(makeASTFunction(
                "lessOrEquals", unique_key_expr_list->children[i]->clone(), std::make_shared<ASTLiteral>(max_key_values[i].get<UInt64>())));
        }
        else if (WhichDataType{unique_key.data_types[i]}.isNativeInt())
        {
            func_and->arguments->children.push_back(makeASTFunction(
                "greaterOrEquals",
                unique_key_expr_list->children[i]->clone(),
                std::make_shared<ASTLiteral>(min_key_values[i].get<Int64>())));
            func_and->arguments->children.push_back(makeASTFunction(
                "lessOrEquals", unique_key_expr_list->children[i]->clone(), std::make_shared<ASTLiteral>(max_key_values[i].get<Int64>())));
        }
        else if (WhichDataType{unique_key.data_types[i]}.isFloat32())
        {
            func_and->arguments->children.push_back(makeASTFunction(
                "greaterOrEquals",
                unique_key_expr_list->children[i]->clone(),
                std::make_shared<ASTLiteral>(min_key_values[i].get<Float32>())));
            func_and->arguments->children.push_back(makeASTFunction(
                "lessOrEquals",
                unique_key_expr_list->children[i]->clone(),
                std::make_shared<ASTLiteral>(max_key_values[i].get<Float32>())));
        }
        else if (WhichDataType{unique_key.data_types[i]}.isFloat64())
        {
            func_and->arguments->children.push_back(makeASTFunction(
                "greaterOrEquals",
                unique_key_expr_list->children[i]->clone(),
                std::make_shared<ASTLiteral>(min_key_values[i].get<Float64>())));
            func_and->arguments->children.push_back(makeASTFunction(
                "lessOrEquals",
                unique_key_expr_list->children[i]->clone(),
                std::make_shared<ASTLiteral>(max_key_values[i].get<Float64>())));
        }
        else if (isDate(unique_key.data_types[i]))
        {
            auto func_to_min_date = makeASTFunction("toDate", std::make_shared<ASTLiteral>(min_key_values[i].get<UInt64>()));
            func_and->arguments->children.push_back(
                makeASTFunction("greaterOrEquals", unique_key_expr_list->children[i]->clone(), func_to_min_date));
            auto func_to_max_date = makeASTFunction("toDate", std::make_shared<ASTLiteral>(max_key_values[i].get<UInt64>()));
            func_and->arguments->children.push_back(
                makeASTFunction("lessOrEquals", unique_key_expr_list->children[i]->clone(), func_to_max_date));
        }
        else if (isDate32(unique_key.data_types[i]))
        {
            auto func_to_min_date32 = makeASTFunction("toDate32", std::make_shared<ASTLiteral>(min_key_values[i].get<UInt64>()));
            func_and->arguments->children.push_back(
                makeASTFunction("greaterOrEquals", unique_key_expr_list->children[i]->clone(), func_to_min_date32));
            auto func_to_max_date32 = makeASTFunction("toDate32", std::make_shared<ASTLiteral>(max_key_values[i].get<UInt64>()));
            func_and->arguments->children.push_back(
                makeASTFunction("lessOrEquals", unique_key_expr_list->children[i]->clone(), func_to_max_date32));
        }
        else if (isDateTime(unique_key.data_types[i]))
        {
            auto func_to_min_datetime = makeASTFunction("toDateTime", std::make_shared<ASTLiteral>(min_key_values[i].get<UInt64>()));
            func_and->arguments->children.push_back(
                makeASTFunction("greaterOrEquals", unique_key_expr_list->children[i]->clone(), func_to_min_datetime));
            auto func_to_max_datetime = makeASTFunction("toDateTime", std::make_shared<ASTLiteral>(max_key_values[i].get<UInt64>()));
            func_and->arguments->children.push_back(
                makeASTFunction("lessOrEquals", unique_key_expr_list->children[i]->clone(), func_to_max_datetime));
        }
        else if (isStringOrFixedString(unique_key.data_types[i]))
        {
            func_and->arguments->children.push_back(makeASTFunction(
                "greaterOrEquals",
                unique_key_expr_list->children[i]->clone(),
                std::make_shared<ASTLiteral>(min_key_values[i].get<String>())));
            func_and->arguments->children.push_back(makeASTFunction(
                "lessOrEquals", unique_key_expr_list->children[i]->clone(), std::make_shared<ASTLiteral>(max_key_values[i].get<String>())));
        }
        else
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Unsupported {} data type of Unique key of StorageUniqueMergeTree",
                unique_key.data_types[i]->getName());
        }
    }

    res_query->setExpression(ASTSelectQuery::Expression::PREWHERE, func_and);

    auto set_query = std::make_shared<ASTSetQuery>();
    SettingsChanges settings;
    settings.push_back({"force_primary_key", 1});
    set_query->changes = std::move(settings);
    res_query->setExpression(ASTSelectQuery::Expression::SETTINGS, set_query);

    return res_query;
}

Block StorageUniqueMergeTree::getSampleBlockWithDeleteOp() const
{
    auto header = getInMemoryMetadataPtr()->getSampleBlock();
    header.insert({DataTypeUInt8{}.createColumn(), std::make_shared<DataTypeUInt8>(), "__delete_op"});
    return header;
}
}
