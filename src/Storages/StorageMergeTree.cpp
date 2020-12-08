#include "StorageMergeTree.h"

#include <Databases/IDatabase.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Common/FieldVisitors.h>
#include <Common/ThreadPool.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/AlterCommands.h>
#include <Storages/PartitionCommands.h>
#include <Storages/MergeTree/MergeTreeBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/MergeTree/PartitionPruner.h>
#include <Disks/StoragePolicy.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Processors/Pipe.h>
#include <optional>

namespace CurrentMetrics
{
    extern const Metric BackgroundPoolTask;
}

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
}

namespace ActionLocks
{
    extern const StorageActionBlockType PartsMerge;
    extern const StorageActionBlockType PartsTTLMerge;
    extern const StorageActionBlockType PartsMove;
}


StorageMergeTree::StorageMergeTree(
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    bool attach,
    Context & context_,
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
        false,      /// require_part_metadata
        attach)
    , reader(*this)
    , writer(*this)
    , merger_mutator(*this, global_context.getSettingsRef().background_pool_size)
    , background_executor(*this, global_context)
    , background_moves_executor(*this, global_context)

{
    loadDataParts(has_force_restore_data_flag);

    if (!attach && !getDataParts().empty())
        throw Exception("Data directory for table already containing data parts - probably it was unclean DROP table or manual intervention. You must either clear directory by hand or use ATTACH TABLE instead of CREATE TABLE if you need to use that parts.", ErrorCodes::INCORRECT_DATA);

    increment.set(getMaxBlockNumber());

    loadMutations();
}


void StorageMergeTree::startup()
{
    clearOldPartsFromFilesystem();
    clearOldWriteAheadLogs();
    clearEmptyParts();

    /// Temporary directories contain incomplete results of merges (after forced restart)
    ///  and don't allow to reinitialize them, so delete each of them immediately
    clearOldTemporaryDirectories(0);

    /// NOTE background task will also do the above cleanups periodically.
    time_after_previous_cleanup.restart();

    try
    {
        background_executor.start();
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


void StorageMergeTree::shutdown()
{
    if (shutdown_called)
        return;
    shutdown_called = true;

    /// Unlock all waiting mutations
    {
        std::lock_guard lock(mutation_wait_mutex);
        mutation_wait_event.notify_all();
    }

    merger_mutator.merges_blocker.cancelForever();
    parts_mover.moves_blocker.cancelForever();

    background_executor.finish();
    background_moves_executor.finish();

    try
    {
        /// We clear all old parts after stopping all background operations.
        /// It's important, because background operations can produce temporary
        /// parts which will remove themselves in their descrutors. If so, we
        /// may have race condition between our remove call and background
        /// process.
        clearOldPartsFromFilesystem(true);
    }
    catch (...)
    {
        /// Example: the case of readonly filesystem, we have failure removing old parts.
        /// Should not prevent table shutdown.
        tryLogCurrentException(log);
    }
}


StorageMergeTree::~StorageMergeTree()
{
    shutdown();
}

void StorageMergeTree::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    unsigned num_streams)
{
    if (auto plan = reader.read(column_names, metadata_snapshot, query_info, context, max_block_size, num_streams))
        query_plan = std::move(*plan);
}

Pipe StorageMergeTree::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    const size_t max_block_size,
    const unsigned num_streams)
{
    QueryPlan plan;
    read(plan, column_names, metadata_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
    return plan.convertToPipe();
}

std::optional<UInt64> StorageMergeTree::totalRows(const Settings &) const
{
    return getTotalActiveSizeInRows();
}

std::optional<UInt64> StorageMergeTree::totalRowsByPartitionPredicate(const SelectQueryInfo & query_info, const Context & context) const
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    PartitionPruner partition_pruner(metadata_snapshot->getPartitionKey(), query_info, context, true /* strict */);
    if (partition_pruner.isUseless())
        return {};
    size_t res = 0;
    auto lock = lockParts();
    for (const auto & part : getDataPartsStateRange(DataPartState::Committed))
    {
        if (!partition_pruner.canBePruned(part))
            res += part->rows_count;
    }
    return res;
}

std::optional<UInt64> StorageMergeTree::totalBytes(const Settings &) const
{
    return getTotalActiveSizeInBytes();
}

BlockOutputStreamPtr StorageMergeTree::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, const Context & context)
{

    const auto & settings = context.getSettingsRef();
    return std::make_shared<MergeTreeBlockOutputStream>(
        *this, metadata_snapshot, settings.max_partitions_per_insert_block);
}

void StorageMergeTree::checkTableCanBeDropped() const
{
    auto table_id = getStorageID();
    global_context.checkTableCanBeDropped(table_id.database_name, table_id.table_name, getTotalActiveSizeInBytes());
}

void StorageMergeTree::drop()
{
    shutdown();
    dropAllData();
}

void StorageMergeTree::truncate(const ASTPtr &, const StorageMetadataPtr &, const Context &, TableExclusiveLockHolder &)
{
    {
        /// Asks to complete merges and does not allow them to start.
        /// This protects against "revival" of data for a removed partition after completion of merge.
        auto merge_blocker = stopMergesAndWait();

        auto parts_to_remove = getDataPartsVector();
        removePartsFromWorkingSet(parts_to_remove, true);

        LOG_INFO(log, "Removed {} parts.", parts_to_remove.size());
    }

    clearOldMutations(true);
    clearOldPartsFromFilesystem();
}


void StorageMergeTree::alter(
    const AlterCommands & commands,
    const Context & context,
    TableLockHolder & table_lock_holder)
{
    auto table_id = getStorageID();

    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    StorageInMemoryMetadata old_metadata = getInMemoryMetadata();
    auto maybe_mutation_commands = commands.getMutationCommands(new_metadata, context.getSettingsRef().materialize_ttl_after_modify, context);
    String mutation_file_name;
    Int64 mutation_version = -1;
    commands.apply(new_metadata, context);

    /// This alter can be performed at new_metadata level only
    if (commands.isSettingsAlter())
    {
        changeSettings(new_metadata.settings_changes, table_lock_holder);

        DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(context, table_id, new_metadata);
    }
    else
    {
        {
            changeSettings(new_metadata.settings_changes, table_lock_holder);
            checkTTLExpressions(new_metadata, old_metadata);
            /// Reinitialize primary key because primary key column types might have changed.
            setProperties(new_metadata, old_metadata);

            DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(context, table_id, new_metadata);

            if (!maybe_mutation_commands.empty())
                mutation_version = startMutation(maybe_mutation_commands, mutation_file_name);
        }

        /// Always execute required mutations synchronously, because alters
        /// should be executed in sequential order.
        if (!maybe_mutation_commands.empty())
            waitForMutation(mutation_version, mutation_file_name);
    }
}


/// While exists, marks parts as 'currently_merging_mutating_parts' and reserves free space on filesystem.
StorageMergeTree::CurrentlyMergingPartsTagger::CurrentlyMergingPartsTagger(
    FutureMergedMutatedPart & future_part_,
    size_t total_size,
    StorageMergeTree & storage_,
    const StorageMetadataPtr & metadata_snapshot,
    bool is_mutation)
    : future_part(future_part_), storage(storage_)
{
    /// Assume mutex is already locked, because this method is called from mergeTask.

    /// if we mutate part, than we should reserve space on the same disk, because mutations possible can create hardlinks
    if (is_mutation)
        reserved_space = storage.tryReserveSpace(total_size, future_part_.parts[0]->volume);
    else
    {
        IMergeTreeDataPart::TTLInfos ttl_infos;
        size_t max_volume_index = 0;
        for (auto & part_ptr : future_part_.parts)
        {
            ttl_infos.update(part_ptr->ttl_infos);
            max_volume_index = std::max(max_volume_index, storage.getStoragePolicy()->getVolumeIndexByDisk(part_ptr->volume->getDisk()));
        }

        reserved_space = storage.tryReserveSpacePreferringTTLRules(metadata_snapshot, total_size, ttl_infos, time(nullptr), max_volume_index);
    }
    if (!reserved_space)
    {
        if (is_mutation)
            throw Exception("Not enough space for mutating part '" + future_part_.parts[0]->name + "'", ErrorCodes::NOT_ENOUGH_SPACE);
        else
            throw Exception("Not enough space for merging parts", ErrorCodes::NOT_ENOUGH_SPACE);
    }

    future_part_.updatePath(storage, reserved_space);

    for (const auto & part : future_part.parts)
    {
        if (storage.currently_merging_mutating_parts.count(part))
            throw Exception("Tagging already tagged part " + part->name + ". This is a bug.", ErrorCodes::LOGICAL_ERROR);
    }
    storage.currently_merging_mutating_parts.insert(future_part.parts.begin(), future_part.parts.end());
}

StorageMergeTree::CurrentlyMergingPartsTagger::~CurrentlyMergingPartsTagger()
{
    std::lock_guard lock(storage.currently_processing_in_background_mutex);

    for (const auto & part : future_part.parts)
    {
        if (!storage.currently_merging_mutating_parts.count(part))
            std::terminate();
        storage.currently_merging_mutating_parts.erase(part);
    }

    storage.currently_processing_in_background_condition.notify_all();
}

Int64 StorageMergeTree::startMutation(const MutationCommands & commands, String & mutation_file_name)
{
    /// Choose any disk, because when we load mutations we search them at each disk
    /// where storage can be placed. See loadMutations().
    auto disk = getStoragePolicy()->getAnyDisk();
    Int64 version;
    {
        std::lock_guard lock(currently_processing_in_background_mutex);

        MergeTreeMutationEntry entry(commands, disk, relative_data_path, insert_increment.get());
        version = increment.get();
        entry.commit(version);
        mutation_file_name = entry.file_name;
        auto insertion = current_mutations_by_id.emplace(mutation_file_name, std::move(entry));
        current_mutations_by_version.emplace(version, insertion.first->second);

        LOG_INFO(log, "Added mutation: {}", mutation_file_name);
    }
    background_executor.triggerTask();
    return version;
}


void StorageMergeTree::updateMutationEntriesErrors(FutureMergedMutatedPart result_part, bool is_successful, const String & exception_message)
{
    /// Update the information about failed parts in the system.mutations table.

    Int64 sources_data_version = result_part.parts.at(0)->info.getDataVersion();
    Int64 result_data_version = result_part.part_info.getDataVersion();
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
                if (!entry.latest_failed_part.empty() && result_part.part_info.contains(entry.latest_failed_part_info))
                {
                    entry.latest_failed_part.clear();
                    entry.latest_failed_part_info = MergeTreePartInfo();
                    entry.latest_fail_time = 0;
                    entry.latest_fail_reason.clear();
                }
            }
            else
            {
                entry.latest_failed_part = result_part.parts.at(0)->name;
                entry.latest_failed_part_info = result_part.parts.at(0)->info;
                entry.latest_fail_time = time(nullptr);
                entry.latest_fail_reason = exception_message;
            }
        }
    }

    std::unique_lock lock(mutation_wait_mutex);
    mutation_wait_event.notify_all();
}

void StorageMergeTree::waitForMutation(Int64 version, const String & file_name)
{
    LOG_INFO(log, "Waiting mutation: {}", file_name);
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
    mutation_ids.insert(file_name);

    auto mutation_status = getIncompleteMutationsStatus(version, &mutation_ids);
    checkMutationStatus(mutation_status, mutation_ids);

    LOG_INFO(log, "Mutation {} done", file_name);
}

void StorageMergeTree::mutate(const MutationCommands & commands, const Context & query_context)
{
    String mutation_file_name;
    Int64 version = startMutation(commands, mutation_file_name);

    if (query_context.getSettingsRef().mutations_sync > 0)
        waitForMutation(version, mutation_file_name);
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

std::optional<MergeTreeMutationStatus> StorageMergeTree::getIncompleteMutationsStatus(Int64 mutation_version, std::set<String> * mutation_ids) const
{
    std::lock_guard lock(currently_processing_in_background_mutex);

    auto current_mutation_it = current_mutations_by_version.find(mutation_version);
    /// Killed
    if (current_mutation_it == current_mutations_by_version.end())
        return {};

    MergeTreeMutationStatus result{.is_done = false};

    const auto & mutation_entry = current_mutation_it->second;

    auto data_parts = getDataPartsVector();
    for (const auto & data_part : data_parts)
    {
        if (data_part->info.getDataVersion() < mutation_version)
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
                    auto mutations_begin_it = current_mutations_by_version.upper_bound(data_part->info.getDataVersion());

                    for (auto it = mutations_begin_it; it != current_mutations_by_version.end(); ++it)
                        /// All mutations with the same failure
                        if (it->second.latest_fail_reason == result.latest_fail_reason)
                            mutation_ids->insert(it->second.file_name);
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
    auto data_parts = getDataPartsVector();
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

    std::optional<MergeTreeMutationEntry> to_kill;
    {
        std::lock_guard lock(currently_processing_in_background_mutex);
        auto it = current_mutations_by_id.find(mutation_id);
        if (it != current_mutations_by_id.end())
        {
            to_kill.emplace(std::move(it->second));
            current_mutations_by_id.erase(it);
            current_mutations_by_version.erase(to_kill->block_number);
        }
    }

    if (!to_kill)
        return CancellationCode::NotFound;

    global_context.getMergeList().cancelPartMutations({}, to_kill->block_number);
    to_kill->removeFile();
    LOG_TRACE(log, "Cancelled part mutations and removed mutation file {}", mutation_id);
    {
        std::lock_guard<std::mutex> lock(mutation_wait_mutex);
        mutation_wait_event.notify_all();
    }

    /// Maybe there is another mutation that was blocked by the killed one. Try to execute it immediately.
    background_executor.triggerTask();

    return CancellationCode::CancelSent;
}


void StorageMergeTree::loadMutations()
{
    for (const auto & [path, disk] : getRelativeDataPathsWithDisks())
    {
        for (auto it = disk->iterateDirectory(path); it->isValid(); it->next())
        {
            if (startsWith(it->name(), "mutation_"))
            {
                MergeTreeMutationEntry entry(disk, path, it->name());
                Int64 block_number = entry.block_number;
                LOG_DEBUG(log, "Loading mutation: {} entry, commands size: {}", it->name(), entry.commands.size());
                auto insertion = current_mutations_by_id.emplace(it->name(), std::move(entry));
                current_mutations_by_version.emplace(block_number, insertion.first->second);
            }
            else if (startsWith(it->name(), "tmp_mutation_"))
            {
                disk->remove(it->path());
            }
        }
    }

    if (!current_mutations_by_version.empty())
        increment.value = std::max(Int64(increment.value.load()), current_mutations_by_version.rbegin()->first);
}

std::shared_ptr<StorageMergeTree::MergeMutateSelectedEntry> StorageMergeTree::selectPartsToMerge(
    const StorageMetadataPtr & metadata_snapshot, bool aggressive, const String & partition_id, bool final, String * out_disable_reason, TableLockHolder & /* table_lock_holder */, bool optimize_skip_merged_partitions, SelectPartsDecision * select_decision_out)
{
    std::unique_lock lock(currently_processing_in_background_mutex);
    auto data_settings = getSettings();

    FutureMergedMutatedPart future_part;

    if (storage_settings.get()->assign_part_uuids)
        future_part.uuid = UUIDHelpers::generateV4();

    /// You must call destructor with unlocked `currently_processing_in_background_mutex`.
    CurrentlyMergingPartsTaggerPtr merging_tagger;
    MergeList::EntryPtr merge_entry;

    auto can_merge = [this, &lock] (const DataPartPtr & left, const DataPartPtr & right, String *) -> bool
    {
        /// This predicate is checked for the first part of each partition.
        /// (left = nullptr, right = "first part of partition")
        if (!left)
            return !currently_merging_mutating_parts.count(right);
        return !currently_merging_mutating_parts.count(left) && !currently_merging_mutating_parts.count(right)
            && getCurrentMutationVersion(left, lock) == getCurrentMutationVersion(right, lock);
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
                out_disable_reason);
        }
        else if (out_disable_reason)
            *out_disable_reason = "Current value of max_source_parts_size is zero";
    }
    else
    {
        while (true)
        {
            UInt64 disk_space = getStoragePolicy()->getMaxUnreservedFreeSpace();
            select_decision = merger_mutator.selectAllPartsToMergeWithinPartition(
                future_part, disk_space, can_merge, partition_id, final, metadata_snapshot, out_disable_reason, optimize_skip_merged_partitions);

            /// If final - we will wait for currently processing merges to finish and continue.
            /// TODO Respect query settings for timeout
            if (final
                && select_decision != SelectPartsDecision::SELECTED
                && !currently_merging_mutating_parts.empty()
                && out_disable_reason
                && out_disable_reason->empty())
            {
                LOG_DEBUG(log, "Waiting for currently running merges ({} parts are merging right now) to perform OPTIMIZE FINAL",
                    currently_merging_mutating_parts.size());

                if (std::cv_status::timeout == currently_processing_in_background_condition.wait_for(
                    lock, std::chrono::seconds(DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC)))
                {
                    *out_disable_reason = "Timeout while waiting for already running merges before running OPTIMIZE with FINAL";
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

    merging_tagger = std::make_unique<CurrentlyMergingPartsTagger>(future_part, MergeTreeDataMergerMutator::estimateNeededDiskSpace(future_part.parts), *this, metadata_snapshot, false);
    return std::make_shared<MergeMutateSelectedEntry>(future_part, std::move(merging_tagger), MutationCommands{});
}

bool StorageMergeTree::merge(
    bool aggressive,
    const String & partition_id,
    bool final,
    bool deduplicate,
    String * out_disable_reason,
    bool optimize_skip_merged_partitions)
{
    auto table_lock_holder = lockForShare(RWLockImpl::NO_QUERY, getSettings()->lock_acquire_timeout_for_background_operations);
    auto metadata_snapshot = getInMemoryMetadataPtr();

    SelectPartsDecision select_decision;

    auto merge_mutate_entry = selectPartsToMerge(metadata_snapshot, aggressive, partition_id, final, out_disable_reason, table_lock_holder, optimize_skip_merged_partitions, &select_decision);

    /// If there is nothing to merge then we treat this merge as successful (needed for optimize final optimization)
    if (select_decision == SelectPartsDecision::NOTHING_TO_MERGE)
        return true;

    if (!merge_mutate_entry)
        return false;

    return mergeSelectedParts(metadata_snapshot, deduplicate, *merge_mutate_entry, table_lock_holder);
}

bool StorageMergeTree::mergeSelectedParts(const StorageMetadataPtr & metadata_snapshot, bool deduplicate, MergeMutateSelectedEntry & merge_mutate_entry, TableLockHolder & table_lock_holder)
{
    auto & future_part = merge_mutate_entry.future_part;
    Stopwatch stopwatch;
    MutableDataPartPtr new_part;
    auto table_id = getStorageID();

    auto merge_list_entry = global_context.getMergeList().insert(table_id.database_name, table_id.table_name, future_part);

    auto write_part_log = [&] (const ExecutionStatus & execution_status)
    {
        writePartLog(
            PartLogElement::MERGE_PARTS,
            execution_status,
            stopwatch.elapsed(),
            future_part.name,
            new_part,
            future_part.parts,
            merge_list_entry.get());
    };

    try
    {
        new_part = merger_mutator.mergePartsToTemporaryPart(
            future_part, metadata_snapshot, *(merge_list_entry), table_lock_holder, time(nullptr),
            global_context, merge_mutate_entry.tagger->reserved_space, deduplicate);

        merger_mutator.renameMergedTemporaryPart(new_part, future_part.parts, nullptr);
        write_part_log({});
    }
    catch (...)
    {
        write_part_log(ExecutionStatus::fromCurrentException());
        throw;
    }

    return true;
}

bool StorageMergeTree::partIsAssignedToBackgroundOperation(const DataPartPtr & part) const
{
    std::lock_guard background_processing_lock(currently_processing_in_background_mutex);
    return currently_merging_mutating_parts.count(part);
}

std::shared_ptr<StorageMergeTree::MergeMutateSelectedEntry> StorageMergeTree::selectPartsToMutate(
    const StorageMetadataPtr & metadata_snapshot, String * /* disable_reason */, TableLockHolder & /* table_lock_holder */)
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    size_t max_ast_elements = global_context.getSettingsRef().max_expanded_ast_elements;

    FutureMergedMutatedPart future_part;
    if (storage_settings.get()->assign_part_uuids)
        future_part.uuid = UUIDHelpers::generateV4();

    MutationCommands commands;

    CurrentlyMergingPartsTaggerPtr tagger;

    if (current_mutations_by_version.empty())
        return {};

    auto mutations_end_it = current_mutations_by_version.end();
    for (const auto & part : getDataPartsVector())
    {
        if (currently_merging_mutating_parts.count(part))
            continue;

        auto mutations_begin_it = current_mutations_by_version.upper_bound(part->info.getDataVersion());
        if (mutations_begin_it == mutations_end_it)
            continue;

        size_t max_source_part_size = merger_mutator.getMaxSourcePartSizeForMutation();
        if (max_source_part_size < part->getBytesOnDisk())
        {
            LOG_DEBUG(log, "Current max source part size for mutation is {} but part size {}. Will not mutate part {}. "
                "Max size depends not only on available space, but also on settings "
                "'number_of_free_entries_in_pool_to_execute_mutation' and 'background_pool_size'",
                max_source_part_size, part->getBytesOnDisk(), part->name);
            continue;
        }

        size_t current_ast_elements = 0;
        for (auto it = mutations_begin_it; it != mutations_end_it; ++it)
        {
            size_t commands_size = 0;
            MutationCommands commands_for_size_validation;
            for (const auto & command : it->second.commands)
            {
                if (command.type != MutationCommand::Type::DROP_COLUMN
                    && command.type != MutationCommand::Type::DROP_INDEX
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
                MutationsInterpreter interpreter(
                    shared_from_this(), metadata_snapshot, commands_for_size_validation, global_context, false);
                commands_size += interpreter.evaluateCommandsSize();
            }

            if (current_ast_elements + commands_size >= max_ast_elements)
                break;

            current_ast_elements += commands_size;
            commands.insert(commands.end(), it->second.commands.begin(), it->second.commands.end());
        }

        auto new_part_info = part->info;
        new_part_info.mutation = current_mutations_by_version.rbegin()->first;

        future_part.parts.push_back(part);
        future_part.part_info = new_part_info;
        future_part.name = part->getNewName(new_part_info);
        future_part.type = part->getType();

        tagger = std::make_unique<CurrentlyMergingPartsTagger>(future_part, MergeTreeDataMergerMutator::estimateNeededDiskSpace({part}), *this, metadata_snapshot, true);
        return std::make_shared<MergeMutateSelectedEntry>(future_part, std::move(tagger), commands);
    }
    return {};
}

bool StorageMergeTree::mutateSelectedPart(const StorageMetadataPtr & metadata_snapshot, MergeMutateSelectedEntry & merge_mutate_entry, TableLockHolder & table_lock_holder)
{
    auto & future_part = merge_mutate_entry.future_part;
    auto table_id = getStorageID();

    auto merge_list_entry = global_context.getMergeList().insert(table_id.database_name, table_id.table_name, future_part);
    Stopwatch stopwatch;
    MutableDataPartPtr new_part;

    auto write_part_log = [&] (const ExecutionStatus & execution_status)
    {
        writePartLog(
            PartLogElement::MUTATE_PART,
            execution_status,
            stopwatch.elapsed(),
            future_part.name,
            new_part,
            future_part.parts,
            merge_list_entry.get());
    };

    try
    {
        new_part = merger_mutator.mutatePartToTemporaryPart(
            future_part, metadata_snapshot, merge_mutate_entry.commands, *(merge_list_entry),
            time(nullptr), global_context, merge_mutate_entry.tagger->reserved_space, table_lock_holder);

        renameTempPartAndReplace(new_part);

        updateMutationEntriesErrors(future_part, true, "");
        write_part_log({});
    }
    catch (...)
    {
        updateMutationEntriesErrors(future_part, false, getCurrentExceptionMessage(false));
        write_part_log(ExecutionStatus::fromCurrentException());
        throw;
    }

    return true;
}

std::optional<JobAndPool> StorageMergeTree::getDataProcessingJob()
{
    if (shutdown_called)
        return {};

    if (merger_mutator.merges_blocker.isCancelled())
        return {};

    auto metadata_snapshot = getInMemoryMetadataPtr();
    std::shared_ptr<MergeMutateSelectedEntry> merge_entry, mutate_entry;

    auto share_lock = lockForShare(RWLockImpl::NO_QUERY, getSettings()->lock_acquire_timeout_for_background_operations);
    merge_entry = selectPartsToMerge(metadata_snapshot, false, {}, false, nullptr, share_lock);
    if (!merge_entry)
        mutate_entry = selectPartsToMutate(metadata_snapshot, nullptr, share_lock);

    if (merge_entry || mutate_entry)
    {
        return JobAndPool{[this, metadata_snapshot, merge_entry, mutate_entry, share_lock] () mutable
        {
            if (merge_entry)
                mergeSelectedParts(metadata_snapshot, false, *merge_entry, share_lock);
            else if (mutate_entry)
                mutateSelectedPart(metadata_snapshot, *mutate_entry, share_lock);
        }, PoolType::MERGE_MUTATE};
    }
    else if (auto lock = time_after_previous_cleanup.compareAndRestartDeferred(1))
    {
        return JobAndPool{[this, share_lock] ()
        {
            /// All use relative_data_path which changes during rename
            /// so execute under share lock.
            clearOldPartsFromFilesystem();
            clearOldTemporaryDirectories();
            clearOldWriteAheadLogs();
            clearOldMutations();
            clearEmptyParts();
        }, PoolType::MERGE_MUTATE};
    }
    return {};
}

Int64 StorageMergeTree::getCurrentMutationVersion(
    const DataPartPtr & part,
    std::unique_lock<std::mutex> & /* currently_processing_in_background_mutex_lock */) const
{
    auto it = current_mutations_by_version.upper_bound(part->info.getDataVersion());
    if (it == current_mutations_by_version.begin())
        return 0;
    --it;
    return it->first;
}

void StorageMergeTree::clearOldMutations(bool truncate)
{
    const auto settings = getSettings();
    if (!truncate && !settings->finished_mutations_to_keep)
        return;

    std::vector<MergeTreeMutationEntry> mutations_to_delete;
    {
        std::lock_guard lock(currently_processing_in_background_mutex);

        if (!truncate && current_mutations_by_version.size() <= settings->finished_mutations_to_keep)
            return;

        auto end_it = current_mutations_by_version.end();
        auto begin_it = current_mutations_by_version.begin();
        size_t to_delete_count = std::distance(begin_it, end_it);

        if (!truncate)
        {
            if (std::optional<Int64> min_version = getMinPartDataVersion())
                end_it = current_mutations_by_version.upper_bound(*min_version);

            size_t done_count = std::distance(begin_it, end_it);
            if (done_count <= settings->finished_mutations_to_keep)
                return;

            to_delete_count = done_count - settings->finished_mutations_to_keep;
        }

        auto it = begin_it;
        for (size_t i = 0; i < to_delete_count; ++i)
        {
            mutations_to_delete.push_back(std::move(it->second));
            current_mutations_by_id.erase(mutations_to_delete.back().file_name);
            it = current_mutations_by_version.erase(it);
        }
    }

    for (auto & mutation : mutations_to_delete)
    {
        LOG_TRACE(log, "Removing mutation: {}", mutation.file_name);
        mutation.removeFile();
    }
}

bool StorageMergeTree::optimize(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Context & context)
{
    String disable_reason;
    if (!partition && final)
    {
        DataPartsVector data_parts = getDataPartsVector();
        std::unordered_set<String> partition_ids;

        for (const DataPartPtr & part : data_parts)
            partition_ids.emplace(part->info.partition_id);

        for (const String & partition_id : partition_ids)
        {
            if (!merge(true, partition_id, true, deduplicate, &disable_reason, context.getSettingsRef().optimize_skip_merged_partitions))
            {
                constexpr const char * message = "Cannot OPTIMIZE table: {}";
                if (disable_reason.empty())
                    disable_reason = "unknown reason";
                LOG_INFO(log, message, disable_reason);

                if (context.getSettingsRef().optimize_throw_if_noop)
                    throw Exception(ErrorCodes::CANNOT_ASSIGN_OPTIMIZE, message, disable_reason);
                return false;
            }
        }
    }
    else
    {
        String partition_id;
        if (partition)
            partition_id = getPartitionIDFromQuery(partition, context);

        if (!merge(true, partition_id, final, deduplicate, &disable_reason, context.getSettingsRef().optimize_skip_merged_partitions))
        {
            constexpr const char * message = "Cannot OPTIMIZE table: {}";
            if (disable_reason.empty())
                disable_reason = "unknown reason";
            LOG_INFO(log, message, disable_reason);

            if (context.getSettingsRef().optimize_throw_if_noop)
                throw Exception(ErrorCodes::CANNOT_ASSIGN_OPTIMIZE, message, disable_reason);
            return false;
        }
    }

    return true;
}

ActionLock StorageMergeTree::stopMergesAndWait()
{
    /// Asks to complete merges and does not allow them to start.
    /// This protects against "revival" of data for a removed partition after completion of merge.
    auto merge_blocker = merger_mutator.merges_blocker.cancel();

    {
        std::unique_lock lock(currently_processing_in_background_mutex);
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
    }

    return merge_blocker;
}


void StorageMergeTree::dropPartition(const ASTPtr & partition, bool detach, bool drop_part, const Context & context, bool throw_if_noop)
{
    {
        /// Asks to complete merges and does not allow them to start.
        /// This protects against "revival" of data for a removed partition after completion of merge.
        auto merge_blocker = stopMergesAndWait();

        auto metadata_snapshot = getInMemoryMetadataPtr();

        MergeTreeData::DataPartsVector parts_to_remove;

        if (drop_part)
        {
            String part_name = partition->as<ASTLiteral &>().value.safeGet<String>();
            auto part = getPartIfExists(part_name, {MergeTreeDataPartState::Committed});

            if (part)
                parts_to_remove.push_back(part);
            else if (throw_if_noop)
                throw Exception("Part " + part_name + " not found, won't try to drop it.", ErrorCodes::NO_SUCH_DATA_PART);
            else
                return;
        }
        else
        {
            String partition_id = getPartitionIDFromQuery(partition, context);
            parts_to_remove = getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);
        }

        // TODO should we throw an exception if parts_to_remove is empty?
        removePartsFromWorkingSet(parts_to_remove, true);

        if (detach)
        {
            /// If DETACH clone parts to detached/ directory
            for (const auto & part : parts_to_remove)
            {
                LOG_INFO(log, "Detaching {}", part->relative_path);
                part->makeCloneInDetached("", metadata_snapshot);
            }
        }

        if (detach)
            LOG_INFO(log, "Detached {} parts.", parts_to_remove.size());
        else
            LOG_INFO(log, "Removed {} parts.", parts_to_remove.size());
    }

    clearOldPartsFromFilesystem();
}


PartitionCommandsResultInfo StorageMergeTree::attachPartition(
    const ASTPtr & partition, const StorageMetadataPtr & /* metadata_snapshot */,
    bool attach_part, const Context & context)
{
    PartitionCommandsResultInfo results;
    PartsTemporaryRename renamed_parts(*this, "detached/");
    MutableDataPartsVector loaded_parts = tryLoadPartsToAttach(partition, attach_part, context, renamed_parts);

    for (size_t i = 0; i < loaded_parts.size(); ++i)
    {
        LOG_INFO(log, "Attaching part {} from {}", loaded_parts[i]->name, renamed_parts.old_and_new_names[i].second);
        String old_name = renamed_parts.old_and_new_names[i].first;
        renameTempPartAndAdd(loaded_parts[i], &increment);
        renamed_parts.old_and_new_names[i].first.clear();

        results.push_back(PartitionCommandResultInfo{
            .partition_id = loaded_parts[i]->info.partition_id,
            .part_name = loaded_parts[i]->name,
            .old_part_name = old_name,
        });

        LOG_INFO(log, "Finished attaching part");
    }

    /// New parts with other data may appear in place of deleted parts.
    context.dropCaches();
    return results;
}

void StorageMergeTree::replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, const Context & context)
{
    auto lock1 = lockForShare(context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);
    auto lock2 = source_table->lockForShare(context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);
    auto source_metadata_snapshot = source_table->getInMemoryMetadataPtr();
    auto my_metadata_snapshot = getInMemoryMetadataPtr();

    Stopwatch watch;
    MergeTreeData & src_data = checkStructureAndGetMergeTreeData(source_table, source_metadata_snapshot, my_metadata_snapshot);
    String partition_id = getPartitionIDFromQuery(partition, context);

    DataPartsVector src_parts = src_data.getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);
    MutableDataPartsVector dst_parts;

    static const String TMP_PREFIX = "tmp_replace_from_";

    for (const DataPartPtr & src_part : src_parts)
    {
        if (!canReplacePartition(src_part))
            throw Exception(
                "Cannot replace partition '" + partition_id + "' because part '" + src_part->name + "' has inconsistent granularity with table",
                ErrorCodes::BAD_ARGUMENTS);

        /// This will generate unique name in scope of current server process.
        Int64 temp_index = insert_increment.get();
        MergeTreePartInfo dst_part_info(partition_id, temp_index, temp_index, src_part->info.level);

        dst_parts.emplace_back(cloneAndLoadDataPartOnSameDisk(src_part, TMP_PREFIX, dst_part_info, my_metadata_snapshot));
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
            Transaction transaction(*this);

            auto data_parts_lock = lockParts();

            /// Populate transaction
            for (MutableDataPartPtr & part : dst_parts)
                renameTempPartAndReplace(part, &increment, &transaction, data_parts_lock);

            transaction.commit(&data_parts_lock);

            /// If it is REPLACE (not ATTACH), remove all parts which max_block_number less then min_block_number of the first new block
            if (replace)
                removePartsInRangeFromWorkingSet(drop_range, true, false, data_parts_lock);
        }

        PartLog::addNewParts(global_context, dst_parts, watch.elapsed());
    }
    catch (...)
    {
        PartLog::addNewParts(global_context, dst_parts, watch.elapsed(), ExecutionStatus::fromCurrentException());
        throw;
    }
}

void StorageMergeTree::movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, const Context & context)
{
    auto lock1 = lockForShare(context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);
    auto lock2 = dest_table->lockForShare(context.getCurrentQueryId(), context.getSettingsRef().lock_acquire_timeout);

    auto dest_table_storage = std::dynamic_pointer_cast<StorageMergeTree>(dest_table);
    if (!dest_table_storage)
        throw Exception("Table " + getStorageID().getNameForLogs() + " supports movePartitionToTable only for MergeTree family of table engines."
                        " Got " + dest_table->getName(), ErrorCodes::NOT_IMPLEMENTED);
    if (dest_table_storage->getStoragePolicy() != this->getStoragePolicy())
        throw Exception("Destination table " + dest_table_storage->getStorageID().getNameForLogs() +
                       " should have the same storage policy of source table " + getStorageID().getNameForLogs() + ". " +
                       getStorageID().getNameForLogs() + ": " + this->getStoragePolicy()->getName() + ", " +
                       dest_table_storage->getStorageID().getNameForLogs() + ": " + dest_table_storage->getStoragePolicy()->getName(), ErrorCodes::UNKNOWN_POLICY);

    auto dest_metadata_snapshot = dest_table->getInMemoryMetadataPtr();
    auto metadata_snapshot = getInMemoryMetadataPtr();
    Stopwatch watch;

    MergeTreeData & src_data = dest_table_storage->checkStructureAndGetMergeTreeData(*this, metadata_snapshot, dest_metadata_snapshot);
    String partition_id = getPartitionIDFromQuery(partition, context);

    DataPartsVector src_parts = src_data.getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);
    MutableDataPartsVector dst_parts;

    static const String TMP_PREFIX = "tmp_move_from_";

    for (const DataPartPtr & src_part : src_parts)
    {
        if (!dest_table_storage->canReplacePartition(src_part))
            throw Exception(
                "Cannot move partition '" + partition_id + "' because part '" + src_part->name + "' has inconsistent granularity with table",
                ErrorCodes::LOGICAL_ERROR);

        /// This will generate unique name in scope of current server process.
        Int64 temp_index = insert_increment.get();
        MergeTreePartInfo dst_part_info(partition_id, temp_index, temp_index, src_part->info.level);

        dst_parts.emplace_back(dest_table_storage->cloneAndLoadDataPartOnSameDisk(src_part, TMP_PREFIX, dst_part_info, dest_metadata_snapshot));
    }

    /// empty part set
    if (dst_parts.empty())
        return;

    /// Move new parts to the destination table. NOTE It doesn't look atomic.
    try
    {
        {
            Transaction transaction(*dest_table_storage);

            auto src_data_parts_lock = lockParts();
            auto dest_data_parts_lock = dest_table_storage->lockParts();

            std::mutex mutex;
            DataPartsLock lock(mutex);

            for (MutableDataPartPtr & part : dst_parts)
                dest_table_storage->renameTempPartAndReplace(part, &increment, &transaction, lock);

            removePartsFromWorkingSet(src_parts, true, lock);
            transaction.commit(&lock);
        }

        clearOldMutations(true);
        clearOldPartsFromFilesystem();

        PartLog::addNewParts(global_context, dst_parts, watch.elapsed());
    }
    catch (...)
    {
        PartLog::addNewParts(global_context, dst_parts, watch.elapsed(), ExecutionStatus::fromCurrentException());
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
        background_executor.triggerTask();
    else if (action_type == ActionLocks::PartsMove)
        background_moves_executor.triggerTask();
}

CheckResults StorageMergeTree::checkData(const ASTPtr & query, const Context & context)
{
    CheckResults results;
    DataPartsVector data_parts;
    if (const auto & check_query = query->as<ASTCheckQuery &>(); check_query.partition)
    {
        String partition_id = getPartitionIDFromQuery(check_query.partition, context);
        data_parts = getDataPartsVectorInPartition(MergeTreeDataPartState::Committed, partition_id);
    }
    else
        data_parts = getDataPartsVector();

    for (auto & part : data_parts)
    {
        auto disk = part->volume->getDisk();
        String part_path = part->getFullRelativePath();
        /// If the checksums file is not present, calculate the checksums and write them to disk.
        String checksums_path = part_path + "checksums.txt";
        String tmp_checksums_path = part_path + "checksums.txt.tmp";
        if (part->isStoredOnDisk() && !disk->exists(checksums_path))
        {
            try
            {
                auto calculated_checksums = checkDataPart(part, false);
                calculated_checksums.checkEqual(part->checksums, true);
                auto out = disk->writeFile(tmp_checksums_path, 4096);
                part->checksums.write(*out);
                disk->moveFile(tmp_checksums_path, checksums_path);
                results.emplace_back(part->name, true, "Checksums recounted and written to disk.");
            }
            catch (const Exception & ex)
            {
                if (disk->exists(tmp_checksums_path))
                    disk->remove(tmp_checksums_path);

                results.emplace_back(part->name, false,
                    "Check of part finished with error: '" + ex.message() + "'");
            }
        }
        else
        {
            try
            {
                checkDataPart(part, true);
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


MutationCommands StorageMergeTree::getFirstAlterMutationCommandsForPart(const DataPartPtr & part) const
{
    std::lock_guard lock(currently_processing_in_background_mutex);

    auto it = current_mutations_by_version.upper_bound(part->info.getDataVersion());
    if (it == current_mutations_by_version.end())
        return {};
    return it->second.commands;
}

void StorageMergeTree::startBackgroundMovesIfNeeded()
{
    if (areBackgroundMovesNeeded())
        background_moves_executor.start();
}

}
