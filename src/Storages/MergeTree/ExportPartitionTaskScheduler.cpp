#include <Storages/MergeTree/ExportPartitionTaskScheduler.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ProfileEvents.h>
#include "Storages/MergeTree/ExportPartitionUtils.h"
#include "Storages/MergeTree/MergeTreePartExportManifest.h"
#include "Storages/MergeTree/ExportPartFromPartitionExportTask.h"
#include "Formats/FormatFactory.h"
#include <Core/Settings.h>

namespace ProfileEvents
{
    extern const Event ExportPartitionZooKeeperRequests;
    extern const Event ExportPartitionZooKeeperGet;
    extern const Event ExportPartitionZooKeeperGetChildren;
    extern const Event ExportPartitionZooKeeperCreate;
    extern const Event ExportPartitionZooKeeperSet;
    extern const Event ExportPartitionZooKeeperRemove;
    extern const Event ExportPartitionZooKeeperMulti;
    extern const Event ExportPartitionZooKeeperExists;
}


namespace DB
{

namespace Setting
{
    extern const SettingsMergeTreePartExportFileAlreadyExistsPolicy export_merge_tree_part_file_already_exists_policy;
}

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
    extern const int LOGICAL_ERROR;
}

namespace
{
    ContextPtr getContextCopyWithTaskSettings(const ContextPtr & context, const ExportReplicatedMergeTreePartitionManifest & manifest)
    {
        auto context_copy = Context::createCopy(context);
        context_copy->makeQueryContextForExportPart();
        context_copy->setCurrentQueryId(manifest.query_id);
        context_copy->setSetting("output_format_parallel_formatting", manifest.parallel_formatting);
        context_copy->setSetting("output_format_parquet_parallel_encoding", manifest.parquet_parallel_encoding);
        context_copy->setSetting("max_threads", manifest.max_threads);
        context_copy->setSetting("export_merge_tree_part_file_already_exists_policy", String(magic_enum::enum_name(manifest.file_already_exists_policy)));
        context_copy->setSetting("export_merge_tree_part_max_bytes_per_file", manifest.max_bytes_per_file);
        context_copy->setSetting("export_merge_tree_part_max_rows_per_file", manifest.max_rows_per_file);

        /// always skip pending mutations and patch parts because we already validated the parts during query processing
        context_copy->setSetting("export_merge_tree_part_throw_on_pending_mutations", false);
        context_copy->setSetting("export_merge_tree_part_throw_on_pending_patch_parts", false);

        return context_copy;
    }
}

ExportPartitionTaskScheduler::ExportPartitionTaskScheduler(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
{
}

void ExportPartitionTaskScheduler::run()
{
    const auto available_move_executors = storage.background_moves_assignee.getAvailableMoveExecutors();

    /// this is subject to TOCTOU - but for now we choose to live with it.
    if (available_move_executors == 0)
    {
        LOG_INFO(storage.log, "ExportPartition scheduler task: No available move executors, skipping");
        return;
    }

    LOG_INFO(storage.log, "ExportPartition scheduler task: Available move executors: {}", available_move_executors);

    std::size_t scheduled_exports_count = 0;

    const uint32_t seed = uint32_t(std::hash<std::string>{}(storage.replica_name)) ^ uint32_t(scheduled_exports_count);
    pcg64_fast rng(seed);

    std::lock_guard lock(storage.export_merge_tree_partition_mutex);

    auto zk = storage.getZooKeeper();

    // Iterate sorted by create_time
    for (auto & entry : storage.export_merge_tree_partition_task_entries_by_create_time)
    {
        if (scheduled_exports_count >= available_move_executors)
        {
            LOG_INFO(storage.log, "ExportPartition scheduler task: Scheduled exports count is greater than available move executors, skipping");
            break;
        }

        const auto & manifest = entry.manifest;
        const auto key = entry.getCompositeKey();
        const auto database = storage.getContext()->resolveDatabase(manifest.destination_database);
        const auto & table = manifest.destination_table;

        /// No need to query zk for status if the local one is not PENDING
        if (entry.status != ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING)
        {
            LOG_INFO(storage.log, "ExportPartition scheduler task: Skipping... Local status is {}", magic_enum::enum_name(entry.status).data());
            continue;
        }

        const auto destination_storage_id = StorageID(QualifiedTableName {database, table});

        const auto destination_storage = DatabaseCatalog::instance().tryGetTable(destination_storage_id, storage.getContext());

        if (!destination_storage)
        {
            LOG_INFO(storage.log, "ExportPartition scheduler task: Failed to reconstruct destination storage: {}, skipping", destination_storage_id.getNameForLogs());
            continue;
        }

        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGet);
        std::string status_in_zk_string;
        if (!zk->tryGet(fs::path(storage.zookeeper_path) / "exports" / key / "status", status_in_zk_string))
        {
            LOG_INFO(storage.log, "ExportPartition scheduler task: Failed to get status, skipping");
            continue;
        }

        const auto status_in_zk = magic_enum::enum_cast<ExportReplicatedMergeTreePartitionTaskEntry::Status>(status_in_zk_string);

        if (!status_in_zk)
        {
            LOG_INFO(storage.log, "ExportPartition scheduler task: Failed to get status from zk, skipping");
            continue;
        }

        if (status_in_zk.value() != ExportReplicatedMergeTreePartitionTaskEntry::Status::PENDING)
        {
            entry.status = status_in_zk.value();
            LOG_INFO(storage.log, "ExportPartition scheduler task: Skipping {}... Status from zk is {}", key, magic_enum::enum_name(entry.status).data());
            continue;
        }

        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGetChildren);
        std::vector<std::string> parts_in_processing_or_pending;
        
        if (Coordination::Error::ZOK != zk->tryGetChildren(fs::path(storage.zookeeper_path) / "exports" / key / "processing", parts_in_processing_or_pending))
        {
            LOG_INFO(storage.log, "ExportPartition scheduler task: Failed to get parts in processing or pending, skipping");
            continue;
        }


        if (parts_in_processing_or_pending.empty())
        {
            LOG_INFO(storage.log, "ExportPartition scheduler task: No parts in processing or pending, skipping");
            continue;
        }

        /// shuffle the parts to reduce the risk of lock collisions
        std::shuffle(parts_in_processing_or_pending.begin(), parts_in_processing_or_pending.end(), rng);

        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGetChildren);
        std::vector<std::string> locked_parts;

        if (Coordination::Error::ZOK != zk->tryGetChildren(fs::path(storage.zookeeper_path) / "exports" / key / "locks", locked_parts))
        {
            LOG_INFO(storage.log, "ExportPartition scheduler task: Failed to get locked parts, skipping");
            continue;
        }

        std::unordered_set<std::string> locked_parts_set(locked_parts.begin(), locked_parts.end());

        for (const auto & zk_part_name : parts_in_processing_or_pending)
        {
            if (scheduled_exports_count >= available_move_executors)
            {
                LOG_INFO(storage.log, "ExportPartition scheduler task: Scheduled exports count is greater than available move executors, skipping");
                break;
            }

            if (locked_parts_set.contains(zk_part_name))
            {
                LOG_INFO(storage.log, "ExportPartition scheduler task: Part {} is locked, skipping", zk_part_name);
                continue;
            }

            const auto part = storage.getPartIfExists(zk_part_name, {MergeTreeDataPartState::Active, MergeTreeDataPartState::Outdated});
            if (!part)
            {
                LOG_INFO(storage.log, "ExportPartition scheduler task: Part {} not found locally, skipping", zk_part_name);
                continue;
            }

            LOG_INFO(storage.log, "ExportPartition scheduler task: Scheduling part export: {}", zk_part_name);

            auto context = getContextCopyWithTaskSettings(storage.getContext(), manifest);

            /// todo arthur this code path does not perform all the validations a simple part export does because we are not calling exportPartToTable directly.
            /// the schema and everything else has been validated when the export partition task was created, but nothing prevents the destination table from being
            /// recreated with a new schema before the export task is scheduled.
            if (manifest.lock_inside_the_task)
            {
                LOG_INFO(storage.log, "ExportPartition scheduler task: Locking part export inside the task");
                std::lock_guard part_export_lock(storage.export_manifests_mutex);

                MergeTreePartExportManifest part_export_manifest(
                    destination_storage,
                    part,
                    manifest.transaction_id,
                    manifest.query_id,
                    context->getSettingsRef()[Setting::export_merge_tree_part_file_already_exists_policy].value,
                    context->getSettingsCopy(),
                    storage.getInMemoryMetadataPtr(),
                    [this, key, zk_part_name, manifest, destination_storage]
                    (MergeTreePartExportManifest::CompletionCallbackResult result)
                    {
                        handlePartExportCompletion(key, zk_part_name, manifest, destination_storage, result);
                    });

                part_export_manifest.task = std::make_shared<ExportPartFromPartitionExportTask>(storage, key, part_export_manifest);

                /// todo arthur this might conflict with the standalone export part. what to do in this case?
                if (!storage.export_manifests.emplace(part_export_manifest).second)
                {
                    LOG_INFO(storage.log, "ExportPartition scheduler task: Part {} is already being exported, skipping", zk_part_name);
                    continue;
                }

                if (!storage.background_moves_assignee.scheduleMoveTask(part_export_manifest.task))
                {
                    storage.export_manifests.erase(part_export_manifest);
                    LOG_INFO(storage.log, "ExportPartition scheduler task: Failed to schedule export part task, skipping");
                    return;
                }
            }
            else
            {
                try
                {
                    LOG_INFO(storage.log, "ExportPartition scheduler task: Exporting part to table");

                    LOG_INFO(storage.log, "ExportPartition scheduler task: Attempting to lock part: {}", zk_part_name);

                    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
                    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperCreate);
                    if (Coordination::Error::ZOK != zk->tryCreate(fs::path(storage.zookeeper_path) / "exports" / key / "locks" / zk_part_name, storage.replica_name, zkutil::CreateMode::Ephemeral))
                    {
                        LOG_INFO(storage.log, "ExportPartition scheduler task: Failed to lock part {}, skipping", zk_part_name);
                        continue;
                    }

                    LOG_INFO(storage.log, "ExportPartition scheduler task: Locked part: {}", zk_part_name);

                    storage.exportPartToTable(
                        part->name,
                        destination_storage_id,
                        manifest.transaction_id,
                        getContextCopyWithTaskSettings(storage.getContext(), manifest),
                        /*allow_outdated_parts*/ true,
                        [this, key, zk_part_name, manifest, destination_storage]
                        (MergeTreePartExportManifest::CompletionCallbackResult result)
                        {
                            handlePartExportCompletion(key, zk_part_name, manifest, destination_storage, result);
                        });
                }
                catch (const Exception &)
                {
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
                    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRemove);
                    zk->tryRemove(fs::path(storage.zookeeper_path) / "exports" / key / "locks" / zk_part_name);
                    /// we should not increment retry_count because the node might just be full
                }
            }

            scheduled_exports_count++;
        }
    }
}

void ExportPartitionTaskScheduler::handlePartExportCompletion(
    const std::string & export_key,
    const std::string & part_name,
    const ExportReplicatedMergeTreePartitionManifest & manifest,
    const StoragePtr & destination_storage,
    const MergeTreePartExportManifest::CompletionCallbackResult & result)
{
    const auto export_path = fs::path(storage.zookeeper_path) / "exports" / export_key;
    const auto processing_parts_path = export_path / "processing";
    const auto processed_part_path = export_path / "processed" / part_name;
    const auto zk = storage.getZooKeeper();

    if (result.success)
    {
        handlePartExportSuccess(manifest, destination_storage, processing_parts_path, processed_part_path, part_name, export_path, zk, result.relative_paths_in_destination_storage);
    }
    else
    {
        handlePartExportFailure(processing_parts_path, part_name, export_path, zk, result.exception, manifest.max_retries);
    }
}

void ExportPartitionTaskScheduler::handlePartExportSuccess(
    const ExportReplicatedMergeTreePartitionManifest & manifest,
    const StoragePtr & destination_storage,
    const std::filesystem::path & processing_parts_path,
    const std::filesystem::path & processed_part_path,
    const std::string & part_name,
    const std::filesystem::path & export_path,
    const zkutil::ZooKeeperPtr & zk,
    const std::vector<String> & relative_paths_in_destination_storage
)
{
    LOG_INFO(storage.log, "ExportPartition scheduler task: Part {} exported successfully, paths size: {}", part_name, relative_paths_in_destination_storage.size());

    for (const auto & relative_path_in_destination_storage : relative_paths_in_destination_storage)
    {
        LOG_INFO(storage.log, "ExportPartition scheduler task: {}", relative_path_in_destination_storage);
    }

    if (!tryToMovePartToProcessed(export_path, processing_parts_path, processed_part_path, part_name, relative_paths_in_destination_storage, zk))
    {
        LOG_INFO(storage.log, "ExportPartition scheduler task: Failed to move part to processed, will not commit export partition");
        return;
    }

    LOG_INFO(storage.log, "ExportPartition scheduler task: Marked part export {} as completed", part_name);

    if (!areAllPartsProcessed(export_path, zk))
    {
        return;
    }

    LOG_INFO(storage.log, "ExportPartition scheduler task: All parts are processed, will try to commit export partition");

    ExportPartitionUtils::commit(manifest, destination_storage, zk, storage.log.load(), export_path, storage.getContext());
}

void ExportPartitionTaskScheduler::handlePartExportFailure(
    const std::filesystem::path & processing_parts_path,
    const std::string & part_name,
    const std::filesystem::path & export_path,
    const zkutil::ZooKeeperPtr & zk,
    const std::optional<Exception> & exception,
    size_t max_retries
)
{
    LOG_INFO(storage.log, "ExportPartition scheduler task: Part {} export failed, will now increment counters", part_name);

    if (!exception)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ExportPartition scheduler task: No exception provided for error handling. Sounds like a bug");
    }

    /// Early exit if the query was cancelled - no need to increment error counts
    if (exception->code() == ErrorCodes::QUERY_WAS_CANCELLED)
    {
        LOG_INFO(storage.log, "ExportPartition scheduler task: Part {} export was cancelled, skipping error handling", part_name);
        return;
    }

    Coordination::Stat locked_by_stat;
    std::string locked_by;

    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGet);
    if (!zk->tryGet(export_path / "locks" / part_name, locked_by, &locked_by_stat))
    {
        LOG_INFO(storage.log, "ExportPartition scheduler task: Part {} is not locked by any replica, will not increment error counts", part_name);
        return;
    }

    if (locked_by != storage.replica_name)
    {
        LOG_INFO(storage.log, "ExportPartition scheduler task: Part {} is locked by another replica, will not increment error counts", part_name);
        return;
    }

    Coordination::Requests ops;

    const auto processing_part_path = processing_parts_path / part_name;

    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGet);
    std::string processing_part_string;

    if (!zk->tryGet(processing_part_path, processing_part_string))
    {
        LOG_INFO(storage.log, "ExportPartition scheduler task: Failed to get processing part, will not increment error counts");
        return;
    }

    /// todo arthur could this have been cached?
    auto processing_part_entry = ExportReplicatedMergeTreePartitionProcessingPartEntry::fromJsonString(processing_part_string);

    processing_part_entry.retry_count++;

    ops.emplace_back(zkutil::makeRemoveRequest(export_path / "locks" / part_name, locked_by_stat.version));
    ops.emplace_back(zkutil::makeSetRequest(processing_part_path, processing_part_entry.toJsonString(), -1));

    LOG_INFO(storage.log, "ExportPartition scheduler task: Updating processing part entry for part {}, retry count: {}, max retries: {}", part_name, processing_part_entry.retry_count, max_retries);

    if (processing_part_entry.retry_count >= max_retries)
    {
        /// just set status in processing_part_path and finished_by
        processing_part_entry.status = ExportReplicatedMergeTreePartitionProcessingPartEntry::Status::FAILED;
        processing_part_entry.finished_by = storage.replica_name;

        ops.emplace_back(zkutil::makeSetRequest(export_path / "status", String(magic_enum::enum_name(ExportReplicatedMergeTreePartitionTaskEntry::Status::FAILED)).data(), -1));
        LOG_INFO(storage.log, "ExportPartition scheduler task: Retry count limit exceeded for part {}, will try to fail the entire task", part_name);
    }
    else
    {
        LOG_INFO(storage.log, "ExportPartition scheduler task: Retry count limit not exceeded for part {}, will increment retry count", part_name);
    }

    const auto exceptions_per_replica_path = export_path / "exceptions_per_replica" / storage.replica_name;
    const auto count_path = exceptions_per_replica_path / "count";
    const auto last_exception_path = exceptions_per_replica_path / "last_exception";

    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperExists);
    if (zk->exists(exceptions_per_replica_path))
    {
        LOG_INFO(storage.log, "ExportPartition scheduler task: Exceptions per replica path exists, no need to create it");
        std::string num_exceptions_string;
        if (zk->tryGet(count_path, num_exceptions_string))
        {
            const auto num_exceptions = parse<size_t>(num_exceptions_string) + 1;
            ops.emplace_back(zkutil::makeSetRequest(count_path, std::to_string(num_exceptions), -1));
        }
        else
        {
            /// TODO maybe we should find a better way to handle this case, not urgent
            LOG_INFO(storage.log, "ExportPartition scheduler task: Failed to get number of exceptions, will not increment it");
        }

        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
        ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGet);

        ops.emplace_back(zkutil::makeSetRequest(last_exception_path / "part", part_name, -1));
        ops.emplace_back(zkutil::makeSetRequest(last_exception_path / "exception", exception->message(), -1));
    }
    else
    {
        LOG_INFO(storage.log, "ExportPartition scheduler task: Exceptions per replica path does not exist, will create it");
        ops.emplace_back(zkutil::makeCreateRequest(exceptions_per_replica_path, "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(count_path, "1", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(last_exception_path, "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(last_exception_path / "part", part_name, zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(last_exception_path / "exception", exception->message(), zkutil::CreateMode::Persistent));
    }

    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperMulti);
    Coordination::Responses responses;
    if (Coordination::Error::ZOK != zk->tryMulti(ops, responses))
    {
        LOG_INFO(storage.log, "ExportPartition scheduler task: All failure mechanism failed, will not try to update it");
        return;
    }

    LOG_INFO(storage.log, "ExportPartition scheduler task: Successfully updated exception counters for part {}", part_name);
}

bool ExportPartitionTaskScheduler::tryToMovePartToProcessed(
    const std::filesystem::path & export_path,
    const std::filesystem::path & processing_parts_path,
    const std::filesystem::path & processed_part_path,
    const std::string & part_name,
    const std::vector<String> & relative_paths_in_destination_storage,
    const zkutil::ZooKeeperPtr & zk
)
{
    Coordination::Stat locked_by_stat;
    std::string locked_by;

    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGet);
    if (!zk->tryGet(export_path / "locks" / part_name, locked_by, &locked_by_stat))
    {
        LOG_INFO(storage.log, "ExportPartition scheduler task: Part {} is not locked by any replica, will not commit or set it as completed", part_name);
        return false;
    }

    /// Is this a good idea? what if the file we just pushed to s3 ends up triggering an exception in the replica that actually locks the part and it does not commit?
    /// I guess we should not throw if file already exists for export partition, hard coded.
    if (locked_by != storage.replica_name)
    {
        LOG_INFO(storage.log, "ExportPartition scheduler task: Part {} is locked by another replica, will not commit or set it as completed", part_name);
        return false;
    }

    Coordination::Requests requests;

    ExportReplicatedMergeTreePartitionProcessedPartEntry processed_part_entry;
    processed_part_entry.part_name = part_name;
    processed_part_entry.paths_in_destination = relative_paths_in_destination_storage;
    processed_part_entry.finished_by = storage.replica_name;

    requests.emplace_back(zkutil::makeRemoveRequest(processing_parts_path / part_name, -1));
    requests.emplace_back(zkutil::makeCreateRequest(processed_part_path, processed_part_entry.toJsonString(), zkutil::CreateMode::Persistent));
    requests.emplace_back(zkutil::makeRemoveRequest(export_path / "locks" / part_name, locked_by_stat.version));

    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperMulti);
    Coordination::Responses responses;
    if (Coordination::Error::ZOK != zk->tryMulti(requests, responses))
    {

        /// todo  arthur remember what to do here
        LOG_INFO(storage.log, "ExportPartition scheduler task: Failed to update export path, skipping");
        return false;
    }

    return true;
}

bool ExportPartitionTaskScheduler::areAllPartsProcessed(
    const std::filesystem::path & export_path,
    const zkutil::ZooKeeperPtr & zk)
{
    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperRequests);
    ProfileEvents::increment(ProfileEvents::ExportPartitionZooKeeperGetChildren);
    Strings parts_in_processing_or_pending;
    if (Coordination::Error::ZOK != zk->tryGetChildren(export_path / "processing", parts_in_processing_or_pending))
    {
        LOG_INFO(storage.log, "ExportPartition scheduler task: Failed to get parts in processing or pending, will not try to commit export partition");
        return false;
    }

    if (!parts_in_processing_or_pending.empty())
    {
        LOG_INFO(storage.log, "ExportPartition scheduler task: There are still parts in processing or pending, will not try to commit export partition");
        return false;
    }

    return true;
}

}
