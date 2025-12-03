#include <Storages/MergeTree/ExportPartTask.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Core/Settings.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/Exception.h>
#include <Common/ProfileEventsScope.h>
#include <Storages/MergeTree/ExportList.h>
#include <Common/setThreadName.h>

namespace ProfileEvents
{
    extern const Event PartsExportDuplicated;
    extern const Event PartsExportFailures;
    extern const Event PartsExports;
    extern const Event PartsExportTotalMilliseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int FILE_ALREADY_EXISTS;
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}

namespace Setting
{
    extern const SettingsUInt64 min_bytes_to_use_direct_io;
}

ExportPartTask::ExportPartTask(MergeTreeData & storage_, const MergeTreePartExportManifest & manifest_, ContextPtr context_)
    : storage(storage_),
    manifest(manifest_),
    local_context(context_)
{
}

bool ExportPartTask::executeStep()
{
    const auto & metadata_snapshot = manifest.metadata_snapshot;

    Names columns_to_read = metadata_snapshot->getColumns().getNamesOfPhysical();

    MergeTreeSequentialSourceType read_type = MergeTreeSequentialSourceType::Export;

    Block block_with_partition_values;
    if (metadata_snapshot->hasPartitionKey())
    {
        /// todo arthur do I need to init minmax_idx?
        block_with_partition_values = manifest.data_part->minmax_idx->getBlock(storage);
    }

    auto destination_storage = DatabaseCatalog::instance().tryGetTable(manifest.destination_storage_id, local_context);
    if (!destination_storage)
    {
        std::lock_guard inner_lock(storage.export_manifests_mutex);

        const auto destination_storage_id_name = manifest.destination_storage_id.getNameForLogs();
        storage.export_manifests.erase(manifest);
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Failed to reconstruct destination storage: {}", destination_storage_id_name);
    }

    auto exports_list_entry = storage.getContext()->getExportsList().insert(
        getStorageID(),
        manifest.destination_storage_id,
        manifest.data_part->getBytesOnDisk(),
        manifest.data_part->name,
        "not_computed_yet",
        manifest.data_part->rows_count,
        manifest.data_part->getBytesOnDisk(),
        manifest.data_part->getBytesUncompressedOnDisk(),
        manifest.create_time,
        local_context);

    SinkToStoragePtr sink;

    try
    {
        sink = destination_storage->import(
            manifest.data_part->name + "_" + manifest.data_part->checksums.getTotalChecksumHex(),
            block_with_partition_values,
            (*exports_list_entry)->destination_file_path,
            manifest.file_already_exists_policy == MergeTreePartExportManifest::FileAlreadyExistsPolicy::overwrite,
            manifest.format_settings,
            local_context);
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::FILE_ALREADY_EXISTS)
        {
            ProfileEvents::increment(ProfileEvents::PartsExportDuplicated);

            /// File already exists and the policy is NO_OP, treat it as success.
            if (manifest.file_already_exists_policy == MergeTreePartExportManifest::FileAlreadyExistsPolicy::skip)
            {
                storage.writePartLog(
                    PartLogElement::Type::EXPORT_PART,
                    {},
                    static_cast<UInt64>((*exports_list_entry)->elapsed * 1000000000),
                    manifest.data_part->name,
                    manifest.data_part,
                    {manifest.data_part},
                    nullptr,
                    nullptr,
                    exports_list_entry.get());

                std::lock_guard inner_lock(storage.export_manifests_mutex);
                storage.export_manifests.erase(manifest);

                ProfileEvents::increment(ProfileEvents::PartsExports);
                ProfileEvents::increment(ProfileEvents::PartsExportTotalMilliseconds, static_cast<UInt64>((*exports_list_entry)->elapsed * 1000));
                return false;
            }
        }

        tryLogCurrentException(__PRETTY_FUNCTION__);

        ProfileEvents::increment(ProfileEvents::PartsExportFailures);

        std::lock_guard inner_lock(storage.export_manifests_mutex);
        storage.export_manifests.erase(manifest);
        return false;
    }

    bool apply_deleted_mask = true;
    bool read_with_direct_io = local_context->getSettingsRef()[Setting::min_bytes_to_use_direct_io] > manifest.data_part->getBytesOnDisk();
    bool prefetch = false;

    MergeTreeData::IMutationsSnapshot::Params mutations_snapshot_params
    {
        .metadata_version = metadata_snapshot->getMetadataVersion(),
        .min_part_metadata_version = manifest.data_part->getMetadataVersion()
    };

    auto mutations_snapshot = storage.getMutationsSnapshot(mutations_snapshot_params);
    auto alter_conversions = MergeTreeData::getAlterConversionsForPart(
        manifest.data_part,
        mutations_snapshot,
        local_context);

    QueryPlan plan_for_part;

    createReadFromPartStep(
        read_type,
        plan_for_part,
        storage,
        storage.getStorageSnapshot(metadata_snapshot, local_context),
        RangesInDataPart(manifest.data_part),
        alter_conversions,
        nullptr,
        columns_to_read,
        nullptr,
        apply_deleted_mask,
        std::nullopt,
        read_with_direct_io,
        prefetch,
        local_context,
        getLogger("ExportPartition"));


    ThreadGroupSwitcher switcher((*exports_list_entry)->thread_group, ThreadName::EXPORT_PART);

    QueryPlanOptimizationSettings optimization_settings(local_context);
    auto pipeline_settings = BuildQueryPipelineSettings(local_context);
    auto builder = plan_for_part.buildQueryPipeline(optimization_settings, pipeline_settings);

    builder->setProgressCallback([&exports_list_entry](const Progress & progress)
    {
        (*exports_list_entry)->bytes_read_uncompressed += progress.read_bytes;
        (*exports_list_entry)->rows_read += progress.read_rows;
        (*exports_list_entry)->elapsed = (*exports_list_entry)->watch.elapsedSeconds();
    });

    pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));

    pipeline.complete(sink);

    try
    {
        CompletedPipelineExecutor exec(pipeline);

        auto is_cancelled_callback = [this]()
        {
            return isCancelled();
        };

        exec.setCancelCallback(is_cancelled_callback, 100);

        exec.execute();

        if (isCancelled())
        {
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Export part was cancelled");
        }

        std::lock_guard inner_lock(storage.export_manifests_mutex);
        storage.writePartLog(
            PartLogElement::Type::EXPORT_PART,
            {},
            static_cast<UInt64>((*exports_list_entry)->elapsed * 1000000000),
            manifest.data_part->name,
            manifest.data_part,
            {manifest.data_part},
            nullptr,
            nullptr,
            exports_list_entry.get());

        storage.export_manifests.erase(manifest);

        ProfileEvents::increment(ProfileEvents::PartsExports);
        ProfileEvents::increment(ProfileEvents::PartsExportTotalMilliseconds, static_cast<UInt64>((*exports_list_entry)->elapsed * 1000));
    }
    catch (const Exception &)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, fmt::format("while exporting the part {}. User should retry.", manifest.data_part->name));

        ProfileEvents::increment(ProfileEvents::PartsExportFailures);

        std::lock_guard inner_lock(storage.export_manifests_mutex);
        storage.writePartLog(
            PartLogElement::Type::EXPORT_PART,
            ExecutionStatus::fromCurrentException("", true),
            static_cast<UInt64>((*exports_list_entry)->elapsed * 1000000000),
            manifest.data_part->name,
            manifest.data_part,
            {manifest.data_part},
            nullptr,
            nullptr,
            exports_list_entry.get());

        storage.export_manifests.erase(manifest);

        throw;
    }
    return false;
}

void ExportPartTask::cancel() noexcept
{
    cancel_requested.store(true);
    pipeline.cancel();
}

bool ExportPartTask::isCancelled() const
{
    return cancel_requested.load() || storage.parts_mover.moves_blocker.isCancelled();
}

void ExportPartTask::onCompleted()
{
}

StorageID ExportPartTask::getStorageID() const
{
    return storage.getStorageID();
}

Priority ExportPartTask::getPriority() const
{
    return Priority{};
}

String ExportPartTask::getQueryId() const
{
    return manifest.transaction_id;
}

}
