#include <mutex>
#include <Storages/MergeTree/ExportPartTask.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Core/Settings.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include "Common/setThreadName.h"
#include <Common/Exception.h>
#include <Common/ProfileEventsScope.h>
#include <Storages/MergeTree/ExportList.h>
#include <Formats/FormatFactory.h>
#include <Databases/enableAllExperimentalSettings.h>
#include <Processors/Sinks/SinkToStorage.h>

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
    extern const SettingsUInt64 export_merge_tree_part_max_bytes_per_file;
    extern const SettingsUInt64 export_merge_tree_part_max_rows_per_file;
    extern const SettingsBool allow_experimental_analyzer;
}

namespace
{
    void materializeSpecialColumns(
        const SharedHeader & header,
        const StorageMetadataPtr & storage_metadata,
        const ContextPtr & local_context,
        QueryPlan & plan_for_part
    )
    {
        const auto readable_columns = storage_metadata->getColumns().getReadable();

        // Enable all experimental settings for default expressions
        // (same pattern as in IMergeTreeReader::evaluateMissingDefaults)
        auto context_for_defaults = Context::createCopy(local_context);
        enableAllExperimentalSettings(context_for_defaults);

        /// Copy the behavior of `IMergeTreeReader`, see https://github.com/ClickHouse/ClickHouse/blob/c45224e3f0a6dd9a9217e5d75723f378ffe0a86a/src/Storages/MergeTree/IMergeTreeReader.cpp#L215
        context_for_defaults->setSetting("enable_analyzer", local_context->getSettingsRef()[Setting::allow_experimental_analyzer].value);

        auto defaults_dag = evaluateMissingDefaults(
            *header,
            readable_columns,
            storage_metadata->getColumns(),
            context_for_defaults);

        if (defaults_dag)
        {
            ActionsDAG base_dag(header->getColumnsWithTypeAndName());

            /// `evaluateMissingDefaults` has a new analyzer path since https://github.com/ClickHouse/ClickHouse/pull/87585
            /// which returns a DAG that does not contain all columns. We need to merge it with the base DAG to get all columns.
            auto merged = ActionsDAG::merge(std::move(base_dag), std::move(*defaults_dag));

            /// Ensure columns are in the correct order matching readable_columns
            merged.removeUnusedActions(readable_columns.getNames(), false);
            merged.addMaterializingOutputActions(/*materialize_sparse=*/ false);
            
            auto expression_step = std::make_unique<ExpressionStep>(
                header,
                std::move(merged));
            expression_step->setStepDescription("Compute alias and default expressions for export");
            plan_for_part.addStep(std::move(expression_step));
        }
    }
}

ExportPartTask::ExportPartTask(MergeTreeData & storage_, const MergeTreePartExportManifest & manifest_)
    : storage(storage_),
    manifest(manifest_)
{
}

bool ExportPartTask::executeStep()
{
    auto local_context = Context::createCopy(storage.getContext());
    local_context->makeQueryContextForExportPart();
    local_context->setCurrentQueryId(manifest.query_id);
    local_context->setSettings(manifest.settings);

    const auto & metadata_snapshot = manifest.metadata_snapshot;

    /// Read only physical columns from the part
    const auto columns_to_read = metadata_snapshot->getColumns().getNamesOfPhysical();

    MergeTreeSequentialSourceType read_type = MergeTreeSequentialSourceType::Export;

    Block block_with_partition_values;
    if (metadata_snapshot->hasPartitionKey())
    {
        /// todo arthur do I need to init minmax_idx?
        block_with_partition_values = manifest.data_part->minmax_idx->getBlock(storage);
    }

    const auto & destination_storage = manifest.destination_storage_ptr;
    const auto destination_storage_id = destination_storage->getStorageID();

    auto exports_list_entry = storage.getContext()->getExportsList().insert(
        getStorageID(),
        destination_storage_id,
        manifest.data_part->getBytesOnDisk(),
        manifest.data_part->name,
        std::vector<std::string>{},
        manifest.data_part->rows_count,
        manifest.data_part->getBytesOnDisk(),
        manifest.data_part->getBytesUncompressedOnDisk(),
        manifest.create_time,
        manifest.query_id,
        local_context);

    SinkToStoragePtr sink;

    const auto new_file_path_callback = [&exports_list_entry](const std::string & file_path)
    {
        std::unique_lock lock((*exports_list_entry)->destination_file_paths_mutex);
        (*exports_list_entry)->destination_file_paths.push_back(file_path);
    };

    try
    {
        sink = destination_storage->import(
            manifest.data_part->name + "_" + manifest.data_part->checksums.getTotalChecksumHex(),
            block_with_partition_values,
            new_file_path_callback,
            manifest.file_already_exists_policy == MergeTreePartExportManifest::FileAlreadyExistsPolicy::overwrite,
            manifest.settings[Setting::export_merge_tree_part_max_bytes_per_file],
            manifest.settings[Setting::export_merge_tree_part_max_rows_per_file],
            getFormatSettings(local_context),
            local_context);

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

        /// We need to support exporting materialized and alias columns to object storage. For some reason, object storage engines don't support them.
        /// This is a hack that materializes the columns before the export so they can be exported to tables that have matching columns
        materializeSpecialColumns(plan_for_part.getCurrentHeader(), metadata_snapshot, local_context, plan_for_part);

        QueryPlanOptimizationSettings optimization_settings(local_context);
        auto pipeline_settings = BuildQueryPipelineSettings(local_context);
        auto builder = plan_for_part.buildQueryPipeline(optimization_settings, pipeline_settings);

        builder->setProgressCallback([&exports_list_entry](const Progress & progress)
        {
            (*exports_list_entry)->bytes_read_uncompressed += progress.read_bytes;
            (*exports_list_entry)->rows_read += progress.read_rows;
        });

        pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));

        pipeline.complete(sink);

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
            (*exports_list_entry)->watch.elapsed(),
            manifest.data_part->name,
            manifest.data_part,
            {manifest.data_part},
            nullptr,
            nullptr,
            {},
            exports_list_entry.get());

        storage.export_manifests.erase(manifest);

        ProfileEvents::increment(ProfileEvents::PartsExports);
        ProfileEvents::increment(ProfileEvents::PartsExportTotalMilliseconds, (*exports_list_entry)->watch.elapsedMilliseconds());

        if (manifest.completion_callback)
            manifest.completion_callback(MergeTreePartExportManifest::CompletionCallbackResult::createSuccess((*exports_list_entry)->destination_file_paths));
    }
    catch (const Exception & e)
    {
        /// If an exception is thrown before the pipeline is started, the sink will not be canceled and might leave buffers open.
        /// Cancel it manually to ensure the buffers are closed.
        if (sink)
        {
            sink->cancel();
        }

        if (e.code() == ErrorCodes::FILE_ALREADY_EXISTS)
        {
            ProfileEvents::increment(ProfileEvents::PartsExportDuplicated);

            /// File already exists and the policy is NO_OP, treat it as success.
            if (manifest.file_already_exists_policy == MergeTreePartExportManifest::FileAlreadyExistsPolicy::skip)
            {
                storage.writePartLog(
                    PartLogElement::Type::EXPORT_PART,
                    {},
                    (*exports_list_entry)->watch.elapsed(),
                    manifest.data_part->name,
                    manifest.data_part,
                    {manifest.data_part},
                    nullptr,
                    nullptr,
                    {},
                    exports_list_entry.get());

                std::lock_guard inner_lock(storage.export_manifests_mutex);
                storage.export_manifests.erase(manifest);

                ProfileEvents::increment(ProfileEvents::PartsExports);
                ProfileEvents::increment(ProfileEvents::PartsExportTotalMilliseconds, (*exports_list_entry)->watch.elapsedMilliseconds());

                if (manifest.completion_callback)
                {
                    manifest.completion_callback(MergeTreePartExportManifest::CompletionCallbackResult::createSuccess((*exports_list_entry)->destination_file_paths));
                }
                    
                return false;
            }
        }

        ProfileEvents::increment(ProfileEvents::PartsExportFailures);

        storage.writePartLog(
            PartLogElement::Type::EXPORT_PART,
            ExecutionStatus::fromCurrentException("", true),
            (*exports_list_entry)->watch.elapsed(),
            manifest.data_part->name,
            manifest.data_part,
            {manifest.data_part},
            nullptr,
            nullptr,
            {},
            exports_list_entry.get());

        std::lock_guard inner_lock(storage.export_manifests_mutex);
        storage.export_manifests.erase(manifest);

        if (manifest.completion_callback)
            manifest.completion_callback(MergeTreePartExportManifest::CompletionCallbackResult::createFailure(e));
        return false;
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
    return manifest.query_id;
}

}
