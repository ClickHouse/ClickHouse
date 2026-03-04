#include <Storages/MergeTree/ExportPartitionUtils.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>
#include "Storages/ExportReplicatedMergeTreePartitionManifest.h"
#include "Storages/ExportReplicatedMergeTreePartitionTaskEntry.h"
#include <filesystem>

namespace DB
{

namespace fs = std::filesystem;

namespace ExportPartitionUtils
{
    /// Collect all the exported paths from the processed parts
    /// If multiRead is supported by the keeper implementation, it is done in a single request
    /// Otherwise, multiple async requests are sent
    std::vector<std::string> getExportedPaths(const LoggerPtr & log, const zkutil::ZooKeeperPtr & zk, const std::string & export_path)
    {
        std::vector<std::string> exported_paths;

        LOG_INFO(log, "ExportPartition: Getting exported paths for {}", export_path);

        const auto processed_parts_path = fs::path(export_path) / "processed";

        std::vector<std::string> processed_parts;
        if (Coordination::Error::ZOK != zk->tryGetChildren(processed_parts_path, processed_parts))
        {
            /// todo arthur do something here
            LOG_INFO(log, "ExportPartition: Failed to get parts children, exiting");
            return {};
        }

        std::vector<std::string> get_paths;

        for (const auto & processed_part : processed_parts)
        {
            get_paths.emplace_back(processed_parts_path / processed_part);
        }

        auto responses = zk->tryGet(get_paths);

        responses.waitForResponses();

        for (size_t i = 0; i < responses.size(); ++i)
        {
            if (responses[i].error != Coordination::Error::ZOK)
            {
                /// todo arthur what to do in this case?
                /// It could be that zk is corrupt, in that case we should fail the task
                /// but it can also be some temporary network issue? not sure
                LOG_INFO(log, "ExportPartition: Failed to get exported path, exiting");
                return {};
            }

            const auto processed_part_entry = ExportReplicatedMergeTreePartitionProcessedPartEntry::fromJsonString(responses[i].data);

            for (const auto & path_in_destination : processed_part_entry.paths_in_destination)
            {
                exported_paths.emplace_back(path_in_destination);
            }
        }

        return exported_paths;
    }

    void commit(
        const ExportReplicatedMergeTreePartitionManifest & manifest,
        const StoragePtr & destination_storage,
        const zkutil::ZooKeeperPtr & zk,
        const LoggerPtr & log,
        const std::string & entry_path,
        const ContextPtr & context)
    {
        const auto exported_paths = ExportPartitionUtils::getExportedPaths(log, zk, entry_path);

        if (exported_paths.empty())
        {
            LOG_WARNING(log, "ExportPartition: No exported paths found, will not commit export. This might be a bug");
            return;
        }

        //// not checking for an exact match because a single part might generate multiple files
        if (exported_paths.size() < manifest.parts.size())
        {
            LOG_WARNING(log, "ExportPartition: Reached the commit phase, but exported paths size is less than the number of parts, will not commit export. This might be a bug");
            return;
        }

        destination_storage->commitExportPartitionTransaction(manifest.transaction_id, manifest.partition_id, exported_paths, context);

        LOG_INFO(log, "ExportPartition: Committed export, mark as completed");
        if (Coordination::Error::ZOK == zk->trySet(fs::path(entry_path) / "status", String(magic_enum::enum_name(ExportReplicatedMergeTreePartitionTaskEntry::Status::COMPLETED)).data(), -1))
        {
            LOG_INFO(log, "ExportPartition: Marked export as completed");
        }
        else
        {
            LOG_INFO(log, "ExportPartition: Failed to mark export as completed, will not try to fix it");
        }
    }
}

}
