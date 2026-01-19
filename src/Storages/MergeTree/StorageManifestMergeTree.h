#pragma once

#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/Manifest/IManifestStorage.h>
#include <Disks/DiskManifest.h>
#include <memory>

namespace DB
{

namespace Manifest
{
    class PartObject;
}

enum class ManifestOpType
{
    PreCommit,
    PreRemove,
    Commit,
};

class StorageManifestMergeTree : public StorageMergeTree
{
public:
    StorageManifestMergeTree(
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata,
        LoadingStrictnessLevel mode,
        ContextMutablePtr context_,
        const String & date_column_name,
        const MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_,
        const String & manifest_storage_type_ = "");

    ~StorageManifestMergeTree() override = default;

    void startup() override;
    void shutdown(bool is_drop) override;

    MergeTreeData::LoadPartResult loadDataPart(
        const MergeTreePartInfo & part_info,
        const String & part_name,
        const DiskPtr & part_disk,
        MergeTreeDataPartState to_state,
        DB::SharedMutex & part_loading_mutex) override;

    String getName() const override { return "ManifestMergeTree"; }

private:
    void initializeManifestStorage();
    void initializeManifest();
    String getManifestStoragePath() const;

    void commitToManifest(
        const DataPartPtr & part,
        ManifestOpType op_type,
        const std::optional<UUID> & part_uuid,
        const std::optional<String> & part_name) const;

    void removeFromManifest(const String & part_uuid) const;

    Manifest::PartObject serialize(
        const DataPartPtr & part,
        ManifestOpType op_type,
        const std::optional<UUID> & part_uuid,
        const std::optional<String> & part_name) const;

    void deserialize(
        const Manifest::PartObject & obj,
        MutableDataPartPtr & part) const;

    /// Find disk for a part using manifest information with self-healing.
    /// Returns the disk where the part is located, and updates manifest if disk info changed.
    DiskPtr findDiskForPart(
        const Manifest::PartObject & part_object,
        const String & part_name,
        const String & part_uuid_str) const;

    String manifest_storage_type;
    ManifestStoragePtr manifest_storage;
    String manifest_storage_path;
    DiskManifestPtr manifest_disk;
};

}
