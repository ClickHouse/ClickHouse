#pragma once

#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/Manifest/IManifestStorage.h>
#include <Storages/MergeTree/Manifest/BaseManifestEntry.h>
#include <Disks/DiskManifest.h>
#include <memory>

namespace DB
{


class StorageMergeTreeWithManifest : public StorageMergeTree
{
public:
    StorageMergeTreeWithManifest(
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata,
        LoadingStrictnessLevel mode,
        ContextMutablePtr context_,
        const String & date_column_name,
        const MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_,
        const String & manifest_storage_type_ = "");

    ~StorageMergeTreeWithManifest() override = default;

    void drop() override;

    String getName() const override { return "MergeTreeWithManifest"; }

    DiskPtr getManifestDisk() const override { return manifest_disk; }

private:
    void initializeManifestIfNeeded();
    void initializeManifestStorage();
    void initializeManifest();
    String getManifestStoragePath() const;

    void commitToManifest(const DataPartPtr & part, ManifestOpType op_type) const override;

    void removeFromManifest(const String & part_uuid) const override;

    BaseManifestEntry serialize(const DataPartPtr & part, ManifestOpType op_type) const;

    MergeTreeData::LoadPartResult loadDataPart(
        const MergeTreePartInfo & part_info,
        const String & part_name,
        const DiskPtr & part_disk,
        MergeTreeDataPartState to_state,
        DB::SharedMutex & part_loading_mutex) override;

    String manifest_storage_type;
    ManifestStoragePtr manifest_storage;
    String manifest_storage_path;
    DiskManifestPtr manifest_disk;
};

}
