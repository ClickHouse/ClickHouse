#pragma once
#include "Disks/ObjectStorages/DiskObjectStorageTransaction.h"

namespace DB
{
struct CopyFileObjectStorageOperation : IDiskObjectStorageOperation
{
    ReadSettings read_settings;
    WriteSettings write_settings;

    /// Local paths
    String from_path;
    String to_path;

    StoredObjects created_objects;
    IObjectStorage & destination_object_storage;

    CopyFileObjectStorageOperation(
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        IObjectStorage & destination_object_storage_,
        const ReadSettings & read_settings_,
        const WriteSettings & write_settings_,
        const String & from_path_,
        const String & to_path_);

    std::string getInfoForLog() const override;
    void execute(MetadataTransactionPtr tx) override;
    void undo() override;
    void finalize() override { }
};

struct ObjectsToRemove
{
    StoredObjects objects;
    UnlinkMetadataFileOperationOutcomePtr unlink_outcome;
};

struct RemoveRecursiveObjectStorageOperation : IDiskObjectStorageOperation
{
    const String path; /// path inside disk with metadata
    const bool keep_all_batch_data;
    const NameSet file_names_remove_metadata_only; /// paths inside the 'this->path'

    /// map from local_path to its remote objects with hardlinks counter
    /// local_path is the path inside 'this->path'
    std::unordered_map<std::string, ObjectsToRemove> objects_to_remove_by_path;

    RemoveRecursiveObjectStorageOperation(
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        const std::string & path_,
        bool keep_all_batch_data_,
        const NameSet & file_names_remove_metadata_only_);

    String getInfoForLog() const override;
    void removeMetadataRecursive(MetadataTransactionPtr tx, const std::string & path_to_remove);
    void execute(MetadataTransactionPtr tx) override;
    void undo() override { }
    void finalize() override;
};

struct RemoveManyObjectStorageOperation : public IDiskObjectStorageOperation
{
    const RemoveBatchRequest remove_paths;
    const bool keep_all_batch_data;
    const NameSet file_names_remove_metadata_only;

    std::vector<String> paths_removed_with_objects;
    std::vector<ObjectsToRemove> objects_to_remove;

    RemoveManyObjectStorageOperation(
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        const RemoveBatchRequest & remove_paths_,
        bool keep_all_batch_data_,
        const NameSet & file_names_remove_metadata_only_);

    std::string getInfoForLog() const override;

    void execute(MetadataTransactionPtr tx) override;
    void undo() override { }
    void finalize() override;
};
}
