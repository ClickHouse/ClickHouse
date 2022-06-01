#pragma once

#include <Disks/IDisk.h>
#include <Core/Types.h>

namespace DB
{

/// Metadata for DiskObjectStorage, stored on local disk
struct DiskObjectStorageMetadata
{
    using Updater = std::function<bool(DiskObjectStorageMetadata & metadata)>;
    /// Metadata file version.
    static constexpr UInt32 VERSION_ABSOLUTE_PATHS = 1;
    static constexpr UInt32 VERSION_RELATIVE_PATHS = 2;
    static constexpr UInt32 VERSION_READ_ONLY_FLAG = 3;

    /// Remote FS objects paths and their sizes.
    std::vector<BlobPathWithSize> remote_fs_objects;

    /// URI
    const String & remote_fs_root_path;

    /// Relative path to metadata file on local FS.
    const String metadata_file_path;

    DiskPtr metadata_disk;

    /// Total size of all remote FS (S3, HDFS) objects.
    size_t total_size = 0;

    /// Number of references (hardlinks) to this metadata file.
    ///
    /// FIXME: Why we are tracking it explicetly, without
    /// info from filesystem????
    UInt32 ref_count = 0;

    /// Flag indicates that file is read only.
    bool read_only = false;

    DiskObjectStorageMetadata(
        const String & remote_fs_root_path_,
        DiskPtr metadata_disk_,
        const String & metadata_file_path_);

    void addObject(const String & path, size_t size);

    static DiskObjectStorageMetadata readMetadata(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_);
    static DiskObjectStorageMetadata readUpdateAndStoreMetadata(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync, Updater updater);
    static void readUpdateStoreMetadataAndRemove(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync, Updater updater);

    static DiskObjectStorageMetadata createAndStoreMetadata(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync);
    static DiskObjectStorageMetadata createUpdateAndStoreMetadata(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync, Updater updater);
    static DiskObjectStorageMetadata createAndStoreMetadataIfNotExists(const String & remote_fs_root_path_, DiskPtr metadata_disk_, const String & metadata_file_path_, bool sync, bool overwrite);

    /// Serialize metadata to string (very same with saveToBuffer)
    std::string serializeToString();

private:
    /// Fsync metadata file if 'sync' flag is set.
    void save(bool sync = false);
    void saveToBuffer(WriteBuffer & buffer, bool sync);
    void load();
};

using DiskObjectStorageMetadataUpdater = std::function<bool(DiskObjectStorageMetadata & metadata)>;

}
