#pragma once

#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Core/Types.h>

namespace DB
{


/// Metadata for DiskObjectStorage, stored on local disk
struct DiskObjectStorageMetadata
{
private:
    /// Metadata file version.
    static constexpr uint32_t VERSION_ABSOLUTE_PATHS = 1;
    static constexpr uint32_t VERSION_RELATIVE_PATHS = 2;
    static constexpr uint32_t VERSION_READ_ONLY_FLAG = 3;

    const std::string & common_metadata_path;

    /// Relative paths of blobs.
    RelativePathsWithSize storage_objects;

    const std::string object_storage_root_path;

    /// Relative path to metadata file on local FS.
    const std::string metadata_file_path;

    /// Total size of all remote FS (S3, HDFS) objects.
    size_t total_size = 0;

    /// Number of references (hardlinks) to this metadata file.
    ///
    /// FIXME: Why we are tracking it explicetly, without
    /// info from filesystem????
    uint32_t ref_count = 0;

    /// Flag indicates that file is read only.
    bool read_only = false;

public:

    DiskObjectStorageMetadata(
        const std::string & common_metadata_path_,
        const std::string & object_storage_root_path_,
        const std::string & metadata_file_path_);

    void addObject(const std::string & path, size_t size);

    void deserialize(ReadBuffer & buf);
    void deserializeFromString(const std::string & data);

    void serialize(WriteBuffer & buf, bool sync) const;
    std::string serializeToString() const;

    std::string getBlobsCommonPrefix() const
    {
        return object_storage_root_path;
    }

    const RelativePathsWithSize & getBlobsRelativePaths() const
    {
        return storage_objects;
    }

    bool isReadOnly() const
    {
        return read_only;
    }

    uint32_t getRefCount() const
    {
        return ref_count;
    }

    uint64_t getTotalSizeBytes() const
    {
        return total_size;
    }

    void incrementRefCount()
    {
        ++ref_count;
    }

    void decrementRefCount()
    {
        --ref_count;
    }

    void resetRefCount()
    {
        ref_count = 0;
    }

    void setReadOnly()
    {
        read_only = true;
    }

    std::string getIndexPath() const;

};

using DiskObjectStorageMetadataPtr = std::unique_ptr<DiskObjectStorageMetadata>;

}
