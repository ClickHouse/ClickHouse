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
    static constexpr UInt32 VERSION_ABSOLUTE_PATHS = 1;
    static constexpr UInt32 VERSION_RELATIVE_PATHS = 2;
    static constexpr UInt32 VERSION_READ_ONLY_FLAG = 3;
    static constexpr UInt32 VERSION_INLINE_DATA = 4;
    static constexpr UInt32 VERSION_FULL_OBJECT_KEY = 5; /// only for reading data

    UInt32 version = VERSION_READ_ONLY_FLAG;

    /// Absolute paths of blobs
    ObjectKeysWithMetadata keys_with_meta;

    const std::string compatible_key_prefix;

    /// Relative path to metadata file on local FS.
    const std::string metadata_file_path;

    /// Total size of all remote FS (S3, HDFS) objects.
    UInt64 total_size = 0;

    /// Number of references (hardlinks) to this metadata file.
    ///
    /// FIXME: Why we are tracking it explicitly, without
    /// info from filesystem????
    UInt32 ref_count = 0;

    /// Flag indicates that file is read only.
    bool read_only = false;

    /// This data will be stored inline
    std::string inline_data;

public:

    DiskObjectStorageMetadata(
        String compatible_key_prefix_,
        String metadata_file_path_);

    void addObject(ObjectStorageKey key, size_t size);

    ObjectKeyWithMetadata popLastObject();

    void deserialize(ReadBuffer & buf);
    void deserializeFromString(const std::string & data);
    /// This method was deleted from public fork recently by Azat
    void createFromSingleObject(ObjectStorageKey object_key, size_t bytes_size, size_t ref_count_, bool is_read_only_);

    void serialize(WriteBuffer & buf, bool sync) const;
    std::string serializeToString() const;

    const ObjectKeysWithMetadata & getKeysWithMeta() const
    {
        return keys_with_meta;
    }

    bool isReadOnly() const
    {
        return read_only;
    }

    UInt32 getRefCount() const
    {
        return ref_count;
    }

    UInt64 getTotalSizeBytes() const
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

    void setInlineData(const std::string & data)
    {
        inline_data = data;
    }

    const std::string & getInlineData() const
    {
        return inline_data;
    }

    static bool getWriteFullObjectKeySetting();
};

using DiskObjectStorageMetadataPtr = std::unique_ptr<DiskObjectStorageMetadata>;

}
