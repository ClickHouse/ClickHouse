#pragma once

#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Disks/IDisk.h>

#include <Core/Types.h>

namespace DB
{

/// Metadata for DiskObjectStorage
struct DiskObjectStorageMetadata
{
    const std::string compatible_key_prefix;
    const std::string metadata_file_path;

    /// All blobs related to this metadata file.
    StoredObjects objects;

    /// This data will be stored inline
    std::string inline_data;

    /// Number of references (hardlinks) to this metadata file.
    int32_t ref_count = 0;

    /// Flag indicates that file is read only.
    bool read_only = false;

private:
    static constexpr uint32_t VERSION_ABSOLUTE_PATHS = 1;
    static constexpr uint32_t VERSION_RELATIVE_PATHS = 2;
    static constexpr uint32_t VERSION_READ_ONLY_FLAG = 3;
    static constexpr uint32_t VERSION_INLINE_DATA = 4;
    static constexpr uint32_t VERSION_FULL_OBJECT_KEY = 5; /// only for reading data
    uint32_t version = VERSION_FULL_OBJECT_KEY;

public:
    DiskObjectStorageMetadata(std::string compatible_key_prefix_, std::string metadata_file_path_);

    void deserialize(ReadBuffer & buf);
    void deserializeFromString(const std::string & data);
    bool tryDeserializeFromString(const std::string & data) noexcept;

    void serialize(WriteBuffer & buf) const;
    std::string serializeToString() const;
};

using DiskObjectStorageMetadataPtr = std::unique_ptr<DiskObjectStorageMetadata>;

}
