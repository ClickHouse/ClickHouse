#include <Disks/DiskRoot.h>

#include <Disks/getUnderlyingDisk.h>

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>

#include <filesystem>


namespace DB
{

namespace
{

constexpr auto LOCAL_FILESYSTEM = "local";

std::optional<String> getObjectStorageDiskFilesystem(const DiskPtr & disk, const DataSourceDescription & description)
{
    switch (description.metadata_type)
    {
        case MetadataStorageType::Local:
            return LOCAL_FILESYSTEM;

        case MetadataStorageType::Plain:
        case MetadataStorageType::PlainRewritable:
        {
            if (description.object_storage_type == ObjectStorageType::Local)
                return LOCAL_FILESYSTEM;

            const auto object_storage = disk->getObjectStorage();
            return object_storage->getDescription() + "/" + object_storage->getObjectsNamespace();
        }

        case MetadataStorageType::Keeper:
        case MetadataStorageType::StaticWeb:
        case MetadataStorageType::WebIndex:
        case MetadataStorageType::Memory:
            return getUnderlyingDisk(disk)->getName();

        case MetadataStorageType::None:
            return {};
    }
}

std::optional<String> getDiskFilesystem(const DiskPtr & disk)
{
    const auto description = disk->getDataSourceDescription();

    switch (description.type)
    {
        case DataSourceType::Local:
            return LOCAL_FILESYSTEM;

        case DataSourceType::ObjectStorage:
            return getObjectStorageDiskFilesystem(disk, description);

        case DataSourceType::RAM:
            return {};
    }
}

String getDiskPath(const DiskPtr & disk)
{
    auto path = std::filesystem::path(disk->getPath()).lexically_normal();
    if (!path.has_filename() && path.has_relative_path())
        path = path.parent_path();

    return path.string();
}

}

std::optional<DiskRoot> getDiskRoot(const DiskPtr & disk)
{
    if (auto filesystem = getDiskFilesystem(disk))
        return DiskRoot{.filesystem = std::move(*filesystem), .path = getDiskPath(disk)};

    return {};
}

}
