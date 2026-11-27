#include <Disks/DiskRoot.h>

#include <Disks/getUnderlyingDisk.h>

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>

#include <filesystem>


namespace DB
{

namespace
{

constexpr auto LOCAL_FILESYSTEM = "local";

String removeLastSlash(String path)
{
    if (path.size() > 1 && path.ends_with('/'))
        path.pop_back();

    return path;
}

String normalizeDirectory(const std::filesystem::path & path)
{
    return removeLastSlash(path.lexically_normal().string());
}

String normalizeKeyPrefix(const std::filesystem::path & prefix)
{
    return removeLastSlash(prefix.string());
}

/// The path that the `encrypted` layers of `disk` append to the path of the underlying disk.
std::filesystem::path getPathAddedByLayers(const DiskPtr & disk)
{
    const String & disk_path = disk->getPath();
    const String & underlying_disk_path = getUnderlyingDisk(disk)->getPath();
    chassert(disk_path.starts_with(underlying_disk_path));

    return (std::filesystem::path("/") / disk_path.substr(underlying_disk_path.size())).lexically_normal().relative_path();
}

}

std::optional<DiskRoot> getDiskRoot(const DiskPtr & disk)
{
    const auto description = disk->getDataSourceDescription();

    switch (description.type)
    {
        case DataSourceType::Local:
            return DiskRoot{.filesystem = LOCAL_FILESYSTEM, .path = normalizeDirectory(disk->getPath())};

        case DataSourceType::RAM:
            return {};

        case DataSourceType::ObjectStorage:
        {
            switch (description.metadata_type)
            {
                case MetadataStorageType::Local:
                    return DiskRoot{.filesystem = LOCAL_FILESYSTEM, .path = normalizeDirectory(disk->getPath())};

                case MetadataStorageType::Plain:
                case MetadataStorageType::PlainRewritable:
                {
                    const auto object_storage = disk->getObjectStorage();
                    const auto path = std::filesystem::path(object_storage->getCommonKeyPrefix()) / getPathAddedByLayers(disk);

                    if (description.object_storage_type == ObjectStorageType::Local)
                        return DiskRoot{.filesystem = LOCAL_FILESYSTEM, .path = normalizeDirectory(path)};
                    else
                        return DiskRoot{.filesystem = object_storage->getDescription() + "/" + object_storage->getObjectsNamespace(), .path = normalizeKeyPrefix(path)};
                }

                case MetadataStorageType::Keeper:
                case MetadataStorageType::StaticWeb:
                case MetadataStorageType::WebIndex:
                case MetadataStorageType::Memory:
                    return DiskRoot{.filesystem = getUnderlyingDisk(disk)->getName(), .path = normalizeDirectory(disk->getPath())};

                case MetadataStorageType::None:
                    return {};
            }
        }
    }
}

}
