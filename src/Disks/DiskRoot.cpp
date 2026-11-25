#include <Disks/DiskRoot.h>

#include <Disks/getUnderlyingDisk.h>

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>

#include <filesystem>


namespace DB
{

namespace
{

constexpr auto LOCAL_FILESYSTEM = "local";

String normalizeRootPath(const std::filesystem::path & path)
{
    auto normalized = path.lexically_normal();
    if (!normalized.has_filename() && normalized.has_relative_path())
        normalized = normalized.parent_path();

    return normalized.string();
}

/// The path that the `encrypted` layers of `disk` append to the path of the underlying disk.
std::filesystem::path getPathAddedByLayers(const DiskPtr & disk)
{
    const String & disk_path = disk->getPath();
    const String & underlying_disk_path = getUnderlyingDisk(disk)->getPath();
    chassert(disk_path.starts_with(underlying_disk_path));

    return std::filesystem::path(disk_path.substr(underlying_disk_path.size())).relative_path();
}

}

std::optional<DiskRoot> getDiskRoot(const DiskPtr & disk)
{
    const auto description = disk->getDataSourceDescription();

    switch (description.type)
    {
        case DataSourceType::Local:
            return DiskRoot{.filesystem = LOCAL_FILESYSTEM, .path = normalizeRootPath(disk->getPath())};

        case DataSourceType::RAM:
            return {};

        case DataSourceType::ObjectStorage:
        {
            switch (description.metadata_type)
            {
                case MetadataStorageType::Local:
                    return DiskRoot{.filesystem = LOCAL_FILESYSTEM, .path = normalizeRootPath(disk->getPath())};

                case MetadataStorageType::Plain:
                case MetadataStorageType::PlainRewritable:
                {
                    const auto object_storage = disk->getObjectStorage();
                    const auto path = std::filesystem::path(object_storage->getCommonKeyPrefix()) / getPathAddedByLayers(disk);

                    if (description.object_storage_type == ObjectStorageType::Local)
                        return DiskRoot{.filesystem = LOCAL_FILESYSTEM, .path = normalizeRootPath(path)};
                    else
                        return DiskRoot{.filesystem = object_storage->getDescription() + "/" + object_storage->getObjectsNamespace(), .path = (path / "").string()};
                }

                case MetadataStorageType::Keeper:
                case MetadataStorageType::StaticWeb:
                case MetadataStorageType::WebIndex:
                case MetadataStorageType::Memory:
                    return DiskRoot{.filesystem = getUnderlyingDisk(disk)->getName(), .path = normalizeRootPath(disk->getPath())};

                case MetadataStorageType::None:
                    return {};
            }
        }
    }
}

}
