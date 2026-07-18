#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableMetadataHelpers.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritablePrefixPath.h>
#include <Disks/DiskObjectStorage/MetadataStorages/NormalizedPath.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Disks/WriteMode.h>

#include <IO/WriteHelpers.h>
#include <IO/WriteSettings.h>
#include <IO/ReadSettings.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

std::string resolvePlainRewritableRelativeObjectKey(
    const PlainRewritableLayout & layout,
    const DirectoryRemoteInfo & directory_info,
    const std::string & file_name,
    const FileRemoteInfo & file_info)
{
    if (!file_info.object_key.empty())
        return file_info.object_key;
    return layout.makeRelativeFileObjectKey(directory_info.remote_path, file_name);
}

void writePlainRewritablePrefixPath(
    IObjectStorage & object_storage,
    const PlainRewritableLayout & layout,
    const std::filesystem::path & logical_directory_path,
    DirectoryRemoteInfo & directory_info)
{
    PlainRewritablePrefixPath prefix_path;
    prefix_path.logical_path = (logical_directory_path / "").string();
    prefix_path.explicit_files = directory_info.explicit_files;
    if (directory_info.explicit_files)
    {
        prefix_path.files.reserve(directory_info.files.size());
        for (const auto & [file_name, file_info] : directory_info.files)
        {
            prefix_path.files.emplace_back(
                file_name, resolvePlainRewritableRelativeObjectKey(layout, directory_info, file_name, file_info));
        }
    }

    const auto metadata_object_key = layout.constructDirectoryObjectKey(directory_info.remote_path);
    StoredObject metadata_object(metadata_object_key, logical_directory_path.string());
    auto buf = object_storage.writeObject(
        metadata_object,
        WriteMode::Rewrite,
        /*object_attributes*/ std::nullopt,
        /*buf_size*/ 128,
        /*settings*/ getWriteSettings());
    writeString(serializePlainRewritablePrefixPath(prefix_path), *buf);
    buf->finalize();

    auto metadata = object_storage.getObjectMetadata(metadata_object.remote_path, /*with_tags=*/ false);
    directory_info.etag = metadata.etag;
    directory_info.last_modified = metadata.last_modified.epochTime();
}

void ensurePlainRewritableDirectoryExplicit(
    IObjectStorage & object_storage,
    InMemoryDirectoryTree & fs_tree,
    const PlainRewritableLayout & layout,
    const std::string & logical_directory_path)
{
    auto directory_info = fs_tree.getDirectoryRemoteInfo(logical_directory_path);
    if (!directory_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", logical_directory_path);
    if (directory_info->explicit_files)
        return;

    directory_info->explicit_files = true;
    for (auto & [file_name, file_info] : directory_info->files)
    {
        if (file_info.object_key.empty())
            file_info.object_key = layout.makeRelativeFileObjectKey(directory_info->remote_path, file_name);
    }

    writePlainRewritablePrefixPath(object_storage, layout, logical_directory_path, *directory_info);
    fs_tree.replaceDirectoryRemoteInfo(logical_directory_path, std::move(*directory_info));
}

void rewritePlainRewritableExplicitPrefixPath(
    IObjectStorage & object_storage,
    InMemoryDirectoryTree & fs_tree,
    const PlainRewritableLayout & layout,
    const std::string & logical_directory_path)
{
    auto directory_info = fs_tree.getDirectoryRemoteInfo(logical_directory_path);
    if (!directory_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", logical_directory_path);
    if (!directory_info->explicit_files)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' is not in explicit form", logical_directory_path);

    writePlainRewritablePrefixPath(object_storage, layout, logical_directory_path, *directory_info);
    fs_tree.replaceDirectoryRemoteInfo(logical_directory_path, std::move(*directory_info));
}

}
