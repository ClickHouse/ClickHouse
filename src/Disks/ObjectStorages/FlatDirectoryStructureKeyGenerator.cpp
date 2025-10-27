#include <Disks/ObjectStorages/FlatDirectoryStructureKeyGenerator.h>
#include <Disks/ObjectStorages/InMemoryDirectoryTree.h>
#include <Processors/Transforms/SelectByIndicesTransform.h>

#include <Common/ObjectStorageKey.h>
#include <Common/getRandomASCIIString.h>

namespace fs = std::filesystem;

namespace DB
{

FlatDirectoryStructureKeyGenerator::FlatDirectoryStructureKeyGenerator(
    String storage_key_prefix_, std::weak_ptr<InMemoryDirectoryTree> tree_)
    : storage_key_prefix(storage_key_prefix_), tree(std::move(tree_))
{
}

ObjectStorageKey FlatDirectoryStructureKeyGenerator::generate(const String & path, bool is_directory, const std::optional<String> & key_prefix) const
{
    const auto tree_ptr = tree.lock();
    const auto fs_path = std::filesystem::path(path);
    const auto prefix = key_prefix.has_value() ? *key_prefix : storage_key_prefix;

    if (is_directory)
    {
        if (auto info = tree_ptr->getDirectoryRemoteInfo(fs_path))
            return ObjectStorageKey::createAsRelative(prefix, info->remote_path);

        return ObjectStorageKey::createAsRelative(prefix, getRandomASCIIString(32));
    }
    else
    {
        const auto directory = fs_path.parent_path();
        const auto filename = fs_path.filename();
        if (filename.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "File name is empty for path '{}'", fs_path.string());

        const auto directory_info = tree_ptr->getDirectoryRemoteInfo(directory);
        if (!directory_info.has_value())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", directory.string());

        return ObjectStorageKey::createAsRelative(prefix, fs::path(directory_info->remote_path) / filename);
    }
}

}
