#include <Disks/ObjectStorages/FlatDirectoryStructureKeyGenerator.h>
#include <Disks/ObjectStorages/InMemoryDirectoryTree.h>
#include <Processors/Transforms/SelectByIndicesTransform.h>

#include <Common/ObjectStorageKey.h>
#include <Common/getRandomASCIIString.h>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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
        auto [exists_direcotory, remote_info] = tree_ptr->existsDirectory(fs_path);
        if (exists_direcotory && !remote_info)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' is virtual", path);

        if (exists_direcotory)
            return ObjectStorageKey::createAsRelative(prefix, remote_info->remote_path);

        return ObjectStorageKey::createAsRelative(prefix, getRandomASCIIString(32));
    }
    else
    {
        const auto directory = fs_path.parent_path();
        const auto filename = fs_path.filename();
        if (filename.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "File name is empty for path '{}'", fs_path.string());

        auto [exists_direcotory, remote_info] = tree_ptr->existsDirectory(fs_path);
        if (!exists_direcotory)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", directory.string());
        else if (exists_direcotory && !remote_info)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' is virtual", path);

        return ObjectStorageKey::createAsRelative(prefix, fs::path(remote_info->remote_path) / filename);
    }
}

}
