#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/FlatDirectoryStructureKeyGenerator.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/InMemoryDirectoryTree.h>

#include <Common/Exception.h>
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

ObjectStorageKey FlatDirectoryStructureKeyGenerator::generate(const String & path) const
{
    const auto tree_ptr = tree.lock();
    const auto fs_path = std::filesystem::path(path);
    const auto directory = fs_path.parent_path();
    const auto filename = fs_path.filename();
    if (filename.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File name is empty for path '{}'", fs_path.string());

    auto [exists_directory, remote_info] = tree_ptr->existsDirectory(directory);
    if (!exists_directory)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", directory.string());
    else if (!remote_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' is virtual", directory.string());

    return ObjectStorageKey::createAsAbsolute(fs::path(storage_key_prefix) / remote_info->remote_path / filename);
}

std::string FlatDirectoryStructureKeyGenerator::generateForDirectory(const String & path) const
{
    const auto tree_ptr = tree.lock();
    const auto fs_path = std::filesystem::path(path);
    auto [exists_directory, remote_info] = tree_ptr->existsDirectory(fs_path);
    if (exists_directory && !remote_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' is virtual", fs_path.string());

    /// Take from cache
    if (exists_directory)
        return remote_info->remote_path;

    /// Generate new one
    return getRandomASCIIString(32);
}

}
