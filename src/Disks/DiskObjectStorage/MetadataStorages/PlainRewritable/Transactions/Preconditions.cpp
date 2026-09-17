#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Transactions/Preconditions.h>

#include <Common/Exception.h>

#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int INCORRECT_DATA;
};

namespace
{

void validateDirectoryPresent(const std::shared_ptr<const FsSnapshot> & fs_tree, const std::filesystem::path & path, const std::string & expected_remote_path)
{
    const auto remote_info = fs_tree->getDirectoryRemoteInfo(path);

    if (!remote_info)
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory '{}' was removed concurrently, cannot reuse its remote path '{}'", path.string(), expected_remote_path);

    if (remote_info->remote_path != expected_remote_path)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Directory '{}' was recreated concurrently, its remote path changed from '{}' to '{}'", path.string(), expected_remote_path, remote_info->remote_path);
}

void validateDirectoryMissing(const std::shared_ptr<const FsSnapshot> & fs_tree, const std::filesystem::path & path)
{
    const auto remote_info = fs_tree->getDirectoryRemoteInfo(path);

    if (remote_info)
        throw Exception(ErrorCodes::DIRECTORY_ALREADY_EXISTS, "Directory '{}' was created concurrently with remote path '{}'", path.string(), remote_info->remote_path);
}

}

void Preconditions::checkDirectoryPresent(std::filesystem::path directory, std::string remote_path)
{
    std::lock_guard lock(mutex);
    expected_directory_remote_paths.emplace(std::move(directory), std::move(remote_path));
}

void Preconditions::checkDirectoryMissing(std::filesystem::path directory)
{
    std::lock_guard lock(mutex);
    expected_missing_directories.emplace(std::move(directory));
}

void Preconditions::runChecks(const std::shared_ptr<const FsSnapshot> & fs_tree)
{
    std::lock_guard lock(mutex);

    for (const auto & [directory, expected_remote_path] : expected_directory_remote_paths)
        validateDirectoryPresent(fs_tree, directory, expected_remote_path);

    for (const auto & directory : expected_missing_directories)
        validateDirectoryMissing(fs_tree, directory);
}

}
