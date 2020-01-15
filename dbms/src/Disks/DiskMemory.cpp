#include "DiskMemory.h"
#include "DiskFactory.h"

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int FILE_DOESNT_EXIST;
    extern const int FILE_ALREADY_EXISTS;
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int CANNOT_DELETE_DIRECTORY;
}

ReservationPtr DiskMemory::reserve(UInt64 /*bytes*/)
{
    throw Exception("Method reserve is not implemented for memory disks", ErrorCodes::NOT_IMPLEMENTED);
}

UInt64 DiskMemory::getTotalSpace() const
{
    return 0;
}

UInt64 DiskMemory::getAvailableSpace() const
{
    return 0;
}

UInt64 DiskMemory::getUnreservedSpace() const
{
    return 0;
}

bool DiskMemory::exists(const String & path) const
{
    std::lock_guard lock(mutex);

    return files.find(path) != files.end();
}

bool DiskMemory::isFile(const String & path) const
{
    std::lock_guard lock(mutex);

    auto iter = files.find(path);
    if (iter == files.end())
        return false;

    return iter->second.type == FileType::File;
}

bool DiskMemory::isDirectory(const String & path) const
{
    std::lock_guard lock(mutex);

    auto iter = files.find(path);
    if (iter == files.end())
        return false;

    return iter->second.type == FileType::Directory;
}

size_t DiskMemory::getFileSize(const String & path) const
{
    std::lock_guard lock(mutex);

    auto iter = files.find(path);
    if (iter == files.end())
        throw Exception("File " + path + " does not exist", ErrorCodes::FILE_DOESNT_EXIST);

    return iter->second.data.size();
}

void DiskMemory::createDirectory(const String & path)
{
    std::lock_guard lock(mutex);

    if (files.find(path) != files.end())
        return;

    String parent_path = parentPath(path);
    if (!parent_path.empty() && files.find(parent_path) == files.end())
        throw Exception(
            "Failed to create directory " + path + ". Parent directory " + parent_path + " does not exist",
            ErrorCodes::DIRECTORY_DOESNT_EXIST);

    files.emplace(path, FileData{FileType::Directory});
}

void DiskMemory::createDirectories(const String & path)
{
    std::lock_guard lock(mutex);

    createDirectoriesImpl(path);
}

void DiskMemory::createDirectoriesImpl(const String & path)
{
    if (files.find(path) != files.end())
        return;

    String parent_path = parentPath(path);
    if (!parent_path.empty())
        createDirectoriesImpl(parent_path);

    files.emplace(path, FileData{FileType::Directory});
}

void DiskMemory::clearDirectory(const String & path)
{
    std::lock_guard lock(mutex);

    if (files.find(path) == files.end())
        throw Exception("Directory " + path + " does not exist", ErrorCodes::DIRECTORY_DOESNT_EXIST);

    for (auto iter = files.begin(); iter != files.end();)
    {
        if (parentPath(iter->first) != path)
        {
            ++iter;
            continue;
        }

        if (iter->second.type == FileType::Directory)
            throw Exception(
                "Failed to clear directory " + path + ". " + iter->first + " is a directory", ErrorCodes::CANNOT_DELETE_DIRECTORY);

        files.erase(iter++);
    }
}

void DiskMemory::moveDirectory(const String & /*from_path*/, const String & /*to_path*/)
{
    throw Exception("Method moveDirectory is not implemented for memory disks", ErrorCodes::NOT_IMPLEMENTED);
}

DiskDirectoryIteratorPtr DiskMemory::iterateDirectory(const String & path)
{
    std::lock_guard lock(mutex);

    if (!path.empty() && files.find(path) == files.end())
        throw Exception("Directory " + path + " does not exist", ErrorCodes::DIRECTORY_DOESNT_EXIST);

    std::vector<String> dir_file_paths;
    for (const auto & file : files)
        if (parentPath(file.first) == path)
            dir_file_paths.push_back(file.first);

    return std::make_unique<DiskMemoryDirectoryIterator>(std::move(dir_file_paths));
}

void DiskMemory::moveFile(const String & from_path, const String & to_path)
{
    std::lock_guard lock(mutex);

    if (files.find(to_path) != files.end())
        throw Exception(
            "Failed to move file from " + from_path + " to " + to_path + ". File " + to_path + " already exist",
            ErrorCodes::FILE_ALREADY_EXISTS);

    replaceFileImpl(from_path, to_path);
}

void DiskMemory::replaceFile(const String & from_path, const String & to_path)
{
    std::lock_guard lock(mutex);

    replaceFileImpl(from_path, to_path);
}

void DiskMemory::replaceFileImpl(const String & from_path, const String & to_path)
{
    String to_parent_path = parentPath(to_path);
    if (!to_parent_path.empty() && files.find(to_parent_path) == files.end())
        throw Exception(
            "Failed to move file from " + from_path + " to " + to_path + ". Directory " + to_parent_path + " does not exist",
            ErrorCodes::DIRECTORY_DOESNT_EXIST);

    auto iter = files.find(from_path);
    if (iter == files.end())
        throw Exception(
            "Failed to move file from " + from_path + " to " + to_path + ". File " + from_path + " does not exist",
            ErrorCodes::FILE_DOESNT_EXIST);

    auto node = files.extract(iter);
    node.key() = to_path;
    files.insert(std::move(node));
}

void DiskMemory::copyFile(const String & /*from_path*/, const String & /*to_path*/)
{
    throw Exception("Method copyFile is not implemented for memory disks", ErrorCodes::NOT_IMPLEMENTED);
}

std::unique_ptr<ReadBuffer> DiskMemory::readFile(const String & path, size_t /*buf_size*/) const
{
    std::lock_guard lock(mutex);

    auto iter = files.find(path);
    if (iter == files.end())
        throw Exception("File " + path + " does not exist", ErrorCodes::FILE_DOESNT_EXIST);

    return std::make_unique<ReadBufferFromString>(iter->second.data);
}

std::unique_ptr<WriteBuffer> DiskMemory::writeFile(const String & path, size_t /*buf_size*/, WriteMode mode)
{
    std::lock_guard lock(mutex);

    auto iter = files.find(path);
    if (iter == files.end())
    {
        String parent_path = parentPath(path);
        if (!parent_path.empty() && files.find(parent_path) == files.end())
            throw Exception(
                "Failed to create file " + path + ". Directory " + parent_path + " does not exist", ErrorCodes::DIRECTORY_DOESNT_EXIST);

        iter = files.emplace(path, FileData{FileType::File}).first;
    }

    if (mode == WriteMode::Append)
        return std::make_unique<WriteBufferFromString>(iter->second.data, WriteBufferFromString::AppendModeTag{});
    else
        return std::make_unique<WriteBufferFromString>(iter->second.data);
}

void DiskMemory::remove(const String & path, bool recursive)
{
    std::lock_guard lock(mutex);

    auto fileIt = files.find(path);
    if (fileIt == files.end())
        return;

    if (fileIt->second.type == FileType::Directory)
    {
        if (recursive)
            clearDirectory(path);
        else if (std::any_of(files.begin(), files.end(), [path](const auto & file) { return parentPath(file.first) == path; }))
            throw Exception("Directory " + path + "is not empty", ErrorCodes::CANNOT_DELETE_DIRECTORY);
    }

    files.erase(fileIt);
}

void registerDiskMemory(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & /*config*/,
                      const String & /*config_prefix*/,
                      const Context & /*context*/) -> DiskPtr { return std::make_shared<DiskMemory>(name); };
    factory.registerDiskType("memory", creator);
}

}
