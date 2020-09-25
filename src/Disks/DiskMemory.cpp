#include "DiskMemory.h"
#include "DiskFactory.h"

#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Poco/Path.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int FILE_DOESNT_EXIST;
    extern const int FILE_ALREADY_EXISTS;
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int CANNOT_DELETE_DIRECTORY;
}


class DiskMemoryDirectoryIterator final : public IDiskDirectoryIterator
{
public:
    explicit DiskMemoryDirectoryIterator(std::vector<Poco::Path> && dir_file_paths_)
        : dir_file_paths(std::move(dir_file_paths_)), iter(dir_file_paths.begin())
    {
    }

    void next() override { ++iter; }

    bool isValid() const override { return iter != dir_file_paths.end(); }

    String path() const override { return (*iter).toString(); }

    String name() const override { return (*iter).getFileName(); }

private:
    std::vector<Poco::Path> dir_file_paths;
    std::vector<Poco::Path>::iterator iter;
};


/// Adapter with actual behaviour as ReadBufferFromString.
class ReadIndirectBuffer final : public ReadBufferFromFileBase
{
public:
    ReadIndirectBuffer(String path_, const String & data_)
        : impl(ReadBufferFromString(data_)), path(std::move(path_))
    {
        internal_buffer = impl.buffer();
        working_buffer = internal_buffer;
        pos = working_buffer.begin();
    }

    std::string getFileName() const override { return path; }

    off_t seek(off_t off, int whence) override
    {
        impl.swap(*this);
        off_t result = impl.seek(off, whence);
        impl.swap(*this);
        return result;
    }

    off_t getPosition() override { return pos - working_buffer.begin(); }

private:
    ReadBufferFromString impl;
    const String path;
};


/// This class is responsible to update files metadata after buffer is finalized.
class WriteIndirectBuffer final : public WriteBufferFromFileBase
{
public:
    WriteIndirectBuffer(DiskMemory * disk_, String path_, WriteMode mode_, size_t buf_size)
        : WriteBufferFromFileBase(buf_size, nullptr, 0), disk(disk_), path(std::move(path_)), mode(mode_)
    {
    }

    ~WriteIndirectBuffer() override
    {
        try
        {
            finalize();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    void finalize() override
    {
        if (impl.isFinished())
            return;

        next();

        /// str() finalizes buffer.
        String value = impl.str();

        auto iter = disk->files.find(path);

        if (iter == disk->files.end())
            throw Exception("File '" + path + "' does not exist", ErrorCodes::FILE_DOESNT_EXIST);

        /// Resize to the actual number of bytes written to string.
        value.resize(count());

        if (mode == WriteMode::Rewrite)
            disk->files.insert_or_assign(path, DiskMemory::FileData{iter->second.type, value});
        else if (mode == WriteMode::Append)
            disk->files.insert_or_assign(path, DiskMemory::FileData{iter->second.type, iter->second.data + value});
    }

    std::string getFileName() const override { return path; }

    void sync() override {}

private:
    void nextImpl() override
    {
        if (!offset())
            return;

        impl.write(working_buffer.begin(), offset());
    }

    WriteBufferFromOwnString impl;
    DiskMemory * disk;
    const String path;
    const WriteMode mode;
};


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
        throw Exception("File '" + path + "' does not exist", ErrorCodes::FILE_DOESNT_EXIST);

    return iter->second.data.length();
}

void DiskMemory::createDirectory(const String & path)
{
    std::lock_guard lock(mutex);

    if (files.find(path) != files.end())
        return;

    String parent_path = parentPath(path);
    if (!parent_path.empty() && files.find(parent_path) == files.end())
        throw Exception(
            "Failed to create directory '" + path + "'. Parent directory " + parent_path + " does not exist",
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
        throw Exception("Directory '" + path + "' does not exist", ErrorCodes::DIRECTORY_DOESNT_EXIST);

    for (auto iter = files.begin(); iter != files.end();)
    {
        if (parentPath(iter->first) != path)
        {
            ++iter;
            continue;
        }

        if (iter->second.type == FileType::Directory)
            throw Exception(
                "Failed to clear directory '" + path + "'. " + iter->first + " is a directory", ErrorCodes::CANNOT_DELETE_DIRECTORY);

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
        throw Exception("Directory '" + path + "' does not exist", ErrorCodes::DIRECTORY_DOESNT_EXIST);

    std::vector<Poco::Path> dir_file_paths;
    for (const auto & file : files)
        if (parentPath(file.first) == path)
            dir_file_paths.emplace_back(file.first);

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

std::unique_ptr<ReadBufferFromFileBase> DiskMemory::readFile(const String & path, size_t /*buf_size*/, size_t, size_t, size_t) const
{
    std::lock_guard lock(mutex);

    auto iter = files.find(path);
    if (iter == files.end())
        throw Exception("File '" + path + "' does not exist", ErrorCodes::FILE_DOESNT_EXIST);

    return std::make_unique<ReadIndirectBuffer>(path, iter->second.data);
}

std::unique_ptr<WriteBufferFromFileBase> DiskMemory::writeFile(const String & path, size_t buf_size, WriteMode mode, size_t, size_t)
{
    std::lock_guard lock(mutex);

    auto iter = files.find(path);
    if (iter == files.end())
    {
        String parent_path = parentPath(path);
        if (!parent_path.empty() && files.find(parent_path) == files.end())
            throw Exception(
                "Failed to create file '" + path + "'. Directory " + parent_path + " does not exist", ErrorCodes::DIRECTORY_DOESNT_EXIST);

        files.emplace(path, FileData{FileType::File});
    }

    return std::make_unique<WriteIndirectBuffer>(this, path, mode, buf_size);
}

void DiskMemory::remove(const String & path)
{
    std::lock_guard lock(mutex);

    auto file_it = files.find(path);
    if (file_it == files.end())
        throw Exception("File '" + path + "' doesn't exist", ErrorCodes::FILE_DOESNT_EXIST);

    if (file_it->second.type == FileType::Directory)
    {
        files.erase(file_it);
        if (std::any_of(files.begin(), files.end(), [path](const auto & file) { return parentPath(file.first) == path; }))
            throw Exception("Directory '" + path + "' is not empty", ErrorCodes::CANNOT_DELETE_DIRECTORY);
    }
    else
    {
        files.erase(file_it);
    }
}

void DiskMemory::removeRecursive(const String & path)
{
    std::lock_guard lock(mutex);

    auto file_it = files.find(path);
    if (file_it == files.end())
        throw Exception("File '" + path + "' doesn't exist", ErrorCodes::FILE_DOESNT_EXIST);

    for (auto iter = files.begin(); iter != files.end();)
    {
        if (iter->first.size() >= path.size() && std::string_view(iter->first.data(), path.size()) == path)
            iter = files.erase(iter);
        else
            ++iter;
    }
}

void DiskMemory::listFiles(const String & path, std::vector<String> & file_names)
{
    std::lock_guard lock(mutex);

    for (auto it = iterateDirectory(path); it->isValid(); it->next())
        file_names.push_back(it->name());
}

void DiskMemory::createHardLink(const String &, const String &)
{
    throw Exception("Method createHardLink is not implemented for memory disks", ErrorCodes::NOT_IMPLEMENTED);
}

void DiskMemory::createFile(const String &)
{
    throw Exception("Method createFile is not implemented for memory disks", ErrorCodes::NOT_IMPLEMENTED);
}

void DiskMemory::setReadOnly(const String &)
{
    throw Exception("Method setReadOnly is not implemented for memory disks", ErrorCodes::NOT_IMPLEMENTED);
}

int DiskMemory::open(const String & /*path*/, mode_t /*mode*/) const
{
    throw Exception("Method open is not implemented for memory disks", ErrorCodes::NOT_IMPLEMENTED);
}

void DiskMemory::close(int /*fd*/) const
{
    throw Exception("Method close is not implemented for memory disks", ErrorCodes::NOT_IMPLEMENTED);
}

void DiskMemory::sync(int /*fd*/) const
{
    throw Exception("Method sync is not implemented for memory disks", ErrorCodes::NOT_IMPLEMENTED);
}

void DiskMemory::truncateFile(const String & path, size_t size)
{
    std::lock_guard lock(mutex);

    auto file_it = files.find(path);
    if (file_it == files.end())
        throw Exception("File '" + path + "' doesn't exist", ErrorCodes::FILE_DOESNT_EXIST);

    file_it->second.data.resize(size);
}


using DiskMemoryPtr = std::shared_ptr<DiskMemory>;


void registerDiskMemory(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & /*config*/,
                      const String & /*config_prefix*/,
                      const Context & /*context*/) -> DiskPtr { return std::make_shared<DiskMemory>(name); };
    factory.registerDiskType("memory", creator);
}

}
