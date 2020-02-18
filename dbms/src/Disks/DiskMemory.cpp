#include "DiskMemory.h"
#include "DiskFactory.h"

#include <IO/ReadBufferFromString.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int FILE_ALREADY_EXISTS;
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int CANNOT_DELETE_DIRECTORY;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}


class DiskMemoryDirectoryIterator : public IDiskDirectoryIterator
{
public:
    explicit DiskMemoryDirectoryIterator(std::vector<String> && dir_file_paths_)
        : dir_file_paths(std::move(dir_file_paths_)), iter(dir_file_paths.begin())
    {
    }

    void next() override { ++iter; }

    bool isValid() const override { return iter != dir_file_paths.end(); }

    String path() const override { return *iter; }

private:
    std::vector<String> dir_file_paths;
    std::vector<String>::iterator iter;
};

ReadIndirectBuffer::ReadIndirectBuffer(String path_, const String & data_)
    : ReadBufferFromFileBase(), buf(ReadBufferFromString(data_)), path(std::move(path_))
{
    internal_buffer = buf.buffer();
    working_buffer = internal_buffer;
    pos = working_buffer.begin();
}

off_t ReadIndirectBuffer::seek(off_t offset, int whence)
{
    if (whence == SEEK_SET)
    {
        if (offset >= 0 && working_buffer.begin() + offset < working_buffer.end())
        {
            pos = working_buffer.begin() + offset;
            return size_t(pos - working_buffer.begin());
        }
        else
            throw Exception(
                "Seek position is out of bounds. "
                "Offset: "
                    + std::to_string(offset) + ", Max: " + std::to_string(size_t(working_buffer.end() - working_buffer.begin())),
                ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);
    }
    else if (whence == SEEK_CUR)
    {
        Position new_pos = pos + offset;
        if (new_pos >= working_buffer.begin() && new_pos < working_buffer.end())
        {
            pos = new_pos;
            return size_t(pos - working_buffer.begin());
        }
        else
            throw Exception(
                "Seek position is out of bounds. "
                "Offset: "
                    + std::to_string(offset) + ", Max: " + std::to_string(size_t(working_buffer.end() - working_buffer.begin())),
                ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);
    }
    else
        throw Exception("Only SEEK_SET and SEEK_CUR seek modes allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
}

off_t ReadIndirectBuffer::getPosition()
{
    return pos - working_buffer.begin();
}

void WriteIndirectBuffer::finalize()
{
    if (isFinished())
        return;

    next();
    WriteBufferFromVector::finalize();

    auto iter = disk->files.find(path);

    if (iter == disk->files.end())
        throw Exception("File '" + path + "' does not exist", ErrorCodes::FILE_DOESNT_EXIST);

    // Resize to the actual number of bytes written to string.
    value.resize(count());

    if (mode == WriteMode::Rewrite)
        disk->files.insert_or_assign(path, DiskMemory::FileData{iter->second.type, value});
    else if (mode == WriteMode::Append)
        disk->files.insert_or_assign(path, DiskMemory::FileData{iter->second.type, iter->second.data + value});
}

WriteIndirectBuffer::~WriteIndirectBuffer()
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

std::unique_ptr<ReadBufferFromFileBase> DiskMemory::readFile(const String & path, size_t /*buf_size*/) const
{
    std::lock_guard lock(mutex);

    auto iter = files.find(path);
    if (iter == files.end())
        throw Exception("File '" + path + "' does not exist", ErrorCodes::FILE_DOESNT_EXIST);

    return std::make_unique<ReadIndirectBuffer>(path, iter->second.data);
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
                "Failed to create file '" + path + "'. Directory " + parent_path + " does not exist", ErrorCodes::DIRECTORY_DOESNT_EXIST);

        files.emplace(path, FileData{FileType::File});
    }

    return std::make_unique<WriteIndirectBuffer>(this, path, mode);
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
