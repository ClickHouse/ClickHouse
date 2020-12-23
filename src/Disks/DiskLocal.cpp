#include "DiskLocal.h"
#include <Common/createHardLink.h>
#include "DiskFactory.h"

#include <Interpreters/Context.h>
#include <Common/filesystemHelpers.h>
#include <Common/quoteString.h>

#include <IO/createReadBufferFromFileBase.h>
#include <IO/createWriteBufferFromFileBase.h>
#include <common/logger_useful.h>
#include <unistd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int PATH_ACCESS_DENIED;
    extern const int INCORRECT_DISK_INDEX;
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_FSYNC;
    extern const int CANNOT_CLOSE_FILE;
    extern const int CANNOT_TRUNCATE_FILE;
}

std::mutex DiskLocal::reservation_mutex;


using DiskLocalPtr = std::shared_ptr<DiskLocal>;

class DiskLocalReservation : public IReservation
{
public:
    DiskLocalReservation(const DiskLocalPtr & disk_, UInt64 size_)
        : disk(disk_), size(size_), metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size_)
    {
    }

    UInt64 getSize() const override { return size; }

    DiskPtr getDisk(size_t i) const override;

    Disks getDisks() const override { return {disk}; }

    void update(UInt64 new_size) override;

    ~DiskLocalReservation() override;

private:
    DiskLocalPtr disk;
    UInt64 size;
    CurrentMetrics::Increment metric_increment;
};


class DiskLocalDirectoryIterator : public IDiskDirectoryIterator
{
public:
    explicit DiskLocalDirectoryIterator(const String & disk_path_, const String & dir_path_)
        : dir_path(dir_path_), iter(disk_path_ + dir_path_)
    {
    }

    void next() override { ++iter; }

    bool isValid() const override { return iter != Poco::DirectoryIterator(); }

    String path() const override
    {
        if (iter->isDirectory())
            return dir_path + iter.name() + '/';
        else
            return dir_path + iter.name();
    }

    String name() const override { return iter.name(); }

private:
    String dir_path;
    Poco::DirectoryIterator iter;
};


ReservationPtr DiskLocal::reserve(UInt64 bytes)
{
    if (!tryReserve(bytes))
        return {};
    return std::make_unique<DiskLocalReservation>(std::static_pointer_cast<DiskLocal>(shared_from_this()), bytes);
}

bool DiskLocal::tryReserve(UInt64 bytes)
{
    std::lock_guard lock(DiskLocal::reservation_mutex);
    if (bytes == 0)
    {
        LOG_DEBUG(&Poco::Logger::get("DiskLocal"), "Reserving 0 bytes on disk {}", backQuote(name));
        ++reservation_count;
        return true;
    }

    auto available_space = getAvailableSpace();
    UInt64 unreserved_space = available_space - std::min(available_space, reserved_bytes);
    if (unreserved_space >= bytes)
    {
        LOG_DEBUG(&Poco::Logger::get("DiskLocal"), "Reserving {} on disk {}, having unreserved {}.",
            ReadableSize(bytes), backQuote(name), ReadableSize(unreserved_space));
        ++reservation_count;
        reserved_bytes += bytes;
        return true;
    }
    return false;
}

UInt64 DiskLocal::getTotalSpace() const
{
    struct statvfs fs;
    if (name == "default") /// for default disk we get space from path/data/
        fs = getStatVFS(disk_path + "data/");
    else
        fs = getStatVFS(disk_path);
    UInt64 total_size = fs.f_blocks * fs.f_bsize;
    if (total_size < keep_free_space_bytes)
        return 0;
    return total_size - keep_free_space_bytes;
}

UInt64 DiskLocal::getAvailableSpace() const
{
    /// we use f_bavail, because part of b_free space is
    /// available for superuser only and for system purposes
    struct statvfs fs;
    if (name == "default") /// for default disk we get space from path/data/
        fs = getStatVFS(disk_path + "data/");
    else
        fs = getStatVFS(disk_path);
    UInt64 total_size = fs.f_bavail * fs.f_bsize;
    if (total_size < keep_free_space_bytes)
        return 0;
    return total_size - keep_free_space_bytes;
}

UInt64 DiskLocal::getUnreservedSpace() const
{
    std::lock_guard lock(DiskLocal::reservation_mutex);
    auto available_space = getAvailableSpace();
    available_space -= std::min(available_space, reserved_bytes);
    return available_space;
}

bool DiskLocal::exists(const String & path) const
{
    return Poco::File(disk_path + path).exists();
}

bool DiskLocal::isFile(const String & path) const
{
    return Poco::File(disk_path + path).isFile();
}

bool DiskLocal::isDirectory(const String & path) const
{
    return Poco::File(disk_path + path).isDirectory();
}

size_t DiskLocal::getFileSize(const String & path) const
{
    return Poco::File(disk_path + path).getSize();
}

void DiskLocal::createDirectory(const String & path)
{
    Poco::File(disk_path + path).createDirectory();
}

void DiskLocal::createDirectories(const String & path)
{
    Poco::File(disk_path + path).createDirectories();
}

void DiskLocal::clearDirectory(const String & path)
{
    std::vector<Poco::File> files;
    Poco::File(disk_path + path).list(files);
    for (auto & file : files)
        file.remove();
}

void DiskLocal::moveDirectory(const String & from_path, const String & to_path)
{
    Poco::File(disk_path + from_path).renameTo(disk_path + to_path);
}

DiskDirectoryIteratorPtr DiskLocal::iterateDirectory(const String & path)
{
    return std::make_unique<DiskLocalDirectoryIterator>(disk_path, path);
}

void DiskLocal::moveFile(const String & from_path, const String & to_path)
{
    Poco::File(disk_path + from_path).renameTo(disk_path + to_path);
}

void DiskLocal::replaceFile(const String & from_path, const String & to_path)
{
    Poco::File from_file(disk_path + from_path);
    Poco::File to_file(disk_path + to_path);
    if (to_file.exists())
    {
        Poco::File tmp_file(disk_path + to_path + ".old");
        to_file.renameTo(tmp_file.path());
        from_file.renameTo(disk_path + to_path);
        tmp_file.remove();
    }
    else
        from_file.renameTo(to_file.path());
}

void DiskLocal::copyFile(const String & from_path, const String & to_path)
{
    Poco::File(disk_path + from_path).copyTo(disk_path + to_path);
}

std::unique_ptr<ReadBufferFromFileBase>
DiskLocal::readFile(const String & path, size_t buf_size, size_t estimated_size, size_t aio_threshold, size_t mmap_threshold) const
{
    return createReadBufferFromFileBase(disk_path + path, estimated_size, aio_threshold, mmap_threshold, buf_size);
}

std::unique_ptr<WriteBufferFromFileBase>
DiskLocal::writeFile(const String & path, size_t buf_size, WriteMode mode, size_t estimated_size, size_t aio_threshold)
{
    int flags = (mode == WriteMode::Append) ? (O_APPEND | O_CREAT | O_WRONLY) : -1;
    return createWriteBufferFromFileBase(disk_path + path, estimated_size, aio_threshold, buf_size, flags);
}

void DiskLocal::remove(const String & path)
{
    Poco::File(disk_path + path).remove(false);
}

void DiskLocal::removeRecursive(const String & path)
{
    Poco::File(disk_path + path).remove(true);
}

void DiskLocal::listFiles(const String & path, std::vector<String> & file_names)
{
    Poco::File(disk_path + path).list(file_names);
}

void DiskLocal::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    Poco::File(disk_path + path).setLastModified(timestamp);
}

Poco::Timestamp DiskLocal::getLastModified(const String & path)
{
    return Poco::File(disk_path + path).getLastModified();
}

void DiskLocal::createHardLink(const String & src_path, const String & dst_path)
{
    DB::createHardLink(disk_path + src_path, disk_path + dst_path);
}

void DiskLocal::truncateFile(const String & path, size_t size)
{
    int res = truncate((disk_path + path).c_str(), size);
    if (-1 == res)
        throwFromErrnoWithPath("Cannot truncate file " + path, path, ErrorCodes::CANNOT_TRUNCATE_FILE);
}

void DiskLocal::createFile(const String & path)
{
    Poco::File(disk_path + path).createFile();
}

void DiskLocal::setReadOnly(const String & path)
{
    Poco::File(disk_path + path).setReadOnly(true);
}

bool inline isSameDiskType(const IDisk & one, const IDisk & another)
{
    return typeid(one) == typeid(another);
}

void DiskLocal::copy(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path)
{
    if (isSameDiskType(*this, *to_disk))
        Poco::File(disk_path + from_path).copyTo(to_disk->getPath() + to_path); /// Use more optimal way.
    else
        IDisk::copy(from_path, to_disk, to_path); /// Copy files through buffers.
}

int DiskLocal::open(const String & path, mode_t mode) const
{
    String full_path = disk_path + path;
    int fd = ::open(full_path.c_str(), mode);
    if (-1 == fd)
        throwFromErrnoWithPath("Cannot open file " + full_path, full_path,
                        errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
    return fd;
}

void DiskLocal::close(int fd) const
{
    if (-1 == ::close(fd))
        throw Exception("Cannot close file", ErrorCodes::CANNOT_CLOSE_FILE);
}

void DiskLocal::sync(int fd) const
{
    if (-1 == ::fsync(fd))
        throw Exception("Cannot fsync", ErrorCodes::CANNOT_FSYNC);
}

DiskPtr DiskLocalReservation::getDisk(size_t i) const
{
    if (i != 0)
    {
        throw Exception("Can't use i != 0 with single disk reservation", ErrorCodes::INCORRECT_DISK_INDEX);
    }
    return disk;
}

void DiskLocalReservation::update(UInt64 new_size)
{
    std::lock_guard lock(DiskLocal::reservation_mutex);
    disk->reserved_bytes -= size;
    size = new_size;
    disk->reserved_bytes += size;
}


DiskLocalReservation::~DiskLocalReservation()
{
    try
    {
        std::lock_guard lock(DiskLocal::reservation_mutex);
        if (disk->reserved_bytes < size)
        {
            disk->reserved_bytes = 0;
            LOG_ERROR(&Poco::Logger::get("DiskLocal"), "Unbalanced reservations size for disk '{}'.", disk->getName());
        }
        else
        {
            disk->reserved_bytes -= size;
        }

        if (disk->reservation_count == 0)
            LOG_ERROR(&Poco::Logger::get("DiskLocal"), "Unbalanced reservation count for disk '{}'.", disk->getName());
        else
            --disk->reservation_count;
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void registerDiskLocal(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      const Context & context) -> DiskPtr {
        String path = config.getString(config_prefix + ".path", "");
        if (name == "default")
        {
            if (!path.empty())
                throw Exception(
                    "\"default\" disk path should be provided in <path> not it <storage_configuration>",
                    ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
            path = context.getPath();
        }
        else
        {
            if (path.empty())
                throw Exception("Disk path can not be empty. Disk " + name, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
            if (path.back() != '/')
                throw Exception("Disk path must end with /. Disk " + name, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
        }

        if (Poco::File disk{path}; !disk.canRead() || !disk.canWrite())
        {
            throw Exception("There is no RW access to disk " + name + " (" + path + ")", ErrorCodes::PATH_ACCESS_DENIED);
        }

        bool has_space_ratio = config.has(config_prefix + ".keep_free_space_ratio");

        if (config.has(config_prefix + ".keep_free_space_bytes") && has_space_ratio)
            throw Exception(
                "Only one of 'keep_free_space_bytes' and 'keep_free_space_ratio' can be specified",
                ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

        UInt64 keep_free_space_bytes = config.getUInt64(config_prefix + ".keep_free_space_bytes", 0);

        if (has_space_ratio)
        {
            auto ratio = config.getDouble(config_prefix + ".keep_free_space_ratio");
            if (ratio < 0 || ratio > 1)
                throw Exception("'keep_free_space_ratio' have to be between 0 and 1", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
            String tmp_path = path;
            if (tmp_path.empty())
                tmp_path = context.getPath();

            // Create tmp disk for getting total disk space.
            keep_free_space_bytes = static_cast<UInt64>(DiskLocal("tmp", tmp_path, 0).getTotalSpace() * ratio);
        }

        return std::make_shared<DiskLocal>(name, path, keep_free_space_bytes);
    };
    factory.registerDiskType("local", creator);
}

}
