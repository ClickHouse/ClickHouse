#include "DiskLocal.h"
#include <Common/createHardLink.h>
#include "DiskFactory.h"

#include <Disks/LocalDirectorySyncGuard.h>
#include <Interpreters/Context.h>
#include <Common/filesystemHelpers.h>
#include <Common/quoteString.h>
#include <IO/createReadBufferFromFileBase.h>

#include <fstream>
#include <unistd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int PATH_ACCESS_DENIED;
    extern const int INCORRECT_DISK_INDEX;
    extern const int CANNOT_TRUNCATE_FILE;
    extern const int CANNOT_UNLINK;
    extern const int CANNOT_RMDIR;
}

std::mutex DiskLocal::reservation_mutex;


using DiskLocalPtr = std::shared_ptr<DiskLocal>;

static void loadDiskLocalConfig(const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextPtr context,
                      String & path,
                      UInt64 & keep_free_space_bytes)
{
    path = config.getString(config_prefix + ".path", "");
    if (name == "default")
    {
        if (!path.empty())
            throw Exception(
                "\"default\" disk path should be provided in <path> not it <storage_configuration>",
                ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
        path = context->getPath();
    }
    else
    {
        if (path.empty())
            throw Exception("Disk path can not be empty. Disk " + name, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
        if (path.back() != '/')
            throw Exception("Disk path must end with /. Disk " + name, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
    }

    if (!FS::canRead(path) || !FS::canWrite(path))
        throw Exception("There is no RW access to the disk " + name + " (" + path + ")", ErrorCodes::PATH_ACCESS_DENIED);

    bool has_space_ratio = config.has(config_prefix + ".keep_free_space_ratio");

    if (config.has(config_prefix + ".keep_free_space_bytes") && has_space_ratio)
        throw Exception(
            "Only one of 'keep_free_space_bytes' and 'keep_free_space_ratio' can be specified",
            ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

    keep_free_space_bytes = config.getUInt64(config_prefix + ".keep_free_space_bytes", 0);

    if (has_space_ratio)
    {
        auto ratio = config.getDouble(config_prefix + ".keep_free_space_ratio");
        if (ratio < 0 || ratio > 1)
            throw Exception("'keep_free_space_ratio' have to be between 0 and 1", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
        String tmp_path = path;
        if (tmp_path.empty())
            tmp_path = context->getPath();

        // Create tmp disk for getting total disk space.
        keep_free_space_bytes = static_cast<UInt64>(DiskLocal("tmp", tmp_path, 0).getTotalSpace() * ratio);
    }
}

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
        : dir_path(dir_path_), entry(fs::path(disk_path_) / dir_path_)
    {
    }

    void next() override { ++entry; }

    bool isValid() const override { return entry != fs::directory_iterator(); }

    String path() const override
    {
        if (entry->is_directory())
            return dir_path / entry->path().filename() / "";
        else
            return dir_path / entry->path().filename();
    }


    String name() const override { return entry->path().filename(); }

private:
    fs::path dir_path;
    fs::directory_iterator entry;
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
        LOG_DEBUG(log, "Reserving 0 bytes on disk {}", backQuote(name));
        ++reservation_count;
        return true;
    }

    auto available_space = getAvailableSpace();
    UInt64 unreserved_space = available_space - std::min(available_space, reserved_bytes);
    if (unreserved_space >= bytes)
    {
        LOG_DEBUG(log, "Reserving {} on disk {}, having unreserved {}.",
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
        fs = getStatVFS((fs::path(disk_path) / "data/").string());
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
        fs = getStatVFS((fs::path(disk_path) / "data/").string());
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
    return fs::exists(fs::path(disk_path) / path);
}

bool DiskLocal::isFile(const String & path) const
{
    return fs::is_regular_file(fs::path(disk_path) / path);
}

bool DiskLocal::isDirectory(const String & path) const
{
    return fs::is_directory(fs::path(disk_path) / path);
}

size_t DiskLocal::getFileSize(const String & path) const
{
    return fs::file_size(fs::path(disk_path) / path);
}

void DiskLocal::createDirectory(const String & path)
{
    fs::create_directory(fs::path(disk_path) / path);
}

void DiskLocal::createDirectories(const String & path)
{
    fs::create_directories(fs::path(disk_path) / path);
}

void DiskLocal::clearDirectory(const String & path)
{
    for (const auto & entry : fs::directory_iterator(fs::path(disk_path) / path))
        fs::remove(entry.path());
}

void DiskLocal::moveDirectory(const String & from_path, const String & to_path)
{
    fs::rename(fs::path(disk_path) / from_path, fs::path(disk_path) / to_path);
}

DiskDirectoryIteratorPtr DiskLocal::iterateDirectory(const String & path)
{
    return std::make_unique<DiskLocalDirectoryIterator>(disk_path, path);
}

void DiskLocal::moveFile(const String & from_path, const String & to_path)
{
    fs::rename(fs::path(disk_path) / from_path, fs::path(disk_path) / to_path);
}

void DiskLocal::replaceFile(const String & from_path, const String & to_path)
{
    fs::path from_file = fs::path(disk_path) / from_path;
    fs::path to_file = fs::path(disk_path) / to_path;
    fs::rename(from_file, to_file);
}

std::unique_ptr<ReadBufferFromFileBase> DiskLocal::readFile(const String & path, const ReadSettings & settings, std::optional<size_t> size) const
{
    return createReadBufferFromFileBase(fs::path(disk_path) / path, settings, size);
}

std::unique_ptr<WriteBufferFromFileBase>
DiskLocal::writeFile(const String & path, size_t buf_size, WriteMode mode)
{
    int flags = (mode == WriteMode::Append) ? (O_APPEND | O_CREAT | O_WRONLY) : -1;
    return std::make_unique<WriteBufferFromFile>(fs::path(disk_path) / path, buf_size, flags);
}

void DiskLocal::removeFile(const String & path)
{
    auto fs_path = fs::path(disk_path) / path;
    if (0 != unlink(fs_path.c_str()))
        throwFromErrnoWithPath("Cannot unlink file " + fs_path.string(), fs_path, ErrorCodes::CANNOT_UNLINK);
}

void DiskLocal::removeFileIfExists(const String & path)
{
    auto fs_path = fs::path(disk_path) / path;
    if (0 != unlink(fs_path.c_str()) && errno != ENOENT)
        throwFromErrnoWithPath("Cannot unlink file " + fs_path.string(), fs_path, ErrorCodes::CANNOT_UNLINK);
}

void DiskLocal::removeDirectory(const String & path)
{
    auto fs_path = fs::path(disk_path) / path;
    if (0 != rmdir(fs_path.c_str()))
        throwFromErrnoWithPath("Cannot rmdir " + fs_path.string(), fs_path, ErrorCodes::CANNOT_RMDIR);
}

void DiskLocal::removeRecursive(const String & path)
{
    fs::remove_all(fs::path(disk_path) / path);
}

void DiskLocal::listFiles(const String & path, std::vector<String> & file_names)
{
    file_names.clear();
    for (const auto & entry : fs::directory_iterator(fs::path(disk_path) / path))
        file_names.emplace_back(entry.path().filename());
}

void DiskLocal::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    FS::setModificationTime(fs::path(disk_path) / path, timestamp.epochTime());
}

Poco::Timestamp DiskLocal::getLastModified(const String & path)
{
    return FS::getModificationTimestamp(fs::path(disk_path) / path);
}

void DiskLocal::createHardLink(const String & src_path, const String & dst_path)
{
    DB::createHardLink(fs::path(disk_path) / src_path, fs::path(disk_path) / dst_path);
}

void DiskLocal::truncateFile(const String & path, size_t size)
{
    int res = truncate((fs::path(disk_path) / path).string().data(), size);
    if (-1 == res)
        throwFromErrnoWithPath("Cannot truncate file " + path, path, ErrorCodes::CANNOT_TRUNCATE_FILE);
}

void DiskLocal::createFile(const String & path)
{
    FS::createFile(fs::path(disk_path) / path);
}

void DiskLocal::setReadOnly(const String & path)
{
    fs::permissions(fs::path(disk_path) / path,
                    fs::perms::owner_write | fs::perms::group_write | fs::perms::others_write,
                    fs::perm_options::remove);
}

bool inline isSameDiskType(const IDisk & one, const IDisk & another)
{
    return typeid(one) == typeid(another);
}

void DiskLocal::copy(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path)
{
    if (isSameDiskType(*this, *to_disk))
    {
        fs::path to = fs::path(to_disk->getPath()) / to_path;
        fs::path from = fs::path(disk_path) / from_path;
        if (from_path.ends_with('/'))
            from = from.parent_path();
        if (fs::is_directory(from))
            to /= from.filename();

        fs::copy(from, to, fs::copy_options::recursive | fs::copy_options::overwrite_existing); /// Use more optimal way.
    }
    else
        copyThroughBuffers(from_path, to_disk, to_path); /// Base implementation.
}

SyncGuardPtr DiskLocal::getDirectorySyncGuard(const String & path) const
{
    return std::make_unique<LocalDirectorySyncGuard>(fs::path(disk_path) / path);
}


void DiskLocal::applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextPtr context, const String & config_prefix, const DisksMap &)
{
    String new_disk_path;
    UInt64 new_keep_free_space_bytes;

    loadDiskLocalConfig(name, config, config_prefix, context, new_disk_path, new_keep_free_space_bytes);

    if (disk_path != new_disk_path)
        throw Exception("Disk path can't be updated from config " + name, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);

    if (keep_free_space_bytes != new_keep_free_space_bytes)
        keep_free_space_bytes = new_keep_free_space_bytes;
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
            LOG_ERROR(disk->log, "Unbalanced reservations size for disk '{}'.", disk->getName());
        }
        else
        {
            disk->reserved_bytes -= size;
        }

        if (disk->reservation_count == 0)
            LOG_ERROR(disk->log, "Unbalanced reservation count for disk '{}'.", disk->getName());
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
                      ContextPtr context,
                      const DisksMap & /*map*/) -> DiskPtr {
        String path;
        UInt64 keep_free_space_bytes;
        loadDiskLocalConfig(name, config, config_prefix, context, path, keep_free_space_bytes);
        return std::make_shared<DiskLocal>(name, path, keep_free_space_bytes);
    };
    factory.registerDiskType("local", creator);
}

}
