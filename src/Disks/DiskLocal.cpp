#include "DiskLocal.h"
#include <Common/Throttler_fwd.h>
#include <Common/createHardLink.h>
#include "DiskFactory.h"

#include <Disks/LocalDirectorySyncGuard.h>
#include <Interpreters/Context.h>
#include <Common/filesystemHelpers.h>
#include <Common/quoteString.h>
#include <Common/atomicRename.h>
#include <Common/formatReadable.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <Disks/loadLocalDiskConfig.h>
#include <Disks/TemporaryFileOnDisk.h>

#include <filesystem>
#include <system_error>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include <Disks/IO/WriteBufferFromTemporaryFile.h>

#include <Common/randomSeed.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <pcg_random.hpp>
#include <Common/logger_useful.h>


namespace CurrentMetrics
{
    extern const Metric DiskSpaceReservedForMerge;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int PATH_ACCESS_DENIED;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_TRUNCATE_FILE;
    extern const int CANNOT_UNLINK;
    extern const int CANNOT_RMDIR;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_STAT;
}

std::mutex DiskLocal::reservation_mutex;


using DiskLocalPtr = std::shared_ptr<DiskLocal>;

std::optional<size_t> fileSizeSafe(const fs::path & path)
{
    std::error_code ec;

    size_t size = fs::file_size(path, ec);
    if (!ec)
        return size;

    if (ec == std::errc::no_such_file_or_directory)
        return std::nullopt;
    if (ec == std::errc::operation_not_supported)
        return std::nullopt;

    throw fs::filesystem_error("DiskLocal", path, ec);
}

class DiskLocalReservation : public IReservation
{
public:
    DiskLocalReservation(const DiskLocalPtr & disk_, UInt64 size_, UInt64 unreserved_space_)
        : disk(disk_)
        , size(size_)
        , unreserved_space(unreserved_space_)
        , metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size_)
    {}

    UInt64 getSize() const override { return size; }
    std::optional<UInt64> getUnreservedSpace() const override { return unreserved_space; }

    DiskPtr getDisk(size_t i) const override
    {
        if (i != 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't use i != 0 with single disk reservation. It's a bug");
        return disk;
    }

    Disks getDisks() const override { return {disk}; }

    void update(UInt64 new_size) override
    {
        std::lock_guard lock(DiskLocal::reservation_mutex);
        disk->reserved_bytes -= size;
        size = new_size;
        disk->reserved_bytes += size;
    }

    ~DiskLocalReservation() override
    {
        try
        {
            std::lock_guard lock(DiskLocal::reservation_mutex);
            if (disk->reserved_bytes < size)
            {
                disk->reserved_bytes = 0;
                LOG_ERROR(getLogger("DiskLocal"), "Unbalanced reservations size for disk '{}'.", disk->getName());
            }
            else
            {
                disk->reserved_bytes -= size;
            }

            if (disk->reservation_count == 0)
                LOG_ERROR(getLogger("DiskLocal"), "Unbalanced reservation count for disk '{}'.", disk->getName());
            else
                --disk->reservation_count;
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

private:
    DiskLocalPtr disk;
    UInt64 size;
    UInt64 unreserved_space;
    CurrentMetrics::Increment metric_increment;
};


class DiskLocalDirectoryIterator final : public IDirectoryIterator
{
public:
    DiskLocalDirectoryIterator() = default;
    DiskLocalDirectoryIterator(const String & disk_path_, const String & dir_path_)
        : dir_path(dir_path_), entry(fs::path(disk_path_) / dir_path_)
    {
    }

    void next() override { ++entry; }

    bool isValid() const override { return entry != fs::directory_iterator(); }

    String path() const override
    {
        if (entry->is_directory())
            return dir_path / entry->path().filename() / "";
        return dir_path / entry->path().filename();
    }

    String name() const override { return entry->path().filename(); }

private:
    fs::path dir_path;
    fs::directory_iterator entry;
};


ReservationPtr DiskLocal::reserve(UInt64 bytes)
{
    auto unreserved_space = tryReserve(bytes);
    if (!unreserved_space.has_value())
        return {};
    return std::make_unique<DiskLocalReservation>(
        std::static_pointer_cast<DiskLocal>(shared_from_this()),
        bytes, unreserved_space.value());
}

std::optional<UInt64> DiskLocal::tryReserve(UInt64 bytes)
{
    std::lock_guard lock(DiskLocal::reservation_mutex);

    auto available_space = getAvailableSpace();

    UInt64 unreserved_space = available_space
        ? *available_space - std::min(*available_space, reserved_bytes)
        : std::numeric_limits<UInt64>::max();

    if (bytes == 0)
    {
        LOG_TRACE(logger, "Reserved 0 bytes on local disk {}", backQuote(name));
        ++reservation_count;
        return {unreserved_space};
    }

    if (unreserved_space >= bytes)
    {
        if (available_space)
        {
            LOG_TRACE(
                logger,
                "Reserved {} on local disk {}, having unreserved {}.",
                ReadableSize(bytes),
                backQuote(name),
                ReadableSize(unreserved_space));
        }
        else
        {
            LOG_TRACE(
                logger,
                "Reserved {} on local disk {}.",
                ReadableSize(bytes),
                backQuote(name));
        }

        ++reservation_count;
        reserved_bytes += bytes;
        return {unreserved_space - bytes};
    }

    LOG_TRACE(logger, "Could not reserve {} on local disk {}. Not enough unreserved space", ReadableSize(bytes), backQuote(name));


    return {};
}

static UInt64 getTotalSpaceByName(const String & name, const String & disk_path, UInt64 keep_free_space_bytes)
{
    struct statvfs fs;
    if (name == "default") /// for default disk we get space from path/data/
        fs = getStatVFS((fs::path(disk_path) / "data" / "").string());
    else
        fs = getStatVFS(disk_path);
    UInt64 total_size = fs.f_blocks * fs.f_frsize;
    if (total_size < keep_free_space_bytes)
        return 0;
    return total_size - keep_free_space_bytes;
}

std::optional<UInt64> DiskLocal::getTotalSpace() const
{
    if (broken || readonly)
        return 0;
    return getTotalSpaceByName(name, disk_path, keep_free_space_bytes);
}

std::optional<UInt64> DiskLocal::getAvailableSpace() const
{
    if (broken || readonly)
        return 0;
    /// we use f_bavail, because part of b_free space is
    /// available for superuser only and for system purposes
    struct statvfs fs;
    if (name == "default") /// for default disk we get space from path/data/
        fs = getStatVFS((fs::path(disk_path) / "data" / "").string());
    else
        fs = getStatVFS(disk_path);
    UInt64 total_size = fs.f_bavail * fs.f_frsize;
    if (total_size < keep_free_space_bytes)
        return 0;
    return total_size - keep_free_space_bytes;
}

std::optional<UInt64> DiskLocal::getUnreservedSpace() const
{
    std::lock_guard lock(DiskLocal::reservation_mutex);
    auto available_space = *getAvailableSpace();
    available_space -= std::min(available_space, reserved_bytes);
    return available_space;
}

bool DiskLocal::existsFileOrDirectory(const String & path) const
{
    return fs::exists(fs::path(disk_path) / path);
}

bool DiskLocal::existsFile(const String & path) const
{
    return fs::is_regular_file(fs::path(disk_path) / path);
}

bool DiskLocal::existsDirectory(const String & path) const
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
        (void)fs::remove(entry.path());
}

void DiskLocal::moveDirectory(const String & from_path, const String & to_path)
{
    fs::rename(fs::path(disk_path) / from_path, fs::path(disk_path) / to_path);
}

DirectoryIteratorPtr DiskLocal::iterateDirectory(const String & path) const
{
    fs::path meta_path = fs::path(disk_path) / path;
    if (!broken && fs::exists(meta_path) && fs::is_directory(meta_path))
        return std::make_unique<DiskLocalDirectoryIterator>(disk_path, path);
    return std::make_unique<DiskLocalDirectoryIterator>();
}

void DiskLocal::moveFile(const String & from_path, const String & to_path)
{
    renameNoReplace(fs::path(disk_path) / from_path, fs::path(disk_path) / to_path);
}

void DiskLocal::replaceFile(const String & from_path, const String & to_path)
{
    fs::path from_file = fs::path(disk_path) / from_path;
    fs::path to_file = fs::path(disk_path) / to_path;
    fs::create_directories(to_file.parent_path());
    fs::rename(from_file, to_file);
}

void DiskLocal::renameExchange(const std::string & old_path, const std::string & new_path)
{
    DB::renameExchange(fs::path(disk_path) / old_path, fs::path(disk_path) / new_path);
}

bool DiskLocal::renameExchangeIfSupported(const std::string & old_path, const std::string & new_path)
{
    return DB::renameExchangeIfSupported(fs::path(disk_path) / old_path, fs::path(disk_path) / new_path);
}

std::unique_ptr<ReadBufferFromFileBase> DiskLocal::readFile(const String & path, const ReadSettings & settings, std::optional<size_t> read_hint, std::optional<size_t> file_size) const
{
    if (!file_size.has_value())
        file_size = fileSizeSafe(fs::path(disk_path) / path);
    return createReadBufferFromFileBase(fs::path(disk_path) / path, settings, read_hint, file_size);
}

std::unique_ptr<WriteBufferFromFileBase>
DiskLocal::writeFile(const String & path, size_t buf_size, WriteMode mode, const WriteSettings & settings)
{
    int flags = (mode == WriteMode::Append) ? (O_APPEND | O_CREAT | O_WRONLY) : -1;
    return std::make_unique<WriteBufferFromFile>(
        fs::path(disk_path) / path,
        buf_size,
        flags,
        settings.local_throttler,
        0666,
        nullptr,
        0,
        settings.use_adaptive_write_buffer,
        settings.adaptive_write_buffer_initial_size);
}

std::vector<String> DiskLocal::getBlobPath(const String & path) const
{
    auto fs_path = fs::path(disk_path) / path;
    return {fs_path};
}

void DiskLocal::writeFileUsingBlobWritingFunction(const String & path, WriteMode mode, WriteBlobFunction && write_blob_function)
{
    auto fs_path = fs::path(disk_path) / path;
    std::move(write_blob_function)({fs_path}, mode, {});
}

void DiskLocal::removeFile(const String & path)
{
    auto fs_path = fs::path(disk_path) / path;
    if (0 != unlink(fs_path.c_str()))
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_UNLINK, fs_path, "Cannot unlink file {}", fs_path);
}

void DiskLocal::removeFileIfExists(const String & path)
{
    auto fs_path = fs::path(disk_path) / path;
    if (0 != unlink(fs_path.c_str()))
    {
        if (errno != ENOENT)
            ErrnoException::throwFromPath(ErrorCodes::CANNOT_UNLINK, fs_path, "Cannot unlink file {}", fs_path);
    }
}

void DiskLocal::removeDirectory(const String & path)
{
    auto fs_path = fs::path(disk_path) / path;
    if (0 != rmdir(fs_path.c_str()))
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_RMDIR, fs_path, "Cannot remove directory {}", fs_path);
}

void DiskLocal::removeDirectoryIfExists(const String & path)
{
    auto fs_path = fs::path(disk_path) / path;
    if (!existsDirectory(fs_path))
        return;
    if (0 != rmdir(fs_path.c_str()))
        if (errno != ENOENT)
            ErrnoException::throwFromPath(ErrorCodes::CANNOT_RMDIR, fs_path, "Cannot remove directory {}", fs_path);
}

void DiskLocal::removeRecursive(const String & path)
{
    (void)fs::remove_all(fs::path(disk_path) / path);
}

void DiskLocal::listFiles(const String & path, std::vector<String> & file_names) const
{
    file_names.clear();
    for (const auto & entry : fs::directory_iterator(fs::path(disk_path) / path))
        file_names.emplace_back(entry.path().filename());
}

void DiskLocal::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    FS::setModificationTime(fs::path(disk_path) / path, timestamp.epochTime());
}

Poco::Timestamp DiskLocal::getLastModified(const String & path) const
{
    return FS::getModificationTimestamp(fs::path(disk_path) / path);
}

time_t DiskLocal::getLastChanged(const String & path) const
{
    return FS::getChangeTime(fs::path(disk_path) / path);
}

void DiskLocal::createHardLink(const String & src_path, const String & dst_path)
{
    DB::createHardLink(fs::path(disk_path) / src_path, fs::path(disk_path) / dst_path);
}

bool DiskLocal::isSymlink(const String & path) const
{
    return FS::isSymlink(fs::path(disk_path) / path);
}

bool DiskLocal::isSymlinkNoThrow(const String & path) const
{
    return FS::isSymlinkNoThrow(fs::path(disk_path) / path);
}

void DiskLocal::createDirectorySymlink(const String & target, const String & link)
{
    auto link_path_inside_disk = fs::path(disk_path) / link;
    /// Symlinks will be relative.
    fs::create_directory_symlink(fs::proximate(fs::path(disk_path) / target, link_path_inside_disk.parent_path()), link_path_inside_disk);
}

String DiskLocal::readSymlink(const fs::path & path) const
{
    return FS::readSymlink(fs::path(disk_path) / path);
}

bool DiskLocal::equivalent(const String & p1, const String & p2) const
{
    return fs::equivalent(fs::path(disk_path) / p1, fs::path(disk_path) / p2);
}

bool DiskLocal::equivalentNoThrow(const String & p1, const String & p2) const
{
    std::error_code ec;
    return fs::equivalent(fs::path(disk_path) / p1, fs::path(disk_path) / p2, ec);
}

void DiskLocal::truncateFile(const String & path, size_t size)
{
    int res = truncate((fs::path(disk_path) / path).string().data(), size);
    if (-1 == res)
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_TRUNCATE_FILE, path, "Cannot truncate {}", path);
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

void DiskLocal::copyDirectoryContent(
    const String & from_dir,
    const std::shared_ptr<IDisk> & to_disk,
    const String & to_dir,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    const std::function<void()> & cancellation_hook)
{
    /// If throttling was configured we cannot use copying directly.
    if (isSameDiskType(*this, *to_disk) && !read_settings.local_throttler && !write_settings.local_throttler)
        fs::copy(fs::path(disk_path) / from_dir, fs::path(to_disk->getPath()) / to_dir, fs::copy_options::recursive | fs::copy_options::overwrite_existing); /// Use more optimal way.
    else
        IDisk::copyDirectoryContent(from_dir, to_disk, to_dir, read_settings, write_settings, cancellation_hook);
}

SyncGuardPtr DiskLocal::getDirectorySyncGuard(const String & path) const
{
    return std::make_unique<LocalDirectorySyncGuard>(fs::path(disk_path) / path);
}


void DiskLocal::applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextPtr context, const String & config_prefix, const DisksMap & disk_map)
{
    String new_disk_path;
    UInt64 new_keep_free_space_bytes;

    loadDiskLocalConfig(name, config, config_prefix, context, new_disk_path, new_keep_free_space_bytes);

    if (disk_path != new_disk_path)
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Disk path can't be updated from config {}", name);

    if (keep_free_space_bytes != new_keep_free_space_bytes)
        keep_free_space_bytes = new_keep_free_space_bytes;

    IDisk::applyNewSettings(config, context, config_prefix, disk_map);
}

DiskLocal::DiskLocal(const String & name_, const String & path_, UInt64 keep_free_space_bytes_,
                     const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
    : IDisk(name_, config, config_prefix)
    , disk_path(path_)
    , keep_free_space_bytes(keep_free_space_bytes_)
    , logger(getLogger("DiskLocal"))
    , data_source_description(getLocalDataSourceDescription(disk_path))
{
}

DiskLocal::DiskLocal(
    const String & name_, const String & path_, UInt64 keep_free_space_bytes_, ContextPtr context,
    const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
    : DiskLocal(name_, path_, keep_free_space_bytes_, config, config_prefix)
{
    auto local_disk_check_period_ms = config.getUInt("local_disk_check_period_ms", 0);
    if (local_disk_check_period_ms > 0)
        disk_checker = std::make_unique<DiskLocalCheckThread>(this, context, local_disk_check_period_ms);
}

DiskLocal::DiskLocal(const String & name_, const String & path_)
    : IDisk(name_)
    , disk_path(path_)
    , keep_free_space_bytes(0)
    , logger(getLogger("DiskLocal"))
    , data_source_description(getLocalDataSourceDescription(disk_path))
{
}

DataSourceDescription DiskLocal::getDataSourceDescription() const
{
    return data_source_description;
}

DataSourceDescription DiskLocal::getLocalDataSourceDescription(const String & path)
{
    DataSourceDescription res;
    res.type = DataSourceType::Local;

    if (auto block_device_id = tryGetBlockDeviceId(path); block_device_id.has_value())
        res.description = *block_device_id;
    else
        res.description = path;
    res.is_encrypted = false;
    res.is_cached = false;
    return res;
}

void DiskLocal::shutdown()
{
    if (disk_checker)
        disk_checker->shutdown();
}

std::optional<UInt32> DiskLocal::readDiskCheckerMagicNumber() const noexcept
try
{
    ReadSettings read_settings;
    /// Proper disk read checking requires direct io
    read_settings.direct_io_threshold = 1;
    auto buf = readFile(disk_checker_path, read_settings, {}, {});
    UInt32 magic_number;
    readIntBinary(magic_number, *buf);
    if (buf->eof())
        return magic_number;
    LOG_WARNING(logger, "The size of disk check magic number is more than 4 bytes. Mark it as read failure");
    return {};
}
catch (...)
{
    tryLogCurrentException(logger, fmt::format("Cannot read correct disk check magic number from from {}{}", disk_path, disk_checker_path));
    return {};
}

bool DiskLocal::canRead() const noexcept
try
{
    if (FS::canRead(fs::path(disk_path) / disk_checker_path))
    {
        auto magic_number = readDiskCheckerMagicNumber();
        if (magic_number && *magic_number == disk_checker_magic_number)
            return true;
    }
    return false;
}
catch (...)
{
    LOG_WARNING(logger, "Cannot achieve read over the disk directory: {}", disk_path);
    return false;
}

struct DiskWriteCheckData
{
    constexpr static size_t PAGE_SIZE_IN_BYTES = 4096;
    char data[PAGE_SIZE_IN_BYTES]{};
    DiskWriteCheckData()
    {
        static const char * magic_string = "ClickHouse disk local write check";
        static size_t magic_string_len = strlen(magic_string);
        memcpy(data, magic_string, magic_string_len);
        memcpy(data + PAGE_SIZE_IN_BYTES - magic_string_len, magic_string, magic_string_len);
    }
};

bool DiskLocal::canWrite() noexcept
try
{
    static DiskWriteCheckData data;
    {
        auto disk_ptr = std::static_pointer_cast<DiskLocal>(shared_from_this());
        auto tmp_file = std::make_unique<TemporaryFileOnDisk>(disk_ptr);
        auto buf = std::make_unique<WriteBufferFromTemporaryFile>(std::move(tmp_file));
        buf->write(data.data, DiskWriteCheckData::PAGE_SIZE_IN_BYTES);
        buf->finalize();
        buf->sync();
    }
    return true;
}
catch (...)
{
    LOG_WARNING(logger, "Cannot achieve write over the disk directory: {}", disk_path);
    return false;
}

void DiskLocal::checkAccessImpl(const String & path)
{
    try
    {
        fs::create_directories(disk_path);
        if (!FS::canWrite(disk_path))
        {
            LOG_ERROR(logger, "Cannot write to the root directory of disk {} ({}).", name, disk_path);
            readonly = true;
            return;
        }
    }
    catch (...)
    {
        LOG_ERROR(logger, "Cannot create the root directory of disk {} ({}).", name, disk_path);
        readonly = true;
        return;
    }

    IDisk::checkAccessImpl(path);
}

void DiskLocal::setup()
{
    try
    {
        if (!FS::canRead(disk_path))
            throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "There is no read access to disk {} ({}).", name, disk_path);
    }
    catch (...)
    {
        LOG_ERROR(logger, "Cannot gain read access of the disk directory: {}", disk_path);
        throw;
    }

    /// If disk checker is disabled, just assume RW by default.
    if (!disk_checker)
        return;

    try
    {
        if (existsFile(disk_checker_path))
        {
            auto magic_number = readDiskCheckerMagicNumber();
            if (magic_number)
                disk_checker_magic_number = *magic_number;
            else
            {
                /// The checker file is incorrect. Mark the magic number to uninitialized and try to generate a new checker file.
                disk_checker_magic_number = -1;
            }
        }
    }
    catch (...)
    {
        LOG_ERROR(logger, "We cannot tell if {} exists anymore, or read from it. Most likely disk {} is broken", disk_checker_path, name);
        throw;
    }

    /// Try to create a new checker file. The disk status can be either broken or readonly.
    if (disk_checker_magic_number == -1)
    {
        try
        {
            pcg32_fast rng(randomSeed());
            UInt32 magic_number = rng();
            {
                auto buf = writeFile(disk_checker_path, 32, WriteMode::Rewrite, {});
                writeIntBinary(magic_number, *buf);
                buf->finalize();
            }
            disk_checker_magic_number = magic_number;
        }
        catch (...)
        {
            LOG_WARNING(
                logger,
                "Cannot create/write to {0}. Disk {1} is either readonly or broken. Without setting up disk checker file, DiskLocalCheckThread "
                "will not be started. Disk is assumed to be RW. Try manually fix the disk and do `SYSTEM RESTART DISK {1}`",
                disk_checker_path,
                name);
            disk_checker_can_check_read = false;
            return;
        }
    }

    if (disk_checker_magic_number == -1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "disk_checker_magic_number is not initialized. It's a bug");
}

void DiskLocal::startupImpl()
{
    broken = false;
    disk_checker_magic_number = -1;
    disk_checker_can_check_read = true;

    try
    {
        setup();
    }
    catch (...)
    {
        tryLogCurrentException(logger, fmt::format("Disk {} is marked as broken during startup", name));
        broken = true;
        /// Disk checker is disabled when failing to start up.
        disk_checker_can_check_read = false;
    }
    if (disk_checker && disk_checker_can_check_read)
        disk_checker->startup();
}

struct stat DiskLocal::stat(const String & path) const
{
    struct stat st;
    auto full_path = fs::path(disk_path) / path;
    if (::stat(full_path.string().c_str(), &st) == 0)
        return st;
    DB::ErrnoException::throwFromPath(DB::ErrorCodes::CANNOT_STAT, path, "Cannot stat file: {}", path);
}

void DiskLocal::chmod(const String & path, mode_t mode)
{
    auto full_path = fs::path(disk_path) / path;
    if (::chmod(full_path.string().c_str(), mode) == 0)
        return;
    DB::ErrnoException::throwFromPath(DB::ErrorCodes::PATH_ACCESS_DENIED, path, "Cannot chmod file: {}", path);
}

void registerDiskLocal(DiskFactory & factory, bool global_skip_access_check)
{
    auto creator = [global_skip_access_check](
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap & map,
        bool, bool) -> DiskPtr
    {
        String path;
        UInt64 keep_free_space_bytes;
        loadDiskLocalConfig(name, config, config_prefix, context, path, keep_free_space_bytes);

        for (const auto & [disk_name, disk_ptr] : map)
            if (path == disk_ptr->getPath())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk {} and disk {} cannot have the same path ({})", name, disk_name, path);

        bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);
        std::shared_ptr<IDisk> disk
            = std::make_shared<DiskLocal>(name, path, keep_free_space_bytes, context, config, config_prefix);
        disk->startup(skip_access_check);
        return disk;
    };
    factory.registerDiskType("local", creator);
}

}
