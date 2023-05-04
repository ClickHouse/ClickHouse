#include "DiskRemote.h"
#include <Common/createHardLink.h>
#include "DiskFactory.h"

#include <Disks/IO/ReadBufferFromRemoteDisk.h>
#include <Disks/IO/WriteBufferFromRemoteDisk.h>
#include <Disks/LocalDirectorySyncGuard.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Interpreters/Context.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/atomicRename.h>
#include <Common/filesystemHelpers.h>
#include <Common/quoteString.h>

#include <fstream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include <Disks/DiskFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/logger_useful.h>
#include <Common/randomSeed.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

std::mutex DiskRemote::reservation_mutex;

using DiskRemotePtr = std::shared_ptr<DiskRemote>;

class DiskRemoteReservation : public IReservation
{
public:
    DiskRemoteReservation(const DiskRemotePtr & disk_, UInt64 size_, UInt64 unreserved_space_)
        : disk(disk_), size(size_), unreserved_space(unreserved_space_)
    {
    }

    UInt64 getSize() const override { return size; }
    UInt64 getUnreservedSpace() const override { return unreserved_space; }

    DiskPtr getDisk(size_t i) const override
    {
        if (i != 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't use i != 0 with single disk reservation. It's a bug");
        return disk;
    }

    Disks getDisks() const override { return {disk}; }

    void update(UInt64 new_size) override
    {
        std::lock_guard lock(DiskRemote::reservation_mutex);
        disk->reserved_bytes -= size;
        size = new_size;
        disk->reserved_bytes += size;
    }

    ~DiskRemoteReservation() override
    {
        try
        {
            std::lock_guard lock(DiskRemote::reservation_mutex);
            if (disk->reserved_bytes < size)
            {
                disk->reserved_bytes = 0;
                LOG_ERROR(&Poco::Logger::get("DiskRemote"), "Unbalanced reservations size for disk '{}'.", disk->getName());
            }
            else
            {
                disk->reserved_bytes -= size;
            }

            if (disk->reservation_count == 0)
                LOG_ERROR(&Poco::Logger::get("DiskRemote"), "Unbalanced reservation count for disk '{}'.", disk->getName());
            else
                --disk->reservation_count;
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

private:
    DiskRemotePtr disk;
    UInt64 size;
    UInt64 unreserved_space;
};

class DiskRemoteDirectoryIterator final : public IDirectoryIterator
{
public:
    DiskRemoteDirectoryIterator() = default;
    DiskRemoteDirectoryIterator(RemoteFSConnectionPool::Entry && conn_, const String & dir_path_) : conn(conn_), dir_path(dir_path_)
    {
        conn->startIterateDirectory(dir_path);
        next();
    }

    ~DiskRemoteDirectoryIterator() override
    {
        // Should read all data from connection before returning it to pool
        while (valid)
            valid = conn->nextDirectoryIteratorEntry(entry);
    }

    void next() override
    {
        if (!valid)
            return;
        valid = conn->nextDirectoryIteratorEntry(entry);
        if (valid)
            trimRight(entry, '/');
    }

    bool isValid() const override { return valid; }

    String path() const override { return entry; }


    String name() const override { return fs::path(entry).filename(); }

private:
    RemoteFSConnectionPool::Entry conn;
    String dir_path;
    String entry;
    bool valid;
};

DiskRemote::DiskRemote(
    const String & name_, const String & host_, UInt16 port_, const String & remote_disk_name_, unsigned max_connections_)
    : IDisk(name_)
    , host(host_)
    , port(port_)
    , remote_disk_name(remote_disk_name_)
    , disk_path(fmt::format("{}:{}/{}", host, port, remote_disk_name)) // TODO maybe change this
    , timeouts( // TODO get timeouts from somewhere else
          Poco::Timespan(1000000), /// Connection timeout.
          Poco::Timespan(1000000), /// Send timeout.
          Poco::Timespan(1000000) /// Receive timeout.
          )
    , conn_pool(max_connections_, host_, port_, remote_disk_name_)
    , logger(&Poco::Logger::get("DiskRemote"))
{
    data_source_description.type = DataSourceType::Remote;
    data_source_description.description = disk_path;
    data_source_description.is_cached = false;
    data_source_description.is_encrypted = false;
}

const String & DiskRemote::getPath() const
{
    return disk_path;
}

ReservationPtr DiskRemote::reserve(UInt64 bytes)
{
    auto unreserved_space = tryReserve(bytes);
    if (!unreserved_space.has_value())
        return {};
    return std::make_unique<DiskRemoteReservation>(
        std::static_pointer_cast<DiskRemote>(shared_from_this()), bytes, unreserved_space.value());
}

std::optional<UInt64> DiskRemote::tryReserve(UInt64 bytes)
{
    std::lock_guard lock(DiskRemote::reservation_mutex);

    UInt64 available_space = getAvailableSpace();
    UInt64 unreserved_space = available_space - std::min(available_space, reserved_bytes);

    if (bytes == 0)
    {
        LOG_TRACE(logger, "Reserved 0 bytes on local disk {}", backQuote(name));
        ++reservation_count;
        return {unreserved_space};
    }

    if (unreserved_space >= bytes)
    {
        LOG_TRACE(
            logger,
            "Reserved {} on local disk {}, having unreserved {}.",
            ReadableSize(bytes),
            backQuote(name),
            ReadableSize(unreserved_space));
        ++reservation_count;
        reserved_bytes += bytes;
        return {unreserved_space - bytes};
    }
    else
    {
        LOG_TRACE(logger, "Could not reserve {} on local disk {}. Not enough unreserved space", ReadableSize(bytes), backQuote(name));
    }

    return {};
}

UInt64 DiskRemote::getTotalSpace() const
{
    auto conn = conn_pool.get(timeouts, true);
    return conn->getTotalSpace();
}

UInt64 DiskRemote::getAvailableSpace() const
{
    auto conn = conn_pool.get(timeouts, true);
    return conn->getAvailableSpace();
}

UInt64 DiskRemote::getUnreservedSpace() const
{
    std::lock_guard lock(DiskRemote::reservation_mutex);
    auto available_space = getAvailableSpace();
    available_space -= std::min(available_space, reserved_bytes);
    return available_space;
}

bool DiskRemote::exists(const String & path) const
{
    auto conn = conn_pool.get(timeouts, true);
    return conn->exists(path);
}

bool DiskRemote::isFile(const String & path) const
{
    auto conn = conn_pool.get(timeouts, true);
    return conn->isFile(path);
}

bool DiskRemote::isDirectory(const String & path) const
{
    auto conn = conn_pool.get(timeouts, true);
    return conn->isDirectory(path);
}

size_t DiskRemote::getFileSize(const String & path) const
{
    auto conn = conn_pool.get(timeouts, true);
    return conn->getFileSize(path);
}

void DiskRemote::createDirectory(const String & path)
{
    auto conn = conn_pool.get(timeouts, true);
    conn->createDirectory(path);
}

void DiskRemote::createDirectories(const String & path)
{
    auto conn = conn_pool.get(timeouts, true);
    conn->createDirectories(path);
}

void DiskRemote::clearDirectory(const String & path)
{
    auto conn = conn_pool.get(timeouts, true);
    conn->clearDirectory(path);
}

void DiskRemote::moveDirectory(const String & from_path, const String & to_path)
{
    auto conn = conn_pool.get(timeouts, true);
    conn->moveDirectory(from_path, to_path);
}

DirectoryIteratorPtr DiskRemote::iterateDirectory(const String & path) const
{
    return std::make_unique<DiskRemoteDirectoryIterator>(conn_pool.get(timeouts, true), path);
}

void DiskRemote::createFile(const String & path)
{
    auto conn = conn_pool.get(timeouts, true);
    conn->createFile(path);
}

void DiskRemote::moveFile(const String & from_path, const String & to_path)
{
    auto conn = conn_pool.get(timeouts, true);
    conn->moveFile(from_path, to_path);
}

void DiskRemote::replaceFile(const String & from_path, const String & to_path)
{
    auto conn = conn_pool.get(timeouts, true);
    conn->replaceFile(from_path, to_path);
}

void DiskRemote::copy(const String & from_path, const std::shared_ptr<IDisk> & to_disk, const String & to_path)
{
    if (to_disk.get() == this)
    {
        auto conn = conn_pool.get(timeouts, true);
        conn->copy(from_path, to_path);
    }
    else
        copyThroughBuffers(from_path, to_disk, to_path, /* copy_root_dir */ true); /// Base implementation.
}

void DiskRemote::copyDirectoryContent(const String & from_dir, const std::shared_ptr<IDisk> & to_disk, const String & to_dir)
{
    if (to_disk.get() == this)
    {
        auto conn = conn_pool.get(timeouts, true);
        conn->copyDirectoryContent(from_dir, to_dir);
    }
    else
        copyThroughBuffers(from_dir, to_disk, to_dir, /* copy_root_dir */ false); /// Base implementation.
}

void DiskRemote::listFiles(const String & path, std::vector<String> & file_names) const
{
    auto conn = conn_pool.get(timeouts, true);
    conn->listFiles(path, file_names);
}

std::unique_ptr<ReadBufferFromFileBase> DiskRemote::readFile(
    const String & path, const ReadSettings & settings, std::optional<size_t> /*read_hint*/, std::optional<size_t> /*file_size*/) const
{
    auto conn = conn_pool.get(timeouts, true);
    return std::make_unique<ReadBufferFromRemoteDisk>(conn, path, settings);
}

std::unique_ptr<WriteBufferFromFileBase> DiskRemote::writeFile(const String & path, size_t buf_size, WriteMode mode, const WriteSettings &)
{
    auto conn = conn_pool.get(timeouts, true);
    conn->startWriteFile(path, buf_size, mode);
    return std::make_unique<WriteBufferFromRemoteDisk>(conn, path, buf_size);
}

void DiskRemote::removeFile(const String & path)
{
    auto conn = conn_pool.get(timeouts, true);
    conn->removeFile(path);
}

void DiskRemote::removeFileIfExists(const String & path)
{
    auto conn = conn_pool.get(timeouts, true);
    conn->removeFileIfExists(path);
}

void DiskRemote::removeDirectory(const String & path)
{
    auto conn = conn_pool.get(timeouts, true);
    conn->removeDirectory(path);
}

void DiskRemote::removeRecursive(const String & path)
{
    auto conn = conn_pool.get(timeouts, true);
    conn->removeRecursive(path);
}

void DiskRemote::setLastModified(const String & path, const Poco::Timestamp & timestamp)
{
    auto conn = conn_pool.get(timeouts, true);
    conn->setLastModified(path, timestamp);
}

Poco::Timestamp DiskRemote::getLastModified(const String & path) const
{
    auto conn = conn_pool.get(timeouts, true);
    return conn->getLastModified(path);
}

time_t DiskRemote::getLastChanged(const String & path) const
{
    auto conn = conn_pool.get(timeouts, true);
    return conn->getLastChanged(path);
}

void DiskRemote::setReadOnly(const String & path)
{
    auto conn = conn_pool.get(timeouts, true);
    conn->setReadOnly(path);
}

void DiskRemote::createHardLink(const String & src_path, const String & dst_path)
{
    auto conn = conn_pool.get(timeouts, true);
    conn->createHardLink(src_path, dst_path);
}

void DiskRemote::truncateFile(const String & path, size_t size)
{
    auto conn = conn_pool.get(timeouts, true);
    conn->truncateFile(path, size);
}

DataSourceDescription DiskRemote::getDataSourceDescription() const
{
    return data_source_description;
}

void DiskRemote::shutdown()
{
    // TODO
}

void DiskRemote::startupImpl(ContextPtr /*context*/)
{
}

void DiskRemote::applyNewSettings(
    const Poco::Util::AbstractConfiguration & /*config*/, ContextPtr /*context*/, const String & /*config_prefix*/, const DisksMap &)
{
    // TODO
}

void registerDiskRemote(DiskFactory & factory, bool global_skip_access_check)
{
    auto creator = [global_skip_access_check](
                       const String & name,
                       const Poco::Util::AbstractConfiguration & config,
                       const String & config_prefix,
                       ContextPtr context,
                       const DisksMap & /*map*/) -> DiskPtr
    {
        String host = config.getString(config_prefix + ".host");
        UInt16 port = static_cast<UInt16>(config.getUInt(config_prefix + ".port"));
        String remote_disk_name = config.getString(config_prefix + ".remote_disk_name");
        unsigned max_connections = config.getUInt(config_prefix + ".port", 20); // TODO define default const

        bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);
        std::shared_ptr<IDisk> disk = std::make_shared<DiskRemote>(name, host, port, remote_disk_name, max_connections);
        disk->startup(context, skip_access_check);
        return disk;
    };
    factory.registerDiskType("remote", creator);
}

} // DB
