#include "DiskLocal.h"
#include "DiskFactory.h"

#include <Interpreters/Context.h>
#include <Common/filesystemHelpers.h>
#include <Common/quoteString.h>


namespace DB
{
std::mutex DiskLocal::mutex;

ReservationPtr DiskLocal::reserve(UInt64 bytes)
{
    if (!tryReserve(bytes))
        return {};
    return std::make_unique<DiskLocalReservation>(std::static_pointer_cast<DiskLocal>(shared_from_this()), bytes);
}

bool DiskLocal::tryReserve(UInt64 bytes)
{
    std::lock_guard lock(mutex);
    if (bytes == 0)
    {
        LOG_DEBUG(&Logger::get("DiskLocal"), "Reserving 0 bytes on disk " << backQuote(name));
        ++reservation_count;
        return true;
    }

    auto available_space = getAvailableSpace();
    UInt64 unreserved_space = available_space - std::min(available_space, reserved_bytes);
    if (unreserved_space >= bytes)
    {
        LOG_DEBUG(
            &Logger::get("DiskLocal"),
            "Reserving " << formatReadableSizeWithBinarySuffix(bytes) << " on disk " << backQuote(name) << ", having unreserved "
                         << formatReadableSizeWithBinarySuffix(unreserved_space) << ".");
        ++reservation_count;
        reserved_bytes += bytes;
        return true;
    }
    return false;
}

UInt64 DiskLocal::getTotalSpace() const
{
    auto fs = getStatVFS(disk_path);
    UInt64 total_size = fs.f_blocks * fs.f_bsize;
    if (total_size < keep_free_space_bytes)
        return 0;
    return total_size - keep_free_space_bytes;
}

UInt64 DiskLocal::getAvailableSpace() const
{
    /// we use f_bavail, because part of b_free space is
    /// available for superuser only and for system purposes
    auto fs = getStatVFS(disk_path);
    UInt64 total_size = fs.f_bavail * fs.f_bsize;
    if (total_size < keep_free_space_bytes)
        return 0;
    return total_size - keep_free_space_bytes;
}

UInt64 DiskLocal::getUnreservedSpace() const
{
    std::lock_guard lock(mutex);
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

void DiskLocal::createDirectory(const String & path)
{
    Poco::File(disk_path + path).createDirectory();
}

void DiskLocal::createDirectories(const String & path)
{
    Poco::File(disk_path + path).createDirectories();
}

DiskDirectoryIteratorPtr DiskLocal::iterateDirectory(const String & path)
{
    return std::make_unique<DiskLocalDirectoryIterator>(disk_path + path);
}

void DiskLocal::moveFile(const String & from_path, const String & to_path)
{
    Poco::File(disk_path + from_path).renameTo(disk_path + to_path);
}

void DiskLocal::copyFile(const String & from_path, const String & to_path)
{
    Poco::File(disk_path + from_path).copyTo(disk_path + to_path);
}

std::unique_ptr<ReadBuffer> DiskLocal::readFile(const String & path) const
{
    return std::make_unique<ReadBufferFromFile>(disk_path + path);
}

std::unique_ptr<WriteBuffer> DiskLocal::writeFile(const String & path)
{
    return std::make_unique<WriteBufferFromFile>(disk_path + path);
}


void DiskLocalReservation::update(UInt64 new_size)
{
    std::lock_guard lock(DiskLocal::mutex);
    disk->reserved_bytes -= size;
    size = new_size;
    disk->reserved_bytes += size;
}

DiskLocalReservation::~DiskLocalReservation()
{
    try
    {
        std::lock_guard lock(DiskLocal::mutex);
        if (disk->reserved_bytes < size)
        {
            disk->reserved_bytes = 0;
            LOG_ERROR(&Logger::get("DiskLocal"), "Unbalanced reservations size for disk '" + disk->getName() + "'.");
        }
        else
        {
            disk->reserved_bytes -= size;
        }

        if (disk->reservation_count == 0)
            LOG_ERROR(&Logger::get("DiskLocal"), "Unbalanced reservation count for disk '" + disk->getName() + "'.");
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
