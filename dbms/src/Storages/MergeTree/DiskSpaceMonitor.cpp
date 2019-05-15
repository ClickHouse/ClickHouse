#include <Storages/MergeTree/DiskSpaceMonitor.h>

#include <Common/escapeForFileName.h>
#include <Poco/File.h>

namespace DB
{

std::map<String, DiskSpaceMonitor::DiskReserve> DiskSpaceMonitor::reserved;
std::mutex DiskSpaceMonitor::mutex;

DiskSelector::DiskSelector(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, String default_path)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    constexpr auto default_disk_name = "default";
    bool has_default_disk = false;
    for (const auto & disk_name : keys)
    {
        if (!std::all_of(disk_name.begin(), disk_name.end(), isWordCharASCII))
            throw Exception("Disk name can contain only alphanumeric and '_' (" + disk_name + ")", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

        auto disk_config_prefix = config_prefix + "." + disk_name;
        UInt64 keep_free_space_bytes = config.getUInt64(disk_config_prefix + ".keep_free_space_bytes", 0);
        String path;
        if (config.has(disk_config_prefix + ".path"))
            path = config.getString(disk_config_prefix + ".path");

        if (disk_name == default_disk_name)
        {
            has_default_disk = true;
            if (!path.empty())
                throw Exception("\"default\" disk path should be provided in <path> not it <storage_configuration>", ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
            disks.emplace(disk_name, std::make_shared<const Disk>(disk_name, default_path, keep_free_space_bytes));
        }
        else
        {
            if (path.empty())
                throw Exception("Disk path can not be empty. Disk " + disk_name, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
            if (path.back() != '/')
                throw Exception("Disk path must end with /. Disk " + disk_name, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
            disks.emplace(disk_name, std::make_shared<const Disk>(disk_name, path, keep_free_space_bytes));
        }
    }
    if (!has_default_disk)
        disks.emplace(default_disk_name, std::make_shared<const Disk>(default_disk_name, default_path, 0));
}

const DiskPtr & DiskSelector::operator[](const String & name) const
{
    auto it = disks.find(name);
    if (it == disks.end())
        throw Exception("Unknown disk " + name, ErrorCodes::UNKNOWN_DISK);
    return it->second;
}

bool DiskSelector::has(const String & name) const
{
    auto it = disks.find(name);
    return it != disks.end();
}

void DiskSelector::add(const DiskPtr & disk)
{
    disks.emplace(disk->getName(), disk);
}

Schema::Volume::Volume(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, const DiskSelector & disk_selector)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    for (const auto & name : keys)
    {
        if (startsWith(name, "disk"))
        {
            auto disk_name = config.getString(config_prefix + "." + name);
            disks.push_back(disk_selector[disk_name]);
        }
    }

    if (disks.empty())
        throw Exception("Volume must contain at least one disk", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

    auto has_max_bytes = config.has(config_prefix + ".max_data_part_size_bytes");
    auto has_max_ratio = config.has(config_prefix + ".max_data_part_size_ratio");
    if (has_max_bytes && has_max_ratio)
    {
        throw Exception("Only one of 'max_data_part_size_bytes' and 'max_data_part_size_ratio' should be specified",
                        ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
    }

    Logger * logger = &Logger::get("StorageConfiguration");

    if (has_max_bytes)
    {
        max_data_part_size = config.getUInt64(config_prefix + ".max_data_part_size_bytes");
    }
    else if (has_max_ratio)
    {
        auto ratio = config.getDouble(config_prefix + ".max_data_part_size_ratio");
        if (ratio < 0)
            throw Exception("'max_data_part_size_ratio' have to be not less then 0",
                            ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
        UInt64 sum_size = 0;
        std::vector<UInt64> sizes;
        for (const auto & disk : disks)
        {
            sizes.push_back(disk->getTotalSpace());
            sum_size += sizes.back();
        }
        max_data_part_size = static_cast<decltype(max_data_part_size)>(sum_size * ratio / disks.size());
        for (size_t i = 0; i != disks.size(); ++i)
            if (sizes[i] < max_data_part_size)
                LOG_WARNING(logger, "Disk " << disks[i]->getName() << " on volume " << config_prefix <<
                                    " have not enough space (" << sizes[i] <<
                                    ") for containing part the size of max_data_part_size (" <<
                                    max_data_part_size << ")");
    }
    else
    {
        max_data_part_size = std::numeric_limits<UInt64>::max();
    }
    constexpr UInt64 SIZE_8MB = 1ull << 23;
    if (max_data_part_size < SIZE_8MB) {
        LOG_WARNING(logger, "Volume max_data_part_size is too low (" << max_data_part_size << " < " << SIZE_8MB << ")");
    }
}

DiskSpaceMonitor::ReservationPtr Schema::Volume::reserve(UInt64 expected_size) const
{
    /// This volume can not store files which size greater than max_data_part_size

    if (expected_size > max_data_part_size)
        return {};

    size_t start_from = last_used.fetch_add(1u, std::memory_order_relaxed);
    for (size_t i = 0; i != disks.size(); ++i)
    {
        size_t index = (start_from + i) % disks.size();
        auto reservation = DiskSpaceMonitor::tryToReserve(disks[index], expected_size);

        if (reservation && *reservation)
            return reservation;
    }
    return {};
}

UInt64 Schema::Volume::getMaxUnreservedFreeSpace() const
{
    UInt64 res = 0;
    for (const auto & disk : disks)
        res = std::max(res, DiskSpaceMonitor::getUnreservedFreeSpace(disk));
    return res;
}

Schema::Schema(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, const DiskSelector & disks)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    for (const auto & name : keys)
    {
        if (!startsWith(name, "volume"))
            throw Exception("Unknown element in config: " + config_prefix + "." + name + ", must be 'volume'",
                            ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
        volumes.emplace_back(config, config_prefix + "." + name, disks);
    }
    if (volumes.empty())
        throw Exception("Schema must contain at least one Volume", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
}

Schema::Disks Schema::getDisks() const
{
    Disks res;
    for (const auto & volume : volumes)
        for (const auto & disk : volume.disks)
            res.push_back(disk);
    return res;
}

DiskPtr Schema::getAnyDisk() const
{
    /// Schema must contain at least one Volume
    /// Volume must contain at least one Disk
    return volumes[0].disks[0];
}

UInt64 Schema::getMaxUnreservedFreeSpace() const
{
    UInt64 res = 0;
    for (const auto & volume : volumes)
        res = std::max(res, volume.getMaxUnreservedFreeSpace());
    return res;
}

DiskSpaceMonitor::ReservationPtr Schema::reserve(UInt64 expected_size) const
{
    for (const auto & volume : volumes)
    {
        auto reservation = volume.reserve(expected_size);
        if (reservation)
            return reservation;
    }
    return {};
}

DiskSpaceMonitor::ReservationPtr Schema::reserveOnMaxDiskWithoutReservation() const
{
    UInt64 max_space = 0;
    DiskPtr max_disk;
    for (const auto & volume : volumes)
    {
        for (const auto &disk : volume.disks)
        {
            auto avail_space = disk->getAvailableSpace();
            if (avail_space > max_space)
            {
                max_space = avail_space;
                max_disk = disk;
            }
        }
    }
    return DiskSpaceMonitor::tryToReserve(max_disk, 0);
}

SchemaSelector::SchemaSelector(const Poco::Util::AbstractConfiguration & config, const String& config_prefix, const DiskSelector & disks)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    Logger * logger = &Logger::get("SchemaSelector");

    for (const auto & name : keys)
    {
        if (!std::all_of(name.begin(), name.end(), isWordCharASCII))
            throw Exception("Schema name can contain only alphanumeric and '_' (" + name + ")", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
        schemas.emplace(name, Schema{config, config_prefix + "." + name, disks});
        LOG_INFO(logger, "Storage schema " << name << " Sloaded");
    }

    constexpr auto default_schema_name = "default";
    constexpr auto default_disk_name = "default";
    if (schemas.find(default_schema_name) == schemas.end())
        schemas.emplace(default_schema_name, Schema(Schema::Volumes{{std::vector<DiskPtr>{disks[default_disk_name]},
                                                                     std::numeric_limits<UInt64>::max()}}));
}

const Schema & SchemaSelector::operator[](const String & name) const
{
    auto it = schemas.find(name);
    if (it == schemas.end())
        throw Exception("Unknown schema " + name, ErrorCodes::UNKNOWN_SCHEMA);
    return it->second;
}

}
