#include <Storages/MergeTree/DiskSpaceMonitor.h>

#include <Common/escapeForFileName.h>
#include <Poco/File.h>

/// @TODO_IGR ASK Does such function already exists?
bool isAlphaNumeric(const std::string & s)
{
    for (auto c : s)
        if (!isalnum(c) && c != '_')
            return false;
    return true;
}

namespace DB
{

std::map<String, DiskSpaceMonitor::DiskReserve> DiskSpaceMonitor::reserved;
std::mutex DiskSpaceMonitor::mutex;

DiskSelector::DiskSelector(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, String default_path)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    constexpr auto default_disk_name = "default";
    for (const auto & disk_name : keys)
    {
        if (!isAlphaNumeric(disk_name))
            throw Exception("Disk name can contain only alphanumeric and '_' (" + disk_name + ")", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

        auto disk_config_prefix = config_prefix + "." + disk_name;
        UInt64 keep_free_space_bytes = config.getUInt64(disk_config_prefix + ".keep_free_space_bytes", 0);
        String path;
        if (config.has(disk_config_prefix + ".path"))
            path = config.getString(disk_config_prefix + ".path");

        if (disk_name == default_disk_name)
        {
            if (!path.empty())
                throw Exception("It is not possible to specify default disk path", ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
            disks.emplace(disk_name, std::make_shared<const Disk>(disk_name, default_path, keep_free_space_bytes));
        }
        else
        {
            if (path.empty())
                throw Exception("Disk path can not be empty. Disk " + disk_name, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
            disks.emplace(disk_name, std::make_shared<const Disk>(disk_name, path, keep_free_space_bytes));
        }
    }
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

    if (disks.empty()) {
        throw Exception("Volume must contain at least one disk", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
    }

    auto has_max_bytes = config.has(config_prefix + ".max_data_part_size_bytes");
    auto has_max_ratio = config.has(config_prefix + ".max_data_part_size_ratio");
    if (has_max_bytes && has_max_ratio)
    {
        throw Exception("Only one of 'max_data_part_size_bytes' and 'max_data_part_size_ratio' should be specified",
                        ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
    }

    if (has_max_bytes) {
        max_data_part_size = config.getUInt64(config_prefix + ".max_data_part_size_bytes");
    } else if (has_max_ratio) {
        auto ratio = config.getDouble(config_prefix + ".max_data_part_size_bytes");
        if (ratio < 0 and ratio > 1) {
            throw Exception("'max_data_part_size_bytes' have to be between 0 and 1",
                            ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
        }
        UInt64 sum_size = 0;
        for (const auto & disk : disks)
            sum_size += disk->getTotalSpace();
        max_data_part_size = static_cast<decltype(max_data_part_size)>(sum_size * ratio);
    } else {
        max_data_part_size = std::numeric_limits<UInt64>::max();
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
    if (volumes.empty()) {
        throw Exception("Schema must contain at least one Volume", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
    }
}

Schema::Disks Schema::getDisks() const
{
    Disks res;
    for (const auto & volume : volumes)
        for (const auto & disk : volume.disks)
            res.push_back(disk);
    return res;
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

SchemaSelector::SchemaSelector(const Poco::Util::AbstractConfiguration & config, const String& config_prefix, const DiskSelector & disks)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    for (const auto & name : keys)
    {
        if (!isAlphaNumeric(name))
            throw Exception("Schema name can contain only alphanumeric and '_' (" + name + ")", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
        schemes.emplace(name, Schema{config, config_prefix + "." + name, disks});
        LOG_INFO(&Logger::get("StatusFile"), "Storage schema " << name << "loaded");  ///@TODO_IGR ASK Logger?
    }

    constexpr auto default_schema_name = "default";
    constexpr auto default_disk_name = "default";
    if (schemes.find(default_schema_name) == schemes.end())
        schemes.emplace(default_schema_name, Schema(Schema::Volumes{{std::vector<DiskPtr>{disks[default_disk_name]},
                                                                     std::numeric_limits<UInt64>::max()}}));
}

const Schema & SchemaSelector::operator[](const String & name) const
{
    auto it = schemes.find(name);
    if (it == schemes.end())
        throw Exception("Unknown schema " + name, ErrorCodes::UNKNOWN_SCHEMA);
    return it->second;
}

}
