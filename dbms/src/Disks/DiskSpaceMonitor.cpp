#include "DiskSpaceMonitor.h"
#include "DiskFactory.h"
#include "DiskLocal.h"

#include <Interpreters/Context.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>

#include <set>
#include <Poco/File.h>


namespace DB
{
DiskSelector::DiskSelector(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, const Context & context)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    auto & factory = DiskFactory::instance();

    constexpr auto default_disk_name = "default";
    bool has_default_disk = false;
    for (const auto & disk_name : keys)
    {
        if (!std::all_of(disk_name.begin(), disk_name.end(), isWordCharASCII))
            throw Exception("Disk name can contain only alphanumeric and '_' (" + disk_name + ")", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

        if (disk_name == default_disk_name)
            has_default_disk = true;

        auto disk_config_prefix = config_prefix + "." + disk_name;

        disks.emplace(disk_name, factory.create(disk_name, config, disk_config_prefix, context));
    }
    if (!has_default_disk)
        disks.emplace(default_disk_name, std::make_shared<DiskLocal>(default_disk_name, context.getPath(), 0));
}


const DiskPtr & DiskSelector::operator[](const String & name) const
{
    auto it = disks.find(name);
    if (it == disks.end())
        throw Exception("Unknown disk " + name, ErrorCodes::UNKNOWN_DISK);
    return it->second;
}


Volume::Volume(
    String name_,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    const DiskSelector & disk_selector)
    : name(std::move(name_))
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    Logger * logger = &Logger::get("StorageConfiguration");

    for (const auto & disk : keys)
    {
        if (startsWith(disk, "disk"))
        {
            auto disk_name = config.getString(config_prefix + "." + disk);
            disks.push_back(disk_selector[disk_name]);
        }
    }

    if (disks.empty())
        throw Exception("Volume must contain at least one disk.", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

    auto has_max_bytes = config.has(config_prefix + ".max_data_part_size_bytes");
    auto has_max_ratio = config.has(config_prefix + ".max_data_part_size_ratio");
    if (has_max_bytes && has_max_ratio)
        throw Exception(
            "Only one of 'max_data_part_size_bytes' and 'max_data_part_size_ratio' should be specified.",
            ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

    if (has_max_bytes)
    {
        max_data_part_size = config.getUInt64(config_prefix + ".max_data_part_size_bytes", 0);
    }
    else if (has_max_ratio)
    {
        auto ratio = config.getDouble(config_prefix + ".max_data_part_size_ratio");
        if (ratio < 0)
            throw Exception("'max_data_part_size_ratio' have to be not less then 0.", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
        UInt64 sum_size = 0;
        std::vector<UInt64> sizes;
        for (const auto & disk : disks)
        {
            sizes.push_back(disk->getTotalSpace());
            sum_size += sizes.back();
        }
        max_data_part_size = static_cast<decltype(max_data_part_size)>(sum_size * ratio / disks.size());
        for (size_t i = 0; i < disks.size(); ++i)
            if (sizes[i] < max_data_part_size)
                LOG_WARNING(
                    logger,
                    "Disk " << backQuote(disks[i]->getName()) << " on volume " << backQuote(config_prefix) << " have not enough space ("
                            << formatReadableSizeWithBinarySuffix(sizes[i]) << ") for containing part the size of max_data_part_size ("
                            << formatReadableSizeWithBinarySuffix(max_data_part_size) << ")");
    }
    constexpr UInt64 MIN_PART_SIZE = 8u * 1024u * 1024u;
    if (max_data_part_size != 0 && max_data_part_size < MIN_PART_SIZE)
        LOG_WARNING(
            logger,
            "Volume " << backQuote(name) << " max_data_part_size is too low (" << formatReadableSizeWithBinarySuffix(max_data_part_size)
                      << " < " << formatReadableSizeWithBinarySuffix(MIN_PART_SIZE) << ")");
}


ReservationPtr Volume::reserve(UInt64 expected_size)
{
    /// This volume can not store files which size greater than max_data_part_size

    if (max_data_part_size != 0 && expected_size > max_data_part_size)
        return {};

    size_t start_from = last_used.fetch_add(1u, std::memory_order_relaxed);
    size_t disks_num = disks.size();
    for (size_t i = 0; i < disks_num; ++i)
    {
        size_t index = (start_from + i) % disks_num;

        auto reservation = disks[index]->reserve(expected_size);

        if (reservation)
            return reservation;
    }
    return {};
}


UInt64 Volume::getMaxUnreservedFreeSpace() const
{
    UInt64 res = 0;
    for (const auto & disk : disks)
        res = std::max(res, disk->getUnreservedSpace());
    return res;
}

StoragePolicy::StoragePolicy(
    String name_,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    const DiskSelector & disks)
    : name(std::move(name_))
{
    String volumes_prefix = config_prefix + ".volumes";
    if (!config.has(volumes_prefix))
        throw Exception("StoragePolicy must contain at least one volume (.volumes)", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(volumes_prefix, keys);

    for (const auto & attr_name : keys)
    {
        if (!std::all_of(attr_name.begin(), attr_name.end(), isWordCharASCII))
            throw Exception(
                "Volume name can contain only alphanumeric and '_' (" + attr_name + ")", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
        volumes.push_back(std::make_shared<Volume>(attr_name, config, volumes_prefix + "." + attr_name, disks));
        if (volumes_names.find(attr_name) != volumes_names.end())
            throw Exception("Volumes names must be unique (" + attr_name + " duplicated)", ErrorCodes::UNKNOWN_POLICY);
        volumes_names[attr_name] = volumes.size() - 1;
    }

    if (volumes.empty())
        throw Exception("StoragePolicy must contain at least one volume.", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

    /// Check that disks are unique in Policy
    std::set<String> disk_names;
    for (const auto & volume : volumes)
    {
        for (const auto & disk : volume->disks)
        {
            if (disk_names.find(disk->getName()) != disk_names.end())
                throw Exception(
                    "Duplicate disk '" + disk->getName() + "' in storage policy '" + name + "'", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

            disk_names.insert(disk->getName());
        }
    }

    move_factor = config.getDouble(config_prefix + ".move_factor", 0.1);
    if (move_factor > 1)
        throw Exception("Disk move factor have to be in [0., 1.] interval, but set to " + toString(move_factor), ErrorCodes::LOGICAL_ERROR);
}


StoragePolicy::StoragePolicy(String name_, Volumes volumes_, double move_factor_)
    : volumes(std::move(volumes_)), name(std::move(name_)), move_factor(move_factor_)
{
    if (volumes.empty())
        throw Exception("StoragePolicy must contain at least one Volume.", ErrorCodes::UNKNOWN_POLICY);

    if (move_factor > 1)
        throw Exception("Disk move factor have to be in [0., 1.] interval, but set to " + toString(move_factor), ErrorCodes::LOGICAL_ERROR);

    for (size_t i = 0; i < volumes.size(); ++i)
    {
        if (volumes_names.find(volumes[i]->getName()) != volumes_names.end())
            throw Exception("Volumes names must be unique (" + volumes[i]->getName() + " duplicated).", ErrorCodes::UNKNOWN_POLICY);
        volumes_names[volumes[i]->getName()] = i;
    }
}


Disks StoragePolicy::getDisks() const
{
    Disks res;
    for (const auto & volume : volumes)
        for (const auto & disk : volume->disks)
            res.push_back(disk);
    return res;
}


DiskPtr StoragePolicy::getAnyDisk() const
{
    /// StoragePolicy must contain at least one Volume
    /// Volume must contain at least one Disk
    if (volumes.empty())
        throw Exception("StoragePolicy has no volumes. It's a bug.", ErrorCodes::NOT_ENOUGH_SPACE);

    if (volumes[0]->disks.empty())
        throw Exception("Volume '" + volumes[0]->getName() + "' has no disks. It's a bug.", ErrorCodes::NOT_ENOUGH_SPACE);

    return volumes[0]->disks[0];
}


DiskPtr StoragePolicy::getDiskByName(const String & disk_name) const
{
    for (auto && volume : volumes)
        for (auto && disk : volume->disks)
            if (disk->getName() == disk_name)
                return disk;
    return {};
}


UInt64 StoragePolicy::getMaxUnreservedFreeSpace() const
{
    UInt64 res = 0;
    for (const auto & volume : volumes)
        res = std::max(res, volume->getMaxUnreservedFreeSpace());
    return res;
}


ReservationPtr StoragePolicy::reserve(UInt64 expected_size, size_t min_volume_index) const
{
    for (size_t i = min_volume_index; i < volumes.size(); ++i)
    {
        const auto & volume = volumes[i];
        auto reservation = volume->reserve(expected_size);
        if (reservation)
            return reservation;
    }
    return {};
}


ReservationPtr StoragePolicy::reserve(UInt64 expected_size) const
{
    return reserve(expected_size, 0);
}


ReservationPtr StoragePolicy::makeEmptyReservationOnLargestDisk() const
{
    UInt64 max_space = 0;
    DiskPtr max_disk;
    for (const auto & volume : volumes)
    {
        for (const auto & disk : volume->disks)
        {
            auto avail_space = disk->getAvailableSpace();
            if (avail_space > max_space)
            {
                max_space = avail_space;
                max_disk = disk;
            }
        }
    }
    return max_disk->reserve(0);
}


size_t StoragePolicy::getVolumeIndexByDisk(const DiskPtr & disk_ptr) const
{
    for (size_t i = 0; i < volumes.size(); ++i)
    {
        const auto & volume = volumes[i];
        for (const auto & disk : volume->disks)
            if (disk->getName() == disk_ptr->getName())
                return i;
    }
    throw Exception("No disk " + disk_ptr->getName() + " in policy " + name, ErrorCodes::UNKNOWN_DISK);
}


StoragePolicySelector::StoragePolicySelector(
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    const DiskSelector & disks)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    for (const auto & name : keys)
    {
        if (!std::all_of(name.begin(), name.end(), isWordCharASCII))
            throw Exception(
                "StoragePolicy name can contain only alphanumeric and '_' (" + name + ")", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

        policies.emplace(name, std::make_shared<StoragePolicy>(name, config, config_prefix + "." + name, disks));
        LOG_INFO(&Logger::get("StoragePolicySelector"), "Storage policy " << backQuote(name) << " loaded");
    }

    constexpr auto default_storage_policy_name = "default";
    constexpr auto default_volume_name = "default";
    constexpr auto default_disk_name = "default";

    /// Add default policy if it's not specified explicetly
    if (policies.find(default_storage_policy_name) == policies.end())
    {
        auto default_volume = std::make_shared<Volume>(default_volume_name, std::vector<DiskPtr>{disks[default_disk_name]}, 0);

        auto default_policy = std::make_shared<StoragePolicy>(default_storage_policy_name, Volumes{default_volume}, 0.0);
        policies.emplace(default_storage_policy_name, default_policy);
    }
}

const StoragePolicyPtr & StoragePolicySelector::operator[](const String & name) const
{
    auto it = policies.find(name);
    if (it == policies.end())
        throw Exception("Unknown StoragePolicy " + name, ErrorCodes::UNKNOWN_POLICY);
    return it->second;
}

}
