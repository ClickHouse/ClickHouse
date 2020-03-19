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

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int UNKNOWN_DISK;
    extern const int UNKNOWN_POLICY;
    extern const int LOGICAL_ERROR;
}


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


DiskSelectorPtr DiskSelector::updateFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, const Context & context) const
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    auto & factory = DiskFactory::instance();

    std::shared_ptr<DiskSelector> result = std::make_shared<DiskSelector>(*this);

    constexpr auto default_disk_name = "default";
    std::set<String> old_disks_minus_new_disks;
    for (const auto & [disk_name, _] : result->disks)
    {
        old_disks_minus_new_disks.insert(disk_name);
    }

    for (const auto & disk_name : keys)
    {
        if (!std::all_of(disk_name.begin(), disk_name.end(), isWordCharASCII))
            throw Exception("Disk name can contain only alphanumeric and '_' (" + disk_name + ")", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

        if (result->disks.count(disk_name) == 0)
        {
            auto disk_config_prefix = config_prefix + "." + disk_name;
            result->disks.emplace(disk_name, factory.create(disk_name, config, disk_config_prefix, context));
        }
        else
        {
            old_disks_minus_new_disks.erase(disk_name);

            /// TODO: Ideally ClickHouse shall complain if disk has changed, but
            /// implementing that may appear as not trivial task.
        }
    }

    old_disks_minus_new_disks.erase(default_disk_name);

    if (!old_disks_minus_new_disks.empty())
    {
        WriteBufferFromOwnString warning;
        if (old_disks_minus_new_disks.size() == 1)
            writeString("Disk ", warning);
        else
            writeString("Disks ", warning);

        int index = 0;
        for (const String & name : old_disks_minus_new_disks)
        {
            if (index++ > 0)
                writeString(", ", warning);
            writeBackQuotedString(name, warning);
        }

        writeString(" disappeared from configuration, this change will be applied after restart of ClickHouse", warning);
        LOG_WARNING(&Logger::get("DiskSelector"), warning.str());
    }

    return result;
}


DiskPtr DiskSelector::get(const String & name) const
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
    DiskSelectorPtr disk_selector)
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
            disks.push_back(disk_selector->get(disk_name));
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

DiskPtr Volume::getNextDisk()
{
    size_t start_from = last_used.fetch_add(1u, std::memory_order_relaxed);
    size_t index = start_from % disks.size();
    return disks[index];
}

ReservationPtr Volume::reserve(UInt64 bytes)
{
    /// This volume can not store files which size greater than max_data_part_size

    if (max_data_part_size != 0 && bytes > max_data_part_size)
        return {};

    size_t start_from = last_used.fetch_add(1u, std::memory_order_relaxed);
    size_t disks_num = disks.size();
    for (size_t i = 0; i < disks_num; ++i)
    {
        size_t index = (start_from + i) % disks_num;

        auto reservation = disks[index]->reserve(bytes);

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
    DiskSelectorPtr disks)
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


bool StoragePolicy::isDefaultPolicy() const
{
    /// Guessing if this policy is default, not 100% correct though.

    if (getName() != "default")
        return false;

    if (volumes.size() != 1)
        return false;

    if (volumes[0]->getName() != "default")
        return false;

    const auto & disks = volumes[0]->disks;
    if (disks.size() != 1)
        return false;

    if (disks[0]->getName() != "default")
        return false;

    return true;
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
        throw Exception("StoragePolicy has no volumes. It's a bug.", ErrorCodes::LOGICAL_ERROR);

    if (volumes[0]->disks.empty())
        throw Exception("Volume '" + volumes[0]->getName() + "' has no disks. It's a bug.", ErrorCodes::LOGICAL_ERROR);

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


ReservationPtr StoragePolicy::reserve(UInt64 bytes, size_t min_volume_index) const
{
    for (size_t i = min_volume_index; i < volumes.size(); ++i)
    {
        const auto & volume = volumes[i];
        auto reservation = volume->reserve(bytes);
        if (reservation)
            return reservation;
    }
    return {};
}


ReservationPtr StoragePolicy::reserve(UInt64 bytes) const
{
    return reserve(bytes, 0);
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


void StoragePolicy::checkCompatibleWith(const StoragePolicyPtr & new_storage_policy) const
{
    std::unordered_set<String> new_volume_names;
    for (const auto & volume : new_storage_policy->getVolumes())
        new_volume_names.insert(volume->getName());

    for (const auto & volume : getVolumes())
    {
        if (new_volume_names.count(volume->getName()) == 0)
            throw Exception("New storage policy shall contain volumes of old one", ErrorCodes::LOGICAL_ERROR);

        std::unordered_set<String> new_disk_names;
        for (const auto & disk : new_storage_policy->getVolumeByName(volume->getName())->disks)
            new_disk_names.insert(disk->getName());

        for (const auto & disk : volume->disks)
            if (new_disk_names.count(disk->getName()) == 0)
                throw Exception("New storage policy shall contain disks of old one", ErrorCodes::LOGICAL_ERROR);
    }
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
    DiskSelectorPtr disks)
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
        auto default_volume = std::make_shared<Volume>(default_volume_name, std::vector<DiskPtr>{disks->get(default_disk_name)}, 0);

        auto default_policy = std::make_shared<StoragePolicy>(default_storage_policy_name, Volumes{default_volume}, 0.0);
        policies.emplace(default_storage_policy_name, default_policy);
    }
}


StoragePolicySelectorPtr StoragePolicySelector::updateFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, DiskSelectorPtr disks) const
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    std::shared_ptr<StoragePolicySelector> result = std::make_shared<StoragePolicySelector>(config, config_prefix, disks);

    constexpr auto default_storage_policy_name = "default";

    for (const auto & [name, policy] : policies)
    {
        if (name != default_storage_policy_name && result->policies.count(name) == 0)
            throw Exception("Storage policy " + backQuote(name) + " is missing in new configuration", ErrorCodes::BAD_ARGUMENTS);

        policy->checkCompatibleWith(result->policies[name]);
    }

    return result;
}


StoragePolicyPtr StoragePolicySelector::get(const String & name) const
{
    auto it = policies.find(name);
    if (it == policies.end())
        throw Exception("Unknown StoragePolicy " + name, ErrorCodes::UNKNOWN_POLICY);

    return it->second;
}

}
