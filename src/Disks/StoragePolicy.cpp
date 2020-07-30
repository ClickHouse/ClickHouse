#include "StoragePolicy.h"
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
        volumes.push_back(std::make_shared<VolumeJBOD>(attr_name, config, volumes_prefix + "." + attr_name, disks));
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
        for (const auto & disk : volume->getDisks())
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

    const auto & disks = volumes[0]->getDisks();
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
        for (const auto & disk : volume->getDisks())
            res.push_back(disk);
    return res;
}


DiskPtr StoragePolicy::getAnyDisk() const
{
    /// StoragePolicy must contain at least one Volume
    /// Volume must contain at least one Disk
    if (volumes.empty())
        throw Exception("StoragePolicy has no volumes. It's a bug.", ErrorCodes::LOGICAL_ERROR);

    if (volumes[0]->getDisks().empty())
        throw Exception("Volume '" + volumes[0]->getName() + "' has no disks. It's a bug.", ErrorCodes::LOGICAL_ERROR);

    return volumes[0]->getDisks()[0];
}


DiskPtr StoragePolicy::getDiskByName(const String & disk_name) const
{
    for (auto && volume : volumes)
        for (auto && disk : volume->getDisks())
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
        for (const auto & disk : volume->getDisks())
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
            throw Exception("New storage policy shall contain volumes of old one", ErrorCodes::BAD_ARGUMENTS);

        std::unordered_set<String> new_disk_names;
        for (const auto & disk : new_storage_policy->getVolumeByName(volume->getName())->getDisks())
            new_disk_names.insert(disk->getName());

        for (const auto & disk : volume->getDisks())
            if (new_disk_names.count(disk->getName()) == 0)
                throw Exception("New storage policy shall contain disks of old one", ErrorCodes::BAD_ARGUMENTS);
    }
}


size_t StoragePolicy::getVolumeIndexByDisk(const DiskPtr & disk_ptr) const
{
    for (size_t i = 0; i < volumes.size(); ++i)
    {
        const auto & volume = volumes[i];
        for (const auto & disk : volume->getDisks())
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
        LOG_INFO(&Poco::Logger::get("StoragePolicySelector"), "Storage policy {} loaded", backQuote(name));
    }

    constexpr auto default_storage_policy_name = "default";
    constexpr auto default_volume_name = "default";
    constexpr auto default_disk_name = "default";

    /// Add default policy if it's not specified explicetly
    if (policies.find(default_storage_policy_name) == policies.end())
    {
        auto default_volume = std::make_shared<VolumeJBOD>(default_volume_name, std::vector<DiskPtr>{disks->get(default_disk_name)}, 0);

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
