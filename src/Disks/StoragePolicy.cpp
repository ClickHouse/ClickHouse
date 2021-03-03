#include "StoragePolicy.h"
#include "DiskFactory.h"
#include "DiskLocal.h"

#include <Interpreters/Context.h>
#include <Common/escapeForFileName.h>
#include <Common/quoteString.h>

#include <set>

#include <Poco/File.h>


namespace
{
    const auto DEFAULT_STORAGE_POLICY_NAME = "default";
    const auto DEFAULT_VOLUME_NAME = "default";
    const auto DEFAULT_DISK_NAME = "default";
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int UNKNOWN_DISK;
    extern const int UNKNOWN_POLICY;
    extern const int UNKNOWN_VOLUME;
    extern const int LOGICAL_ERROR;
    extern const int NOT_ENOUGH_SPACE;
}


StoragePolicy::StoragePolicy(
    String name_,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    DiskSelectorPtr disks)
    : name(std::move(name_))
{
    Poco::Util::AbstractConfiguration::Keys keys;
    String volumes_prefix = config_prefix + ".volumes";

    if (!config.has(volumes_prefix))
    {
        if (name != DEFAULT_STORAGE_POLICY_NAME)
            throw Exception("Storage policy " + backQuote(name) + " must contain at least one volume (.volumes)", ErrorCodes::NO_ELEMENTS_IN_CONFIG);
    }
    else
    {
        config.keys(volumes_prefix, keys);
    }

    for (const auto & attr_name : keys)
    {
        if (!std::all_of(attr_name.begin(), attr_name.end(), isWordCharASCII))
            throw Exception(
                "Volume name can contain only alphanumeric and '_' in storage policy " + backQuote(name) + " (" + attr_name + ")", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);
        volumes.emplace_back(createVolumeFromConfig(attr_name, config, volumes_prefix + "." + attr_name, disks));
    }

    if (volumes.empty() && name == DEFAULT_STORAGE_POLICY_NAME)
    {
        auto default_volume = std::make_shared<VolumeJBOD>(DEFAULT_VOLUME_NAME, std::vector<DiskPtr>{disks->get(DEFAULT_DISK_NAME)}, 0, false);
        volumes.emplace_back(std::move(default_volume));
    }

    if (volumes.empty())
        throw Exception("Storage policy " + backQuote(name) + " must contain at least one volume.", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    const double default_move_factor = volumes.size() > 1 ? 0.1 : 0.0;
    move_factor = config.getDouble(config_prefix + ".move_factor", default_move_factor);
    if (move_factor > 1)
        throw Exception("Disk move factor have to be in [0., 1.] interval, but set to " + toString(move_factor) + " in storage policy " + backQuote(name), ErrorCodes::LOGICAL_ERROR);

    buildVolumeIndices();
}


StoragePolicy::StoragePolicy(String name_, Volumes volumes_, double move_factor_)
    : volumes(std::move(volumes_)), name(std::move(name_)), move_factor(move_factor_)
{
    if (volumes.empty())
        throw Exception("Storage policy " + backQuote(name) + " must contain at least one Volume.", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

    if (move_factor > 1)
        throw Exception("Disk move factor have to be in [0., 1.] interval, but set to " + toString(move_factor) + " in storage policy " + backQuote(name), ErrorCodes::LOGICAL_ERROR);

    buildVolumeIndices();
}


StoragePolicy::StoragePolicy(const StoragePolicy & storage_policy,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        DiskSelectorPtr disks)
    : StoragePolicy(storage_policy.getName(), config, config_prefix, disks)
{
    for (auto & volume : volumes)
    {
        if (storage_policy.volume_index_by_volume_name.count(volume->getName()) > 0)
        {
            auto old_volume = storage_policy.getVolumeByName(volume->getName());
            try
            {
                auto new_volume = updateVolumeFromConfig(old_volume, config, config_prefix + ".volumes." + volume->getName(), disks);
                volume = std::move(new_volume);
            }
            catch (Exception & e)
            {
                /// Default policies are allowed to be missed in configuration.
                if (e.code() != ErrorCodes::NO_ELEMENTS_IN_CONFIG || storage_policy.getName() != DEFAULT_STORAGE_POLICY_NAME)
                    throw;

                Poco::Util::AbstractConfiguration::Keys keys;
                config.keys(config_prefix, keys);
                if (!keys.empty())
                    throw;
            }
        }
    }
}


bool StoragePolicy::isDefaultPolicy() const
{
    /// Guessing if this policy is default, not 100% correct though.

    if (getName() != DEFAULT_STORAGE_POLICY_NAME)
        return false;

    if (volumes.size() != 1)
        return false;

    if (volumes[0]->getName() != DEFAULT_VOLUME_NAME)
        return false;

    const auto & disks = volumes[0]->getDisks();
    if (disks.size() != 1)
        return false;

    if (disks[0]->getName() != DEFAULT_DISK_NAME)
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
        throw Exception("Storage policy " + backQuote(name) + " has no volumes. It's a bug.", ErrorCodes::LOGICAL_ERROR);

    if (volumes[0]->getDisks().empty())
        throw Exception("Volume " + backQuote(name) + "." + backQuote(volumes[0]->getName()) + " has no disks. It's a bug.", ErrorCodes::LOGICAL_ERROR);

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


ReservationPtr StoragePolicy::reserveAndCheck(UInt64 bytes) const
{
    if (auto res = reserve(bytes, 0))
        return res;
    throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "Cannot reserve {}, not enough space", ReadableSize(bytes));
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
    auto reservation = max_disk->reserve(0);
    if (!reservation)
    {
        /// I'm not sure if it's really a logical error, but exception message
        /// "Cannot reserve 0 bytes" looks too strange to throw it with another exception code.
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot reserve 0 bytes");
    }
    return reservation;
}


VolumePtr StoragePolicy::getVolume(size_t index) const
{
    if (index < volume_index_by_volume_name.size())
        return volumes[index];
    else
        throw Exception("No volume with index " + std::to_string(index) + " in storage policy " + backQuote(name), ErrorCodes::UNKNOWN_VOLUME);
}


VolumePtr StoragePolicy::getVolumeByName(const String & volume_name) const
{
    auto it = volume_index_by_volume_name.find(volume_name);
    if (it == volume_index_by_volume_name.end())
        throw Exception("No such volume " + backQuote(volume_name) + " in storage policy " + backQuote(name), ErrorCodes::UNKNOWN_VOLUME);
    return getVolume(it->second);
}


void StoragePolicy::checkCompatibleWith(const StoragePolicyPtr & new_storage_policy) const
{
    std::unordered_set<String> new_volume_names;
    for (const auto & volume : new_storage_policy->getVolumes())
        new_volume_names.insert(volume->getName());

    for (const auto & volume : getVolumes())
    {
        if (new_volume_names.count(volume->getName()) == 0)
            throw Exception("New storage policy " + backQuote(name) + " shall contain volumes of old one", ErrorCodes::BAD_ARGUMENTS);

        std::unordered_set<String> new_disk_names;
        for (const auto & disk : new_storage_policy->getVolumeByName(volume->getName())->getDisks())
            new_disk_names.insert(disk->getName());

        for (const auto & disk : volume->getDisks())
            if (new_disk_names.count(disk->getName()) == 0)
                throw Exception("New storage policy " + backQuote(name) + " shall contain disks of old one", ErrorCodes::BAD_ARGUMENTS);
    }
}


size_t StoragePolicy::getVolumeIndexByDisk(const DiskPtr & disk_ptr) const
{
    auto it = volume_index_by_disk_name.find(disk_ptr->getName());
    if (it != volume_index_by_disk_name.end())
        return it->second;
    else
        throw Exception("No disk " + backQuote(disk_ptr->getName()) + " in policy " + backQuote(name), ErrorCodes::UNKNOWN_DISK);
}


void StoragePolicy::buildVolumeIndices()
{
    for (size_t index = 0; index < volumes.size(); ++index)
    {
        const VolumePtr & volume = volumes[index];

        if (volume_index_by_volume_name.find(volume->getName()) != volume_index_by_volume_name.end())
            throw Exception("Volume names must be unique in storage policy "
                    + backQuote(name) + " (" + backQuote(volume->getName()) + " is duplicated)"
                , ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

        volume_index_by_volume_name[volume->getName()] = index;

        for (const auto & disk : volume->getDisks())
        {
            const String & disk_name = disk->getName();

            if (volume_index_by_disk_name.find(disk_name) != volume_index_by_disk_name.end())
                throw Exception("Disk names must be unique in storage policy "
                        + backQuote(name) + " (" + backQuote(disk_name) + " is duplicated)"
                    , ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

            volume_index_by_disk_name[disk_name] = index;
        }
    }
}

bool StoragePolicy::hasAnyVolumeWithDisabledMerges() const
{
    for (const auto & volume : volumes)
        if (volume->areMergesAvoided())
            return true;
    return false;
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
                "Storage policy name can contain only alphanumeric and '_' (" + backQuote(name) + ")", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

        policies.emplace(name, std::make_shared<StoragePolicy>(name, config, config_prefix + "." + name, disks));
        LOG_INFO(&Poco::Logger::get("StoragePolicySelector"), "Storage policy {} loaded", backQuote(name));
    }

    /// Add default policy if it isn't explicitly specified.
    if (policies.find(DEFAULT_STORAGE_POLICY_NAME) == policies.end())
    {
        auto default_policy = std::make_shared<StoragePolicy>(DEFAULT_STORAGE_POLICY_NAME, config, config_prefix + "." + DEFAULT_STORAGE_POLICY_NAME, disks);
        policies.emplace(DEFAULT_STORAGE_POLICY_NAME, std::move(default_policy));
    }
}


StoragePolicySelectorPtr StoragePolicySelector::updateFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, DiskSelectorPtr disks) const
{
    std::shared_ptr<StoragePolicySelector> result = std::make_shared<StoragePolicySelector>(config, config_prefix, disks);

    /// First pass, check.
    for (const auto & [name, policy] : policies)
    {
        if (result->policies.count(name) == 0)
            throw Exception("Storage policy " + backQuote(name) + " is missing in new configuration", ErrorCodes::BAD_ARGUMENTS);

        policy->checkCompatibleWith(result->policies[name]);
    }

    /// Second pass, load.
    for (const auto & [name, policy] : policies)
    {
        result->policies[name] = std::make_shared<StoragePolicy>(*policy, config, config_prefix + "." + name, disks);
    }

    return result;
}


StoragePolicyPtr StoragePolicySelector::get(const String & name) const
{
    auto it = policies.find(name);
    if (it == policies.end())
        throw Exception("Unknown storage policy " + backQuote(name), ErrorCodes::UNKNOWN_POLICY);

    return it->second;
}

}
