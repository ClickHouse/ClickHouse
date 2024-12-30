#include "StoragePolicy.h"
#include "DiskFactory.h"
#include "DiskLocal.h"
#include "createVolume.h"

#include <Interpreters/Context.h>
#include <Common/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/formatReadable.h>
#include <Common/quoteString.h>
#include <Common/logger_useful.h>
#include <Disks/VolumeJBOD.h>

#include <algorithm>
#include <set>


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
    extern const int INVALID_CONFIG_PARAMETER;
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
    , log(getLogger("StoragePolicy (" + name + ")"))
{
    Poco::Util::AbstractConfiguration::Keys keys;
    String volumes_prefix = config_prefix + ".volumes";

    if (!config.has(volumes_prefix))
    {
        if (name != DEFAULT_STORAGE_POLICY_NAME)
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Storage policy {} must contain at least one volume (.volumes)", backQuote(name));
    }
    else
    {
        config.keys(volumes_prefix, keys);
    }

    std::set<UInt64> volume_priorities;

    for (const auto & attr_name : keys)
    {
        if (!std::all_of(attr_name.begin(), attr_name.end(), isWordCharASCII))
            throw Exception(ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG,
                            "Volume name can contain only alphanumeric and '_' in storage policy {} ({})",
                            backQuote(name), attr_name);
        volumes.emplace_back(createVolumeFromConfig(attr_name, config, volumes_prefix + "." + attr_name, disks));

        UInt64 last_priority = volumes.back()->volume_priority;
        if (last_priority != std::numeric_limits<UInt64>::max() && !volume_priorities.insert(last_priority).second)
        {
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "volume_priority values must be unique across the policy");
        }
    }

    if (!volume_priorities.empty())
    {
        /// Check that priority values cover the range from 1 to N (lowest explicit priority)
        if (*volume_priorities.begin() != 1 || *volume_priorities.rbegin() != volume_priorities.size())
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "volume_priority values must cover the range from 1 to N (lowest priority specified) without gaps");

        std::stable_sort(
            volumes.begin(), volumes.end(),
            [](const VolumePtr a, const VolumePtr b) { return a->volume_priority < b->volume_priority; });
    }

    if (volumes.empty() && name == DEFAULT_STORAGE_POLICY_NAME)
    {
        auto default_volume = std::make_shared<VolumeJBOD>(DEFAULT_VOLUME_NAME,
            std::vector<DiskPtr>{disks->get(DEFAULT_DISK_NAME)},
            /* max_data_part_size_= */ 0,
            /* are_merges_avoided_= */ false,
            /* perform_ttl_move_on_insert_= */ true,
            VolumeLoadBalancing::ROUND_ROBIN,
            /* least_used_ttl_ms_= */ 60'000);
        volumes.emplace_back(std::move(default_volume));
    }

    if (volumes.empty())
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Storage policy {} must contain at least one volume.", backQuote(name));

    const double default_move_factor = volumes.size() > 1 ? 0.1 : 0.0;
    move_factor = config.getDouble(config_prefix + ".move_factor", default_move_factor);
    if (move_factor > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Disk move factor have to be in [0., 1.] interval, but set to {} in storage policy {}",
                        toString(move_factor), backQuote(name));

    buildVolumeIndices();
    LOG_TRACE(log, "Storage policy {} created, total volumes {}", name, volumes.size());
}


StoragePolicy::StoragePolicy(String name_, Volumes volumes_, double move_factor_)
    : volumes(std::move(volumes_))
    , name(std::move(name_))
    , move_factor(move_factor_)
    , log(getLogger("StoragePolicy (" + name + ")"))
{
    if (volumes.empty())
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Storage policy {} must contain at least one Volume.", backQuote(name));

    if (move_factor > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Disk move factor have to be in [0., 1.] interval, but set to {} in storage policy {}",
                        toString(move_factor), backQuote(name));

    buildVolumeIndices();
    LOG_TRACE(log, "Storage policy {} created, total volumes {}", name, volumes.size());
}


StoragePolicy::StoragePolicy(StoragePolicyPtr storage_policy,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        DiskSelectorPtr disks)
    : StoragePolicy(storage_policy->getName(), config, config_prefix, disks)
{
    for (auto & volume : volumes)
    {
        if (storage_policy->containsVolume(volume->getName()))
        {
            auto old_volume = storage_policy->getVolumeByName(volume->getName());
            try
            {
                auto new_volume = updateVolumeFromConfig(old_volume, config, config_prefix + ".volumes." + volume->getName(), disks);
                volume = std::move(new_volume);
            }
            catch (Exception & e)
            {
                /// Default policies are allowed to be missed in configuration.
                if (e.code() != ErrorCodes::NO_ELEMENTS_IN_CONFIG || storage_policy->getName() != DEFAULT_STORAGE_POLICY_NAME)
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Storage policy {} has no volumes. It's a bug.", backQuote(name));

    for (const auto & volume : volumes)
    {
        if (volume->getDisks().empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Volume '{}' has no disks. It's a bug", volume->getName());
        for (const auto & disk : volume->getDisks())
        {
            if (!disk->isBroken())
                return disk;
        }
    }

    throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "All disks in storage policy {} are broken", name);
}


DiskPtr StoragePolicy::tryGetDiskByName(const String & disk_name) const
{
    for (auto && volume : volumes)
        for (auto && disk : volume->getDisks())
            if (disk->getName() == disk_name)
                return disk;
    return {};
}


UInt64 StoragePolicy::getMaxUnreservedFreeSpace() const
{
    std::optional<UInt64> res;
    for (const auto & volume : volumes)
    {
        auto volume_unreserved_space = volume->getMaxUnreservedFreeSpace();
        if (!volume_unreserved_space)
            return -1ULL; /// There is at least one unlimited disk.

        if (!res || *volume_unreserved_space > *res)
            res = volume_unreserved_space;
    }
    return res.value_or(-1ULL);
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
    LOG_TRACE(log, "Could not reserve {} from volume index {}, total volumes {}", ReadableSize(bytes), min_volume_index, volumes.size());

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
    bool found_bottomless_disk = false;
    DiskPtr max_disk;

    for (const auto & volume : volumes)
    {
        for (const auto & disk : volume->getDisks())
        {
            auto available_space = disk->getAvailableSpace();

            if (!available_space)
            {
                max_disk = disk;
                found_bottomless_disk = true;
                break;
            }

            if (*available_space > max_space)
            {
                max_space = *available_space;
                max_disk = disk;
            }
        }

        if (found_bottomless_disk)
            break;
    }

    if (!max_disk)
        throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "There is no space on any disk in storage policy: {}. "
            "It's likely all disks are broken", name);

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
    throw Exception(ErrorCodes::UNKNOWN_VOLUME, "No volume with index {} in storage policy {}", std::to_string(index), backQuote(name));
}


VolumePtr StoragePolicy::tryGetVolumeByName(const String & volume_name) const
{
    auto it = volume_index_by_volume_name.find(volume_name);
    if (it == volume_index_by_volume_name.end())
        return nullptr;
    return getVolume(it->second);
}


void StoragePolicy::checkCompatibleWith(const StoragePolicyPtr & new_storage_policy) const
{
    /// Do not check volumes for temporary policy because their names are automatically generated
    bool skip_volume_check = this->getName().starts_with(StoragePolicySelector::TMP_STORAGE_POLICY_PREFIX)
        || new_storage_policy->getName().starts_with(StoragePolicySelector::TMP_STORAGE_POLICY_PREFIX);

    if (!skip_volume_check)
    {
        std::unordered_set<String> new_volume_names;
        for (const auto & volume : new_storage_policy->getVolumes())
            new_volume_names.insert(volume->getName());

        for (const auto & volume : getVolumes())
        {
            if (!new_volume_names.contains(volume->getName()))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "New storage policy {} shall contain volumes of the old storage policy {}",
                    backQuote(new_storage_policy->getName()),
                    backQuote(name));

            std::unordered_set<String> new_disk_names;
            for (const auto & disk : new_storage_policy->getVolumeByName(volume->getName())->getDisks())
                new_disk_names.insert(disk->getName());

            for (const auto & disk : volume->getDisks())
                if (!new_disk_names.contains(disk->getName()))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "New storage policy {} shall contain disks of the old storage policy {}",
                        backQuote(new_storage_policy->getName()),
                        backQuote(name));
        }
    }
    else
    {
        std::unordered_set<String> new_disk_names;
        for (const auto & disk : new_storage_policy->getDisks())
            new_disk_names.insert(disk->getName());

        for (const auto & disk : this->getDisks())
            if (!new_disk_names.contains(disk->getName()))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "New storage policy {} shall contain disks of the old storage policy {}",
                    backQuote(new_storage_policy->getName()),
                    backQuote(name));
    }
}


std::optional<size_t> StoragePolicy::tryGetVolumeIndexByDiskName(const String & disk_name) const
{
    auto it = volume_index_by_disk_name.find(disk_name);
    if (it != volume_index_by_disk_name.end())
        return it->second;
    return {};
}


void StoragePolicy::buildVolumeIndices()
{
    for (size_t index = 0; index < volumes.size(); ++index)
    {
        const VolumePtr & volume = volumes[index];

        if (volume_index_by_volume_name.find(volume->getName()) != volume_index_by_volume_name.end())
            throw Exception(ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG,
                            "Volume names must be unique in storage policy {} ({} "
                            "is duplicated)" , backQuote(name), backQuote(volume->getName()));

        volume_index_by_volume_name[volume->getName()] = index;

        for (const auto & disk : volume->getDisks())
        {
            const String & disk_name = disk->getName();

            if (volume_index_by_disk_name.find(disk_name) != volume_index_by_disk_name.end())
                throw Exception(ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG,
                                "Disk names must be unique in storage policy {} ({} "
                                "is duplicated)" , backQuote(name), backQuote(disk_name));

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

bool StoragePolicy::containsVolume(const String & volume_name) const
{
    return volume_index_by_volume_name.contains(volume_name);
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
            throw Exception(ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG,
                            "Storage policy name can contain only alphanumeric and '_' ({})", backQuote(name));

        /*
         * A customization point for StoragePolicy, here one can add his own policy, for example, based on policy's name
         * if (name == "MyCustomPolicy")
         *      policies.emplace(name, std::make_shared<CustomPolicy>(name, config, config_prefix + "." + name, disks));
         *  else
         */

        policies.emplace(name, std::make_shared<StoragePolicy>(name, config, config_prefix + "." + name, disks));
        LOG_INFO(getLogger("StoragePolicySelector"), "Storage policy {} loaded", backQuote(name));
    }

    /// Add default policy if it isn't explicitly specified.
    if (policies.find(DEFAULT_STORAGE_POLICY_NAME) == policies.end())
    {
        auto default_policy = std::make_shared<StoragePolicy>(DEFAULT_STORAGE_POLICY_NAME, config, config_prefix + "." + DEFAULT_STORAGE_POLICY_NAME, disks);
        policies.emplace(DEFAULT_STORAGE_POLICY_NAME, std::move(default_policy));
    }
}


StoragePolicySelectorPtr StoragePolicySelector::updateFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, DiskSelectorPtr disks, Strings & new_disks) const
{
    std::shared_ptr<StoragePolicySelector> result = std::make_shared<StoragePolicySelector>(config, config_prefix, disks);
    std::set<String> disks_before_reload;
    std::set<String> disks_after_reload;
    /// First pass, check.
    for (const auto & [name, policy] : policies)
    {
        if (!name.starts_with(TMP_STORAGE_POLICY_PREFIX))
        {
            if (!result->policies.contains(name))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Storage policy {} is missing in new configuration", backQuote(name));

            policy->checkCompatibleWith(result->policies[name]);
        }

        for (const auto & disk : policy->getDisks())
        {
            disks_before_reload.insert(disk->getName());
        }
    }

    /// Second pass, load.
    for (const auto & [name, policy] : policies)
    {
        /// Do not reload from config temporary storage policy, because it is not present in config.
        if (name.starts_with(TMP_STORAGE_POLICY_PREFIX))
            result->policies[name] = policy;
        else
            result->policies[name] = std::make_shared<StoragePolicy>(policy, config, config_prefix + "." + name, disks);

        for (const auto & disk : result->policies[name]->getDisks())
            disks_after_reload.insert(disk->getName());
    }

    std::set_difference(
        disks_after_reload.begin(),
        disks_after_reload.end(),
        disks_before_reload.begin(),
        disks_before_reload.end(),
        std::back_inserter(new_disks));

    return result;
}

StoragePolicyPtr StoragePolicySelector::tryGet(const String & name) const
{
    auto it = policies.find(name);
    if (it == policies.end())
        return nullptr;

    return it->second;
}

StoragePolicyPtr StoragePolicySelector::get(const String & name) const
{
    auto policy = tryGet(name);
    if (!policy)
        throw Exception(ErrorCodes::UNKNOWN_POLICY, "Unknown storage policy {}", backQuote(name));

    return policy;
}

void StoragePolicySelector::add(StoragePolicyPtr storage_policy)
{
    auto [_, inserted] = policies.emplace(storage_policy->getName(), storage_policy);
    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "StoragePolicy is already present in StoragePolicySelector");
}

}
