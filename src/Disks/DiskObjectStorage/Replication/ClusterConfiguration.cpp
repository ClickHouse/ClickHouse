#include <Disks/DiskObjectStorage/Replication/ClusterConfiguration.h>
#include <Disks/DiskObjectStorage/Replication/Location.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>

#include <Common/Exception.h>
#include <Common/UniqueLock.h>
#include <Common/logger_useful.h>

#include <fmt/ranges.h>

#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ClusterConfiguration::ClusterConfiguration(std::unordered_map<Location, LocationInfo> locations_)
    : locations(std::move(locations_))
{
    LOG_DEBUG(getLogger("ClusterConfiguration"), "Cluster Configuration: {}", locations | std::views::keys | std::ranges::to<std::vector<std::string>>());
}

void ClusterConfiguration::applyNewSettings(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    UniqueLock guard(mutex);

    Locations updated_locations;
    config.keys(config_prefix + ".locations", updated_locations);

    for (const auto & location : updated_locations)
    {
        auto & info = locations.at(location);
        info.local = config.getBool(config_prefix + ".locations." + location + ".local");
        info.enabled = config.getBool(config_prefix + ".locations." + location + ".enabled");
    }
}

std::unordered_map<Location, LocationInfo> ClusterConfiguration::getConfiguration() const
{
    SharedLockGuard guard(mutex);
    return locations;
}

bool ClusterConfiguration::areAllLocationsEnabled(const LocationSet & to_check) const
{
    SharedLockGuard guard(mutex);

    for (const auto & location : to_check)
        if (!locations.at(location).enabled)
            return false;

    return true;
}

bool ClusterConfiguration::isLocalLocationIn(const Locations & to_check) const
{
    SharedLockGuard guard(mutex);

    for (const auto & location : to_check)
        if (locations.at(location).local)
            return true;

    return false;
}

bool ClusterConfiguration::isLocationEnabled(const Location & to_check) const
{
    SharedLockGuard guard(mutex);
    return locations.at(to_check).enabled;
}

Locations ClusterConfiguration::findComplement(Locations redacted, bool use_only_enabled) const
{
    return findComplement(redacted | std::ranges::to<LocationSet>(), use_only_enabled);
}

Locations ClusterConfiguration::findComplement(LocationSet redacted, bool use_only_enabled) const
{
    SharedLockGuard guard(mutex);

    Locations result;
    for (const auto & [location, info] : locations)
        if (!redacted.contains(location))
            if (!use_only_enabled || info.enabled)
                result.push_back(location);

    return result;
}

Locations ClusterConfiguration::getEnabledLocations() const
{
    SharedLockGuard guard(mutex);

    Locations result;
    for (const auto & [location, info] : locations)
        if (info.enabled)
            result.push_back(location);

    return result;
}

Location ClusterConfiguration::getLocalLocation() const
{
    SharedLockGuard guard(mutex);

    for (const auto & [location, info] : locations)
        if (info.local)
            return location;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no local location in cluster configuration");
}

}
