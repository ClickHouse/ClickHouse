#pragma once

#include <Disks/DiskObjectStorage/Replication/Location.h>

#include <Common/SharedMutex.h>

#include <base/defines.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <unordered_map>

namespace DB
{

struct LocationInfo
{
    bool enabled;
    bool local;
    std::string config_prefix;
};

class ClusterConfiguration
{
public:
    explicit ClusterConfiguration(std::unordered_map<Location, LocationInfo> locations_);

    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);

    std::unordered_map<Location, LocationInfo> getConfiguration() const;
    bool areAllLocationsEnabled(const LocationSet & to_check) const;
    bool isLocalLocationIn(const Locations & to_check) const;
    bool isLocationEnabled(const Location & to_check) const;

    Locations findComplement(Locations redacted, bool use_only_enabled = false) const;
    Locations findComplement(LocationSet redacted, bool use_only_enabled = false) const;

    Locations getEnabledLocations() const;
    Location getLocalLocation() const;

private:
    mutable SharedMutex mutex;
    std::unordered_map<Location, LocationInfo> locations TSA_GUARDED_BY(mutex);
};

using ClusterConfigurationPtr = std::shared_ptr<ClusterConfiguration>;

}
