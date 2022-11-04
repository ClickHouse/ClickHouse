#pragma once

#include <Disks/DiskFactory.h>
#include <Disks/IDisk.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <map>

namespace DB
{

class DiskSelector;
using DiskSelectorPtr = std::shared_ptr<const DiskSelector>;

/// Parse .xml configuration and store information about disks
/// Mostly used for introspection.
class DiskSelector
{
public:
    DiskSelector(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context);
    DiskSelector(const DiskSelector & from) = default;

    DiskSelectorPtr updateFromConfig(
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context
    ) const;

    /// Get disk by name
    DiskPtr get(const String & name) const;

    /// Get all disks with names
    const DisksMap & getDisksMap() const { return disks; }
    void addToDiskMap(const String & name, DiskPtr disk)
    {
        disks.emplace(name, disk);
    }

    void shutdown()
    {
        for (auto & e : disks)
            e.second->shutdown();
    }

private:
    DisksMap disks;
};

}
