#pragma once

#include <Disks/DiskSelector.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

class KeeperDiskSelector
{
public:
    void initialize(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context);

    DiskSelectorPtr updateFromConfig(
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context) const;

    /// Get disk by name
    DiskPtr get(const String & name) const;

    DiskPtr tryGet(const String & name) const;

    /// Get all disks with names
    const DisksMap & getDisksMap() const;

    void shutdown();

private:
    mutable std::mutex disk_selector_mutex;
    DiskSelectorPtr disk_selector;
};

using KeeperDiskSelectorPtr = std::shared_ptr<KeeperDiskSelector>;


}
