#pragma once

#include <Interpreters/Context.h>
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
    DiskSelector(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, const Context & context);
    DiskSelector(const DiskSelector & from) : disks(from.disks) { }

    DiskSelectorPtr
    updateFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, const Context & context) const;

    /// Get disk by name
    DiskPtr get(const String & name) const;

    /// Get all disks with names
    const auto & getDisksMap() const { return disks; }

private:
    std::map<String, DiskPtr> disks;
};

}
