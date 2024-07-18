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
    static constexpr auto TMP_INTERNAL_DISK_PREFIX = "__tmp_internal_";

    DiskSelector() = default;
    DiskSelector(const DiskSelector & from) = default;

    using DiskValidator = std::function<bool(const Poco::Util::AbstractConfiguration & config, const String & disk_config_prefix)>;
    void initialize(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context, DiskValidator disk_validator = {});

    DiskSelectorPtr updateFromConfig(
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context) const;

    /// Get disk by name
    DiskPtr get(const String & name) const;

    DiskPtr tryGet(const String & name) const;

    /// Get all disks with names
    const DisksMap & getDisksMap() const;

    void addToDiskMap(const String & name, DiskPtr disk);

    void shutdown();

private:
    DisksMap disks;
    bool is_initialized = false;

    void assertInitialized() const;
};

}
