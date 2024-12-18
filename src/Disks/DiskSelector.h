#pragma once

#include <Disks/DiskFactory.h>
#include <Disks/IDisk.h>
#include "base/types.h"

#include <map>
#include <optional>

namespace Poco::Util
{
    class AbstractConfiguration;
};

namespace DB
{

class DiskSelector;
using DiskSelectorPtr = std::shared_ptr<const DiskSelector>;

/// Parse .xml configuration and store information about disks
/// Mostly used for introspection.
class DiskSelector
{
    using DiskCreator = std::function<DiskPtr()>;
    using PostponedDisksMap = std::map<String, DiskCreator>;

public:
    static constexpr auto TMP_INTERNAL_DISK_PREFIX = "__tmp_internal_";

    explicit DiskSelector(std::unordered_set<String> skip_types_ = {}) : skip_types(std::move(skip_types_)) { }
    DiskSelector(const DiskSelector & from) = default;

    using DiskValidator = std::function<bool(const Poco::Util::AbstractConfiguration & config, const String & disk_config_prefix, const String & disk_name)>;
    void initialize(
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        DiskValidator disk_validator = {},
        bool lazy_disk_creation = false);

    DiskSelectorPtr updateFromConfig(
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context) const;

    /// Get disk by name
    DiskPtr get(const String & name) const;

    DiskPtr tryGet(const String & name) const;

    /// Get all disks with names

    void addToDiskMap(const String & name, DiskPtr disk);

    void shutdown();

    inline static const String DEFAULT_DISK_NAME = "default";
    inline static const String LOCAL_DISK_NAME = "local";

private:
    DisksMap created_disks;
    PostponedDisksMap postponed_disks;

    bool is_initialized = false;

    const DisksMap & getDisksMap() const;

    void assertInitialized() const;

    const std::unordered_set<String> skip_types;

    bool throw_away_local_on_update = false;
};

}
