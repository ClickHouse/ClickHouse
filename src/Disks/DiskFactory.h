#pragma once

#include <Disks/IDisk.h>
#include <Interpreters/Context_fwd.h>
#include <Common/Documentation.h>
#include <base/types.h>

#include <boost/noncopyable.hpp>
#include <Poco/Util/AbstractConfiguration.h>

#include <functional>
#include <map>
#include <unordered_map>


namespace DB
{

class ConfigurationWithUsageTracking;

using DisksMap = std::map<String, DiskPtr, std::less<>>;
/**
 * Disk factory. Responsible for creating new disk objects.
 */
class DiskFactory final : private boost::noncopyable
{
public:
    using Creator = std::function<DiskPtr(
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap & map,
        bool attach,
        bool custom_disk)>;

    static DiskFactory & instance();

    void registerDiskType(const String & disk_type, Creator creator, Documentation documentation = {});

    /// Returns the names of all registered disk types.
    std::vector<String> getAllRegisteredNames() const; // STYLE_CHECK_ALLOW_STD_CONTAINERS

    /// Returns the embedded documentation for a disk type (empty if none was registered).
    Documentation getDocumentation(const String & disk_type) const;

    DiskPtr create(
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap & map,
        bool attach = false,
        bool custom_disk = false,
        const std::unordered_set<String> & skip_types = {}) const;

    /// Apply a reloaded configuration to a disk that already exists, and report the elements of its
    /// definition that nothing reads. The reload does not go through `create`, so the check is done
    /// here, over the keys read while the disk was created plus the keys read by `applyNewSettings`.
    /// The elements that are unknown for sure are reported before the disk is changed.
    void applyNewSettings(
        const DiskPtr & disk,
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap & map) const;

    /// The configuration to keep in a `local` disk that is created implicitly, without a section
    /// in the configuration (the `default` disk). It records the elements a `local` disk supports,
    /// so that a section added for this disk later is checked by `applyNewSettings` as well.
    static std::shared_ptr<const ConfigurationWithUsageTracking> trackImplicitLocalDisk(
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context);

    void clearRegistry();

private:
    /// Report the elements of the disk definition that nothing has read while the disk was created.
    static void checkForUnknownKeys(
        const ConfigurationWithUsageTracking & tracked_config,
        const String & name,
        const String & disk_type,
        const String & config_prefix,
        const ContextPtr & context,
        bool skip_used_sections);

    using DiskTypeRegistry = std::unordered_map<String, Creator>;
    DiskTypeRegistry registry;

    /// Embedded documentation, keyed by disk type.
    std::unordered_map<String, Documentation> documentations;
};

}
