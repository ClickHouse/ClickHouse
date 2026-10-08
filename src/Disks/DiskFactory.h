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

    void clearRegistry();

private:
    using DiskTypeRegistry = std::unordered_map<String, Creator>;
    DiskTypeRegistry registry;

    /// Embedded documentation, keyed by disk type.
    std::unordered_map<String, Documentation> documentations;
};

}
