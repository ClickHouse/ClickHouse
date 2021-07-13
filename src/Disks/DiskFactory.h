#pragma once

#include <Disks/IDisk.h>
#include <Interpreters/Context_fwd.h>
#include <common/types.h>

#include <boost/noncopyable.hpp>
#include <Poco/Util/AbstractConfiguration.h>

#include <functional>
#include <unordered_map>


namespace DB
{

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
        ContextPtr context)>;

    static DiskFactory & instance();

    void registerDiskType(const String & disk_type, Creator creator);

    DiskPtr create(
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context) const;

private:
    using DiskTypeRegistry = std::unordered_map<String, Creator>;
    DiskTypeRegistry registry;
};

}
