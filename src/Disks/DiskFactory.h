#pragma once

#include <common/types.h>
#include <Disks/IDisk.h>

#include <functional>
#include <unordered_map>
#include <boost/noncopyable.hpp>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
class Context;

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
        const Context & context)>;

    static DiskFactory & instance();

    void registerDiskType(const String & disk_type, Creator creator);

    DiskPtr create(
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        const Context & context) const;

private:
    using DiskTypeRegistry = std::unordered_map<String, Creator>;
    DiskTypeRegistry registry;
};

}
