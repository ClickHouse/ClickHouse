#pragma once

#include <Core/Types.h>
#include <Disks/IDisk.h>

#include <functional>
#include <unordered_map>
#include <boost/noncopyable.hpp>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
class Context;

class DiskFactory final : private boost::noncopyable
{
public:
    using Creator = std::function<DiskPtr(
        const String & name, const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        const Context & context)>;

    static DiskFactory & instance();

    void registerDisk(const String & disk_type, Creator creator);

    DiskPtr get(
        const String & name, const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        const Context & context) const;

private:
    using DiskTypeRegistry = std::unordered_map<String, Creator>;
    DiskTypeRegistry registry;
};

}
