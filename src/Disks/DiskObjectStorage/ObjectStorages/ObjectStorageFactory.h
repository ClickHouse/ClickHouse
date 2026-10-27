#pragma once
#include <boost/noncopyable.hpp>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>

namespace DB
{

/// How the endpoint of an object storage is checked against remote_url_allow_hosts.
/// Anything but `None` also means the disk comes from user SQL, which Azure uses to restrict the server's identity.
enum class RemoteHostFilterCheck : uint8_t
{
    /// Endpoint from the server config: not checked.
    None,
    /// Custom disk loaded from existing metadata (server start, UNDROP): a rejection is reported, the load continues.
    Warn,
    /// Custom disk defined by a query: a rejection throws.
    Enforce,
};

class ObjectStorageFactory final : private boost::noncopyable
{
public:
    using Creator = std::function<ObjectStoragePtr(
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ContextPtr & context,
        bool run_access_check,
        bool run_local_paths_check,
        RemoteHostFilterCheck remote_host_filter_check)>;

    static ObjectStorageFactory & instance();

    void registerObjectStorageType(const std::string & type, Creator creator);

    ObjectStoragePtr create(
        const std::string & name,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ContextPtr & context,
        bool run_access_check,
        bool run_local_paths_check,
        RemoteHostFilterCheck remote_host_filter_check) const;

    void clearRegistry();

private:
    using Registry = std::unordered_map<String, Creator>;
    Registry registry;
};

}
