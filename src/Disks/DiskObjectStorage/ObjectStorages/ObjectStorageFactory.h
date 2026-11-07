#pragma once
#include <boost/noncopyable.hpp>
#include <Access/Common/AccessType.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>

#include <optional>

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

    /// The source a user needs a grant on to define a disk of this type in SQL.
    struct SourceAccess
    {
        AccessTypeObjects::Source source;
        /// The type cannot write by contract, so `READ` is enough.
        bool read_only = false;
    };

    static ObjectStorageFactory & instance();

    void registerObjectStorageType(const std::string & type, Creator creator, std::optional<SourceAccess> source_access);

    bool isRegistered(const std::string & type) const;

    /// The source declared by a registered type, `std::nullopt` if it has none or is not registered.
    std::optional<SourceAccess> getSourceAccessObject(const std::string & type) const;

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
    struct RegisteredType
    {
        Creator creator;
        std::optional<SourceAccess> source_access;
    };

    using Registry = std::unordered_map<String, RegisteredType>;
    Registry registry;
};

}
