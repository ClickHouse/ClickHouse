#pragma once

#include <Access/MemoryAccessStorage.h>
#include <Common/SettingsChanges.h>
#include <base/defines.h>
#include <base/types.h>
#include <chrono>
#include <mutex>
#include <set>
#include <unordered_map>


namespace Poco::Util
{
    class AbstractConfiguration;
}


namespace DB
{
class AccessControl;

/// Implementation of `IAccessStorage` which authenticates users unknown to other storages with a server
/// from `http_authentication_servers`. A successful response creates a user with `HTTP` authentication
/// in memory. The roles from the response are not granted to the user: they are returned in `AuthResult`
/// and are valid only for this authentication.
class HTTPAccessStorage : public IAccessStorage
{
public:
    static constexpr char STORAGE_TYPE[] = "http";

    HTTPAccessStorage(const String & storage_name_, AccessControl & access_control_, const Poco::Util::AbstractConfiguration & config, const String & prefix);

    const char * getStorageType() const override { return STORAGE_TYPE; }
    String getStorageParamsJSON() const override;
    bool isReadOnly() const override { return true; }
    bool exists(const UUID & id) const override { return memory_storage.exists(id); }

private:
    std::optional<UUID> findImpl(AccessEntityType type, const String & name) const override { return memory_storage.find(type, name); }
    std::vector<UUID> findAllImpl(AccessEntityType type) const override { return memory_storage.findAll(type); }
    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override { return memory_storage.read(id, throw_if_not_exists); }
    std::optional<AuthResult> authenticateImpl(const Credentials & credentials, const Poco::Net::IPAddress & address, const ExternalAuthenticators & external_authenticators, const ClientInfo & client_info, bool throw_if_user_not_exists, bool allow_no_password, bool allow_plaintext_password) const override;

    /// Parsed body of a successful response of the authentication server.
    struct Response
    {
        Strings roles;
        SettingsChanges settings;
    };

    struct CacheEntry
    {
        UInt128 hash;
        std::chrono::steady_clock::time_point time;
        Response response;
    };

    std::vector<UUID> resolveRoles(const Strings & role_names, const String & user_name) const;
    UUID materializeUser(const String & user_name) const;

    AccessControl & access_control;
    String server_name;
    std::set<String> allowed_roles;
    String allowed_role_prefix;
    std::chrono::seconds cache_ttl{0};

    mutable std::mutex cache_mutex;
    mutable std::unordered_map<String, CacheEntry> cache TSA_GUARDED_BY(cache_mutex); /// user name -> cache entry
    mutable MemoryAccessStorage memory_storage;
};

}
