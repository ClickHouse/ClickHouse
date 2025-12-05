#pragma once

#include <Access/MemoryAccessStorage.h>
#include <Access/Credentials.h>
#include <Common/re2.h>
#include <base/types.h>
#include <base/scope_guard.h>
#include <map>
#include <mutex>
#include <set>
#include <vector>


namespace Poco
{
    namespace Util
    {
        class AbstractConfiguration;
    }
}


namespace DB
{
class AccessControl;

/// Implementation of IAccessStorage which allows to import user data from oauth server using access token.
/// Normally, this should be unified with LDAPAccessStorage, but not done to minimize changes to code that is common with upstream.
class TokenAccessStorage : public IAccessStorage
{
public:
    static constexpr char STORAGE_TYPE[] = "token";

    explicit TokenAccessStorage(const String & storage_name_, AccessControl & access_control_, const Poco::Util::AbstractConfiguration & config, const String & prefix);
    ~TokenAccessStorage() override = default;

    // IAccessStorage implementations.
    const char * getStorageType() const override;
    String getStorageParamsJSON() const override;
    bool isReadOnly() const override { return true; }
    bool exists(const UUID & id) const override;

private:
    mutable std::recursive_mutex mutex; // Note: Reentrance possible by internal role lookup via access_control
    AccessControl & access_control;
    const Poco::Util::AbstractConfiguration & config;
    const String & prefix;

    String provider_name;
    std::optional<re2::RE2> roles_filter = std::nullopt;

    std::set<String> common_role_names;                         // role name that should be granted to all users at all times
    mutable std::map<String, std::set<String>> user_external_roles;
    mutable std::map<String, std::set<String>> users_per_roles; // role name -> user names (...it should be granted to; may but don't have to exist for common roles)
    mutable std::map<String, std::set<String>> roles_per_users; // user name -> role names (...that should be granted to it; may but don't have to include common roles)
    mutable std::map<UUID, String> granted_role_names;          // (currently granted) role id -> its name
    mutable std::map<String, UUID> granted_role_ids;            // (currently granted) role name -> its id
    mutable MemoryAccessStorage memory_storage;
    scope_guard role_change_subscription;

    void processRoleChange(const UUID & id, const AccessEntityPtr & entity);

    bool areTokenCredentialsValidNoLock(const User & user, const Credentials & credentials, const ExternalAuthenticators & external_authenticators) const;

    void applyRoleChangeNoLock(bool grant, const UUID & role_id, const String & role_name);
    void assignRolesNoLock(User & user, const std::set<String> & external_roles) const;
    void updateAssignedRolesNoLock(const UUID & id, const String & user_name, const std::set<String> & external_roles) const;

protected:
    std::optional<UUID> findImpl(AccessEntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(AccessEntityType type) const override;
    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override;
    std::optional<std::pair<String, AccessEntityType>> readNameWithTypeImpl(const UUID & id, bool throw_if_not_exists) const override;
    std::optional<AuthResult> authenticateImpl(const Credentials & credentials,
                                               const Poco::Net::IPAddress & address,
                                               const ExternalAuthenticators & external_authenticators,
                                               const ClientInfo & client_info,
                                               bool throw_if_user_not_exists,
                                               bool allow_no_password,
                                               bool allow_plaintext_password) const override;
};
}
