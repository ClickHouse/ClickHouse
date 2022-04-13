#pragma once

#include <Access/MemoryAccessStorage.h>
#include <Access/LDAPClient.h>
#include <Access/Credentials.h>
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

/// Implementation of IAccessStorage which allows attaching users from a remote LDAP server.
/// Currently, any user name will be treated as a name of an existing remote user,
/// a user info entity will be created, with LDAP authentication type.
class LDAPAccessStorage : public IAccessStorage
{
public:
    static constexpr char STORAGE_TYPE[] = "ldap";

    explicit LDAPAccessStorage(const String & storage_name_, AccessControl * access_control_, const Poco::Util::AbstractConfiguration & config, const String & prefix);
    virtual ~LDAPAccessStorage() override = default;

    String getLDAPServerName() const;

    // IAccessStorage implementations.
    virtual const char * getStorageType() const override;
    virtual String getStorageParamsJSON() const override;
    virtual bool isReadOnly() const override { return true; }
    virtual bool exists(const UUID & id) const override;
    virtual bool hasSubscription(const UUID & id) const override;
    virtual bool hasSubscription(AccessEntityType type) const override;

private: // IAccessStorage implementations.
    virtual std::optional<UUID> findImpl(AccessEntityType type, const String & name) const override;
    virtual std::vector<UUID> findAllImpl(AccessEntityType type) const override;
    virtual AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override;
    virtual std::optional<String> readNameImpl(const UUID & id, bool throw_if_not_exists) const override;
    virtual scope_guard subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const override;
    virtual scope_guard subscribeForChangesImpl(AccessEntityType type, const OnChangedHandler & handler) const override;
    virtual std::optional<UUID> authenticateImpl(const Credentials & credentials, const Poco::Net::IPAddress & address, const ExternalAuthenticators & external_authenticators, bool throw_if_user_not_exists, bool allow_no_password, bool allow_plaintext_password) const override;

    void setConfiguration(AccessControl * access_control_, const Poco::Util::AbstractConfiguration & config, const String & prefix);
    void processRoleChange(const UUID & id, const AccessEntityPtr & entity);

    void applyRoleChangeNoLock(bool grant, const UUID & role_id, const String & role_name);
    void assignRolesNoLock(User & user, const LDAPClient::SearchResultsList & external_roles) const;
    void assignRolesNoLock(User & user, const LDAPClient::SearchResultsList & external_roles, const std::size_t external_roles_hash) const;
    void updateAssignedRolesNoLock(const UUID & id, const String & user_name, const LDAPClient::SearchResultsList & external_roles) const;
    std::set<String> mapExternalRolesNoLock(const LDAPClient::SearchResultsList & external_roles) const;
    bool areLDAPCredentialsValidNoLock(const User & user, const Credentials & credentials,
        const ExternalAuthenticators & external_authenticators, LDAPClient::SearchResultsList & role_search_results) const;

    mutable std::recursive_mutex mutex;
    AccessControl * access_control = nullptr;
    String ldap_server_name;
    LDAPClient::RoleSearchParamsList role_search_params;
    std::set<String> common_role_names;                         // role name that should be granted to all users at all times
    mutable std::map<String, std::size_t> external_role_hashes; // user name -> LDAPClient::SearchResultsList hash (most recently retrieved and processed)
    mutable std::map<String, std::set<String>> users_per_roles; // role name -> user names (...it should be granted to; may but don't have to exist for common roles)
    mutable std::map<String, std::set<String>> roles_per_users; // user name -> role names (...that should be granted to it; may but don't have to include common roles)
    mutable std::map<UUID, String> granted_role_names;          // (currently granted) role id -> its name
    mutable std::map<String, UUID> granted_role_ids;            // (currently granted) role name -> its id
    scope_guard role_change_subscription;
    mutable MemoryAccessStorage memory_storage;
};
}
