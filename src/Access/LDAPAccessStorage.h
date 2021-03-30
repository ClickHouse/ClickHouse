#pragma once

#include <Access/MemoryAccessStorage.h>
#include <Core/Types.h>
#include <ext/scope_guard.h>
#include <map>
#include <mutex>
#include <set>


namespace Poco
{
    namespace Util
    {
        class AbstractConfiguration;
    }
}


namespace DB
{
class AccessControlManager;

/// Implementation of IAccessStorage which allows attaching users from a remote LDAP server.
/// Currently, any user name will be treated as a name of an existing remote user,
/// a user info entity will be created, with LDAP_SERVER authentication type.
class LDAPAccessStorage : public IAccessStorage
{
public:
    static constexpr char STORAGE_TYPE[] = "ldap";

    explicit LDAPAccessStorage(const String & storage_name_, AccessControlManager * access_control_manager_, const Poco::Util::AbstractConfiguration & config, const String & prefix);
    virtual ~LDAPAccessStorage() override = default;

    String getLDAPServerName() const;

public: // IAccessStorage implementations.
    virtual const char * getStorageType() const override;
    virtual String getStorageParamsJSON() const override;

private: // IAccessStorage implementations.
    virtual std::optional<UUID> findImpl(EntityType type, const String & name) const override;
    virtual std::vector<UUID> findAllImpl(EntityType type) const override;
    virtual bool existsImpl(const UUID & id) const override;
    virtual AccessEntityPtr readImpl(const UUID & id) const override;
    virtual String readNameImpl(const UUID & id) const override;
    virtual bool canInsertImpl(const AccessEntityPtr &) const override;
    virtual UUID insertImpl(const AccessEntityPtr & entity, bool replace_if_exists) override;
    virtual void removeImpl(const UUID & id) override;
    virtual void updateImpl(const UUID & id, const UpdateFunc & update_func) override;
    virtual ext::scope_guard subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const override;
    virtual ext::scope_guard subscribeForChangesImpl(EntityType type, const OnChangedHandler & handler) const override;
    virtual bool hasSubscriptionImpl(const UUID & id) const override;
    virtual bool hasSubscriptionImpl(EntityType type) const override;
    virtual UUID loginImpl(const String & user_name, const String & password, const Poco::Net::IPAddress & address, const ExternalAuthenticators & external_authenticators) const override;
    virtual UUID getIDOfLoggedUserImpl(const String & user_name) const override;

private:
    void setConfiguration(AccessControlManager * access_control_manager_, const Poco::Util::AbstractConfiguration & config, const String & prefix);
    void processRoleChange(const UUID & id, const AccessEntityPtr & entity);
    void checkAllDefaultRoleNamesFoundNoLock() const;

    [[noreturn]] static void throwDefaultRoleNotFound(const String & role_name);

    mutable std::recursive_mutex mutex;
    AccessControlManager * access_control_manager = nullptr;
    String ldap_server;
    std::set<String> default_role_names;
    std::map<UUID, String> roles_of_interest;
    ext::scope_guard role_change_subscription;
    mutable MemoryAccessStorage memory_storage;
};
}
