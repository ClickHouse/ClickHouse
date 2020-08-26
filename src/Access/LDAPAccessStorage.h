#pragma once

#include <Access/MemoryAccessStorage.h>
#include <Core/Types.h>
#include <ext/scope_guard.h>
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

    explicit LDAPAccessStorage(const String & storage_name_ = STORAGE_TYPE);
    virtual ~LDAPAccessStorage() override = default;

    void setConfiguration(AccessControlManager * access_control_manager_, const Poco::Util::AbstractConfiguration & config, const String & prefix = "");

public: // IAccessStorage implementations.
    virtual const char * getStorageType() const override;
    virtual bool isStorageReadOnly() const override;

private: // IAccessStorage implementations.
    virtual std::optional<UUID> findImpl(EntityType type, const String & name) const override;
    virtual std::optional<UUID> findOrGenerateImpl(EntityType type, const String & name) const override;
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

private:
    bool isConfiguredNoLock() const;
    void processRoleChange(const UUID & id, const AccessEntityPtr & entity);

    mutable std::recursive_mutex mutex;
    String ldap_server;
    std::set<String> roles;
    AccessControlManager * access_control_manager = nullptr;
    ext::scope_guard role_change_subscription;
    mutable std::set<UUID> roles_of_interest;
    mutable MemoryAccessStorage memory_storage;
};
}
