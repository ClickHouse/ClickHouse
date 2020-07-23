#pragma once

#include <Access/MemoryAccessStorage.h>
#include <Core/Types.h>
#include <mutex>


namespace Poco
{
    namespace Util
    {
        class AbstractConfiguration;
    }
}


namespace DB
{
/// Implementation of IAccessStorage which allows attaching users from a remote LDAP server.
/// Currently, any user name will be treated as a name of an existing remote user,
/// a user info entity will be created, with LDAP_SERVER authentication type.
class LDAPAccessStorage : public IAccessStorage
{
public:
    LDAPAccessStorage();

    void setConfiguration(const Poco::Util::AbstractConfiguration & config, IAccessStorage * top_enclosing_storage_);

private: // IAccessStorage implementations.
    std::optional<UUID> findImpl(EntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(EntityType type) const override;
    bool existsImpl(const UUID & id) const override;
    AccessEntityPtr readImpl(const UUID & id) const override;
    String readNameImpl(const UUID & id) const override;
    bool canInsertImpl(const AccessEntityPtr &) const override;
    UUID insertImpl(const AccessEntityPtr & entity, bool replace_if_exists) override;
    void removeImpl(const UUID & id) override;
    void updateImpl(const UUID & id, const UpdateFunc & update_func) override;
    ext::scope_guard subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const override;
    ext::scope_guard subscribeForChangesImpl(EntityType type, const OnChangedHandler & handler) const override;
    bool hasSubscriptionImpl(const UUID & id) const override;
    bool hasSubscriptionImpl(EntityType type) const override;

private:
    bool isConfiguredNoLock() const;

    mutable std::recursive_mutex mutex;
    String ldap_server;
    String user_template;
    IAccessStorage * top_enclosing_storage = nullptr;
    mutable bool helper_lookup_in_progress = false;
    mutable MemoryAccessStorage memory_storage;
};
}
