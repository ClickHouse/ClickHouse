#pragma once

#include <Access/MemoryAccessStorage.h>


namespace Poco
{
    namespace Util
    {
        class AbstractConfiguration;
    }
}


namespace DB
{
/// Implementation of IAccessStorage which loads all from users.xml periodically.
class UsersConfigAccessStorage : public IAccessStorage
{
public:
    UsersConfigAccessStorage();
    ~UsersConfigAccessStorage() override;

    void loadFromConfig(const Poco::Util::AbstractConfiguration & config);

private:
    std::optional<UUID> findImpl(std::type_index type, const String & name) const override;
    std::vector<UUID> findAllImpl(std::type_index type) const override;
    bool existsImpl(const UUID & id) const override;
    AccessEntityPtr readImpl(const UUID & id) const override;
    String readNameImpl(const UUID & id) const override;
    UUID insertImpl(const AccessEntityPtr & entity, bool replace_if_exists) override;
    void removeImpl(const UUID & id) override;
    void updateImpl(const UUID & id, const UpdateFunc & update_func) override;
    SubscriptionPtr subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const override;
    SubscriptionPtr subscribeForChangesImpl(std::type_index type, const OnChangedHandler & handler) const override;
    bool hasSubscriptionImpl(const UUID & id) const override;
    bool hasSubscriptionImpl(std::type_index type) const override;

    MemoryAccessStorage memory_storage;
};
}
