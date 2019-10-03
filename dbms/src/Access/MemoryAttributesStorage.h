#pragma once

#include <Access/IAttributesStorage.h>
#include <list>
#include <map>
#include <unordered_map>
#include <mutex>


namespace DB
{
/// Implementation of IAttributesStorage which keeps all data in memory.
class MemoryAttributesStorage : public IAttributesStorage
{
public:
    MemoryAttributesStorage();
    ~MemoryAttributesStorage() override;
    const String & getStorageName() const override;

protected:
    std::vector<UUID> findPrefixedImpl(const String & prefix, const Type & type) const override;
    std::optional<UUID> findImpl(const String & name, const Type & type) const override;
    bool existsImpl(const UUID & id) const override;
    AttributesPtr readImpl(const UUID & id) const override;
    std::pair<String, const Type *> readNameAndTypeImpl(const UUID &id) const;
    UUID insertImpl(const IAttributes & attrs, bool replace_if_exists) override;
    void removeImpl(const UUID & id) override;
    void updateImpl(const UUID & id, const UpdateFunc & update_func) override;
    SubscriptionPtr subscribeForNewImpl(const String & prefix, const Type & type, const OnNewHandler & on_new) const override;
    SubscriptionPtr subscribeForChangesImpl(const UUID & id, const OnChangedHandler & on_changed) const override;

private:
    struct Entry
    {
        AttributesPtr attrs;
        mutable std::list<OnChangedHandler> on_changed_handlers;
    };

    class SubscriptionForNew;
    class SubscriptionForChanges;
    void removeSubscription(size_t namespace_idx, const std::multimap<String, OnNewHandler>::iterator & handler_it) const;
    void removeSubscription(const UUID & id, const std::list<OnChangedHandler>::iterator & handler_it) const;

    std::vector<std::map<String, UUID>> all_names; /// IDs by namespace_idx and name
    std::unordered_map<UUID, Entry> entries; /// entries by ID
    mutable std::vector<std::multimap<String, OnNewHandler>> on_new_handlers; /// "on_new" handlers by namespace_idx and prefix
    mutable std::mutex mutex;
};
}
