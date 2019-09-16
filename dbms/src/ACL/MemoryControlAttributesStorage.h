#pragma once

#include <ACL/IControlAttributesStorage.h>
#include <list>
#include <map>
#include <unordered_map>
#include <mutex>


namespace DB
{
/// Implementation of IControlAttributesStorage which keeps all data in memory.
class MemoryControlAttributesStorage : public IControlAttributesStorage
{
public:
    MemoryControlAttributesStorage();
    ~MemoryControlAttributesStorage() override;
    const String & getStorageName() const override { return storage_name; }

    std::vector<UUID> findPrefixed(const String & prefix, const Type & type) const override;
    std::optional<UUID> find(const String & name, const Type & type) const override;
    bool exists(const UUID & id) const override;
    void write(const Changes & changes) override;

protected:
    AttributesPtr tryReadImpl(const UUID & id) const override;
    SubscriptionPtr subscribeForChangesImpl(const UUID & id, const OnChangedHandler & on_changed) const override;
    SubscriptionPtr subscribeForNewImpl(const String & prefix, const Type & type, const OnNewHandler & on_new) const override;

private:
    const String storage_name{"Memory"};

    class PreparedChanges;

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
