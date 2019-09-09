#pragma once

#include <ACL/IACLAttributesStorage.h>
#include <ACL/ACLAttributesType.h>
#include <list>
#include <unordered_map>
#include <mutex>


namespace DB
{
/// Implementation of IACLAttributesStorage which keeps all data in memory.
class MemoryACLAttributesStorage : public IACLAttributesStorage
{
public:
    MemoryACLAttributesStorage();
    ~MemoryACLAttributesStorage() override;
    const String & getStorageName() const { return storage_name; }

    std::vector<UUID> findAll(ACLAttributesType type) const override;
    std::optional<UUID> find(const String & name, ACLAttributesType type) const override;
    bool exists(const UUID & id) const override;

    std::pair<UUID, bool> insert(const IACLAttributes & attrs, bool if_not_exists) override;
    void write(const Changes & changes) override;

protected:
    ACLAttributesPtr readImpl(const UUID & id) const override;
    SubscriptionPtr subscribeForChangesImpl(const UUID & id, const OnChangedFunction & on_changed) const override;

private:
    const String storage_name{"Memory"};

    static size_t indexOfType(ACLAttributesType type);

    struct Entry
    {
        ACLAttributesPtr attrs;
        mutable std::list<OnChangedFunction> on_changed_functions;
    };

    class SubscriptionImpl;
    void removeSubscription(const UUID & id, const std::list<OnChangedFunction>::iterator & functions_it) const;

    std::unordered_map<String, UUID> names[static_cast<size_t>(ACLAttributesType::MAX)];
    std::unordered_map<UUID, Entry> entries;
    mutable std::mutex mutex;
};
}
