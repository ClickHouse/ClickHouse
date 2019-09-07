#pragma once

#include <ACL/IACLAttributesStorage.h>
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

    std::vector<UUID> findAll(IACLAttributes::Type type) const override;
    UUID find(const String & name, IACLAttributes::Type type) const override;
    bool exists(const UUID & id) const override;

    Status insert(const IACLAttributes & attrs, UUID & id) override;
    Status remove(const UUID & id) override;

    Status readImpl(const UUID & id, ACLAttributesPtr & attrs) const override;
    Status writeImpl(const UUID & id, const MakeChangeFunction & make_change) override;
    SubscriptionPtr subscribeForChangesImpl(const UUID & id, const OnChangedFunction & on_changed) const override;

private:
    const String storage_name{"Memory"};

    struct Entry
    {
        ACLAttributesPtr attrs;
        mutable std::list<OnChangedFunction> on_changed_functions;
    };

    class SubscriptionImpl;
    void removeSubscription(const UUID & id, const std::list<OnChangedFunction>::iterator & functions_it) const;

    static constexpr size_t MAX_TYPE = 4;
    std::unordered_map<String, UUID> names[MAX_TYPE];
    std::unordered_map<UUID, Entry> entries;
    mutable std::mutex mutex;
};
}
