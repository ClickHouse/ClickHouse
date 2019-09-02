#pragma once

#include <Account/IAccessControlStorage.h>
#include <list>
#include <unordered_map>
#include <mutex>


namespace DB
{
/// Implementation of IAccessControlStorage which keeps all data in memory.
class MemoryAccessControlStorage : public IAccessControlStorage
{
public:
    MemoryAccessControlStorage();
    ~MemoryAccessControlStorage() override;
    const String & getStorageName() const { return storage_name; }

    std::vector<UUID> findAll(ElementType type) const override;
    UUID find(const String & name, ElementType type) const override;
    bool exists(const UUID & id) const override;

    Status insert(const Attributes & attrs, UUID & id) override;
    Status remove(const UUID & id) override;

    Status read(const UUID & id, AttributesPtr & attrs) const override;
    Status write(const UUID & id, const MakeChangeFunction & make_change) override;

    std::unique_ptr<Subscription> subscribeForNew(ElementType type, const OnNewAttributesFunction & on_new_attrs) const override;
    std::unique_ptr<Subscription> subscribeForChanges(const UUID & id, const OnChangedFunction & on_changed) const override;

private:
    const String storage_name{"Memory"};

    struct Entry
    {
        AttributesPtr attrs;
        mutable std::list<OnChangedFunction> on_changed_functions;
    };

    class SubscriptionForNew;
    class SubscriptionForChanges;
    void removeSubscriptionForNew(ElementType type, const std::list<OnNewAttributesFunction>::iterator & functions_it) const;
    void removeSubscriptionForChanges(const UUID & id, const std::list<OnChangedFunction>::iterator & functions_it) const;

    static constexpr size_t MAX_TYPE = 3;
    std::unordered_map<String, UUID> names[MAX_TYPE];
    std::unordered_map<UUID, Entry> entries;
    mutable std::list<OnNewAttributesFunction> on_new_attrs_functions[MAX_TYPE];
    mutable std::mutex mutex;
};
}
