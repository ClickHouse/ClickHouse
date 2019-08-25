#pragma once

#include <Account/IAccessStorage.h>
#include <list>
#include <unordered_map>
#include <mutex>


namespace DB
{
/// Implementation of IAccessControlStorage which keeps all data in memory.
class MemoryAccessStorage : public IAccessStorage
{
public:
    MemoryAccessStorage();
    ~MemoryAccessStorage() override;
    const String & getStorageName() const { return storage_name; }

    std::vector<UUID> findAll(Type type) const override;
    std::optional<UUID> find(const String & name, Type type) const override;
    bool exists(const UUID & id) const override;

    UUID create(const Attributes & initial_attrs) override;
    void drop(const UUID & id) override;

    AttributesPtr read(const UUID & id) const override;

    std::pair<AttributesPtr, std::unique_ptr<Subscription>>
    readAndSubscribe(const UUID & id, const OnChangedFunction & on_changed) const override;

    void write(const UUID & id, const MakeChangeFunction & make_change) override;

private:
    const String storage_name{"Memory"};

    struct Entry
    {
        AttributesPtr attrs;
        mutable std::list<OnChangedFunction> on_changed_functions;
    };

    Entry & getEntry(const UUID & id);
    const Entry & getEntry(const UUID & id) const;

    class SubscriptionImpl;
    void removeSubscription(const UUID & id, const std::list<OnChangedFunction>::iterator & functions_it) const;

    static constexpr size_t MAX_TYPE = 3;
    std::unordered_map<String, UUID> names[MAX_TYPE];
    std::unordered_map<UUID, Entry> entries;
    mutable std::mutex mutex;
};
}
