#pragma once

#include <Common/CurrentMetrics.h>
#include <Access/AccessChangesNotifier.h>
#include <Access/IAccessStorage.h>


namespace CurrentMetrics
{
    extern const Metric AttachedAccessEntity;
}

namespace DB
{

/// Base class for storages that actually store access entities.
/// The implementations of this class MUST be thread-safe.
class AccessStorageBase : public IAccessStorage
{
public:
    explicit AccessStorageBase(UInt64 access_entities_num_limit_, const String & storage_name_, AccessChangesNotifier & changes_notifier_)
        : IAccessStorage(access_entities_num_limit_, storage_name_), changes_notifier(changes_notifier_)
    {
    }

protected:
    struct Entry
    {
        UUID id;
        String name;
        AccessEntityType type;
        mutable AccessEntityPtr entity; /// may be nullptr, if the entity hasn't been loaded yet.
    };

    void insertEntry(
        UUID id,
        String name,
        AccessEntityType type,
        AccessEntityPtr entity)
    {
        auto attached_count = CurrentMetrics::get(CurrentMetrics::AttachedAccessEntity) + 1;
        if (entityLimitReached(attached_count))
            throwTooManyEntities(attached_count);

        auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];

        auto & entry = entries_by_id[id];
        entry.id = id;
        entry.type = type;
        entry.name = name;
        entry.entity = entity;
        entries_by_name[entry.name] = &entry;

        changes_notifier.onEntityAdded(id, entity);

        CurrentMetrics::add(CurrentMetrics::AttachedAccessEntity);
    }

    void removeEntry(
        UUID id,
        String name,
        AccessEntityType type)
    {
        auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
        entries_by_name.erase(name);
        entries_by_id.erase(id);

        changes_notifier.onEntityRemoved(id, type);

        CurrentMetrics::sub(CurrentMetrics::AttachedAccessEntity);
    }

    AccessChangesNotifier & changes_notifier;
    std::unordered_map<UUID, Entry> entries_by_id;
    std::unordered_map<std::string_view, Entry *> entries_by_name_and_type[static_cast<size_t>(AccessEntityType::MAX)];
};

}
