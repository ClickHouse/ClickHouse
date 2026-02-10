#pragma once

#include <Access/IAccessEntity.h>
#include <base/scope_guard.h>
#include <list>
#include <queue>
#include <unordered_map>


namespace DB
{

/// Helper class implementing subscriptions and notifications in access management.
class AccessChangesNotifier
{
public:
    AccessChangesNotifier();
    ~AccessChangesNotifier();

    using OnChangedHandler
        = std::function<void(const UUID & /* id */, const AccessEntityPtr & /* new or changed entity, null if removed */)>;

    /// Subscribes for all changes.
    /// Can return nullptr if cannot subscribe (identifier not found) or if it doesn't make sense (the storage is read-only).
    scope_guard subscribeForChanges(AccessEntityType type, const OnChangedHandler & handler);

    template <typename EntityClassT>
    scope_guard subscribeForChanges(OnChangedHandler handler)
    {
        return subscribeForChanges(EntityClassT::TYPE, handler);
    }

    /// Subscribes for changes of a specific entry.
    /// Can return nullptr if cannot subscribe (identifier not found) or if it doesn't make sense (the storage is read-only).
    scope_guard subscribeForChanges(const UUID & id, const OnChangedHandler & handler);
    scope_guard subscribeForChanges(const std::vector<UUID> & ids, const OnChangedHandler & handler);

    /// Called by access storages after a new access entity has been added.
    void onEntityAdded(const UUID & id, const AccessEntityPtr & new_entity);

    /// Called by access storages after an access entity has been changed.
    void onEntityUpdated(const UUID & id, const AccessEntityPtr & changed_entity);

    /// Called by access storages after an access entity has been removed.
    void onEntityRemoved(const UUID & id, AccessEntityType type);

    /// Sends notifications to subscribers about changes in access entities
    /// (added with previous calls onEntityAdded(), onEntityUpdated(), onEntityRemoved()).
    void sendNotifications();

private:
    struct Handlers
    {
        std::unordered_map<UUID, std::list<OnChangedHandler>> by_id;
        std::list<OnChangedHandler> by_type[static_cast<size_t>(AccessEntityType::MAX)];
        std::mutex mutex;
    };

    /// shared_ptr is here for safety because AccessChangesNotifier can be destroyed before all subscriptions are removed.
    std::shared_ptr<Handlers> handlers;

    struct Event
    {
        UUID id;
        AccessEntityPtr entity;
        AccessEntityType type;
    };
    std::queue<Event> queue;
    std::mutex queue_mutex;
    std::mutex sending_notifications;
};

}
