#pragma once

#include <Access/IAccessEntity.h>
#include <base/scope_guard.h>
#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>


namespace DB
{

/// Helper class implementing subscriptions and notifications in access management.
class AccessChangesNotifier
{
public:
    AccessChangesNotifier();
    ~AccessChangesNotifier();

    /// A single access entity change. `entity` is the new or changed entity, or null if the entity was removed.
    struct Change
    {
        UUID id;
        AccessEntityPtr entity;
        AccessEntityType type{};
    };

    /// A handler is called once per delivered batch with the changes of that batch that match the subscription.
    /// sendNotifications drains the current queue into a batch, delivers it, and repeats until the queue is empty.
    /// A refresh that touches many entities therefore arrives as a single call, and handler-enqueued changes are
    /// also delivered before sendNotifications returns (as additional batches).
    using OnChangedHandler = std::function<void(const std::vector<Change> & changes)>;

    /// Subscribes for all changes of entities of a given type.
    /// A by-type handler is called on every sendNotifications(), even when no entity of that type changed
    /// (with an empty `changes`), so a recomputation that threw (and left its work pending) is retried on
    /// the next call without waiting for a fresh access change; it must therefore be cheap when idle.
    scope_guard subscribeForChanges(AccessEntityType type, const OnChangedHandler & handler);

    template <typename EntityClassT>
    scope_guard subscribeForChanges(OnChangedHandler handler)
    {
        return subscribeForChanges(EntityClassT::TYPE, handler);
    }

    /// Subscribes for changes of a specific entry.
    /// A by-id handler is called only when its entity actually changed in the batch.
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

    std::vector<Change> queue;
    std::mutex queue_mutex;
    std::mutex sending_notifications;
};

}
