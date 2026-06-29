#pragma once

#include <Access/IAccessEntity.h>
#include <base/scope_guard.h>
#include <list>
#include <mutex>
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

    using OnBatchFinishedHandler = std::function<void()>;

    /// Subscribes for the end of a notification batch: the handler is called once per sendNotifications()
    /// after all the per-entity notifications queued so far have been dispatched. Lets subscribers coalesce
    /// expensive per-entity recomputations into a single one per batch (e.g. a full refresh delivers one
    /// notification per entity, but the derived state only needs to be rebuilt once). The handler is also
    /// called when the batch was empty, so a recomputation that threw (and left its work pending) is retried
    /// on the next call without waiting for a fresh access change; it must therefore be cheap when idle.
    scope_guard subscribeForBatchFinished(const OnBatchFinishedHandler & handler);

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
        std::list<OnBatchFinishedHandler> on_batch_finished;
        std::mutex mutex;
    };

    /// shared_ptr is here for safety because AccessChangesNotifier can be destroyed before all subscriptions are removed.
    std::shared_ptr<Handlers> handlers;

    struct Event
    {
        UUID id;
        AccessEntityPtr entity;
        AccessEntityType type{};
    };
    std::queue<Event> queue;
    std::mutex queue_mutex;
    std::mutex sending_notifications;
};

}
