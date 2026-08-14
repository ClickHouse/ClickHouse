#include <Access/AccessChangesNotifier.h>
#include <Common/Exception.h>

#include <utility>


namespace DB
{

AccessChangesNotifier::AccessChangesNotifier() : handlers(std::make_shared<Handlers>())
{
}

AccessChangesNotifier::~AccessChangesNotifier() = default;

void AccessChangesNotifier::onEntityAdded(const UUID & id, const AccessEntityPtr & new_entity)
{
    std::lock_guard lock{queue_mutex};
    Change change;
    change.id = id;
    change.entity = new_entity;
    change.type = new_entity->getType();
    queue.push_back(std::move(change));
}

void AccessChangesNotifier::onEntityUpdated(const UUID & id, const AccessEntityPtr & changed_entity)
{
    std::lock_guard lock{queue_mutex};
    Change change;
    change.id = id;
    change.entity = changed_entity;
    change.type = changed_entity->getType();
    queue.push_back(std::move(change));
}

void AccessChangesNotifier::onEntityRemoved(const UUID & id, AccessEntityType type)
{
    std::lock_guard lock{queue_mutex};
    Change change;
    change.id = id;
    change.type = type;
    queue.push_back(std::move(change));
}

scope_guard AccessChangesNotifier::subscribeForChanges(AccessEntityType type, const OnChangedHandler & handler)
{
    std::lock_guard lock{handlers->mutex};
    auto & list = handlers->by_type[static_cast<size_t>(type)];
    list.push_back(handler);
    auto handler_it = std::prev(list.end());

    return [my_handlers = handlers, type, handler_it]
    {
        std::lock_guard lock2{my_handlers->mutex};
        auto & list2 = my_handlers->by_type[static_cast<size_t>(type)];
        list2.erase(handler_it);
    };
}

scope_guard AccessChangesNotifier::subscribeForChanges(const UUID & id, const OnChangedHandler & handler)
{
    std::lock_guard lock{handlers->mutex};
    auto & list = handlers->by_id[id];
    list.push_back(handler);
    auto handler_it = std::prev(list.end());

    /// Capture `id`, not the map iterator: inserting another by-id subscription can rehash `by_id` and
    /// invalidate the iterator (the `std::list` iterator `handler_it` stays valid across rehashes).
    return [my_handlers = handlers, id, handler_it]
    {
        std::lock_guard lock2{my_handlers->mutex};
        auto it = my_handlers->by_id.find(id);
        if (it == my_handlers->by_id.end())
            return;
        auto & list2 = it->second;
        list2.erase(handler_it);
        if (list2.empty())
            my_handlers->by_id.erase(it);
    };
}


scope_guard AccessChangesNotifier::subscribeForChanges(const std::vector<UUID> & ids, const OnChangedHandler & handler)
{
    scope_guard subscriptions;
    for (const auto & id : ids)
        subscriptions.join(subscribeForChanges(id, handler));
    return subscriptions;
}

void AccessChangesNotifier::sendNotifications()
{
    /// Only one thread can send notifications at any time.
    std::lock_guard sending_notifications_lock{sending_notifications};

    /// Deliver in batches until the queue is empty.
    ///
    /// A handler may enqueue more changes while it runs (for example LDAP role mapping reacts to a Role change
    /// by updating the mapped users), and those must be delivered before this call returns -
    /// otherwise sessions keep stale access until some unrelated change flushes the queue.
    ///
    /// We always run at least one pass, even when the queue is empty,
    /// so by-type handlers fire and a recomputation that previously threw is retried (see subscribeForChanges).
    for (;;)
    {
        /// Drain the whole queue into a single batch.
        std::vector<Change> batch;
        {
            std::lock_guard queue_lock{queue_mutex};
            batch = std::exchange(queue, {});
        }

        /// Group the batch by entity type (one copy of each change).
        std::vector<Change> changes_by_type[static_cast<size_t>(AccessEntityType::MAX)];
        for (const auto & change : batch)
            changes_by_type[static_cast<size_t>(change.type)].push_back(change);

        /// Snapshot the handlers and the changes to deliver to each of them, then call them without
        /// `handlers->mutex` held (a handler may add or remove subscriptions, which takes that mutex).
        const std::vector<Change> no_changes;
        std::unordered_map<UUID, std::vector<Change>> changes_by_id; /// built only for subscribed ids
        std::vector<std::pair<OnChangedHandler, const std::vector<Change> *>> calls;
        {
            std::lock_guard handlers_lock{handlers->mutex};

            /// Every by-type handler is called even when nothing of its type changed (with an empty batch)
            /// so a recomputation that threw is retried on the next call.
            for (size_t type = 0; type != static_cast<size_t>(AccessEntityType::MAX); ++type)
            {
                const auto & changes = changes_by_type[type];
                for (const auto & handler : handlers->by_type[type])
                    calls.emplace_back(handler, changes.empty() ? &no_changes : &changes);
            }

            /// By-id handlers are called only for ids that actually changed.
            /// Collect per-id changes only for ids someone subscribed to.
            if (!handlers->by_id.empty())
            {
                for (const auto & change : batch)
                {
                    if (handlers->by_id.contains(change.id))
                        changes_by_id[change.id].push_back(change);
                }
                for (const auto & [id, changes] : changes_by_id)
                {
                    for (const auto & handler : handlers->by_id.at(id))
                        calls.emplace_back(handler, &changes);
                }
            }
        }

        for (const auto & [handler, changes] : calls)
        {
            try
            {
                handler(*changes);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }

        /// Stop once a pass produced no new changes.
        std::lock_guard queue_lock{queue_mutex};
        if (queue.empty())
            break;
    }
}

}
