#include <Access/AccessChangesNotifier.h>
#include <boost/range/algorithm/copy.hpp>


namespace DB
{

AccessChangesNotifier::AccessChangesNotifier() : handlers(std::make_shared<Handlers>())
{
}

AccessChangesNotifier::~AccessChangesNotifier() = default;

void AccessChangesNotifier::onEntityAdded(const UUID & id, const AccessEntityPtr & new_entity)
{
    std::lock_guard lock{queue_mutex};
    Event event;
    event.id = id;
    event.entity = new_entity;
    event.type = new_entity->getType();
    queue.push(std::move(event));
}

void AccessChangesNotifier::onEntityUpdated(const UUID & id, const AccessEntityPtr & changed_entity)
{
    std::lock_guard lock{queue_mutex};
    Event event;
    event.id = id;
    event.entity = changed_entity;
    event.type = changed_entity->getType();
    queue.push(std::move(event));
}

void AccessChangesNotifier::onEntityRemoved(const UUID & id, AccessEntityType type)
{
    std::lock_guard lock{queue_mutex};
    Event event;
    event.id = id;
    event.type = type;
    queue.push(std::move(event));
}

scope_guard AccessChangesNotifier::subscribeForChanges(AccessEntityType type, const OnChangedHandler & handler)
{
    std::lock_guard lock{handlers->mutex};
    auto & list = handlers->by_type[static_cast<size_t>(type)];
    list.push_back(handler);
    auto handler_it = std::prev(list.end());

    return [handlers=handlers, type, handler_it]
    {
        std::lock_guard lock2{handlers->mutex};
        auto & list2 = handlers->by_type[static_cast<size_t>(type)];
        list2.erase(handler_it);
    };
}

scope_guard AccessChangesNotifier::subscribeForChanges(const UUID & id, const OnChangedHandler & handler)
{
    std::lock_guard lock{handlers->mutex};
    auto it = handlers->by_id.emplace(id, std::list<OnChangedHandler>{}).first;
    auto & list = it->second;
    list.push_back(handler);
    auto handler_it = std::prev(list.end());

    return [handlers=handlers, it, handler_it]
    {
        std::lock_guard lock2{handlers->mutex};
        auto & list2 = it->second;
        list2.erase(handler_it);
        if (list2.empty())
            handlers->by_id.erase(it);
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
    /// Only one thread can send notification at any time.
    std::lock_guard sending_notifications_lock{sending_notifications};

    std::unique_lock queue_lock{queue_mutex};
    while (!queue.empty())
    {
        auto event = std::move(queue.front());
        queue.pop();
        queue_lock.unlock();

        std::vector<OnChangedHandler> current_handlers;
        {
            std::lock_guard handlers_lock{handlers->mutex};
            boost::range::copy(handlers->by_type[static_cast<size_t>(event.type)], std::back_inserter(current_handlers));
            auto it = handlers->by_id.find(event.id);
            if (it != handlers->by_id.end())
                boost::range::copy(it->second, std::back_inserter(current_handlers));
        }

        for (const auto & handler : current_handlers)
        {
            try
            {
                handler(event.id, event.entity);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }

        queue_lock.lock();
    }
}

}
