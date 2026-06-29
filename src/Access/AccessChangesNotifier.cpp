#include <Access/AccessChangesNotifier.h>
#include <Common/CurrentThread.h>
#include <Common/CurrentThreadHelpers.h>
#include <Common/Exception.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <boost/range/algorithm/copy.hpp>


namespace DB
{

namespace
{
    /// While in scope, detach the calling thread's client log queue so that logs emitted here go to
    /// the server log only and are not forwarded to a connected client. The batch-finished handlers
    /// recompute server-wide access caches; they merely run on whichever client thread triggered the
    /// access change, so their diagnostics must not leak into that client's log stream.
    class ScopedSuppressClientLogs
    {
    public:
        ScopedSuppressClientLogs()
        {
            saved_queue = CurrentThread::getInternalTextLogsQueue();
            if (saved_queue)
            {
                /// A non-null queue implies a current thread, so reading the level is safe.
                saved_level = currentThreadLogsLevel();
                CurrentThread::attachInternalTextLogsQueue(nullptr, LogsLevel::none);
            }
        }

        ~ScopedSuppressClientLogs()
        {
            if (saved_queue)
                CurrentThread::attachInternalTextLogsQueue(saved_queue, saved_level);
        }

        ScopedSuppressClientLogs(const ScopedSuppressClientLogs &) = delete;
        ScopedSuppressClientLogs & operator=(const ScopedSuppressClientLogs &) = delete;

    private:
        std::shared_ptr<InternalTextLogsQueue> saved_queue;
        LogsLevel saved_level = LogsLevel::none;
    };
}

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
    auto it = handlers->by_id.emplace(id, std::list<OnChangedHandler>{}).first;
    auto & list = it->second;
    list.push_back(handler);
    auto handler_it = std::prev(list.end());

    return [my_handlers = handlers, it, handler_it]
    {
        std::lock_guard lock2{my_handlers->mutex};
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

scope_guard AccessChangesNotifier::subscribeForBatchFinished(const OnBatchFinishedHandler & handler)
{
    std::lock_guard lock{handlers->mutex};
    auto & list = handlers->on_batch_finished;
    list.push_back(handler);
    auto handler_it = std::prev(list.end());

    return [my_handlers = handlers, handler_it]
    {
        std::lock_guard lock2{my_handlers->mutex};
        my_handlers->on_batch_finished.erase(handler_it);
    };
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
    queue_lock.unlock();

    /// Run the coalesced per-batch work unconditionally, even when this call drained nothing: a
    /// per-batch handler whose previous rebuild threw left its work pending, and the per-entity flag
    /// guarding that rebuild is only cleared on success, so the retry must not depend on a fresh event
    /// being queued (an up-to-date `SYSTEM RELOAD USERS` diffs to zero and would otherwise never retry).
    /// Each handler is cheap when nothing is pending (a mutex + a flag check). Copied out so handlers
    /// run without `handlers->mutex` held.
    std::vector<OnBatchFinishedHandler> batch_finished_handlers;
    {
        std::lock_guard handlers_lock{handlers->mutex};
        boost::range::copy(handlers->on_batch_finished, std::back_inserter(batch_finished_handlers));
    }

    /// The recompute logs belong in the server log, not in the triggering client's stream.
    ScopedSuppressClientLogs suppress_client_logs;
    for (const auto & handler : batch_finished_handlers)
    {
        try
        {
            handler();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

}
