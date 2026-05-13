#include <Interpreters/NamedScalars/SharedNamedScalarsWatcher.h>

#include <Common/ConcurrentBoundedQueue.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

#include <Interpreters/NamedScalars/NamedScalar.h>
#include <Interpreters/NamedScalars/NamedScalarsManager.h>

#include <base/sleep.h>

#include <limits>
#include <unordered_set>

namespace DB
{

namespace
{
constexpr size_t WATCHER_QUEUE_POLL_TIMEOUT_MS = 10000;
}

SharedNamedScalarsWatcher::SharedNamedScalarsWatcher(
    ContextPtr global_context_,
    WatchableNamedScalarDefinitionStorePtr definition_store_,
    NamedScalarsManager & manager_)
    : global_context(std::move(global_context_))
    , definition_store(std::move(definition_store_))
    , manager(manager_)
    , log(getLogger("SharedNamedScalarsWatcher"))
    , queue(std::make_shared<ConcurrentBoundedQueue<Wakeup>>(std::numeric_limits<size_t>::max()))
    , resync_requested(std::make_shared<std::atomic<bool>>(false))
{
}

SharedNamedScalarsWatcher::~SharedNamedScalarsWatcher()
{
    stop();
}

void SharedNamedScalarsWatcher::start()
{
    if (running.exchange(true))
        return;
    /// First reconcile runs synchronously: avoids a "not found" window
    /// for entries that already exist in Keeper. A wedged Keeper just
    /// throws; the watch thread's `!loaded` loop retries.
    try
    {
        initialLoad();
        loaded = true;
    }
    catch (...)
    {
        tryLogCurrentException(log, "initial load failed; watcher will retry in the background");
    }
    thread = ThreadFromGlobalPool(&SharedNamedScalarsWatcher::watchLoop, this);
}

void SharedNamedScalarsWatcher::stop()
{
    if (!running.exchange(false))
        return;
    queue->finish();
    if (thread.joinable())
        thread.join();
}

void SharedNamedScalarsWatcher::pokeResyncAll()
{
    if (!running)
        return;
    requestResync(queue, resync_requested);
}

void SharedNamedScalarsWatcher::requestResync(
    const std::shared_ptr<ConcurrentBoundedQueue<Wakeup>> & queue,
    const std::shared_ptr<std::atomic<bool>> & resync_requested)
{
    if (!resync_requested->exchange(true))
    {
        if (!queue->emplace(Wakeup{}))
            resync_requested->store(false);
    }
}

void SharedNamedScalarsWatcher::watchLoop()
{
    setThreadName(ThreadName::SHARED_NAMED_SCALARS);
    LOG_DEBUG(log, "Shared named_scalars watcher started");

    while (running)
    {
        try
        {
            if (!loaded)
            {
                initialLoad();
                loaded = true;
            }

            Wakeup wakeup;
            if (!queue->tryPop(wakeup, WATCHER_QUEUE_POLL_TIMEOUT_MS))
                continue;

            /// Drain the flag in a loop: a request that arrived between
            /// `exchange(false)` returning true and `resyncAll()` finishing
            /// must still be served before we go back to sleep.
            while (resync_requested->exchange(false))
                resyncAll();
        }
        catch (...)
        {
            /// Don't reset resync_requested here: a watch callback that fired
            /// during the failing reconcile would have its wake-up dropped.
            /// The next iteration will exchange(false) and serve it.
            tryLogCurrentException(log, "Shared named_scalars watcher loop");
            loaded = false;
            sleepForSeconds(5);
        }
    }

    LOG_DEBUG(log, "Shared named_scalars watcher stopped");
}

void SharedNamedScalarsWatcher::initialLoad()
{
    auto component_guard = Coordination::setCurrentComponent("SharedNamedScalarsWatcher::initialLoad");
    Strings names = readDefinitionsAndInstallChildrenWatch();
    for (const auto & name : names)
        reconcileScalar(name);
}

void SharedNamedScalarsWatcher::resyncAll()
{
    auto component_guard = Coordination::setCurrentComponent("SharedNamedScalarsWatcher::resyncAll");
    Strings names = readDefinitionsAndInstallChildrenWatch();
    std::unordered_set<String> present(names.begin(), names.end());

    for (const auto & scalar : manager.listAllScalars())
    {
        const auto & scalar_name = scalar->getName();
        if (!present.contains(scalar_name))
            manager.dropScalar(scalar_name);
    }

    for (const auto & name : names)
        reconcileScalar(name);
}

void SharedNamedScalarsWatcher::reconcileScalar(const String & name)
{
    auto component_guard = Coordination::setCurrentComponent("SharedNamedScalarsWatcher::reconcileScalar");
    String definition_blob;
    if (!readDefinitionData(name, definition_blob))
    {
        manager.dropScalar(name);
        return;
    }

    manager.installStoredDefinition(
        global_context,
        name,
        definition_blob,
        log);
}

Strings SharedNamedScalarsWatcher::readDefinitionsAndInstallChildrenWatch()
{
    return definition_store->listDefinitionsWithChildrenWatch(
        [q = queue, requested = resync_requested]
        {
            requestResync(q, requested);
        });
}

bool SharedNamedScalarsWatcher::readDefinitionData(const String & name, String & out)
{
    return definition_store->readDefinitionWithDataWatch(
        name,
        out,
        [q = queue, requested = resync_requested]
        {
            requestResync(q, requested);
        });
}

}
