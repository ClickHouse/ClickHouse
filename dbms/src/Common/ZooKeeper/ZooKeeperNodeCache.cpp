#include "ZooKeeperNodeCache.h"

namespace zkutil
{

ZooKeeperNodeCache::ZooKeeperNodeCache(GetZooKeeper get_zookeeper_)
    : get_zookeeper(std::move(get_zookeeper_))
    , context(std::make_shared<Context>())
{
}

ZooKeeperNodeCache::GetResult ZooKeeperNodeCache::get(const std::string & path, EventPtr watch_event)
{
    Coordination::WatchCallback watch_callback;
    if (watch_event)
        watch_callback = [watch_event](const Coordination::WatchResponse &) { watch_event->set(); };

    return get(path, watch_callback);
}

ZooKeeperNodeCache::GetResult ZooKeeperNodeCache::get(const std::string & path, Coordination::WatchCallback caller_watch_callback)
{
    zkutil::ZooKeeperPtr zookeeper;
    std::unordered_set<std::string> invalidated_paths;
    {
        std::lock_guard<std::mutex> lock(context->mutex);

        if (!context->zookeeper)
        {
            /// Possibly, there was a previous session and it has expired. Clear the cache.
            node_cache.clear();

            context->zookeeper = get_zookeeper();
        }
        zookeeper = context->zookeeper;

        invalidated_paths.swap(context->invalidated_paths);
    }

    if (!zookeeper)
        throw DB::Exception("Could not get znode: `" + path + "'. ZooKeeper not configured.", DB::ErrorCodes::NO_ZOOKEEPER);

    for (const auto & invalidated_path : invalidated_paths)
        node_cache.erase(invalidated_path);

    auto cache_it = node_cache.find(path);
    if (cache_it != node_cache.end())
        return cache_it->second;

    std::weak_ptr<Context> weak_context(context);
    auto watch_callback = [weak_context, caller_watch_callback](const Coordination::WatchResponse & response)
    {
        if (!(response.type != Coordination::SESSION || response.state == Coordination::EXPIRED_SESSION))
            return;

        auto owned_context = weak_context.lock();
        if (!owned_context)
            return;

        bool changed = false;
        {
            std::lock_guard<std::mutex> lock(owned_context->mutex);

            if (response.type != Coordination::SESSION)
                changed = owned_context->invalidated_paths.emplace(response.path).second;
            else if (response.state == Coordination::EXPIRED_SESSION)
            {
                owned_context->zookeeper = nullptr;
                owned_context->invalidated_paths.clear();
                changed = true;
            }
        }
        if (changed && caller_watch_callback)
            caller_watch_callback(response);
    };

    GetResult result;

    result.exists = zookeeper->tryGetWatch(path, result.contents, &result.stat, watch_callback);
    if (result.exists)
    {
        node_cache.emplace(path, result);
        return result;
    }

    /// Node doesn't exist. We must set a watch on node creation (because it wasn't set by tryGetWatch).

    result.exists = zookeeper->existsWatch(path, &result.stat, watch_callback);
    if (!result.exists)
    {
        node_cache.emplace(path, result);
        return result;
    }

    /// Node was created between the two previous calls, try again. Watch is already set.

    result.exists = zookeeper->tryGet(path, result.contents, &result.stat);
    node_cache.emplace(path, result);
    return result;
}

}
