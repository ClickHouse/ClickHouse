#include "ZooKeeperNodeCache.h"

namespace zkutil
{

ZooKeeperNodeCache::ZooKeeperNodeCache(GetZooKeeper get_zookeeper_)
    : get_zookeeper(std::move(get_zookeeper_))
    , context(std::make_shared<Context>())
{
}

ZooKeeperNodeCache::GetResult ZooKeeperNodeCache::get(const std::string & path)
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

    auto watch_callback = [context=context](const Coordination::WatchResponse & response)
    {
        if (!(response.type != Coordination::SESSION || response.state == Coordination::EXPIRED_SESSION))
            return;

        bool changed = false;
        {
            std::lock_guard<std::mutex> lock(context->mutex);

            if (response.type != Coordination::SESSION)
                changed = context->invalidated_paths.emplace(response.path).second;
            else if (response.state == Coordination::EXPIRED_SESSION)
            {
                context->zookeeper = nullptr;
                context->invalidated_paths.clear();
                changed = true;
            }
        }
        if (changed)
            context->changed_event.set();
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
