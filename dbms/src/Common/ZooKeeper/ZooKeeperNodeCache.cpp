#include "ZooKeeperNodeCache.h"

namespace zkutil
{

ZooKeeperNodeCache::ZooKeeperNodeCache(GetZooKeeper get_zookeeper_)
    : get_zookeeper(std::move(get_zookeeper_))
    , context(std::make_shared<Context>())
{
}

std::optional<std::string> ZooKeeperNodeCache::get(const std::string & path)
{
    zkutil::ZooKeeperPtr zookeeper;
    std::unordered_set<std::string> invalidated_paths;
    {
        std::lock_guard<std::mutex> lock(context->mutex);

        if (!context->zookeeper)
        {
            /// Possibly, there was a previous session and it has expired. Clear the cache.
            nonexistent_nodes.clear();
            node_cache.clear();

            context->zookeeper = get_zookeeper();
        }
        zookeeper = context->zookeeper;

        invalidated_paths.swap(context->invalidated_paths);
    }

    if (!zookeeper)
        throw DB::Exception("Could not get znode: `" + path + "'. ZooKeeper not configured.", DB::ErrorCodes::NO_ZOOKEEPER);

    for (const auto & path : invalidated_paths)
    {
        nonexistent_nodes.erase(path);
        node_cache.erase(path);
    }

    if (nonexistent_nodes.count(path))
        return std::nullopt;

    auto watch_callback = [context=context](const ZooKeeperImpl::ZooKeeper::WatchResponse & response)
    {
        if (!(response.type != ZooKeeperImpl::ZooKeeper::SESSION || response.state == ZooKeeperImpl::ZooKeeper::EXPIRED_SESSION))
            return;

        bool changed = false;
        {
            std::lock_guard<std::mutex> lock(context->mutex);

            if (response.type != ZooKeeperImpl::ZooKeeper::SESSION)
                changed = context->invalidated_paths.emplace(response.path).second;
            else if (response.state == ZooKeeperImpl::ZooKeeper::EXPIRED_SESSION)
            {
                context->zookeeper = nullptr;
                context->invalidated_paths.clear();
                changed = true;
            }
        }
        if (changed)
            context->changed_event.set();
    };

    std::string contents;

    auto cache_it = node_cache.find(path);
    if (cache_it != node_cache.end())
    {
        return cache_it->second;
    }

    if (zookeeper->tryGetWatch(path, contents, /* stat = */nullptr, watch_callback))
    {
        node_cache.emplace(path, contents);
        return contents;
    }

    /// Node doesn't exist. Create a watch on node creation.
    nonexistent_nodes.insert(path);

    if (!zookeeper->existsWatch(path, /* stat = */nullptr, watch_callback))
        return std::nullopt;

    /// Node was created between the two previous calls, try again. Watch is already set.
    if (zookeeper->tryGet(path, contents))
    {
        nonexistent_nodes.erase(path);
        node_cache.emplace(path, contents);
        return contents;
    }

    return std::nullopt;
}

}
