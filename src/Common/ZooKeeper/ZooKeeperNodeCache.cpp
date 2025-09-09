#include <Common/ZooKeeper/ZooKeeperNodeCache.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int NO_ZOOKEEPER;
    }
}

namespace zkutil
{

ZooKeeperNodeCache::ZooKeeperNodeCache(GetZooKeeper get_zookeeper_)
    : get_zookeeper(std::move(get_zookeeper_))
    , context(std::make_shared<Context>())
{
}

ZooKeeperNodeCache::ZNode ZooKeeperNodeCache::get(const std::string & path, Coordination::WatchCallbackPtrOrEventPtr caller_watch_callback)
{
    std::unordered_set<std::string> invalidated_paths;
    {
        std::lock_guard lock(context->mutex);

        if (context->all_paths_invalidated)
        {
            /// Possibly, there was a previous session and it has expired. Clear the cache.
            path_to_cached_znode.clear();
            context->all_paths_invalidated = false;
        }

        invalidated_paths.swap(context->invalidated_paths);
    }

    zkutil::ZooKeeperPtr zookeeper = get_zookeeper();
    if (!zookeeper)
        throw DB::Exception(DB::ErrorCodes::NO_ZOOKEEPER, "Could not get znode: '{}'. ZooKeeper not configured.", path);

    for (const auto & invalidated_path : invalidated_paths)
        path_to_cached_znode.erase(invalidated_path);

    auto cache_it = path_to_cached_znode.find(path);
    if (cache_it != path_to_cached_znode.end())
        return cache_it->second;

    Coordination::WatchCallbackPtrOrEventPtr wrapped_watch_callback;
    {
        std::lock_guard watches_lock(watches_mutex);
        auto & caller_to_internal_callbacks = names_watch_map[path];
        auto it = caller_to_internal_callbacks.find(caller_watch_callback);
        if (it != caller_to_internal_callbacks.end())
            wrapped_watch_callback = it->second;
        else
        {
            std::weak_ptr<Context> weak_context(context);

            wrapped_watch_callback = std::make_shared<Coordination::WatchCallback>([weak_context, caller_watch_callback](const Coordination::WatchResponse & response)
            {
                if (!(response.type != Coordination::SESSION || response.state == Coordination::EXPIRED_SESSION))
                    return;

                auto owned_context = weak_context.lock();
                if (!owned_context)
                    return;

                bool changed = false;
                {
                    std::lock_guard ctx_lock(owned_context->mutex);

                    if (response.type != Coordination::SESSION)
                        changed = owned_context->invalidated_paths.emplace(response.path).second;
                    else if (response.state == Coordination::EXPIRED_SESSION)
                    {
                        owned_context->invalidated_paths.clear();
                        owned_context->all_paths_invalidated = true;
                        changed = true;
                    }
                }
                if (changed && caller_watch_callback)
                    caller_watch_callback(response);
            });

            caller_to_internal_callbacks.emplace(caller_watch_callback, wrapped_watch_callback);
        }
    }

    ZNode result;

    result.exists = zookeeper->tryGetWatch(path, result.contents, &result.stat, wrapped_watch_callback);
    if (result.exists)
    {
        path_to_cached_znode.emplace(path, result);
        return result;
    }

    /// Node doesn't exist. We must set a watch on node creation (because it wasn't set by tryGetWatch).

    result.exists = zookeeper->existsWatch(path, &result.stat, wrapped_watch_callback);
    if (!result.exists)
    {
        path_to_cached_znode.emplace(path, result);
        return result;
    }

    /// Node was created between the two previous calls, try again. Watch is already set.

    result.exists = zookeeper->tryGet(path, result.contents, &result.stat);
    path_to_cached_znode.emplace(path, result);
    return result;
}

void ZooKeeperNodeCache::sync()
{
    get_zookeeper()->sync("/");
}

}
