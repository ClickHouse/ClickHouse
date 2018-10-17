#pragma once

#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <memory>
#include <optional>
#include <Poco/Event.h>
#include "ZooKeeper.h"
#include "Common.h"

namespace DB
{
    namespace ErrorCodes
    {
        extern const int NO_ZOOKEEPER;
    }
}

namespace zkutil
{

/// This class allows querying the contents of ZooKeeper nodes and caching the results.
/// Watches are set for cached nodes and for nodes that were nonexistent at the time of query.
/// After a watch fires, the callback or event that was passed by the user is notified.
///
/// NOTE: methods of this class are not thread-safe.
///
/// Intended use case: if you need one thread to watch changes in several nodes.
/// If instead you use simple a watch event for this, watches will accumulate for nodes that do not change
/// or change rarely.
class ZooKeeperNodeCache
{
public:
    ZooKeeperNodeCache(GetZooKeeper get_zookeeper);

    ZooKeeperNodeCache(const ZooKeeperNodeCache &) = delete;
    ZooKeeperNodeCache(ZooKeeperNodeCache &&) = default;

    struct GetResult
    {
        bool exists = false;
        std::string contents;
        Coordination::Stat stat;
    };

    GetResult get(const std::string & path, EventPtr watch_event);
    GetResult get(const std::string & path, Coordination::WatchCallback watch_callback);

private:
    GetZooKeeper get_zookeeper;

    struct Context
    {
        std::mutex mutex;
        zkutil::ZooKeeperPtr zookeeper;
        std::unordered_set<std::string> invalidated_paths;
    };

    std::shared_ptr<Context> context;

    std::unordered_map<std::string, GetResult> node_cache;
};

}
