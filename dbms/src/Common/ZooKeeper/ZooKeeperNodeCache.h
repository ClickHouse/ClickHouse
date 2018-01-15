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
/// After a watch fires, a notification is generated for the change event.
/// NOTE: methods of this class are not thread-safe.
class ZooKeeperNodeCache
{
public:
    ZooKeeperNodeCache(GetZooKeeper get_zookeeper);

    ZooKeeperNodeCache(const ZooKeeperNodeCache &) = delete;
    ZooKeeperNodeCache(ZooKeeperNodeCache &&) = default;

    std::optional<std::string> get(const std::string & path);

    Poco::Event & getChangedEvent() { return context->changed_event; }

private:
    GetZooKeeper get_zookeeper;

    struct Context
    {
        Poco::Event changed_event;

        std::mutex mutex;
        zkutil::ZooKeeperPtr zookeeper;
        std::unordered_set<std::string> invalidated_paths;
    };

    std::shared_ptr<Context> context;

    std::unordered_set<std::string> nonexistent_nodes;
    std::unordered_map<std::string, std::string> node_cache;
};

}
