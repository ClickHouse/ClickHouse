#pragma once

#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Coordination/SessionExpiryQueue.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace DB
{

using namespace DB;
struct NuKeeperStorageRequest;
using NuKeeperStorageRequestPtr = std::shared_ptr<NuKeeperStorageRequest>;
using ResponseCallback = std::function<void(const Coordination::ZooKeeperResponsePtr &)>;
using ChildrenSet = std::unordered_set<std::string>;

class NuKeeperStorage
{
public:
    int64_t session_id_counter{0};

    struct Node
    {
        String data;
        Coordination::ACLs acls{};
        bool is_ephemeral = false;
        bool is_sequental = false;
        Coordination::Stat stat{};
        int32_t seq_num = 0;
        ChildrenSet children{};
    };

    struct ResponseForSession
    {
        int64_t session_id;
        Coordination::ZooKeeperResponsePtr response;
    };

    using ResponsesForSessions = std::vector<ResponseForSession>;

    struct RequestForSession
    {
        int64_t session_id;
        Coordination::ZooKeeperRequestPtr request;
    };

    using RequestsForSessions = std::vector<RequestForSession>;

    using Container = std::unordered_map<std::string, Node>;
    using Ephemerals = std::unordered_map<int64_t, std::unordered_set<std::string>>;
    using SessionAndWatcher = std::unordered_map<int64_t, std::unordered_set<std::string>>;
    using SessionAndTimeout = std::unordered_map<int64_t, long>;
    using SessionIDs = std::vector<int64_t>;

    using Watches = std::map<String /* path, relative of root_path */, SessionIDs>;

    Container container;
    Ephemerals ephemerals;
    SessionAndWatcher sessions_and_watchers;
    SessionExpiryQueue session_expiry_queue;
    SessionAndTimeout session_and_timeout;

    int64_t zxid{0};
    bool finalized{false};

    Watches watches;
    Watches list_watches;   /// Watches for 'list' request (watches on children).

    void clearDeadWatches(int64_t session_id);

    int64_t getZXID()
    {
        return zxid++;
    }

public:
    NuKeeperStorage(int64_t tick_time_ms);

    int64_t getSessionID(int64_t session_timeout_ms)
    {
        auto result = session_id_counter++;
        session_and_timeout.emplace(result, session_timeout_ms);
        session_expiry_queue.update(result, session_timeout_ms);
        return result;
    }

    ResponsesForSessions processRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id);

    void finalize();

    std::unordered_set<int64_t> getDeadSessions()
    {
        return session_expiry_queue.getExpiredSessions();
    }
};

}
