#pragma once

#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Coordination/SessionExpiryQueue.h>
#include <Coordination/SnapshotableHashTable.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace DB
{

using namespace DB;
struct KeeperStorageRequest;
using KeeperStorageRequestPtr = std::shared_ptr<KeeperStorageRequest>;
using ResponseCallback = std::function<void(const Coordination::ZooKeeperResponsePtr &)>;
using ChildrenSet = std::unordered_set<std::string>;
using SessionAndTimeout = std::unordered_map<int64_t, int64_t>;

struct KeeperStorageSnapshot;

class KeeperStorage
{
public:
    int64_t session_id_counter{1};

    struct Node
    {
        String data;
        Coordination::ACLs acls{};
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

    using Container = SnapshotableHashTable<Node>;
    using Ephemerals = std::unordered_map<int64_t, std::unordered_set<std::string>>;
    using SessionAndWatcher = std::unordered_map<int64_t, std::unordered_set<std::string>>;
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

    int64_t getZXID() const
    {
        return zxid;
    }

public:
    KeeperStorage(int64_t tick_time_ms);

    int64_t getSessionID(int64_t session_timeout_ms)
    {
        auto result = session_id_counter++;
        session_and_timeout.emplace(result, session_timeout_ms);
        session_expiry_queue.update(result, session_timeout_ms);
        return result;
    }

    void addSessionID(int64_t session_id, int64_t session_timeout_ms)
    {
        session_and_timeout.emplace(session_id, session_timeout_ms);
        session_expiry_queue.update(session_id, session_timeout_ms);
    }

    ResponsesForSessions processRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id, std::optional<int64_t> new_last_zxid);

    void finalize();

    void enableSnapshotMode()
    {
        container.enableSnapshotMode();
    }

    void disableSnapshotMode()
    {
        container.disableSnapshotMode();
    }

    Container::const_iterator getSnapshotIteratorBegin() const
    {
        return container.begin();
    }

    void clearGarbageAfterSnapshot()
    {
        container.clearOutdatedNodes();
    }

    const SessionAndTimeout & getActiveSessions() const
    {
        return session_and_timeout;
    }

    std::unordered_set<int64_t> getDeadSessions()
    {
        return session_expiry_queue.getExpiredSessions();
    }
};

using KeeperStoragePtr = std::unique_ptr<KeeperStorage>;

}
