#pragma once

#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Coordination/SessionExpiryQueue.h>
#include <Coordination/ACLMap.h>
#include <Coordination/SnapshotableHashTable.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace DB
{

using namespace DB;
struct KeeperStorageRequestProcessor;
using KeeperStorageRequestProcessorPtr = std::shared_ptr<KeeperStorageRequestProcessor>;
using ResponseCallback = std::function<void(const Coordination::ZooKeeperResponsePtr &)>;
using ChildrenSet = std::unordered_set<std::string>;
using SessionAndTimeout = std::unordered_map<int64_t, int64_t>;

struct KeeperStorageSnapshot;

/// Keeper state machine almost equal to the ZooKeeper's state machine.
/// Implements all logic of operations, data changes, sessions allocation.
/// In-memory and not thread safe.
class KeeperStorage
{
public:
    int64_t session_id_counter{1};

    struct Node
    {
        String data;
        uint64_t acl_id = 0; /// 0 -- no ACL by default
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

    struct AuthID
    {
        std::string scheme;
        std::string id;

        bool operator==(const AuthID & other) const
        {
            return scheme == other.scheme && id == other.id;
        }
    };

    using RequestsForSessions = std::vector<RequestForSession>;

    using Container = SnapshotableHashTable<Node>;
    using Ephemerals = std::unordered_map<int64_t, std::unordered_set<std::string>>;
    using SessionAndWatcher = std::unordered_map<int64_t, std::unordered_set<std::string>>;
    using SessionIDs = std::vector<int64_t>;

    /// Just vector of SHA1 from user:password
    using AuthIDs = std::vector<AuthID>;
    using SessionAndAuth = std::unordered_map<int64_t, AuthIDs>;
    SessionAndAuth session_and_auth;

    using Watches = std::map<String /* path, relative of root_path */, SessionIDs>;

    /// Main hashtable with nodes. Contain all information about data.
    /// All other structures expect session_and_timeout can be restored from
    /// container.
    Container container;

    /// Mapping session_id -> set of ephemeral nodes paths
    Ephemerals ephemerals;
    /// Mapping sessuib_id -> set of watched nodes paths
    SessionAndWatcher sessions_and_watchers;
    /// Expiration queue for session, allows to get dead sessions at some point of time
    SessionExpiryQueue session_expiry_queue;
    /// All active sessions with timeout
    SessionAndTimeout session_and_timeout;

    /// ACLMap for more compact ACLs storage inside nodes.
    ACLMap acl_map;

    /// Global id of all requests applied to storage
    int64_t zxid{0};
    bool finalized{false};

    /// Currently active watches (node_path -> subscribed sessions)
    Watches watches;
    Watches list_watches;   /// Watches for 'list' request (watches on children).

    void clearDeadWatches(int64_t session_id);

    /// Get current zxid
    int64_t getZXID() const
    {
        return zxid;
    }

    const String superdigest;

public:
    KeeperStorage(int64_t tick_time_ms, const String & superdigest_);

    /// Allocate new session id with the specified timeouts
    int64_t getSessionID(int64_t session_timeout_ms)
    {
        auto result = session_id_counter++;
        session_and_timeout.emplace(result, session_timeout_ms);
        session_expiry_queue.addNewSessionOrUpdate(result, session_timeout_ms);
        return result;
    }

    /// Add session id. Used when restoring KeeperStorage from snapshot.
    void addSessionID(int64_t session_id, int64_t session_timeout_ms)
    {
        session_and_timeout.emplace(session_id, session_timeout_ms);
        session_expiry_queue.addNewSessionOrUpdate(session_id, session_timeout_ms);
    }

    /// Process user request and return response.
    /// check_acl = false only when converting data from ZooKeeper.
    ResponsesForSessions processRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id, std::optional<int64_t> new_last_zxid, bool check_acl = true);

    void finalize();

    /// Set of methods for creating snapshots

    /// Turn on snapshot mode, so data inside Container is not deleted, but replaced with new version.
    void enableSnapshotMode()
    {
        container.enableSnapshotMode();
    }

    /// Turn off snapshot mode.
    void disableSnapshotMode()
    {
        container.disableSnapshotMode();
    }

    Container::const_iterator getSnapshotIteratorBegin() const
    {
        return container.begin();
    }

    /// Clear outdated data from internal container.
    void clearGarbageAfterSnapshot()
    {
        container.clearOutdatedNodes();
    }

    /// Get all active sessions
    const SessionAndTimeout & getActiveSessions() const
    {
        return session_and_timeout;
    }

    /// Get all dead sessions
    std::vector<int64_t> getDeadSessions()
    {
        return session_expiry_queue.getExpiredSessions();
    }
};

using KeeperStoragePtr = std::unique_ptr<KeeperStorage>;

}
