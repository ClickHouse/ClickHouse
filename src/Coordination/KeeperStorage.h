#pragma once

#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Coordination/SessionExpiryQueue.h>
#include <Coordination/ACLMap.h>
#include <Coordination/SnapshotableHashTable.h>
#include <IO/WriteBufferFromString.h>
#include <unordered_map>
#include <vector>

#include <absl/container/flat_hash_set.h>

namespace DB
{

struct KeeperStorageRequestProcessor;
using KeeperStorageRequestProcessorPtr = std::shared_ptr<KeeperStorageRequestProcessor>;
using ResponseCallback = std::function<void(const Coordination::ZooKeeperResponsePtr &)>;
using ChildrenSet = absl::flat_hash_set<StringRef, StringRefHash>;
using SessionAndTimeout = std::unordered_map<int64_t, int64_t>;

struct KeeperStorageSnapshot;

/// Keeper state machine almost equal to the ZooKeeper's state machine.
/// Implements all logic of operations, data changes, sessions allocation.
/// In-memory and not thread safe.
class KeeperStorage
{
public:

    struct Node
    {
        uint64_t acl_id = 0; /// 0 -- no ACL by default
        bool is_sequental = false;
        Coordination::Stat stat{};
        int32_t seq_num = 0;
        uint64_t size_bytes; // save size to avoid calculate every time

        Node() : size_bytes(sizeof(Node)) { }

        /// Object memory size
        uint64_t sizeInBytes() const
        {
            return size_bytes;
        }

        void setData(String new_data);

        const auto & getData() const noexcept
        {
            return data;
        }

        void addChild(StringRef child_path);

        void removeChild(StringRef child_path);

        const auto & getChildren() const noexcept
        {
            return children;
        }
    private:
        String data;
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
        int64_t time;
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
    using Watches = std::map<String /* path, relative of root_path */, SessionIDs>;

    int64_t session_id_counter{1};

    SessionAndAuth session_and_auth;

    /// Main hashtable with nodes. Contain all information about data.
    /// All other structures expect session_and_timeout can be restored from
    /// container.
    Container container;

    /// Mapping session_id -> set of ephemeral nodes paths
    Ephemerals ephemerals;
    /// Mapping session_id -> set of watched nodes paths
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
    ResponsesForSessions processRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id, int64_t time, std::optional<int64_t> new_last_zxid, bool check_acl = true);

    void finalize();

    /// Set of methods for creating snapshots

    /// Turn on snapshot mode, so data inside Container is not deleted, but replaced with new version.
    void enableSnapshotMode(size_t up_to_version)
    {
        container.enableSnapshotMode(up_to_version);

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
    std::vector<int64_t> getDeadSessions() const
    {
        return session_expiry_queue.getExpiredSessions();
    }

    /// Introspection functions mostly used in 4-letter commands
    uint64_t getNodesCount() const
    {
        return container.size();
    }

    uint64_t getApproximateDataSize() const
    {
        return container.getApproximateDataSize();
    }

    uint64_t getArenaDataSize() const
    {
        return container.keyArenaSize();
    }


    uint64_t getTotalWatchesCount() const;

    uint64_t getWatchedPathsCount() const
    {
        return watches.size() + list_watches.size();
    }

    uint64_t getSessionsWithWatchesCount() const;

    uint64_t getSessionWithEphemeralNodesCount() const
    {
        return ephemerals.size();
    }
    uint64_t getTotalEphemeralNodesCount() const;

    void dumpWatches(WriteBufferFromOwnString & buf) const;
    void dumpWatchesByPath(WriteBufferFromOwnString & buf) const;
    void dumpSessionsAndEphemerals(WriteBufferFromOwnString & buf) const;
};

using KeeperStoragePtr = std::unique_ptr<KeeperStorage>;

}
