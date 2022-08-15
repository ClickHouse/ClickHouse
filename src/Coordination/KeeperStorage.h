#pragma once

#include <unordered_map>
#include <vector>
#include <Coordination/ACLMap.h>
#include <Coordination/SessionExpiryQueue.h>
#include <Coordination/SnapshotableHashTable.h>
#include <IO/WriteBufferFromString.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Coordination/KeeperContext.h>

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
        uint64_t sizeInBytes() const { return size_bytes; }

        void setData(String new_data);

        const auto & getData() const noexcept { return data; }

        void addChild(StringRef child_path);

        void removeChild(StringRef child_path);

        const auto & getChildren() const noexcept { return children; }

        // Invalidate the calculated digest so it's recalculated again on the next
        // getDigest call
        void invalidateDigestCache() const;

        // get the calculated digest of the node
        UInt64 getDigest(std::string_view path) const;

        // copy only necessary information for preprocessing and digest calculation
        // (e.g. we don't need to copy list of children)
        void shallowCopy(const Node & other);

    private:
        String data;
        ChildrenSet children{};
        mutable std::optional<UInt64> cached_digest;
    };

    enum DigestVersion : uint8_t
    {
        NO_DIGEST = 0,
        V1 = 1,
        V2 = 2  // added system nodes that modify the digest on startup so digest from V0 is invalid
    };

    static constexpr auto CURRENT_DIGEST_VERSION = DigestVersion::V2;

    struct ResponseForSession
    {
        int64_t session_id;
        Coordination::ZooKeeperResponsePtr response;
    };
    using ResponsesForSessions = std::vector<ResponseForSession>;

    struct Digest
    {
        DigestVersion version{DigestVersion::NO_DIGEST};
        uint64_t value{0};
    };

    static bool checkDigest(const Digest & first, const Digest & second)
    {
        if (first.version != second.version)
            return true;

        if (first.version == DigestVersion::NO_DIGEST)
            return true;

        return first.value == second.value;
    }

    struct RequestForSession
    {
        int64_t session_id;
        int64_t time;
        Coordination::ZooKeeperRequestPtr request;
        int64_t zxid{0};
        std::optional<Digest> digest;
    };

    struct AuthID
    {
        std::string scheme;
        std::string id;

        bool operator==(const AuthID & other) const { return scheme == other.scheme && id == other.id; }
    };

    using RequestsForSessions = std::vector<RequestForSession>;

    using Container = SnapshotableHashTable<Node>;
    using Ephemerals = std::unordered_map<int64_t, std::unordered_set<std::string>>;
    using SessionAndWatcher = std::unordered_map<int64_t, std::unordered_set<std::string>>;
    using SessionIDs = std::unordered_set<int64_t>;

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

    // Applying ZooKeeper request to storage consists of two steps:
    //  - preprocessing which, instead of applying the changes directly to storage,
    //    generates deltas with those changes, denoted with the request ZXID
    //  - processing which applies deltas with the correct ZXID to the storage
    //
    // Delta objects allow us two things:
    //  - fetch the latest, uncommitted state of an object by getting the committed
    //    state of that same object from the storage and applying the deltas
    //    in the same order as they are defined
    //  - quickly commit the changes to the storage
    struct CreateNodeDelta
    {
        Coordination::Stat stat;
        bool is_sequental;
        Coordination::ACLs acls;
        String data;
    };

    struct RemoveNodeDelta
    {
        int32_t version{-1};
        int64_t ephemeral_owner{0};
    };

    struct UpdateNodeDelta
    {
        std::function<void(Node &)> update_fn;
        int32_t version{-1};
    };

    struct SetACLDelta
    {
        Coordination::ACLs acls;
        int32_t version{-1};
    };

    struct ErrorDelta
    {
        Coordination::Error error;
    };

    struct FailedMultiDelta
    {
        std::vector<Coordination::Error> error_codes;
    };

    // Denotes end of a subrequest in multi request
    struct SubDeltaEnd
    {
    };

    struct AddAuthDelta
    {
        int64_t session_id;
        AuthID auth_id;
    };

    using Operation = std::
        variant<CreateNodeDelta, RemoveNodeDelta, UpdateNodeDelta, SetACLDelta, AddAuthDelta, ErrorDelta, SubDeltaEnd, FailedMultiDelta>;

    struct Delta
    {
        Delta(String path_, int64_t zxid_, Operation operation_) : path(std::move(path_)), zxid(zxid_), operation(std::move(operation_)) { }

        Delta(int64_t zxid_, Coordination::Error error) : Delta("", zxid_, ErrorDelta{error}) { }

        Delta(int64_t zxid_, Operation subdelta) : Delta("", zxid_, subdelta) { }

        String path;
        int64_t zxid;
        Operation operation;
    };

    struct UncommittedState
    {
        explicit UncommittedState(KeeperStorage & storage_) : storage(storage_) { }

        void addDeltas(std::vector<Delta> new_deltas);
        void commit(int64_t commit_zxid);
        void rollback(int64_t rollback_zxid);

        std::shared_ptr<Node> getNode(StringRef path) const;
        Coordination::ACLs getACLs(StringRef path) const;

        void applyDelta(const Delta & delta);

        bool hasACL(int64_t session_id, bool is_local, std::function<bool(const AuthID &)> predicate)
        {
            for (const auto & session_auth : storage.session_and_auth[session_id])
            {
                if (predicate(session_auth))
                    return true;
            }

            if (is_local)
                return false;

            for (const auto & delta : deltas)
            {
                if (const auto * auth_delta = std::get_if<KeeperStorage::AddAuthDelta>(&delta.operation);
                    auth_delta && auth_delta->session_id == session_id && predicate(auth_delta->auth_id))
                    return true;
            }

            return false;
        }

        std::shared_ptr<Node> tryGetNodeFromStorage(StringRef path) const;

        struct UncommittedNode
        {
            std::shared_ptr<Node> node{nullptr};
            Coordination::ACLs acls{};
            int64_t zxid{0};
        };

        mutable std::unordered_map<std::string, UncommittedNode> nodes;
        std::list<Delta> deltas;
        KeeperStorage & storage;
    };

    UncommittedState uncommitted_state{*this};

    Coordination::Error commit(int64_t zxid);

    // Create node in the storage
    // Returns false if it failed to create the node, true otherwise
    // We don't care about the exact failure because we should've caught it during preprocessing
    bool createNode(
        const std::string & path,
        String data,
        const Coordination::Stat & stat,
        bool is_sequental,
        Coordination::ACLs node_acls);

    // Remove node in the storage
    // Returns false if it failed to remove the node, true otherwise
    // We don't care about the exact failure because we should've caught it during preprocessing
    bool removeNode(const std::string & path, int32_t version);

    bool checkACL(StringRef path, int32_t permissions, int64_t session_id, bool is_local);

    void unregisterEphemeralPath(int64_t session_id, const std::string & path);

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

    // older Keeper node (pre V5 snapshots) can create snapshots and receive logs from newer Keeper nodes
    // this can lead to some inconsistencies, e.g. from snapshot it will use log_idx as zxid
    // while the log will have a smaller zxid because it's generated by the newer nodes
    // we save the value loaded from snapshot to know when is it okay to have
    // smaller zxid in newer requests
    int64_t old_snapshot_zxid{0};

    struct TransactionInfo
    {
        int64_t zxid;
        Digest nodes_digest;
    };

    std::deque<TransactionInfo> uncommitted_transactions;

    uint64_t nodes_digest{0};

    bool finalized{false};

    /// Currently active watches (node_path -> subscribed sessions)
    Watches watches;
    Watches list_watches; /// Watches for 'list' request (watches on children).

    void clearDeadWatches(int64_t session_id);

    /// Get current committed zxid
    int64_t getZXID() const { return zxid; }

    int64_t getNextZXID() const
    {
        if (uncommitted_transactions.empty())
            return zxid + 1;

        return uncommitted_transactions.back().zxid + 1;
    }

    Digest getNodesDigest(bool committed) const;

    KeeperContextPtr keeper_context;

    const String superdigest;

    bool initialized{false};

    KeeperStorage(int64_t tick_time_ms, const String & superdigest_, const KeeperContextPtr & keeper_context_, bool initialize_system_nodes = true);

    void initializeSystemNodes();

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

    UInt64 calculateNodesDigest(UInt64 current_digest, const std::vector<Delta> & new_deltas) const;

    /// Process user request and return response.
    /// check_acl = false only when converting data from ZooKeeper.
    ResponsesForSessions processRequest(
        const Coordination::ZooKeeperRequestPtr & request,
        int64_t session_id,
        std::optional<int64_t> new_last_zxid,
        bool check_acl = true,
        bool is_local = false);
    void preprocessRequest(
        const Coordination::ZooKeeperRequestPtr & request,
        int64_t session_id,
        int64_t time,
        int64_t new_last_zxid,
        bool check_acl = true,
        std::optional<Digest> digest = std::nullopt);
    void rollbackRequest(int64_t rollback_zxid, bool allow_missing);

    void finalize();

    /// Set of methods for creating snapshots

    /// Turn on snapshot mode, so data inside Container is not deleted, but replaced with new version.
    void enableSnapshotMode(size_t up_to_version) { container.enableSnapshotMode(up_to_version); }

    /// Turn off snapshot mode.
    void disableSnapshotMode() { container.disableSnapshotMode(); }

    Container::const_iterator getSnapshotIteratorBegin() const { return container.begin(); }

    /// Clear outdated data from internal container.
    void clearGarbageAfterSnapshot() { container.clearOutdatedNodes(); }

    /// Get all active sessions
    const SessionAndTimeout & getActiveSessions() const { return session_and_timeout; }

    /// Get all dead sessions
    std::vector<int64_t> getDeadSessions() const { return session_expiry_queue.getExpiredSessions(); }

    /// Introspection functions mostly used in 4-letter commands
    uint64_t getNodesCount() const { return container.size(); }

    uint64_t getApproximateDataSize() const { return container.getApproximateDataSize(); }

    uint64_t getArenaDataSize() const { return container.keyArenaSize(); }

    uint64_t getTotalWatchesCount() const;

    uint64_t getWatchedPathsCount() const { return watches.size() + list_watches.size(); }

    uint64_t getSessionsWithWatchesCount() const;

    uint64_t getSessionWithEphemeralNodesCount() const { return ephemerals.size(); }
    uint64_t getTotalEphemeralNodesCount() const;

    void dumpWatches(WriteBufferFromOwnString & buf) const;
    void dumpWatchesByPath(WriteBufferFromOwnString & buf) const;
    void dumpSessionsAndEphemerals(WriteBufferFromOwnString & buf) const;

private:
    void removeDigest(const Node & node, std::string_view path);
    void addDigest(const Node & node, std::string_view path);
};

using KeeperStoragePtr = std::unique_ptr<KeeperStorage>;

}
