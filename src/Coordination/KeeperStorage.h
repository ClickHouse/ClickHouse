#pragma once

#include <deque>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <Coordination/ACLMap.h>
#include <Coordination/SessionExpiryQueue.h>
#include <Coordination/SnapshotableHashTable.h>
#include <Coordination/KeeperCommon.h>
#include <Coordination/KeeperReadThreadPool.h>
#include <Common/StringHashForHeterogeneousLookup.h>
#include <Common/SharedMutex.h>
#include <Common/Concepts.h>

#include <base/defines.h>
#include <memory>

#include <Coordination/CompactChildrenSet.h>

namespace DB
{

class KeeperContext;
using KeeperContextPtr = std::shared_ptr<KeeperContext>;

struct KeeperSnapshotReader;

using ResponseCallback = std::function<void(const Coordination::ZooKeeperResponsePtr &)>;

/// Iterator over nodes in KeeperStorage, frozen at the moment in time when
/// KeeperNodeStreamForSnapshot was created. Creation locks storage_mutex but is relatively fast,
/// then the long-running iteration can proceed after the mutex is unlocked.
struct KeeperNodeStreamForSnapshot
{
    /// Total number of nodes that `next` will report. Storage must provide it in advance.
    size_t node_count = 0;

    virtual bool next(std::string_view & out_path, std::string_view & out_data, KeeperNodeStats & out_stats) = 0;

    virtual ~KeeperNodeStreamForSnapshot() { chassert(node_count == 0); }
};

struct KeeperStorageStats
{
    uint64_t nodes_count = 0;
    uint64_t approximate_data_size = 0;

    uint64_t total_watches_count = 0;
    uint64_t watched_paths_count = 0;
    uint64_t sessions_with_watches_count = 0;
    uint64_t session_with_ephemeral_nodes_count = 0;
    uint64_t total_emphemeral_nodes_count = 0;
    int64_t last_committed_zxid = 0;
};

[[noreturn]] void onStorageInconsistency(std::string_view message);

/// Keeper state machine almost equal to the ZooKeeper's state machine.
/// Implements all logic of operations, data changes, sessions allocation.
/// In-memory and not generally thread safe, though preprocessRequest and processRequest can run in parallel.
/// Split into two parts:
///  * KeeperStorageImpl is responsible for the storing and manipulating the actual nodes, which
///    usually take lots of memory/space and need to be careful about performance.
///  * KeeperStorage base class that manages everything else: sessions, watches, set of ephemeral
///    nodes, ACLs, digest, deltas.
class KeeperStorage
{
public:
    static String generateDigest(const String & userdata);

    struct AuthID
    {
        std::string scheme;
        std::string id;

        bool operator==(const AuthID & other) const { return scheme == other.scheme && id == other.id; }
    };

    using Ephemerals = std::unordered_map<int64_t, std::unordered_set<std::string>>;

    enum class WatchType : uint8_t
    {
        WATCH,
        LIST_WATCH,
        PERSISTENT_WATCH,
        PERSISTENT_LIST_WATCH,
        PERSISTENT_RECURSIVE_WATCH,
    };

    struct WatchInfo
    {
        std::string_view path;
        WatchType type;

        bool operator==(const WatchInfo &) const = default;
    };

    struct WatchInfoHash
    {
        UInt64 operator()(WatchInfo info) const;
    };

    using SessionAndWatcher = std::unordered_map<int64_t, std::unordered_set<WatchInfo, WatchInfoHash>>;
    using SessionIDs = std::unordered_set<int64_t>;

    /// Just vector of SHA1 from user:password
    using AuthIDs = std::vector<AuthID>;
    using SessionAndAuth = std::unordered_map<int64_t, AuthIDs>;
    using Watches = std::unordered_map<
        String /* path, relative of root_path */,
        SessionIDs,
        StringHashForHeterogeneousLookup,
        StringHashForHeterogeneousLookup::transparent_key_equal>;

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
        ACLId acl_id;
        String data;
    };

    struct RemoveNodeDelta
    {
        KeeperNodeStats stat;
        String data;
    };

    struct UpdateNodeStatDelta
    {
        explicit UpdateNodeStatDelta(const KeeperNodeStats & stats)
            : old_stats(stats), new_stats(stats) {}

        KeeperNodeStats old_stats;
        KeeperNodeStats new_stats;
    };

    struct UpdateNodeDataDelta
    {
        std::string old_data;
        std::string new_data;
    };

    struct ErrorDelta
    {
        Coordination::Error error;
    };

    struct FailedMultiDelta
    {
        size_t failed_pos = std::numeric_limits<size_t>::max();
        Coordination::Error failed_pos_error = Coordination::Error::ZOK;
        Coordination::Error global_error = Coordination::Error::ZOK;
    };

    // Denotes end of a subrequest in multi request
    struct SubDeltaEnd
    {
    };

    struct AddAuthDelta
    {
        int64_t session_id;
        std::shared_ptr<KeeperStorage::AuthID> auth_id;
    };

    struct CloseSessionDelta
    {
        int64_t session_id;
    };

    using Operation = std::variant<
        /// Node-related deltas are handled by KeeperStorageImpl.
        CreateNodeDelta,
        RemoveNodeDelta,
        UpdateNodeStatDelta,
        UpdateNodeDataDelta,

        /// Other deltas are handled by base KeeperStorage.
        AddAuthDelta,
        ErrorDelta,
        SubDeltaEnd,
        FailedMultiDelta,
        CloseSessionDelta>;

    struct Delta
    {
        Delta(String path_, int64_t zxid_, Operation operation_) : path(std::move(path_)), zxid(zxid_), operation(std::move(operation_)) { }

        Delta(int64_t zxid_, Coordination::Error error) : Delta("", zxid_, ErrorDelta{error}) { }

        Delta(int64_t zxid_, Operation subdelta) : Delta("", zxid_, subdelta) { }

        String path;
        int64_t zxid;
        Operation operation;
    };

    using DeltaIterator = std::list<KeeperStorage::Delta>::const_iterator;
    using DeltaRange = std::ranges::subrange<DeltaIterator>;

    mutable std::mutex transaction_mutex;

    /// Global id of all requests applied to storage
    int64_t zxid TSA_GUARDED_BY(transaction_mutex) = 0;

    // older Keeper node (pre V5 snapshots) can create snapshots and receive logs from newer Keeper nodes
    // this can lead to some inconsistencies, e.g. from snapshot it will use log_idx as zxid
    // while the log will have a smaller zxid because it's generated by the newer nodes
    // we save the value loaded from snapshot to know when is it okay to have
    // smaller zxid in newer requests
    int64_t old_snapshot_zxid{0};

    uint64_t nodes_digest = 0;

    int64_t session_id_counter{1};

    mutable SharedMutex auth_mutex;
    SessionAndAuth committed_session_and_auth;

    /// ACLMap for more compact ACLs storage inside nodes.
    ACLMap acl_map;

    KeeperContextPtr keeper_context;

    mutable SharedMutex storage_mutex;

    const String superdigest;

    std::mutex ephemeral_mutex;
    /// Mapping session_id -> set of ephemeral nodes paths
    Ephemerals committed_ephemerals;
    size_t committed_ephemeral_nodes{0};

    /// All active sessions with timeout
    SessionAndTimeout session_and_timeout;

    /// Currently active watches (node_path -> subscribed sessions)
    Watches watches;
    Watches list_watches; /// Watches for 'list' request (watches on children).
    Watches persistent_watches;
    Watches persistent_list_watches;
    Watches persistent_recursive_watches;

    /// Mapping session_id -> set of watched nodes paths
    SessionAndWatcher sessions_and_watchers;

    KeeperReadThreadPool read_thread_pool;

    static bool checkDigest(const KeeperDigest & first, const KeeperDigest & second);

    void finalize();

    bool isFinalized() const;

    /// Get current committed zxid
    int64_t getZXID() const;

    int64_t getNextZXID() const;

    /// Allocate new session id with the specified timeouts
    int64_t getSessionID(int64_t session_timeout_ms);

    /// Add session id. Used when restoring KeeperStorage from snapshot.
    void addSessionID(int64_t session_id, int64_t session_timeout_ms) TSA_NO_THREAD_SAFETY_ANALYSIS;

    /// Get all dead sessions
    std::vector<int64_t> getDeadSessions() const;

    /// Get all active sessions
    SessionAndTimeout getActiveSessions() const;

    KeeperDigest getNodesDigest(bool committed, bool lock_transaction_mutex) const;

    /// Introspection function mostly used in 4-letter commands
    virtual KeeperStorageStats getStorageStats() const = 0;

    /// (A little slower than accessing `container` directly, so most request processing should
    ///  be a template and use KeeperStorageImpl instead.)
    virtual bool getCommittedNodeSlow(std::string_view path, KeeperNodeStats * out_stats = nullptr, std::string * out_data = nullptr) = 0;

    /// Directly create a committed node. Used to set up system nodes and by tests.
    virtual bool addSystemNodeIfNotExists(std::string_view path, const KeeperNodeStats & stats, std::string_view data, bool update_parent_num_children, uint64_t * out_digest) = 0;

    uint64_t getTotalWatchesCount() const;

    void dumpWatches(WriteBufferFromOwnString & buf) const;
    void dumpWatchesByPath(WriteBufferFromOwnString & buf) const;
    void dumpSessionsAndEphemerals(WriteBufferFromOwnString & buf) const;

    bool containsWatch(const String & path, Coordination::CheckWatchRequest::CheckWatchType check_type) const;
    void addPersistentWatch(const String & path, Coordination::AddWatchRequest::AddWatchMode mode, int64_t session_id);

    bool removePersistentWatch(const String& path, Coordination::RemoveWatchRequest::WatchType type, int64_t session_id);
protected:
    /// If initialize_system_nodes is false, container starts with no nodes at all, not even "/".
    KeeperStorage(int64_t tick_time_ms, const String & superdigest_, const KeeperContextPtr & keeper_context_);

    /// Expiration queue for session, allows to get dead sessions at some point of time
    SessionExpiryQueue session_expiry_queue;

    struct TransactionInfo
    {
        int64_t zxid{-1};
        KeeperDigest nodes_digest;
        /// index in storage of the log containing the transaction
        int64_t log_idx = 0;
    };

    std::list<TransactionInfo> uncommitted_transactions TSA_GUARDED_BY(transaction_mutex);

public:
    /// For the duration of a preprocessRequest call, these fields accumulate changes made by the
    /// transaction that's being preprocessed.
    /// These deltas are already applied to UncommittedState; if the transaction fails, they must be
    /// rolled back.
    int64_t staging_zxid = -1;
    KeeperDigest staging_digest;
    std::list<Delta> staging_deltas;

protected:

    std::atomic<bool> finalized{false};

    size_t total_watches_count = 0;

    void clearDeadWatches(int64_t session_id);
    int64_t getNextZXIDLocked() const TSA_REQUIRES(transaction_mutex);

    std::atomic<bool> initialized{false};

    /// Does only createStreams-finishStreams on `reader`, the caller does everything before and after that.
    virtual void loadNodesFromSnapshot(KeeperSnapshotReader & reader, uint64_t * out_digest) = 0;

    virtual void commitDelta(const Delta & delta, uint64_t * digest) = 0;
    virtual void cleanupUncommittedState(int64_t commit_zxid) = 0;
    virtual void rollbackUncommittedDelta(const Delta & delta) = 0;
    virtual void cleanupAfterRollback(std::vector<uint64_t> rollbacked_zxids) = 0;
    /// Call to calculate digest after preparing all uncommitted changes for a given zxid.
    virtual uint64_t updateNodesDigest(uint64_t current_digest, uint64_t zxid) const = 0;

public:
    struct UncommittedState
    {
        explicit UncommittedState(KeeperStorage & storage_);

        ~UncommittedState();

        void addDeltas(std::list<Delta> new_deltas);
        void cleanup(int64_t commit_zxid);
        void rollback(int64_t rollback_zxid);
        void rollback(std::list<Delta> rollback_deltas);

        bool hasACL(int64_t session_id, bool committed, std::function<bool(const AuthID &)> predicate) const;

        void forEachAuthInSession(int64_t session_id, std::function<void(const AuthID &)> func) const;

        std::unordered_map<int64_t, std::unordered_set<int64_t>> closed_sessions_to_zxids;

        Ephemerals ephemerals;

        /// for each session, store list of uncommitted auths with their ZXID
        /// TODO: If there are lots of uncommitted Close requests, this grows big, and request
        ///       preprocessing becomes super slow (tens of seconds) because cleanup iterates over
        ///       this whole map. Maybe do something about it.
        std::unordered_map<int64_t, std::list<std::pair<int64_t, std::shared_ptr<AuthID>>>> session_and_auth;

        mutable std::mutex deltas_mutex;
        std::list<Delta> deltas TSA_GUARDED_BY(deltas_mutex);
        KeeperStorage & storage;
    };

    UncommittedState uncommitted_state{*this};

    /// 0 if no uncommitted requests.
    uint64_t getLastUncommittedLogIdx() const;

    Coordination::Error commit(DeltaRange deltas);

    /// Used internally by `preprocess` and `processLocal` implementations.
    /// They usually have acl_id readily available, so we don't have to look up the node here.
    bool checkACL(ACLId acl_id, int32_t permissions, int64_t session_id, bool committed) const;

    /// Used externally. Locks storage mutex and looks up the node.
    bool checkCommittedACL(std::string_view path, int32_t permissions, int64_t session_id);

    /// If initialize_system_nodes is false, container starts with no nodes at all, not even "/".
    static std::unique_ptr<KeeperStorage> create(int64_t tick_time_ms, const String & superdigest_, const KeeperContextPtr & keeper_context_, bool initialize_system_nodes = true);

    virtual ~KeeperStorage();

    void initializeSystemNodes() TSA_NO_THREAD_SAFETY_ANALYSIS;

    /// Must be called on an empty storage, created with initialize_system_nodes = false.
    /// Caller has already read everything before the nodes, this method starts from createStreams.
    /// TODO: When we have chunked snapshots, probably pass a ThreadPool here.
    void loadFromSnapshot(KeeperSnapshotReader & reader) TSA_NO_THREAD_SAFETY_ANALYSIS;

    /// Caller must hold storage mutex.
    /// At most one stream can exist at any given time.
    /// Stream must be destroyed using finishWritingSnapshot (with storage mutex held), otherwise
    /// destructor fails assert.
    virtual std::unique_ptr<KeeperNodeStreamForSnapshot> beginWritingSnapshot() = 0;
    virtual void finishWritingSnapshot(std::unique_ptr<KeeperNodeStreamForSnapshot> stream) = 0;

    /// Process a batch of local read requests (no deltas, no commit).
    virtual KeeperResponsesForSessions processLocalRequests(
        const KeeperRequestsForSessions & requests,
        bool check_acl = true) = 0;
    /// Pre-validate uncommitted request and apply it to uncommitted state.
    /// check_acl = false only when converting data from ZooKeeper.
    virtual KeeperDigest preprocessRequest(
        const Coordination::ZooKeeperRequestPtr & request,
        int64_t session_id,
        int64_t time,
        int64_t new_last_zxid,
        bool check_acl = true,
        std::optional<KeeperDigest> digest = std::nullopt,
        int64_t log_idx = 0) = 0;
    /// Commit a previously preprocessed request. Apply the changes to the committed state.
    /// Produce response for the request + triggered watch notifications.
    virtual KeeperResponsesForSessions processRequest(
        const Coordination::ZooKeeperRequestPtr & request,
        int64_t session_id,
        std::optional<int64_t> new_last_zxid) = 0;

    void rollbackRequest(int64_t rollback_zxid, bool allow_missing);

    KeeperResponsesForSessions setWatches(
        int64_t last_zxid,
        const std::vector<String> & watches_paths,
        const std::vector<String> & list_watches_paths,
        const std::vector<String> & exist_watches_paths,
        const std::vector<String> & persistent_watches_paths,
        const std::vector<String> & persistent_recursive_watches_paths,
        int64_t session_id);

    /// Register watches from a request/response pair.
    void updateWatches(
        const Coordination::ZooKeeperRequestPtr & zk_request,
        const Coordination::Response * response,
        int64_t session_id);

    virtual void recalculateStats() = 0;

    std::pair<KeeperResponsesForSessions, Int64> processWatchesImpl(
        std::string_view path, Coordination::Event event_type);
};

/// Remove `path` from the set of ephemeral paths owned by `session_id`.
void unregisterEphemeralPath(KeeperStorage::Ephemerals & ephemerals, int64_t session_id, const std::string & path, bool throw_if_missing);

}
