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

/// KeeperMemNode should have as minimal size as possible to reduce memory footprint
/// of stored nodes
/// New fields should be added to the struct only if it's really necessary
struct KeeperMemNode
{
    KeeperNodeStats stats;
    std::unique_ptr<char[]> data{nullptr};
    mutable uint64_t cached_digest = 0;

    KeeperMemNode() = default;

    KeeperMemNode & operator=(const KeeperMemNode & other);
    KeeperMemNode(const KeeperMemNode & other);

    KeeperMemNode & operator=(KeeperMemNode && other) noexcept;
    KeeperMemNode(KeeperMemNode && other) noexcept;

    bool empty() const;

    /// Object memory size
    uint64_t sizeInBytes() const;

    void setData(const String & new_data);

    std::string_view getData() const noexcept { return {data.get(), stats.data_size}; }

    void addChild(std::string_view child_path);

    void removeChild(std::string_view child_path);

    template <typename Self>
    auto & getChildren(this Self & self)
    {
        return self.children;
    }

    // Invalidate the calculated digest so it's recalculated again on the next
    // getDigest call
    void invalidateDigestCache() const;

    // get the calculated digest of the node
    UInt64 getDigest(std::string_view path) const;

    // copy only necessary information for preprocessing and digest calculation
    // (e.g. we don't need to copy list of children)
    void shallowCopy(const KeeperMemNode & other);

    // copy data from node that is left only for snapshot write
    // e.g. we don't need list of children when writing snapshots so we can
    // move it to the new copy of node
    KeeperMemNode copyFromSnapshotNode();
private:
    CompactChildrenSet children{};
};

/// Going to >160 bytes pushes to jemalloc bin #10 (192 bytes).
#if !defined(ADDRESS_SANITIZER) && !defined(MEMORY_SANITIZER)
static_assert(sizeof(KeeperMemNode) <= 160);
#endif

struct KeeperStorageStats
{
    KeeperStorageStats() = default;
    KeeperStorageStats(const KeeperStorageStats & other);
    KeeperStorageStats & operator=(const KeeperStorageStats & other);

    std::atomic<uint64_t> nodes_count = 0;
    std::atomic<uint64_t> approximate_data_size = 0;
    std::atomic<uint64_t> total_watches_count = 0;
    std::atomic<uint64_t> watched_paths_count = 0;
    std::atomic<uint64_t> sessions_with_watches_count = 0;
    std::atomic<uint64_t> session_with_ephemeral_nodes_count = 0;
    std::atomic<uint64_t> total_emphemeral_nodes_count = 0;
    std::atomic<int64_t> last_zxid = 0;
};

/// Keeper state machine almost equal to the ZooKeeper's state machine.
/// Implements all logic of operations, data changes, sessions allocation.
/// In-memory and not thread safe.
class KeeperStorage
{
public:
    using Container = SnapshotableHashTable<KeeperMemNode>;
    using Node = KeeperMemNode;

#if !defined(ADDRESS_SANITIZER) && !defined(MEMORY_SANITIZER)
    static_assert(sizeof(CompactChildrenSet) == 16);
    static_assert(sizeof(KeeperMemNode) == 104);
    static_assert(
        sizeof(ListNode<Node>) <= 128,
        "std::list node containing ListNode<Node> is > 144 bytes (sizeof(ListNode<Node>) + 16 bytes for pointers) which will increase "
        "memory consumption");
    static_assert(std::is_nothrow_move_assignable_v<CompactChildrenSet>);
    static_assert(std::is_nothrow_move_constructible_v<CompactChildrenSet>);
#endif

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

    struct Delta;

    using DeltaIterator = std::list<KeeperStorage::Delta>::const_iterator;
    struct DeltaRange
    {
        DeltaIterator begin_it;
        DeltaIterator end_it;

        DeltaIterator begin() const;
        DeltaIterator end() const;
        bool empty() const;
        const KeeperStorage::Delta & front() const;
    };

    /// Element of RemoveRecursive's list of nodes to remove.
    struct SubtreeNodeToRemove
    {
        std::string path;
        ACLId acl_id = 0;
        std::optional<int64_t> ephemeral_owner;
    };

    KeeperStorageStats stats;

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

    const KeeperStorageStats & getStorageStats() const;

    uint64_t getSessionWithEphemeralNodesCount() const;
    uint64_t getTotalEphemeralNodesCount() const;

    uint64_t getWatchedPathsCount() const;

    uint64_t getSessionsWithWatchesCount() const;
    uint64_t getTotalWatchesCount() const;

    void dumpWatches(WriteBufferFromOwnString & buf) const;
    void dumpWatchesByPath(WriteBufferFromOwnString & buf) const;
    void dumpSessionsAndEphemerals(WriteBufferFromOwnString & buf) const;

    bool containsWatch(const String & path, Coordination::CheckWatchRequest::CheckWatchType check_type) const;
    void addPersistentWatch(const String & path, Coordination::AddWatchRequest::AddWatchMode mode, int64_t session_id);

    bool removePersistentWatch(const String& path, Coordination::RemoveWatchRequest::WatchType type, int64_t session_id);
protected:
    KeeperStorage(int64_t tick_time_ms, const KeeperContextPtr & keeper_context, const String & superdigest_);

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

public:
    /// Main hashtable with nodes. Contain all information about data.
    /// All other structures expect session_and_timeout can be restored from
    /// container.
    Container container;

    struct UncommittedNodeRef;

    struct UncommittedState
    {
        explicit UncommittedState(KeeperStorage & storage_);

        ~UncommittedState();

        void addDeltas(std::list<Delta> new_deltas);
        void cleanup(int64_t commit_zxid);
        void rollback(int64_t rollback_zxid);
        void rollback(std::list<Delta> rollback_deltas);

        UncommittedNodeRef getNode(std::string_view path) const;

        void rollbackDelta(const Delta & delta);

        /// Update digest with new nodes
        UInt64 updateNodesDigest(UInt64 current_digest, UInt64 zxid) const;

        bool hasACL(int64_t session_id, bool committed, std::function<bool(const AuthID &)> predicate) const;

        void forEachAuthInSession(int64_t session_id, std::function<void(const AuthID &)> func) const;

        std::shared_ptr<Node> tryGetNodeFromStorage(std::string_view path) const;

        std::unordered_map<int64_t, std::unordered_set<int64_t>> closed_sessions_to_zxids;

        struct UncommittedNode
        {
            std::shared_ptr<Node> node{nullptr};
            /// Tracks which zxids have been applied to this uncommitted node.
            /// Typically 1-3 entries; vector is faster than unordered_set at this size.
            /// May contain duplicates when a Multi operation applies multiple deltas
            /// with the same zxid to the same node — this is harmless because erasure
            /// uses std::erase (removes all matches) and only emptiness is checked.
            /// TODO: Store just the max zxid instead of the whole set. Clean up when commit zxid
            ///       reaches that value. It may end up overestimated on rollback, but that's fine,
            ///       it would just delay cleanup a little. But maybe this won't work because of the
            ///       zxid_to_nodes[0] thing, research that first.
            std::vector<uint64_t> applied_zxids{};

            void materializeACL(const ACLMap & current_acl_map);
        };

        /// zxid_to_nodes stores iterators of nodes map
        /// so we should be careful when removing nodes from it
        /// Note: we rely on unordered_map iterators staying valid even after rehash. The standard
        ///       doesn't guarantee it, but apparently the libc++ we're using has this property.
        mutable std::unordered_map<
            std::string,
            UncommittedNode,
            StringHashForHeterogeneousLookup,
            StringHashForHeterogeneousLookup::transparent_key_equal>
            nodes;

        using NodesIterator = decltype(nodes)::iterator;
        struct NodesIteratorHash
        {
            auto operator()(NodesIterator it) const
            {
                return std::hash<std::string_view>{}(it->first);
            }
        };

        Ephemerals ephemerals;

        /// for each session, store list of uncommitted auths with their ZXID
        /// TODO: If there are lots of uncommitted Close requests, this grows big, and request
        ///       preprocessing becomes super slow (tens of seconds) because cleanup iterates over
        ///       this whole map. Maybe do something about it.
        std::unordered_map<int64_t, std::list<std::pair<int64_t, std::shared_ptr<AuthID>>>> session_and_auth;

        /// Mapping of uncommitted transaction to all it's modified nodes for a faster cleanup.
        /// zxid_to_nodes[0] contains nodes that were duplicated from committed container to
        /// UncommittedState::nodes, but weren't necessarily updated.
        using ZxidToNodes = std::map<int64_t, std::unordered_set<NodesIterator, NodesIteratorHash>>;
        mutable ZxidToNodes zxid_to_nodes;

        mutable std::mutex deltas_mutex;
        std::list<Delta> deltas TSA_GUARDED_BY(deltas_mutex);
        KeeperStorage & storage;
    };

    struct UncommittedNodeRef
    {
        std::optional<typename UncommittedState::NodesIterator> it{};

        const Node * get() const { return it ? (*it)->second.node.get() : nullptr; }

        /// Mutable access to the node. Should only be used from `prepare*` functions, together
        /// with producing deltas matching the changes made to the Node.
        Node * getMut() const
        {
            chassert(it.has_value());
            chassert((*it)->second.node != nullptr);
            return (*it)->second.node.get();
        }
    };

    struct NodeStreamForSnapshot : public KeeperNodeStreamForSnapshot
    {
        size_t next_node_idx = 0;
        KeeperStorage::Container::const_iterator it;

        bool next(std::string_view & out_path, std::string_view & out_data, KeeperNodeStats & out_stats) override;
    };

    UncommittedState uncommitted_state{*this};

    /// 0 if no uncommitted requests.
    uint64_t getLastUncommittedLogIdx() const;

    Coordination::Error commit(DeltaRange deltas);

    // Create node in the storage
    // Returns false if it failed to create the node, true otherwise
    // We don't care about the exact failure because we should've caught it during preprocessing
    bool
    createNode(const std::string & path, String data, const Coordination::Stat & stat, ACLId acl_id, bool update_digest);

    // Remove node in the storage
    // Returns false if it failed to remove the node, true otherwise
    // We don't care about the exact failure because we should've caught it during preprocessing
    bool removeNode(const std::string & path, int32_t version, bool update_digest);

    /// Used internally by `preprocess` and `processLocal` implementations.
    /// They usually have acl_id readily available, so we don't have to look up the node here.
    bool checkACL(ACLId acl_id, int32_t permissions, int64_t session_id, bool committed) const;

    /// Used externally. Locks storage mutex and looks up the node.
    bool checkCommittedACL(std::string_view path, int32_t permissions, int64_t session_id);

    KeeperStorage(int64_t tick_time_ms, const String & superdigest_, const KeeperContextPtr & keeper_context_, bool initialize_system_nodes = true);
    ~KeeperStorage();

    void initializeSystemNodes() TSA_NO_THREAD_SAFETY_ANALYSIS;

    /// Must be called on an empty storage, created with initialize_system_nodes = false.
    /// Caller has already read everything before the nodes, this method starts from createStreams.
    /// TODO: When we have chunked snapshots, probably pass a ThreadPool here.
    void loadNodesFromSnapshot(KeeperSnapshotReader & reader) TSA_NO_THREAD_SAFETY_ANALYSIS;

    /// Caller must hold storage mutex.
    /// At most one stream can exist at any given time.
    /// Stream must be destroyed using finishWritingSnapshot (with storage mutex held), otherwise
    /// destructor fails assert.
    std::unique_ptr<KeeperNodeStreamForSnapshot> beginWritingSnapshot();
    void finishWritingSnapshot(std::unique_ptr<KeeperNodeStreamForSnapshot> stream);

    /// Process user request and return response.
    /// check_acl = false only when converting data from ZooKeeper.
    KeeperResponsesForSessions processRequest(
        const Coordination::ZooKeeperRequestPtr & request,
        int64_t session_id,
        std::optional<int64_t> new_last_zxid);

    /// Process a batch of local read requests (no deltas, no commit).
    KeeperResponsesForSessions processLocalRequests(
        const KeeperRequestsForSessions & requests,
        bool check_acl = true);
    KeeperDigest preprocessRequest(
        const Coordination::ZooKeeperRequestPtr & request,
        int64_t session_id,
        int64_t time,
        int64_t new_last_zxid,
        bool check_acl = true,
        std::optional<KeeperDigest> digest = std::nullopt,
        int64_t log_idx = 0);
    void rollbackRequest(int64_t rollback_zxid, bool allow_missing);

    KeeperResponsesForSessions setWatches(
        int64_t last_zxid,
        const std::vector<String> & watches_paths,
        const std::vector<String> & list_watches_paths,
        const std::vector<String> & exist_watches_paths,
        const std::vector<String> & persistent_watches_paths,
        const std::vector<String> & persistent_recursive_watches_paths,
        int64_t session_id);

    /// Introspection functions mostly used in 4-letter commands
    uint64_t getNodesCount() const;

    uint64_t getApproximateDataSize() const;

    uint64_t getArenaDataSize() const;

    void updateStats();

    /// Register watches from a request/response pair.
    void updateWatches(
        const Coordination::ZooKeeperRequestPtr & zk_request,
        const Coordination::Response * response,
        int64_t session_id);

    void recalculateStats();

private:
    void removeDigest(const Node & node, std::string_view path);
    void addDigest(const Node & node, std::string_view path);

public:
    /// Functions that mutate UncommittedState, add corresponding deltas to staging_deltas, and
    /// update staging_digest.
    /// These are public because they're called from the free `preprocess` request handlers.
    void prepareUpdateNodeStat(std::string_view path, UncommittedNodeRef node, const KeeperNodeStats & new_stats);
    void prepareUpdateNodeData(std::string_view path, UncommittedNodeRef node, const KeeperNodeStats & new_stats, std::string_view new_data);
    void prepareCreateNode(
        std::string_view parent_path, UncommittedNodeRef parent,
        const KeeperNodeStats & new_parent_stats,
        std::string_view path, UncommittedNodeRef node, const Coordination::Stat & stat,
        ACLId acl_id, std::string_view data);
    void prepareRemoveNode(
        std::string_view parent_path, UncommittedNodeRef parent,
        const KeeperNodeStats & new_parent_stats,
        std::string_view path, UncommittedNodeRef node);
    /// (`nodes_to_remove` must be a node + the set of all its descendants, not arbitrary set of
    ///  nodes. Because we don't update children stats on parents of removed nodes, expecting those
    ///  parents to also be in the set to be removed, except for the outermost `parent`.)
    void prepareRemoveRecursive(
        std::string_view parent_path, UncommittedNodeRef parent,
        const KeeperNodeStats & new_parent_stats,
        std::deque<SubtreeNodeToRemove> nodes_to_remove);
    void prepareRemoveEphemeralNodes(const std::unordered_set<std::string> & paths, int64_t session_id);
    void prepareAddAuth(std::shared_ptr<KeeperStorage::AuthID> new_auth, int64_t session_id);

    /// Helpers used by other `prepare*` implementations.
    void prepareWriteCommon(std::string_view path, UncommittedNodeRef node);
    void prepareRemoveNodeWithoutUpdatingParent(std::string_view path, UncommittedNodeRef node);
};

}
