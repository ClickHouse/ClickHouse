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

using ResponseCallback = std::function<void(const Coordination::ZooKeeperResponsePtr &)>;
struct NodeStats
{
    int64_t czxid{0};
    int64_t mzxid{0};
    int64_t pzxid{0};

    int64_t mtime{0};

    int32_t version{0};
    int32_t cversion{0};
    int32_t aversion{0};

    uint32_t data_size{0};

    void copyStats(const Coordination::Stat & stat);

    bool isContainer() const
    {
        return is_ephemeral_and_ctime.is_container;
    }

    void setContainer()
    {
        is_ephemeral_and_ctime.is_container = true;
    }

    bool isEphemeral() const
    {
        return is_ephemeral_and_ctime.is_ephemeral;
    }

    int64_t ephemeralOwner() const
    {
        if (isEphemeral())
            return ephemeral_or_seq_num_or_ttl.ephemeral_owner;

        /// ZooKeeper-compatible sentinel for container nodes (matches CONTAINER_EPHEMERAL_OWNER in ZK)
        if (isContainer())
            return std::numeric_limits<int64_t>::min();

        return 0;
    }

    void setEphemeralOwner(int64_t ephemeral_owner)
    {
        is_ephemeral_and_ctime.is_ephemeral = true;
        ephemeral_or_seq_num_or_ttl.ephemeral_owner = ephemeral_owner;
    }

    int64_t seqNum() const
    {
        if (isEphemeral() || isTTL())
            return 0;

        return ephemeral_or_seq_num_or_ttl.seq_num;
    }

    void setSeqNum(int64_t seq_num)
    {
        ephemeral_or_seq_num_or_ttl.seq_num = seq_num;
    }

    void increaseSeqNum()
    {
        chassert(!isEphemeral() && !isTTL());
        ++ephemeral_or_seq_num_or_ttl.seq_num;
    }

    bool isTTL() const
    {
        return is_ephemeral_and_ctime.is_ttl;
    }

    int64_t ttl() const
    {
        chassert(isTTL());
        return ephemeral_or_seq_num_or_ttl.ttl;
    }

    void setTTL(int64_t ttl_)
    {
        is_ephemeral_and_ctime.is_ttl = true;
        ephemeral_or_seq_num_or_ttl.ttl = ttl_;
    }

    int64_t destroyTime() const
    {
        chassert(isTTL());
        return mtime + ttl();
    }

    int64_t ctime() const
    {
        return is_ephemeral_and_ctime.ctime;
    }

    void setCtime(uint64_t ctime)
    {
        is_ephemeral_and_ctime.ctime = ctime;
    }

private:
    /// as ctime can't be negative because it stores the timestamp when the
    /// node was created, we can use the high bits for flags
    struct
    {
        int64_t ctime : 61;
        int64_t is_container : 1;
        int64_t is_ephemeral : 1;
        int64_t is_ttl : 1;
    } is_ephemeral_and_ctime{0, false, false, false};

    /// ephemeral nodes cannot have children, so a node either stores
    /// ephemeral_owner (the owning session) OR seq_num (the counter
    /// for generating sequential children names under this node).
    /// TTL nodes cannot have children either (in this implementation), so for
    /// them this slot stores the ttl interval instead of seq_num. The active
    /// member is selected by is_ephemeral / is_ttl.
    union
    {
        int64_t ephemeral_owner;
        int64_t seq_num;
        int64_t ttl;
    } ephemeral_or_seq_num_or_ttl{0};
};

/// KeeperMemNode should have as minimal size as possible to reduce memory footprint
/// of stored nodes
/// New fields should be added to the struct only if it's really necessary
struct KeeperMemNode
{
    NodeStats stats;
    std::unique_ptr<char[]> data{nullptr};
    mutable uint64_t cached_digest = 0;

    ACLId acl_id = 0; /// 0 -- no ACL by default
    int32_t num_children = 0;

    int32_t numChildren() const
    {
        if (stats.isEphemeral())
            return 0;
        return num_children;
    }

    void setNumChildren(int32_t value) { num_children = value; }
    void increaseNumChildren() { ++num_children; }
    void decreaseNumChildren() { --num_children; }

    KeeperMemNode() = default;

    KeeperMemNode & operator=(const KeeperMemNode & other);
    KeeperMemNode(const KeeperMemNode & other);

    KeeperMemNode & operator=(KeeperMemNode && other) noexcept;
    KeeperMemNode(KeeperMemNode && other) noexcept;

    bool empty() const;

    void copyStats(const Coordination::Stat & stat);

    void setResponseStat(Coordination::Stat & response_stat) const;

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

    /// Paths of nodes that may carry TTL
    mutable std::unordered_set<
        String,
        StringHashForHeterogeneousLookup,
        StringHashForHeterogeneousLookup::transparent_key_equal>
        ttl_paths TSA_GUARDED_BY(storage_mutex);

    std::atomic<UInt64> committed_ttl_nodes{0};

    /// Paths of all container nodes (auto-deleted when last child is removed)
    mutable std::unordered_set<
        String,
        StringHashForHeterogeneousLookup,
        StringHashForHeterogeneousLookup::transparent_key_equal>
        container_paths TSA_GUARDED_BY(storage_mutex);

    std::atomic<UInt64> committed_container_nodes{0};

    struct UncommittedNodeRef;

    struct UncommittedState
    {
        explicit UncommittedState(KeeperStorage & storage_);

        ~UncommittedState();

        void addDeltas(std::list<Delta> new_deltas);
        void cleanup(int64_t commit_zxid);
        void rollback(int64_t rollback_zxid);
        void rollback(std::list<Delta> rollback_deltas);

        UncommittedNodeRef getNode(std::string_view path, bool should_lock_storage = true) const;

        void rollbackDelta(const Delta & delta);

        /// Update digest with new nodes
        UInt64 updateNodesDigest(UInt64 current_digest, UInt64 zxid) const;

        bool hasACL(int64_t session_id, bool committed, std::function<bool(const AuthID &)> predicate) const;

        void forEachAuthInSession(int64_t session_id, std::function<void(const AuthID &)> func) const;

        std::shared_ptr<Node> tryGetNodeFromStorage(std::string_view path, bool should_lock_storage = true) const;

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

    UncommittedState uncommitted_state{*this};

    /// 0 if no uncommitted requests.
    uint64_t getLastUncommittedLogIdx() const;

    Coordination::Error commit(DeltaRange deltas);

    // Create node in the storage
    // Returns false if it failed to create the node, true otherwise
    // We don't care about the exact failure because we should've caught it during preprocessing
    // createNode and removeNode mutate ttl_paths (guarded by storage_mutex). They are part of the
    // commit path, which is reached through the dual-mode process/processImpl templates: the same
    // instantiation serves local reads under a shared lock and writes under an exclusive lock, so a
    // single TSA_REQUIRES/TSA_REQUIRES_SHARED contract cannot describe it. That is why the whole
    // storage_mutex-protected core (including all container access) is left unanalyzed; we follow the
    // same convention here. The lock is always held exclusively when these run (see commit callers).
    bool
    createNode(
        const std::string & path,
        String data,
        const Coordination::Stat & stat,
        ACLId acl_id,
        bool update_digest,
        std::optional<int64_t> ttl,
        bool is_container = false) TSA_NO_THREAD_SAFETY_ANALYSIS;

    // Remove node in the storage
    // Returns false if it failed to remove the node, true otherwise
    // We don't care about the exact failure because we should've caught it during preprocessing
    bool removeNode(const std::string & path, int32_t version, bool update_digest) TSA_NO_THREAD_SAFETY_ANALYSIS;

    /// Used internally by `preprocess` and `processLocal` implementations.
    /// They usually have acl_id readily available, so we don't have to look up the node here.
    bool checkACL(ACLId acl_id, int32_t permissions, int64_t session_id, bool committed) const;

    /// Used externally. Locks storage mutex and looks up the node.
    bool checkCommittedACL(std::string_view path, int32_t permissions, int64_t session_id);

    KeeperStorage(int64_t tick_time_ms, const String & superdigest_, const KeeperContextPtr & keeper_context_, bool initialize_system_nodes = true);
    ~KeeperStorage();

    void initializeSystemNodes() TSA_NO_THREAD_SAFETY_ANALYSIS;

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

    /// Set of methods for creating snapshots

    /// Turn on snapshot mode, so data inside Container is not deleted, but replaced with new version.
    void enableSnapshotMode(size_t up_to_version);

    /// Turn off snapshot mode.
    void disableSnapshotMode();

    Container::const_iterator getSnapshotIteratorBegin() const;

    /// Clear outdated data from internal container.
    void clearGarbageAfterSnapshot();

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

    std::vector<std::pair<std::string, Int32>> collectExpiredTTLPaths(int64_t now_ms, size_t batch_size) const;

    /// Container nodes eligible for GC: childless after having had children, or never
    /// populated and empty past max_never_used_interval_ms.
    std::vector<std::pair<std::string, Int32>> collectContainerCandidates(size_t batch_size, UInt64 max_never_used_interval_ms) const;

    /// Used by tests.
    bool containsTTLPath(const std::string & path) const;

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
    void prepareUpdateNodeStat(std::string_view path, UncommittedNodeRef node, const NodeStats & new_stats, int32_t new_num_children);
    void prepareUpdateNodeData(std::string_view path, UncommittedNodeRef node, const NodeStats & new_stats, std::string_view new_data);
    void prepareUpdateNodeACL(std::string_view path, UncommittedNodeRef node, const NodeStats & new_stats, ACLId new_acl_id);
    void prepareCreateNode(
        std::string_view parent_path, UncommittedNodeRef parent,
        const NodeStats & new_parent_stats, int32_t new_parent_num_children,
        std::string_view path, UncommittedNodeRef node, const Coordination::Stat & stat,
        ACLId acl_id, std::string_view data, std::optional<int64_t> ttl = std::nullopt, bool is_container = false);
    void prepareRemoveNode(
        std::string_view parent_path, UncommittedNodeRef parent,
        const NodeStats & new_parent_stats, int32_t new_parent_num_children,
        std::string_view path, UncommittedNodeRef node);
    /// (`nodes_to_remove` must be a node + the set of all its descendants, not arbitrary set of
    ///  nodes. Because we don't update children stats on parents of removed nodes, expecting those
    ///  parents to also be in the set to be removed, except for the outermost `parent`.)
    void prepareRemoveRecursive(
        std::string_view parent_path, UncommittedNodeRef parent,
        const NodeStats & new_parent_stats, int32_t new_parent_num_children,
        std::deque<SubtreeNodeToRemove> nodes_to_remove);
    void prepareRemoveEphemeralNodes(const std::unordered_set<std::string> & paths, int64_t session_id);
    void prepareAddAuth(std::shared_ptr<KeeperStorage::AuthID> new_auth, int64_t session_id);

    /// Helpers used by other `prepare*` implementations.
    void prepareWriteCommon(std::string_view path, UncommittedNodeRef node);
    void prepareRemoveNodeWithoutUpdatingParent(std::string_view path, UncommittedNodeRef node);
};

}
