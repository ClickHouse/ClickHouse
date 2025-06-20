#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <Coordination/ACLMap.h>
#include <Coordination/SessionExpiryQueue.h>
#include <Coordination/SnapshotableHashTable.h>
#include <Coordination/KeeperCommon.h>
#include <Common/StringHashForHeterogeneousLookup.h>
#include <Common/SharedMutex.h>
#include <Common/Concepts.h>

#include <base/defines.h>

#include <absl/container/flat_hash_set.h>

#include "config.h"
#if USE_ROCKSDB
#include <Coordination/RocksDBContainer.h>
#endif

namespace DB
{

class KeeperContext;
using KeeperContextPtr = std::shared_ptr<KeeperContext>;

using ResponseCallback = std::function<void(const Coordination::ZooKeeperResponsePtr &)>;
using ChildrenSet = absl::flat_hash_set<StringRef, StringRefHash>;

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

    bool isEphemeral() const
    {
        return is_ephemeral_and_ctime.is_ephemeral;
    }

    int64_t ephemeralOwner() const
    {
        if (isEphemeral())
            return ephemeral_or_children_data.ephemeral_owner;

        return 0;
    }

    void setEphemeralOwner(int64_t ephemeral_owner)
    {
        is_ephemeral_and_ctime.is_ephemeral = true;
        ephemeral_or_children_data.ephemeral_owner = ephemeral_owner;
    }

    int32_t numChildren() const
    {
        if (isEphemeral())
            return 0;

        return ephemeral_or_children_data.children_info.num_children;
    }

    void setNumChildren(int32_t num_children)
    {
        is_ephemeral_and_ctime.is_ephemeral = false;
        ephemeral_or_children_data.children_info.num_children = num_children;
    }

    void increaseNumChildren()
    {
        chassert(!isEphemeral());
        ++ephemeral_or_children_data.children_info.num_children;
    }

    void decreaseNumChildren()
    {
        chassert(!isEphemeral());
        --ephemeral_or_children_data.children_info.num_children;
    }

    int32_t seqNum() const
    {
        if (isEphemeral())
            return 0;

        return ephemeral_or_children_data.children_info.seq_num;
    }

    void setSeqNum(int32_t seq_num)
    {
        ephemeral_or_children_data.children_info.seq_num = seq_num;
    }

    void increaseSeqNum()
    {
        chassert(!isEphemeral());
        ++ephemeral_or_children_data.children_info.seq_num;
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
    /// node was created, we can use the MSB for a bool
    struct
    {
        int64_t is_ephemeral : 1;
        int64_t ctime : 63;
    } is_ephemeral_and_ctime{false, 0};

    /// ephemeral notes cannot have children so a node can set either
    /// ephemeral_owner OR seq_num + num_children
    union
    {
        int64_t ephemeral_owner;
        struct
        {
            int32_t seq_num;
            int32_t num_children;
        } children_info;
    } ephemeral_or_children_data{0};
};

/// KeeperRocksNodeInfo is used in RocksDB keeper.
/// It is serialized directly as POD to RocksDB.
struct KeeperRocksNodeInfo
{
    NodeStats stats;
    uint64_t acl_id = 0; /// 0 -- no ACL by default

    /// dummy interface for test
    void addChild(StringRef) {}
    auto getChildren() const
    {
        return std::vector<int>(stats.numChildren());
    }

    void copyStats(const Coordination::Stat & stat);
};

/// KeeperRocksNode is the memory structure used by RocksDB
struct KeeperRocksNode : public KeeperRocksNodeInfo
{
#if USE_ROCKSDB
    friend struct RocksDBContainer<KeeperRocksNode>;
#endif
    using Meta = KeeperRocksNodeInfo;

    uint64_t size_bytes = 0; // only for compatible, should be deprecated

    uint64_t sizeInBytes() const { return stats.data_size + sizeof(KeeperRocksNodeInfo); }

    void setData(String new_data)
    {
        stats.data_size = static_cast<uint32_t>(new_data.size());
        if (stats.data_size != 0)
        {
            data = std::unique_ptr<char[]>(new char[new_data.size()]);
            memcpy(data.get(), new_data.data(), stats.data_size);
        }
    }

    void shallowCopy(const KeeperRocksNode & other)
    {
        stats = other.stats;
        acl_id = other.acl_id;
        if (stats.data_size != 0)
        {
            data = std::unique_ptr<char[]>(new char[stats.data_size]);
            memcpy(data.get(), other.data.get(), stats.data_size);
        }

        /// cached_digest = other.cached_digest;
    }
    void invalidateDigestCache() const;
    UInt64 getDigest(std::string_view path) const;
    String getEncodedString();
    void decodeFromString(const String & buffer_str);
    void recalculateSize() {}
    std::string_view getData() const noexcept { return {data.get(), stats.data_size}; }

    void setResponseStat(Coordination::Stat & response_stat) const;

    void reset()
    {
        serialized = false;
    }
    bool empty() const
    {
        return stats.data_size == 0 && stats.mzxid == 0;
    }
    std::unique_ptr<char[]> data{nullptr};
    mutable UInt64 cached_digest = 0; /// we cached digest for this node.
private:
    bool serialized = false;
};

/// KeeperMemNode should have as minimal size as possible to reduce memory footprint
/// of stored nodes
/// New fields should be added to the struct only if it's really necessary
struct KeeperMemNode
{
    NodeStats stats;
    std::unique_ptr<char[]> data{nullptr};
    mutable uint64_t cached_digest = 0;

    uint64_t acl_id = 0; /// 0 -- no ACL by default

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

    void addChild(StringRef child_path);

    void removeChild(StringRef child_path);

    const auto & getChildren() const noexcept { return children; }
    auto & getChildren() { return children; }

    // Invalidate the calculated digest so it's recalculated again on the next
    // getDigest call
    void invalidateDigestCache() const;

    // get the calculated digest of the node
    UInt64 getDigest(std::string_view path) const;

    // copy only necessary information for preprocessing and digest calculation
    // (e.g. we don't need to copy list of children)
    void shallowCopy(const KeeperMemNode & other);
private:
    ChildrenSet children{};
};

struct KeeperStorageStats
{
    std::atomic<uint64_t> nodes_count = 0;
    std::atomic<uint64_t> approximate_data_size = 0;
    std::atomic<uint64_t> total_watches_count = 0;
    std::atomic<uint64_t> watched_paths_count = 0;
    std::atomic<uint64_t> sessions_with_watches_count = 0;
    std::atomic<uint64_t> session_with_ephemeral_nodes_count = 0;
    std::atomic<uint64_t> total_emphemeral_nodes_count = 0;
    std::atomic<int64_t> last_zxid = 0;
};

class KeeperStorageBase
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
    struct WatchInfo
    {
        std::string_view path;
        bool is_list_watch;

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

    using DeltaIterator = std::list<KeeperStorageBase::Delta>::const_iterator;
    struct DeltaRange
    {
        DeltaIterator begin_it;
        DeltaIterator end_it;

        DeltaIterator begin() const;
        DeltaIterator end() const;
        bool empty() const;
        const KeeperStorageBase::Delta & front() const;
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
protected:
    KeeperStorageBase(int64_t tick_time_ms, const KeeperContextPtr & keeper_context, const String & superdigest_);

    /// Expiration queue for session, allows to get dead sessions at some point of time
    SessionExpiryQueue session_expiry_queue;

    struct TransactionInfo
    {
        int64_t zxid;
        KeeperDigest nodes_digest;
        /// index in storage of the log containing the transaction
        int64_t log_idx = 0;
    };

    std::list<TransactionInfo> uncommitted_transactions TSA_GUARDED_BY(transaction_mutex);

    std::atomic<bool> finalized{false};

    /// Mapping session_id -> set of watched nodes paths
    SessionAndWatcher sessions_and_watchers;
    size_t total_watches_count = 0;

    void clearDeadWatches(int64_t session_id);
    int64_t getNextZXIDLocked() const TSA_REQUIRES(transaction_mutex);

    std::atomic<bool> initialized{false};
};

/// Keeper state machine almost equal to the ZooKeeper's state machine.
/// Implements all logic of operations, data changes, sessions allocation.
/// In-memory and not thread safe.
template<typename TContainer>
class KeeperStorage : public KeeperStorageBase
{
public:
    using Container = TContainer;
    using Node = Container::Node;

#if !defined(ADDRESS_SANITIZER) && !defined(MEMORY_SANITIZER)
    static_assert(
        sizeof(ListNode<Node>) <= 144,
        "std::list node containing ListNode<Node> is > 160 bytes (sizeof(ListNode<Node>) + 16 bytes for pointers) which will increase "
        "memory consumption");
#endif


#if USE_ROCKSDB
    static constexpr bool use_rocksdb = std::is_same_v<Container, RocksDBContainer<KeeperRocksNode>>;
#else
    static constexpr bool use_rocksdb = false;
#endif

    /// Main hashtable with nodes. Contain all information about data.
    /// All other structures expect session_and_timeout can be restored from
    /// container.
    Container container;

    struct UncommittedState
    {
        explicit UncommittedState(KeeperStorage & storage_) : storage(storage_) { }

        ~UncommittedState();

        void addDeltas(std::list<Delta> new_deltas);
        void cleanup(int64_t commit_zxid);
        void rollback(int64_t rollback_zxid);
        void rollback(std::list<Delta> rollback_deltas);

        std::shared_ptr<Node> getNode(StringRef path, bool should_lock_storage = true) const;
        const Node * getActualNodeView(StringRef path, const Node & storage_node) const;

        Coordination::ACLs getACLs(StringRef path) const;

        void applyDeltas(const std::list<Delta> & new_deltas, uint64_t * digest);
        void applyDelta(const Delta & delta, uint64_t * digest);
        void rollbackDelta(const Delta & delta);

        /// Update digest with new nodes
        UInt64 updateNodesDigest(UInt64 current_digest, UInt64 zxid) const;

        bool hasACL(int64_t session_id, bool is_local, std::function<bool(const AuthID &)> predicate) const;

        void forEachAuthInSession(int64_t session_id, std::function<void(const AuthID &)> func) const;

        std::shared_ptr<Node> tryGetNodeFromStorage(StringRef path, bool should_lock_storage = true) const;

        std::unordered_map<int64_t, std::unordered_set<int64_t>> closed_sessions_to_zxids;

        struct UncommittedNode
        {
            std::shared_ptr<Node> node{nullptr};
            std::optional<Coordination::ACLs> acls{};
            std::unordered_set<uint64_t> applied_zxids{};

            void materializeACL(const ACLMap & current_acl_map);
        };

        /// zxid_to_nodes stores iterators of nodes map
        /// so we should be careful when removing nodes from it
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
        std::unordered_map<int64_t, std::list<std::pair<int64_t, std::shared_ptr<AuthID>>>> session_and_auth;

        /// mapping of uncommitted transaction to all it's modified nodes for a faster cleanup
        using ZxidToNodes = std::map<int64_t, std::unordered_set<NodesIterator, NodesIteratorHash>>;
        mutable ZxidToNodes zxid_to_nodes;

        mutable std::mutex deltas_mutex;
        std::list<Delta> deltas TSA_GUARDED_BY(deltas_mutex);
        KeeperStorage<Container> & storage;
    };

    UncommittedState uncommitted_state{*this};

    // Apply uncommitted state to another storage using only transactions
    // with zxid > last_zxid
    void applyUncommittedState(KeeperStorage & other, int64_t last_log_idx);

    Coordination::Error commit(DeltaRange deltas);

    // Create node in the storage
    // Returns false if it failed to create the node, true otherwise
    // We don't care about the exact failure because we should've caught it during preprocessing
    bool
    createNode(const std::string & path, String data, const Coordination::Stat & stat, Coordination::ACLs node_acls, bool update_digest);

    // Remove node in the storage
    // Returns false if it failed to remove the node, true otherwise
    // We don't care about the exact failure because we should've caught it during preprocessing
    bool removeNode(const std::string & path, int32_t version, bool update_digest);

    bool checkACL(StringRef path, int32_t permissions, int64_t session_id, bool is_local);

    KeeperStorage(int64_t tick_time_ms, const String & superdigest_, const KeeperContextPtr & keeper_context_, bool initialize_system_nodes = true);

    void initializeSystemNodes() TSA_NO_THREAD_SAFETY_ANALYSIS;

    UInt64 calculateNodesDigest(UInt64 current_digest, const std::list<Delta> & new_deltas) const;

    /// Process user request and return response.
    /// check_acl = false only when converting data from ZooKeeper.
    KeeperResponsesForSessions processRequest(
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

    /// Introspection functions mostly used in 4-letter commands
    uint64_t getNodesCount() const;

    uint64_t getApproximateDataSize() const;

    uint64_t getArenaDataSize() const;

    void updateStats();

    void recalculateStats();
private:
    void removeDigest(const Node & node, std::string_view path);
    void addDigest(const Node & node, std::string_view path);
};

}
