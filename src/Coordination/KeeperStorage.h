#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <Coordination/ACLMap.h>
#include <Coordination/SessionExpiryQueue.h>
#include <Coordination/SnapshotableHashTable.h>
#include "Common/StringHashForHeterogeneousLookup.h"
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
using SessionAndTimeout = std::unordered_map<int64_t, int64_t>;

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
        is_ephemeral_and_ctime.is_ephemeral = ephemeral_owner != 0;
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
        bool is_ephemeral : 1;
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

class KeeperStorageBase
{
public:

    enum DigestVersion : uint8_t
    {
        NO_DIGEST = 0,
        V1 = 1,
        V2 = 2, // added system nodes that modify the digest on startup so digest from V0 is invalid
        V3 = 3, // fixed bug with casting, removed duplicate czxid usage
        V4 = 4  // 0 is not a valid digest value
    };

    struct Digest
    {
        DigestVersion version{DigestVersion::NO_DIGEST};
        uint64_t value{0};
    };

    struct ResponseForSession
    {
        int64_t session_id;
        Coordination::ZooKeeperResponsePtr response;
        Coordination::ZooKeeperRequestPtr request = nullptr;
    };
    using ResponsesForSessions = std::vector<ResponseForSession>;

    struct RequestForSession
    {
        int64_t session_id;
        int64_t time{0};
        Coordination::ZooKeeperRequestPtr request;
        int64_t zxid{0};
        std::optional<Digest> digest;
        int64_t log_idx{0};
        bool use_xid_64{false};
    };
    using RequestsForSessions = std::vector<RequestForSession>;

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
        auto operator()(WatchInfo info) const
        {
            SipHash hash;
            hash.update(info.path);
            hash.update(info.is_list_watch);
            return hash.get64();
        }
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
        Coordination::ACLs acls;
        String data;
    };

    struct RemoveNodeDelta
    {
        int32_t version{-1};
        NodeStats stat;
        Coordination::ACLs acls;
        String data;
    };

    struct UpdateNodeStatDelta
    {
        template <is_any_of<KeeperMemNode, KeeperRocksNode> Node>
        explicit UpdateNodeStatDelta(const Node & node)
            : old_stats(node.stats)
            , new_stats(node.stats)
        {}

        NodeStats old_stats;
        NodeStats new_stats;
        int32_t version{-1};
    };

    struct UpdateNodeDataDelta
    {

        std::string old_data;
        std::string new_data;
        int32_t version{-1};
    };

    struct SetACLDelta
    {
        Coordination::ACLs old_acls;
        Coordination::ACLs new_acls;
        int32_t version{-1};
    };

    struct ErrorDelta
    {
        Coordination::Error error;
    };

    struct FailedMultiDelta
    {
        std::vector<Coordination::Error> error_codes;
        Coordination::Error global_error{Coordination::Error::ZOK};
    };

    // Denotes end of a subrequest in multi request
    struct SubDeltaEnd
    {
    };

    struct AddAuthDelta
    {
        int64_t session_id;
        std::shared_ptr<AuthID> auth_id;
    };

    struct CloseSessionDelta
    {
        int64_t session_id;
    };

    using Operation = std::variant<
        CreateNodeDelta,
        RemoveNodeDelta,
        UpdateNodeStatDelta,
        UpdateNodeDataDelta,
        SetACLDelta,
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

    using DeltaIterator = std::list<KeeperStorageBase::Delta>::const_iterator;
    struct DeltaRange
    {
        DeltaIterator begin_it;
        DeltaIterator end_it;

        auto begin() const
        {
            return begin_it;
        }

        auto end() const
        {
            return end_it;
        }

        bool empty() const
        {
            return begin_it == end_it;
        }

        const auto & front() const
        {
            return *begin_it;
        }
    };

    struct Stats
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

    Stats stats;

    static bool checkDigest(const Digest & first, const Digest & second);
};


/// Keeper state machine almost equal to the ZooKeeper's state machine.
/// Implements all logic of operations, data changes, sessions allocation.
/// In-memory and not thread safe.
template<typename Container_>
class KeeperStorage : public KeeperStorageBase
{
public:
    using Container = Container_;
    using Node = Container::Node;

#if !defined(ADDRESS_SANITIZER) && !defined(MEMORY_SANITIZER)
    static_assert(
        sizeof(ListNode<Node>) <= 144,
        "std::list node containing ListNode<Node> is > 160 bytes (sizeof(ListNode<Node>) + 16 bytes for pointers) which will increase "
        "memory consumption");
#endif


#if USE_ROCKSDB
    static constexpr bool use_rocksdb = std::is_same_v<Container_, RocksDBContainer<KeeperRocksNode>>;
#else
    static constexpr bool use_rocksdb = false;
#endif

    static constexpr auto CURRENT_DIGEST_VERSION = DigestVersion::V4;

    static String generateDigest(const String & userdata);

    int64_t session_id_counter{1};

    mutable SharedMutex auth_mutex;
    SessionAndAuth committed_session_and_auth;

    mutable SharedMutex storage_mutex;
    /// Main hashtable with nodes. Contain all information about data.
    /// All other structures expect session_and_timeout can be restored from
    /// container.
    Container container;

    struct UncommittedState
    {
        explicit UncommittedState(KeeperStorage & storage_) : storage(storage_) { }

        void addDeltas(std::list<Delta> new_deltas);
        void cleanup(int64_t commit_zxid);
        void rollback(int64_t rollback_zxid);
        void rollback(std::list<Delta> rollback_deltas);

        std::shared_ptr<Node> getNode(StringRef path, bool should_lock_storage = true) const;
        const Node * getActualNodeView(StringRef path, const Node & storage_node) const;

        Coordination::ACLs getACLs(StringRef path) const;

        void applyDeltas(const std::list<Delta> & new_deltas);
        void applyDelta(const Delta & delta);
        void rollbackDelta(const Delta & delta);

        bool hasACL(int64_t session_id, bool is_local, std::function<bool(const AuthID &)> predicate) const;

        void forEachAuthInSession(int64_t session_id, std::function<void(const AuthID &)> func) const;

        std::shared_ptr<Node> tryGetNodeFromStorage(StringRef path, bool should_lock_storage = true) const;

        std::unordered_set<int64_t> closed_sessions;

        using ZxidToNodes = std::map<int64_t, std::unordered_set<std::string_view>>;
        struct UncommittedNode
        {
            std::shared_ptr<Node> node{nullptr};
            std::optional<Coordination::ACLs> acls{};
            std::unordered_set<uint64_t> applied_zxids{};

            void materializeACL(const ACLMap & current_acl_map);
        };

        struct PathCmp
        {
            auto operator()(const std::string_view a,
                            const std::string_view b) const
            {
                size_t level_a = std::count(a.begin(), a.end(), '/');
                size_t level_b = std::count(b.begin(), b.end(), '/');
                return level_a < level_b || (level_a == level_b && a < b);
            }

            using is_transparent = void; // required to make find() work with different type than key_type
        };

        Ephemerals ephemerals;

        std::unordered_map<int64_t, std::list<std::pair<int64_t, std::shared_ptr<AuthID>>>> session_and_auth;

        mutable std::map<std::string, UncommittedNode, PathCmp> nodes;
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
    bool createNode(
        const std::string & path,
        String data,
        const Coordination::Stat & stat,
        Coordination::ACLs node_acls);

    // Remove node in the storage
    // Returns false if it failed to remove the node, true otherwise
    // We don't care about the exact failure because we should've caught it during preprocessing
    bool removeNode(const std::string & path, int32_t version);

    bool checkACL(StringRef path, int32_t permissions, int64_t session_id, bool is_local);

    std::mutex ephemeral_mutex;
    /// Mapping session_id -> set of ephemeral nodes paths
    Ephemerals committed_ephemerals;
    size_t committed_ephemeral_nodes{0};

    /// Expiration queue for session, allows to get dead sessions at some point of time
    SessionExpiryQueue session_expiry_queue;
    /// All active sessions with timeout
    SessionAndTimeout session_and_timeout;

    /// ACLMap for more compact ACLs storage inside nodes.
    ACLMap acl_map;

    mutable std::mutex transaction_mutex;

    /// Global id of all requests applied to storage
    int64_t zxid TSA_GUARDED_BY(transaction_mutex) = 0;

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
        /// index in storage of the log containing the transaction
        int64_t log_idx = 0;
    };

    std::list<TransactionInfo> uncommitted_transactions TSA_GUARDED_BY(transaction_mutex);

    uint64_t nodes_digest = 0;

    std::atomic<bool> finalized{false};


    /// Mapping session_id -> set of watched nodes paths
    SessionAndWatcher sessions_and_watchers;
    size_t total_watches_count = 0;

    /// Currently active watches (node_path -> subscribed sessions)
    Watches watches;
    Watches list_watches; /// Watches for 'list' request (watches on children).

    void clearDeadWatches(int64_t session_id);

    /// Get current committed zxid
    int64_t getZXID() const;

    int64_t getNextZXID() const;
    int64_t getNextZXIDLocked() const TSA_REQUIRES(transaction_mutex);

    Digest getNodesDigest(bool committed, bool lock_transaction_mutex) const;

    KeeperContextPtr keeper_context;

    const String superdigest;

    std::atomic<bool> initialized{false};

    KeeperStorage(int64_t tick_time_ms, const String & superdigest_, const KeeperContextPtr & keeper_context_, bool initialize_system_nodes = true);

    void initializeSystemNodes() TSA_NO_THREAD_SAFETY_ANALYSIS;

    /// Allocate new session id with the specified timeouts
    int64_t getSessionID(int64_t session_timeout_ms);

    /// Add session id. Used when restoring KeeperStorage from snapshot.
    void addSessionID(int64_t session_id, int64_t session_timeout_ms) TSA_NO_THREAD_SAFETY_ANALYSIS;

    UInt64 calculateNodesDigest(UInt64 current_digest, const std::list<Delta> & new_deltas) const;

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
        std::optional<Digest> digest = std::nullopt,
        int64_t log_idx = 0);
    void rollbackRequest(int64_t rollback_zxid, bool allow_missing);

    void finalize();

    bool isFinalized() const;

    /// Set of methods for creating snapshots

    /// Turn on snapshot mode, so data inside Container is not deleted, but replaced with new version.
    void enableSnapshotMode(size_t up_to_version);

    /// Turn off snapshot mode.
    void disableSnapshotMode();

    Container::const_iterator getSnapshotIteratorBegin() const;

    /// Clear outdated data from internal container.
    void clearGarbageAfterSnapshot();

    /// Get all active sessions
    SessionAndTimeout getActiveSessions() const;

    /// Get all dead sessions
    std::vector<int64_t> getDeadSessions() const;

    void updateStats();
    const Stats & getStorageStats() const;

    /// Introspection functions mostly used in 4-letter commands
    uint64_t getNodesCount() const;

    uint64_t getApproximateDataSize() const;

    uint64_t getArenaDataSize() const;

    uint64_t getTotalWatchesCount() const;

    uint64_t getWatchedPathsCount() const;

    uint64_t getSessionsWithWatchesCount() const;

    uint64_t getSessionWithEphemeralNodesCount() const;
    uint64_t getTotalEphemeralNodesCount() const;

    void dumpWatches(WriteBufferFromOwnString & buf) const;
    void dumpWatchesByPath(WriteBufferFromOwnString & buf) const;
    void dumpSessionsAndEphemerals(WriteBufferFromOwnString & buf) const;

    void recalculateStats();
private:
    void removeDigest(const Node & node, std::string_view path);
    void addDigest(const Node & node, std::string_view path);
};

using KeeperMemoryStorage = KeeperStorage<SnapshotableHashTable<KeeperMemNode>>;
#if USE_ROCKSDB
using KeeperRocksStorage = KeeperStorage<RocksDBContainer<KeeperRocksNode>>;
#endif

}
