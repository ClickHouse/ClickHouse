#pragma once

#include <unordered_map>
#include <vector>
#include <Coordination/ACLMap.h>
#include <Coordination/SessionExpiryQueue.h>
#include <Coordination/SnapshotableHashTable.h>

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

/// KeeperRocksNodeInfo is used in RocksDB keeper.
/// It is serialized directly as POD to RocksDB.
struct KeeperRocksNodeInfo
{
    int64_t czxid{0};
    int64_t mzxid{0};
    int64_t pzxid{0};
    uint64_t acl_id = 0; /// 0 -- no ACL by default

    int64_t mtime{0};

    int32_t version{0};
    int32_t cversion{0};
    int32_t aversion{0};

    int32_t seq_num = 0;
    mutable UInt64 digest = 0; /// we cached digest for this node.

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
        ephemeral_or_children_data.children_info.num_children = num_children;
    }

    /// dummy interface for test
    void addChild(StringRef) {}
    auto getChildren() const
    {
        return std::vector<int>(numChildren());
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

    void setSeqNum(int32_t seq_num_)
    {
        ephemeral_or_children_data.children_info.seq_num = seq_num_;
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

    uint64_t sizeInBytes() const { return data_size + sizeof(KeeperRocksNodeInfo); }
    void setData(String new_data)
    {
        data_size = static_cast<uint32_t>(new_data.size());
        if (data_size != 0)
        {
            data = std::unique_ptr<char[]>(new char[new_data.size()]);
            memcpy(data.get(), new_data.data(), data_size);
        }
    }

    void shallowCopy(const KeeperRocksNode & other)
    {
        czxid = other.czxid;
        mzxid = other.mzxid;
        pzxid = other.pzxid;
        acl_id = other.acl_id; /// 0 -- no ACL by default

        mtime = other.mtime;

        is_ephemeral_and_ctime = other.is_ephemeral_and_ctime;

        ephemeral_or_children_data = other.ephemeral_or_children_data;

        data_size = other.data_size;
        if (data_size != 0)
        {
            data = std::unique_ptr<char[]>(new char[data_size]);
            memcpy(data.get(), other.data.get(), data_size);
        }

        version = other.version;
        cversion = other.cversion;
        aversion = other.aversion;

        /// cached_digest = other.cached_digest;
    }
    void invalidateDigestCache() const;
    UInt64 getDigest(std::string_view path) const;
    String getEncodedString();
    void decodeFromString(const String & buffer_str);
    void recalculateSize() {}
    std::string_view getData() const noexcept { return {data.get(), data_size}; }

    void setResponseStat(Coordination::Stat & response_stat) const
    {
        response_stat.czxid = czxid;
        response_stat.mzxid = mzxid;
        response_stat.ctime = ctime();
        response_stat.mtime = mtime;
        response_stat.version = version;
        response_stat.cversion = cversion;
        response_stat.aversion = aversion;
        response_stat.ephemeralOwner = ephemeralOwner();
        response_stat.dataLength = static_cast<int32_t>(data_size);
        response_stat.numChildren = numChildren();
        response_stat.pzxid = pzxid;
    }

    void reset()
    {
        serialized = false;
    }
    bool empty() const
    {
        return data_size == 0 && mzxid == 0;
    }
    std::unique_ptr<char[]> data{nullptr};
    uint32_t data_size{0};
private:
    bool serialized = false;
};

/// KeeperMemNode should have as minimal size as possible to reduce memory footprint
/// of stored nodes
/// New fields should be added to the struct only if it's really necessary
struct KeeperMemNode
{
    int64_t czxid{0};
    int64_t mzxid{0};
    int64_t pzxid{0};
    uint64_t acl_id = 0; /// 0 -- no ACL by default

    int64_t mtime{0};

    std::unique_ptr<char[]> data{nullptr};
    uint32_t data_size{0};

    int32_t version{0};
    int32_t cversion{0};
    int32_t aversion{0};

    mutable uint64_t cached_digest = 0;

    KeeperMemNode() = default;

    KeeperMemNode & operator=(const KeeperMemNode & other);
    KeeperMemNode(const KeeperMemNode & other);

    KeeperMemNode & operator=(KeeperMemNode && other) noexcept;
    KeeperMemNode(KeeperMemNode && other) noexcept;

    bool empty() const;

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

    void copyStats(const Coordination::Stat & stat);

    void setResponseStat(Coordination::Stat & response_stat) const;

    /// Object memory size
    uint64_t sizeInBytes() const;

    void setData(const String & new_data);

    std::string_view getData() const noexcept { return {data.get(), data_size}; }

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
    };
    using RequestsForSessions = std::vector<RequestForSession>;

    struct AuthID
    {
        std::string scheme;
        std::string id;

        bool operator==(const AuthID & other) const { return scheme == other.scheme && id == other.id; }
    };

    using Ephemerals = std::unordered_map<int64_t, std::unordered_set<std::string>>;
    using SessionAndWatcher = std::unordered_map<int64_t, std::unordered_set<std::string>>;
    using SessionIDs = std::unordered_set<int64_t>;

    /// Just vector of SHA1 from user:password
    using AuthIDs = std::vector<AuthID>;
    using SessionAndAuth = std::unordered_map<int64_t, AuthIDs>;
    using Watches = std::unordered_map<String /* path, relative of root_path */, SessionIDs>;

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

    struct CloseSessionDelta
    {
        int64_t session_id;
    };

    using Operation = std::
        variant<CreateNodeDelta, RemoveNodeDelta, UpdateNodeDelta, SetACLDelta, AddAuthDelta, ErrorDelta, SubDeltaEnd, FailedMultiDelta, CloseSessionDelta>;

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

        void addDelta(Delta new_delta);
        void addDeltas(std::vector<Delta> new_deltas);
        void commit(int64_t commit_zxid);
        void rollback(int64_t rollback_zxid);

        std::shared_ptr<Node> getNode(StringRef path) const;
        Coordination::ACLs getACLs(StringRef path) const;

        void applyDelta(const Delta & delta);

        bool hasACL(int64_t session_id, bool is_local, std::function<bool(const AuthID &)> predicate) const;

        void forEachAuthInSession(int64_t session_id, std::function<void(const AuthID &)> func) const;

        std::shared_ptr<Node> tryGetNodeFromStorage(StringRef path) const;

        std::unordered_map<int64_t, std::list<const AuthID *>> session_and_auth;
        std::unordered_set<int64_t> closed_sessions;

        struct UncommittedNode
        {
            std::shared_ptr<Node> node{nullptr};
            Coordination::ACLs acls{};
            int64_t zxid{0};
        };

        struct Hash
        {
            auto operator()(const std::string_view view) const
            {
                SipHash hash;
                hash.update(view);
                return hash.get64();
            }

            using is_transparent = void; // required to make find() work with different type than key_type
        };

        struct Equal
        {
            auto operator()(const std::string_view a,
                            const std::string_view b) const
            {
                return a == b;
            }

            using is_transparent = void; // required to make find() work with different type than key_type
        };

        mutable std::unordered_map<std::string, UncommittedNode, Hash, Equal> nodes;
        std::unordered_map<std::string, std::list<const Delta *>, Hash, Equal> deltas_for_path;

        std::list<Delta> deltas;
        KeeperStorage<Container> & storage;
    };

    UncommittedState uncommitted_state{*this};

    // Apply uncommitted state to another storage using only transactions
    // with zxid > last_zxid
    void applyUncommittedState(KeeperStorage & other, int64_t last_log_idx);

    Coordination::Error commit(int64_t zxid);

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
        /// index in storage of the log containing the transaction
        int64_t log_idx = 0;
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
        std::optional<Digest> digest = std::nullopt,
        int64_t log_idx = 0);
    void rollbackRequest(int64_t rollback_zxid, bool allow_missing);

    void finalize();

    bool isFinalized() const;

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
