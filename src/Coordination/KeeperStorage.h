#pragma once

#include <iterator>
#include <cstddef>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Coordination/SessionExpiryQueue.h>
#include <Coordination/ACLMap.h>
#include <Coordination/SnapshotableHashTable.h>
#include <iterator>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <Common/HashTable/robin_hood.h>

namespace DB
{
template <typename T>
class DoubleSet
{
public:
    using US = robin_hood::unordered_set<T>;
    using inner_iterator = typename US::iterator;
    using Self = DoubleSet<T>;
    using value_type = T;
private:
    std::shared_ptr<US> store[2];
    bool rehashing{false};
    inner_iterator cur; // rehash iterator
    float max_load_factor = 0.7;
    std::size_t count{0};
public:
    DoubleSet() {
        store[0] = std::make_shared<US>();
        //store[0]->reserve(1000000);
        store[1] = std::make_shared<US>();
        //store[1]->reserve(2000000);
    }
    DoubleSet(const Self & rhs) {
        store[0] = rhs.store[0];
        store[1] = rhs.store[1];
        rehashing = rhs.rehashing;
        cur = rhs.cur;
        max_load_factor = rhs.max_load_factor;
        count = rhs.count;
    }
    ~DoubleSet() = default;
    void reseve(size_t t)
    {
        store[0]->reserve(t);
    }
    template <bool IsConst>
    struct Iter
    {
    public:
        using UsPtr = typename std::conditional<IsConst, Self const*, Self*>::type;
        using difference_type = std::ptrdiff_t;
        using value_type = typename Self::value_type;
        using reference = typename std::conditional<IsConst, value_type const&, value_type&>::type;
        using pointer = typename std::conditional<IsConst, value_type const*, value_type*>::type;
        using iterator_category = std::forward_iterator_tag;
        using inner_iter = typename std::conditional<IsConst, typename US::const_iterator, typename US::iterator>::type;

        Iter() = default;
        Iter(UsPtr ptr, inner_iter iter, int idx) : ref(ptr), m_iter(iter), index(idx) {}

        reference operator*() const { return *m_iter; }
        pointer operator->() { return &*m_iter; }
        Iter& operator++() noexcept
        {
            ++m_iter;
            if (index == 0 && ref->rehashing && m_iter == ref->store[index]->end())
            {
                m_iter = ref->store[1]->begin();
            }
            return *this;
        }
        Iter operator++(int) noexcept { Iter tmp = *this; ++(*this); return tmp; }
        template <bool O>
        bool operator==(Iter<O> const& o) const noexcept
        {
            return m_iter == o.m_iter;
        }
        template <bool O>
        bool operator!=(Iter<O> const& o) const noexcept
        {
            return m_iter != o.m_iter;
        }
        UsPtr ref;
        inner_iter m_iter;
        int index{0};
    };
    using iterator = Iter<false>;
    using const_iterator = Iter<true>;

    void move()
    {
        store[1]->insert(*cur);
        if (++cur == store[0]->end())
        {
            rehashing = false;
            store[0] = store[1];
            store[1] = std::make_shared<US>();
            store[1]->reserve(store[0]->size() * 2);
            std::cout << "rehash end, size " << store[0]->size() << ", count " << count << std::endl;
        }
    }
    std::pair<iterator,bool> insert(const T & t)
    {
        ++count;
        if (rehashing)
        {
            auto res = store[1]->insert(t);
            move();
            return std::make_pair(iterator(this, res.first, 1), res.second);
        }
        if (store[0]->load_factor() > max_load_factor)
        {
            std::cout << "rehash begin, size " << store[0]->size() << ", count " << count << std::endl;
            rehashing = true;
            cur = store[0]->begin();
            store[1]->reserve(store[0]->size()*2);
            move();
            auto res = store[1]->insert(t);
            return std::make_pair(iterator(this, res.first, 1), res.second);
        }
        auto res = store[0]->insert(t);
        return std::make_pair(iterator(this, res.first, 0), res.second);
    }
    void erase(const T & t)
    {
        std::size_t del = 0;
        del = store[0]->erase(t);
        if (rehashing)
        {
            del += store[1]->erase(t);
            move();
        }
        if (del) --count;
    }
    std::size_t size() const
    {
        //std::cout << "hash 0 " << store[0]->size() << ", hash 1 " << store[1]->size() << std::endl;
        return count;
    }
    bool empty() const { return count == 0; }
    const_iterator find(const T & key) const
    {
        auto res =  store[0]->find(key);
        if (res != store[0]->end())
            return const_iterator(this, res, 0);
        if (rehashing)
            return const_iterator(this, store[1]->find(key), 1);
        return const_iterator(this, store[0]->end(), 0);
    }
    bool contains(const T& t) const
    {
        if (store[0]->contains(t))
            return true;
        if (rehashing && store[1]->contains(t)) {
            return true;
        }
        return false;
    }
    const_iterator begin() const
    {
        return const_iterator(this, store[0]->begin(), 0);
    }
    iterator begin()
    {
        return iterator(this, store[0]->begin(), 0);
    }
    const_iterator end() const
    {
        if (rehashing) {
            return const_iterator(this, store[1]->end(), 1); 
        }
        return const_iterator(this, store[0]->end(), 0); 
    }
    size_t htsize()
    { 
        return store[0]->size(); 
    }
};
struct KeeperStorageRequestProcessor;
using KeeperStorageRequestProcessorPtr = std::shared_ptr<KeeperStorageRequestProcessor>;
using ResponseCallback = std::function<void(const Coordination::ZooKeeperResponsePtr &)>;
//using ChildrenSet = std::unordered_set<std::string>;
using ChildrenSet = DoubleSet<std::string>;
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
        String data;
        uint64_t acl_id = 0; /// 0 -- no ACL by default
        bool is_sequental = false;
        Coordination::Stat stat{};
        int32_t seq_num = 0;
        ChildrenSet children{};
        uint64_t size_bytes; // save size to avoid calculate every time

        Node()
        {
            size_bytes = sizeof(size_bytes);
            size_bytes += data.size();
            size_bytes += sizeof(acl_id);
            size_bytes += sizeof(is_sequental);
            size_bytes += sizeof(stat);
            size_bytes += sizeof(seq_num);
        }
        /// Object memory size
        uint64_t sizeInBytes() const
        {
            return size_bytes;
        }
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
    using Watches = std::map<String /* path, relative of root_path */, SessionIDs>;

public:
    int64_t session_id_counter{1};

    SessionAndAuth session_and_auth;

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

    /// Introspection functions mostly used in 4-letter commands
    uint64_t getNodesCount() const
    {
        return container.size();
    }

    uint64_t getApproximateDataSize() const
    {
        return container.getApproximateDataSize();
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
