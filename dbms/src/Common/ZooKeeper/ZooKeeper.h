#pragma once

#include "Types.h"
#include "KeeperException.h"
#include <Poco/Util/LayeredConfiguration.h>
#include <unordered_set>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <unistd.h>
#include <common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>


namespace ProfileEvents
{
    extern const Event CannotRemoveEphemeralNode;
}

namespace CurrentMetrics
{
    extern const Metric EphemeralNode;
}


namespace zkutil
{

const UInt32 DEFAULT_SESSION_TIMEOUT = 30000;
const UInt32 MEDIUM_SESSION_TIMEOUT = 120000;
const UInt32 BIG_SESSION_TIMEOUT = 600000;

/// Preferred size of multi() command (in number of ops)
constexpr size_t MULTI_BATCH_SIZE = 100;

struct WatchContext;
struct MultiTransactionInfo;

/// ZooKeeper session. The interface is substantially different from the usual libzookeeper API.
///
/// Poco::Event objects are used for watches. The event is set only once on the first
/// watch notification.
/// Callback-based watch interface is also provided.
///
/// Read-only methods retry retry_num times if recoverable errors like OperationTimeout
/// or ConnectionLoss are encountered.
///
/// Modifying methods do not retry, because it leads to problems of the double-delete type.
///
/// Methods with names not starting at try- raise KeeperException on any error.
class ZooKeeper
{
public:
    using Ptr = std::shared_ptr<ZooKeeper>;

    ZooKeeper(const std::string & hosts, const std::string & identity = "",
              int32_t session_timeout_ms = DEFAULT_SESSION_TIMEOUT, bool check_root_exists = false);

    /** Config of the form:
        <zookeeper>
            <node>
                <host>example1</host>
                <port>2181</port>
            </node>
            <node>
                <host>example2</host>
                <port>2181</port>
            </node>
            <session_timeout_ms>30000</session_timeout_ms>
            <!-- Optional. Chroot suffix. Should exist. -->
            <root>/path/to/zookeeper/node</root>
            <!-- Optional. Zookeeper digest ACL string. -->
            <identity>user:password</identity>
        </zookeeper>
    */
    ZooKeeper(const Poco::Util::AbstractConfiguration & config, const std::string & config_name);

    ~ZooKeeper();

    /// Creates a new session with the same parameters. This method can be used for reconnecting
    /// after the session has expired.
    /// This object remains unchanged, and the new session is returned.
    Ptr startNewSession() const;

    /// Returns true, if the session has expired forever.
    /// This is possible only if the connection has been established, then lost and re-established
    /// again, but too late.
    /// In contrast, if, for instance, the server name or port is misconfigured, connection
    /// attempts will continue indefinitely, expired() will return false and all method calls
    /// will raise ConnectionLoss exception.
    /// Also returns true if is_dirty flag is set - a request to close the session ASAP.
    bool expired();

    ACLPtr getDefaultACL();

    void setDefaultACL(ACLPtr new_acl);

    /// Create a znode. ACL set by setDefaultACL is used (full access to everybody by default).
    /// Throw an exception if something went wrong.
    std::string create(const std::string & path, const std::string & data, int32_t mode);

    /// Does not throw in the following cases:
    /// * The parent for the created node does not exist
    /// * The parent is ephemeral.
    /// * The node already exists.
    /// In case of other errors throws an exception.
    int32_t tryCreate(const std::string & path, const std::string & data, int32_t mode, std::string & path_created);
    int32_t tryCreate(const std::string & path, const std::string & data, int32_t mode);
    int32_t tryCreateWithRetries(const std::string & path, const std::string & data, int32_t mode,
                                 std::string & path_created, size_t * attempt = nullptr);

    /// Create a Persistent node.
    /// Does nothing if the node already exists.
    /// Retries on ConnectionLoss or OperationTimeout.
    void createIfNotExists(const std::string & path, const std::string & data);

    /// Creates all non-existent ancestors of the given path with empty contents.
    /// Does not create the node itself.
    void createAncestors(const std::string & path);

    /// Remove the node if the version matches. (if version == -1, remove any version).
    void remove(const std::string & path, int32_t version = -1);

    /// Removes the node. In case of network errors tries to remove again.
    /// ZNONODE error for the second and the following tries is ignored.
    void removeWithRetries(const std::string & path, int32_t version = -1);

    /// Doesn't throw in the following cases:
    /// * The node doesn't exist
    /// * Versions don't match
    /// * The node has children.
    int32_t tryRemove(const std::string & path, int32_t version = -1);
    /// Retries in case of network errors, returns ZNONODE if the node is already removed.
    int32_t tryRemoveWithRetries(const std::string & path, int32_t version = -1, size_t * attempt = nullptr);

    /// The same, but sets is_dirty flag if all removal attempts were unsuccessful.
    /// This is needed because the session might still exist after all retries,
    /// even if more time than session_timeout has passed.
    /// So we do not rely on the ephemeral node being deleted and set is_dirty to
    /// try and close the session ASAP.
    /**     Ridiculously Long Delay to Expire
            When disconnects do happen, the common case should be a very* quick
            reconnect to another server, but an extended network outage may
            introduce a long delay before a client can reconnect to the ZooKeep‐
            er service. Some developers wonder why the ZooKeeper client li‐
            brary doesn’t simply decide at some point (perhaps twice the session
            timeout) that enough is enough and kill the session itself.
            There are two answers to this. First, ZooKeeper leaves this kind of
            policy decision up to the developer. Developers can easily implement
            such a policy by closing the handle themselves. Second, when a Zoo‐
            Keeper ensemble goes down, time freezes. Thus, when the ensemble is
            brought back up, session timeouts are restarted. If processes using
            ZooKeeper hang in there, they may find out that the long timeout was
            due to an extended ensemble failure that has recovered and pick right
            up where they left off without any additional startup delay.

            ZooKeeper: Distributed Process Coordination p118
      */
    int32_t tryRemoveEphemeralNodeWithRetries(const std::string & path, int32_t version = -1, size_t * attempt = nullptr);

    bool exists(const std::string & path, Stat * stat = nullptr, const EventPtr & watch = nullptr);
    bool existsWatch(const std::string & path, Stat * stat, const WatchCallback & watch_callback);

    std::string get(const std::string & path, Stat * stat = nullptr, const EventPtr & watch = nullptr);

    /// Doesn't not throw in the following cases:
    /// * The node doesn't exist. Returns false in this case.
    bool tryGet(const std::string & path, std::string & res, Stat * stat = nullptr, const EventPtr & watch = nullptr, int * code = nullptr);

    bool tryGetWatch(const std::string & path, std::string & res, Stat * stat, const WatchCallback & watch_callback, int * code = nullptr);

    void set(const std::string & path, const std::string & data,
            int32_t version = -1, Stat * stat = nullptr);

    /// Creates the node if it doesn't exist. Updates its contents otherwise.
    void createOrUpdate(const std::string & path, const std::string & data, int32_t mode);

    /// Doesn't not throw in the following cases:
    /// * The node doesn't exist.
    /// * Versions do not match.
    int32_t trySet(const std::string & path, const std::string & data,
                            int32_t version = -1, Stat * stat = nullptr);

    Strings getChildren(const std::string & path,
                        Stat * stat = nullptr,
                        const EventPtr & watch = nullptr);

    /// Doesn't not throw in the following cases:
    /// * The node doesn't exist.
    int32_t tryGetChildren(const std::string & path, Strings & res,
                        Stat * stat = nullptr,
                        const EventPtr & watch = nullptr);

    /// Performs several operations in a transaction.
    /// Throws on every error.
    OpResultsPtr multi(const Ops & ops);

    /// Throws only if some operation has returned an "unexpected" error
    /// - an error that would cause the corresponding try- method to throw.
    int32_t tryMulti(const Ops & ops, OpResultsPtr * out_results = nullptr);
    /// Like previous one, but does not throw any ZooKeeper exceptions
    int32_t tryMultiUnsafe(const Ops & ops, MultiTransactionInfo & info);
    /// Use only with read-only operations.
    int32_t tryMultiWithRetries(const Ops & ops, OpResultsPtr * out_results = nullptr, size_t * attempt = nullptr);

    Int64 getClientID();

    /// Remove the node with the subtree. If someone concurrently adds or removes a node
    /// in the subtree, the result is undefined.
    void removeRecursive(const std::string & path);

    /// Remove the node with the subtree. If someone concurrently removes a node in the subtree,
    /// this will not cause errors.
    /// For instance, you can call this method twice concurrently for the same node and the end
    /// result would be the same as for the single call.
    void tryRemoveRecursive(const std::string & path);

    /// Wait for the node to disappear or return immediately if it doesn't exist.
    void waitForDisappear(const std::string & path);

    /// Async interface (a small subset of operations is implemented).
    ///
    /// Usage:
    ///
    /// // Non-blocking calls:
    /// auto future1 = zk.asyncGet("/path1");
    /// auto future2 = zk.asyncGet("/path2");
    /// ...
    ///
    /// // These calls can block until the operations are completed:
    /// auto result1 = future1.get();
    /// auto result2 = future2.get();
    ///
    /// Future should not be destroyed before the result is gotten.

    template <typename Result, typename... TaskParams>
    class Future
    {
    friend class ZooKeeper;
    private:
        using Task = std::packaged_task<Result (TaskParams...)>;
        using TaskPtr = std::unique_ptr<Task>;
        using TaskPtrPtr = std::unique_ptr<TaskPtr>;

        /// Everything is complicated.
        ///
        /// In libzookeeper async interface a function (e.g. zoo_aget)
        /// accepts a pointer to a standalone callback function and void* pointer to the context
        /// which is then passed to the callback.
        /// The caller is responsible for ensuring that the context lives until the callback
        /// is finished and we can't simply pass ownership of the context into function object.
        /// Instead, we save the context in a Future object and return it to the caller.
        /// The context will live until the Future lives.
        /// Context data is wrapped in an unique_ptr so that its address (which is passed to
        /// libzookeeper) remains unchanged after the Future is returned from the function.
        ///
        /// The second problem is that after std::promise has been fulfilled, and the user
        /// has gotten the result from std::future, the Future object can be destroyed
        /// before the std::promise::set_value() call that fulfils the promise completes in another
        /// thread.
        /// See http://stackoverflow.com/questions/10843304/race-condition-in-pthread-once
        /// To combat this we use the second unique_ptr. Inside the callback, the void* context
        /// is cast to unique_ptr and moved into the local unique_ptr to prolong the lifetime of
        /// the context data.

        TaskPtrPtr task;
        std::future<Result> future;

        template <typename... Args>
        Future(Args &&... args) :
            task(std::make_unique<TaskPtr>(std::make_unique<Task>(std::forward<Args>(args)...))),
            future((*task)->get_future()) {}

    public:
        Result get()
        {
            return future.get();
        }

        Future(Future &&) = default;
        Future & operator= (Future &&) = default;

        ~Future()
        {
            /// If nobody has waited for the result, we must wait for it before the object is
            /// destroyed, because the object contents can still be used in the callback.
            if (future.valid())
                future.wait();
        }
    };


    struct ValueAndStat
    {
        std::string value;
        Stat stat;
    };

    using GetFuture = Future<ValueAndStat, int, const char *, int, const Stat *>;
    GetFuture asyncGet(const std::string & path);


    struct ValueAndStatAndExists
    {
        std::string value;
        Stat stat;
        bool exists;
    };

    using TryGetFuture = Future<ValueAndStatAndExists, int, const char *, int, const Stat *>;
    TryGetFuture asyncTryGet(const std::string & path);


    struct StatAndExists
    {
        Stat stat;
        bool exists;
    };

    using ExistsFuture = Future<StatAndExists, int, const Stat *>;
    ExistsFuture asyncExists(const std::string & path);


    using GetChildrenFuture = Future<Strings, int, const String_vector *>;
    GetChildrenFuture asyncGetChildren(const std::string & path);


    using RemoveFuture = Future<void, int>;
    RemoveFuture asyncRemove(const std::string & path, int32_t version = -1);

    /// Doesn't throw in the following cases:
    /// * The node doesn't exist
    /// * The versions do not match
    /// * The node has children
    using TryRemoveFuture = Future<int32_t, int>;
    TryRemoveFuture asyncTryRemove(const std::string & path, int32_t version = -1);

    struct OpResultsAndCode
    {
        OpResultsPtr results;
        std::shared_ptr<Ops> ops_ptr;
        int code;
    };

    using MultiFuture = Future<OpResultsAndCode, int>;
    MultiFuture asyncMulti(const Ops & ops);
    /// Like the previous one but don't throw any exceptions on future.get()
    MultiFuture tryAsyncMulti(const Ops & ops);


    static std::string error2string(int32_t code);

    /// Max size of node contents in bytes.
    /// In 3.4.5 max node size is 1Mb.
    static const size_t MAX_NODE_SIZE = 1048576;

    /// Length of the suffix that ZooKeeper adds to sequential nodes.
    /// In fact it is smaller, but round it up for convenience.
    static const size_t SEQUENTIAL_SUFFIX_SIZE = 64;


    zhandle_t * getHandle() { return impl; }

private:
    friend struct WatchContext;
    friend class EphemeralNodeHolder;

    void init(const std::string & hosts, const std::string & identity,
              int32_t session_timeout_ms, bool check_root_exists);
    void removeChildrenRecursive(const std::string & path);
    void tryRemoveChildrenRecursive(const std::string & path);

    static WatchCallback callbackForEvent(const EventPtr & event);
    WatchContext * createContext(WatchCallback && callback);
    static void destroyContext(WatchContext * context);
    static void processCallback(zhandle_t * zh, int type, int state, const char * path, void * watcher_ctx);

    template <typename T>
    int32_t retry(T && operation, size_t * attempt = nullptr)
    {
        int32_t code = operation();
        if (attempt)
            *attempt = 0;
        for (size_t i = 0; (i < retry_num) && (code == ZOPERATIONTIMEOUT || code == ZCONNECTIONLOSS); ++i)
        {
            if (attempt)
                *attempt = i;

            /// If the connection has been lost, wait timeout/3 hoping for connection re-establishment.
            static const int MAX_SLEEP_TIME = 10;
            if (code == ZCONNECTIONLOSS)
                usleep(std::min(session_timeout_ms * 1000 / 3, MAX_SLEEP_TIME * 1000 * 1000));

            LOG_WARNING(log, "Error on attempt " << i << ": " << error2string(code)  << ". Retry");
            code = operation();
        }

        return code;
    }

    /// The following methods don't throw exceptions but return error codes.
    int32_t createImpl(const std::string & path, const std::string & data, int32_t mode, std::string & path_created);
    int32_t removeImpl(const std::string & path, int32_t version = -1);
    int32_t getImpl(const std::string & path, std::string & res, Stat * stat, WatchCallback watch_callback);
    int32_t setImpl(const std::string & path, const std::string & data, int32_t version = -1, Stat * stat = nullptr);
    int32_t getChildrenImpl(const std::string & path, Strings & res, Stat * stat, WatchCallback watch_callback);
    int32_t multiImpl(const Ops & ops, OpResultsPtr * out_results = nullptr);
    int32_t existsImpl(const std::string & path, Stat * stat_, WatchCallback watch_callback);

    MultiFuture asyncMultiImpl(const zkutil::Ops & ops_, bool throw_exception);

    std::string hosts;
    std::string identity;
    int32_t session_timeout_ms;

    std::mutex mutex;
    ACLPtr default_acl;
    zhandle_t * impl;

    std::unordered_set<WatchContext *> watch_context_store;

    /// Retries number in case of OperationTimeout or ConnectionLoss errors.
    static constexpr size_t retry_num = 3;
    Logger * log = nullptr;

    /// If true, there were unsuccessfull attempts to remove ephemeral nodes.
    /// It is better to close the session to remove ephemeral nodes with certainty
    /// instead of continuing to use re-established session.
    bool is_dirty = false;
};


using ZooKeeperPtr = ZooKeeper::Ptr;


/// Creates an ephemeral node in the constructor, removes it in the destructor.
class EphemeralNodeHolder
{
public:
    using Ptr = std::shared_ptr<EphemeralNodeHolder>;

    EphemeralNodeHolder(const std::string & path_, ZooKeeper & zookeeper_, bool create, bool sequential, const std::string & data)
        : path(path_), zookeeper(zookeeper_)
    {
        if (create)
            path = zookeeper.create(path, data, sequential ? CreateMode::EphemeralSequential : CreateMode::Ephemeral);
    }

    std::string getPath() const
    {
        return path;
    }

    static Ptr create(const std::string & path, ZooKeeper & zookeeper, const std::string & data = "")
    {
        return std::make_shared<EphemeralNodeHolder>(path, zookeeper, true, false, data);
    }

    static Ptr createSequential(const std::string & path, ZooKeeper & zookeeper, const std::string & data = "")
    {
        return std::make_shared<EphemeralNodeHolder>(path, zookeeper, true, true, data);
    }

    static Ptr existing(const std::string & path, ZooKeeper & zookeeper)
    {
        return std::make_shared<EphemeralNodeHolder>(path, zookeeper, false, false, "");
    }

    ~EphemeralNodeHolder()
    {
        try
        {
            /// Important: if the ZooKeeper is temporarily unavailable, repeated attempts to
            /// delete the node are made.
            /// Otherwise it is possible that EphemeralNodeHolder is destroyed,
            /// but the session has recovered and the node in ZooKeeper remains for the long time.
            zookeeper.tryRemoveEphemeralNodeWithRetries(path);
        }
        catch (const KeeperException & e)
        {
            ProfileEvents::increment(ProfileEvents::CannotRemoveEphemeralNode);
            DB::tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

private:
    std::string path;
    ZooKeeper & zookeeper;
    CurrentMetrics::Increment metric_increment{CurrentMetrics::EphemeralNode};
};

using EphemeralNodeHolderPtr = EphemeralNodeHolder::Ptr;


/// Simple structure to handle transaction execution results
struct MultiTransactionInfo
{
    MultiTransactionInfo() = default;

    const Ops * ops = nullptr;
    int32_t code = ZOK;
    OpResultsPtr op_results;

    bool empty() const
    {
        return ops == nullptr;
    }

    bool hasFailedOp() const
    {
        return zkutil::isUserError(code);
    }

    const Op & getFailedOp() const
    {
        return *ops->at(getFailedOpIndex(op_results, code));
    }

    KeeperException getException() const
    {
        if (hasFailedOp())
        {
            size_t i = getFailedOpIndex(op_results, code);
            return KeeperException("Transaction failed at op #" + std::to_string(i) + ": " + ops->at(i)->describe(), code);
        }
        else
            return KeeperException(code);
    }
};

}
