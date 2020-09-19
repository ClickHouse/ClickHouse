#pragma once

#include "Types.h"
#include <Poco/Util/LayeredConfiguration.h>
#include <unordered_set>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <unistd.h>


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
const UInt32 DEFAULT_OPERATION_TIMEOUT = 10000;

/// Preferred size of multi() command (in number of ops)
constexpr size_t MULTI_BATCH_SIZE = 100;


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

    ZooKeeper(const std::string & hosts_, const std::string & identity_ = "",
              int32_t session_timeout_ms_ = DEFAULT_SESSION_TIMEOUT,
              int32_t operation_timeout_ms_ = DEFAULT_OPERATION_TIMEOUT,
              const std::string & chroot_ = "",
              const std::string & implementation_ = "zookeeper");

    /** Config of the form:
        <zookeeper>
            <node>
                <host>example1</host>
                <port>2181</port>
                <!-- Optional. Enables communication over SSL . -->
                <secure>1</secure>
            </node>
            <node>
                <host>example2</host>
                <port>2181</port>
                <!-- Optional. Enables communication over SSL . -->
                <secure>1</secure>
            </node>
            <session_timeout_ms>30000</session_timeout_ms>
            <operation_timeout_ms>10000</operation_timeout_ms>
            <!-- Optional. Chroot suffix. Should exist. -->
            <root>/path/to/zookeeper/node</root>
            <!-- Optional. Zookeeper digest ACL string. -->
            <identity>user:password</identity>
        </zookeeper>
    */
    ZooKeeper(const Poco::Util::AbstractConfiguration & config, const std::string & config_name);

    /// Creates a new session with the same parameters. This method can be used for reconnecting
    /// after the session has expired.
    /// This object remains unchanged, and the new session is returned.
    Ptr startNewSession() const;

    bool configChanged(const Poco::Util::AbstractConfiguration & config, const std::string & config_name) const;

    /// Returns true, if the session has expired.
    bool expired();

    /// Create a znode.
    /// Throw an exception if something went wrong.
    std::string create(const std::string & path, const std::string & data, int32_t mode);

    /// Does not throw in the following cases:
    /// * The parent for the created node does not exist
    /// * The parent is ephemeral.
    /// * The node already exists.
    /// In case of other errors throws an exception.
    Coordination::Error tryCreate(const std::string & path, const std::string & data, int32_t mode, std::string & path_created);
    Coordination::Error tryCreate(const std::string & path, const std::string & data, int32_t mode);

    /// Create a Persistent node.
    /// Does nothing if the node already exists.
    void createIfNotExists(const std::string & path, const std::string & data);

    /// Creates all non-existent ancestors of the given path with empty contents.
    /// Does not create the node itself.
    void createAncestors(const std::string & path);

    /// Remove the node if the version matches. (if version == -1, remove any version).
    void remove(const std::string & path, int32_t version = -1);

    /// Doesn't throw in the following cases:
    /// * The node doesn't exist
    /// * Versions don't match
    /// * The node has children.
    Coordination::Error tryRemove(const std::string & path, int32_t version = -1);

    bool exists(const std::string & path, Coordination::Stat * stat = nullptr, const EventPtr & watch = nullptr);
    bool existsWatch(const std::string & path, Coordination::Stat * stat, Coordination::WatchCallback watch_callback);

    std::string get(const std::string & path, Coordination::Stat * stat = nullptr, const EventPtr & watch = nullptr);
    std::string getWatch(const std::string & path, Coordination::Stat * stat, Coordination::WatchCallback watch_callback);

    /// Doesn't not throw in the following cases:
    /// * The node doesn't exist. Returns false in this case.
    bool tryGet(const std::string & path, std::string & res, Coordination::Stat * stat = nullptr, const EventPtr & watch = nullptr,
                Coordination::Error * code = nullptr);

    bool tryGetWatch(const std::string & path, std::string & res, Coordination::Stat * stat, Coordination::WatchCallback watch_callback,
                     Coordination::Error * code = nullptr);

    void set(const std::string & path, const std::string & data,
             int32_t version = -1, Coordination::Stat * stat = nullptr);

    /// Creates the node if it doesn't exist. Updates its contents otherwise.
    void createOrUpdate(const std::string & path, const std::string & data, int32_t mode);

    /// Doesn't not throw in the following cases:
    /// * The node doesn't exist.
    /// * Versions do not match.
    Coordination::Error trySet(const std::string & path, const std::string & data,
                   int32_t version = -1, Coordination::Stat * stat = nullptr);

    Strings getChildren(const std::string & path,
                        Coordination::Stat * stat = nullptr,
                        const EventPtr & watch = nullptr);

    Strings getChildrenWatch(const std::string & path,
                             Coordination::Stat * stat,
                             Coordination::WatchCallback watch_callback);

    /// Doesn't not throw in the following cases:
    /// * The node doesn't exist.
    Coordination::Error tryGetChildren(const std::string & path, Strings & res,
                           Coordination::Stat * stat = nullptr,
                           const EventPtr & watch = nullptr);

    Coordination::Error tryGetChildrenWatch(const std::string & path, Strings & res,
                                Coordination::Stat * stat,
                                Coordination::WatchCallback watch_callback);

    /// Performs several operations in a transaction.
    /// Throws on every error.
    Coordination::Responses multi(const Coordination::Requests & requests);
    /// Throws only if some operation has returned an "unexpected" error
    /// - an error that would cause the corresponding try- method to throw.
    Coordination::Error tryMulti(const Coordination::Requests & requests, Coordination::Responses & responses);
    /// Throws nothing (even session expired errors)
    Coordination::Error tryMultiNoThrow(const Coordination::Requests & requests, Coordination::Responses & responses);

    Int64 getClientID();

    /// Remove the node with the subtree. If someone concurrently adds or removes a node
    /// in the subtree, the result is undefined.
    void removeRecursive(const std::string & path);

    /// Remove the node with the subtree. If someone concurrently removes a node in the subtree,
    /// this will not cause errors.
    /// For instance, you can call this method twice concurrently for the same node and the end
    /// result would be the same as for the single call.
    void tryRemoveRecursive(const std::string & path);

    /// Remove all children nodes (non recursive).
    void removeChildren(const std::string & path);

    using WaitCondition = std::function<bool()>;
    /// Wait for the node to disappear or return immediately if it doesn't exist.
    /// If condition is specified, it is used to return early (when condition returns false)
    /// The function returns true if waited and false if waiting was interrupted by condition.
    bool waitForDisappear(const std::string & path, const WaitCondition & condition = {});

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

    using FutureCreate = std::future<Coordination::CreateResponse>;
    FutureCreate asyncCreate(const std::string & path, const std::string & data, int32_t mode);

    using FutureGet = std::future<Coordination::GetResponse>;
    FutureGet asyncGet(const std::string & path);

    FutureGet asyncTryGet(const std::string & path);

    using FutureExists = std::future<Coordination::ExistsResponse>;
    FutureExists asyncExists(const std::string & path);

    using FutureGetChildren = std::future<Coordination::ListResponse>;
    FutureGetChildren asyncGetChildren(const std::string & path);

    using FutureSet = std::future<Coordination::SetResponse>;
    FutureSet asyncSet(const std::string & path, const std::string & data, int32_t version = -1);

    using FutureRemove = std::future<Coordination::RemoveResponse>;
    FutureRemove asyncRemove(const std::string & path, int32_t version = -1);

    /// Doesn't throw in the following cases:
    /// * The node doesn't exist
    /// * The versions do not match
    /// * The node has children
    FutureRemove asyncTryRemove(const std::string & path, int32_t version = -1);

    using FutureMulti = std::future<Coordination::MultiResponse>;
    FutureMulti asyncMulti(const Coordination::Requests & ops);

    /// Like the previous one but don't throw any exceptions on future.get()
    FutureMulti tryAsyncMulti(const Coordination::Requests & ops);

private:
    friend class EphemeralNodeHolder;

    void init(const std::string & implementation_, const std::string & hosts_, const std::string & identity_,
              int32_t session_timeout_ms_, int32_t operation_timeout_ms_, const std::string & chroot_);

    void removeChildrenRecursive(const std::string & path);
    void tryRemoveChildrenRecursive(const std::string & path);

    /// The following methods don't throw exceptions but return error codes.
    Coordination::Error createImpl(const std::string & path, const std::string & data, int32_t mode, std::string & path_created);
    Coordination::Error removeImpl(const std::string & path, int32_t version);
    Coordination::Error getImpl(
        const std::string & path, std::string & res, Coordination::Stat * stat, Coordination::WatchCallback watch_callback);
    Coordination::Error setImpl(const std::string & path, const std::string & data, int32_t version, Coordination::Stat * stat);
    Coordination::Error getChildrenImpl(
        const std::string & path, Strings & res, Coordination::Stat * stat, Coordination::WatchCallback watch_callback);
    Coordination::Error multiImpl(const Coordination::Requests & requests, Coordination::Responses & responses);
    Coordination::Error existsImpl(const std::string & path, Coordination::Stat * stat_, Coordination::WatchCallback watch_callback);

    std::unique_ptr<Coordination::IKeeper> impl;

    std::string hosts;
    std::string identity;
    int32_t session_timeout_ms;
    int32_t operation_timeout_ms;
    std::string chroot;
    std::string implementation;

    std::mutex mutex;

    Poco::Logger * log = nullptr;
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
            zookeeper.tryRemove(path);
        }
        catch (...)
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
}
