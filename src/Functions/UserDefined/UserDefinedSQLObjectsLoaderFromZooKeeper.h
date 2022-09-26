#pragma once

#include <Functions/UserDefined/IUserDefinedSQLObjectsLoader.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/Common.h>


namespace DB
{

/// Loads user-defined sql objects from ZooKeeper.
class UserDefinedSQLObjectsLoaderFromZooKeeper : public IUserDefinedSQLObjectsLoader
{
public:
    UserDefinedSQLObjectsLoaderFromZooKeeper(const ContextPtr & global_context_, const String & zookeeper_path_);
    ~UserDefinedSQLObjectsLoaderFromZooKeeper() override;

    bool isReplicated() const override { return true; }
    void loadObjects(bool ignore_network_errors, bool start_watching) override;
    void stopWatching() override;
    void reloadObjects() override;
    void reloadObject(UserDefinedSQLObjectType object_type, const String & object_name) override;
    bool storeObject(UserDefinedSQLObjectType object_type, const String & object_name, const IAST & create_object_query, bool throw_if_exists, bool replace_if_exists, const Settings & settings) override;
    bool removeObject(UserDefinedSQLObjectType object_type, const String & object_name, bool throw_if_not_exists) override;

private:
    void initZooKeeperIfNeeded();
    zkutil::ZooKeeperPtr getZooKeeper();
    zkutil::ZooKeeperPtr getZooKeeperNoLock() TSA_REQUIRES(cached_zookeeper_ptr_mutex);
    void createRootNodes(const zkutil::ZooKeeperPtr & zookeeper);

    ASTPtr tryLoadObject(const zkutil::ZooKeeperPtr & zookeeper, UserDefinedSQLObjectType object_type, const String & object_name);
    String getNodePath(UserDefinedSQLObjectType object_type, const String & object_name) const;

    void startWatchingThread();
    void stopWatchingThread();
    void runWatchingThread();

    void refresh();
    void refreshObjects(const zkutil::ZooKeeperPtr & zookeeper, bool force_refresh_all);
    void refreshObject(const zkutil::ZooKeeperPtr & zookeeper, UserDefinedSQLObjectType object_type, const String & object_name);

    void resetAfterError();

    const ContextPtr global_context;
    String zookeeper_path;
    Poco::Logger * const log;

    zkutil::ZooKeeperPtr cached_zookeeper_ptr TSA_GUARDED_BY(cached_zookeeper_ptr_mutex);
    std::mutex cached_zookeeper_ptr_mutex;

    std::atomic<bool> watching = false;
    ThreadFromGlobalPool watching_thread;
    std::shared_ptr<ConcurrentBoundedQueue<String>> watch_queue;
};

}
