#pragma once

#include <Functions/UserDefined/IUserDefinedSQLObjectsLoader.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/ZooKeeperCachingGetter.h>


namespace DB
{

/// Loads user-defined sql objects from ZooKeeper.
class UserDefinedSQLObjectsLoaderFromZooKeeper : public IUserDefinedSQLObjectsLoader
{
public:
    UserDefinedSQLObjectsLoaderFromZooKeeper(const ContextPtr & global_context_, const String & zookeeper_path_);
    ~UserDefinedSQLObjectsLoaderFromZooKeeper() override;

    bool isReplicated() const override { return true; }
    String getReplicationID() const override { return zookeeper_path; }

    void loadObjects() override;
    void stopWatching() override;
    void reloadObjects() override;
    void reloadObject(UserDefinedSQLObjectType object_type, const String & object_name) override;

    bool storeObject(
        UserDefinedSQLObjectType object_type,
        const String & object_name,
        const IAST & create_object_query,
        bool throw_if_exists,
        bool replace_if_exists,
        const Settings & settings) override;
    bool removeObject(UserDefinedSQLObjectType object_type, const String & object_name, bool throw_if_not_exists) override;

private:
    void processWatchQueue();

    zkutil::ZooKeeperPtr getZooKeeper();
    void initZooKeeperIfNeeded();
    void resetAfterError();

    void startWatchingThread();
    void stopWatchingThread();

    void createRootNodes(const zkutil::ZooKeeperPtr & zookeeper);

    ASTPtr tryLoadObject(const zkutil::ZooKeeperPtr & zookeeper, UserDefinedSQLObjectType object_type, const String & object_name);
    void refreshObject(const zkutil::ZooKeeperPtr & zookeeper, UserDefinedSQLObjectType object_type, const String & object_name);

    bool getObjectDataAndSetWatch(
        const zkutil::ZooKeeperPtr & zookeeper,
        String & data,
        const String & path,
        UserDefinedSQLObjectType object_type,
        const String & object_name);
    Strings getObjectNamesAndSetWatch(const zkutil::ZooKeeperPtr & zookeeper, UserDefinedSQLObjectType object_type);
    ASTPtr parseObjectData(const String & object_data, UserDefinedSQLObjectType object_type);

    void refreshAllObjects(const zkutil::ZooKeeperPtr & zookeeper);
    void refreshObjects(const zkutil::ZooKeeperPtr & zookeeper, UserDefinedSQLObjectType object_type);
    void syncObjects(const zkutil::ZooKeeperPtr & zookeeper, UserDefinedSQLObjectType object_type);

    ContextPtr global_context;

    zkutil::ZooKeeperCachingGetter zookeeper_getter;
    String zookeeper_path;
    std::atomic<bool> objects_loaded = false;

    ThreadFromGlobalPool watching_thread;
    std::atomic<bool> watching_flag = false;

    using UserDefinedSQLObjectTypeAndName = std::pair<UserDefinedSQLObjectType, String>;
    std::shared_ptr<ConcurrentBoundedQueue<UserDefinedSQLObjectTypeAndName>> watch_queue;

    Poco::Logger * log;
};

}
