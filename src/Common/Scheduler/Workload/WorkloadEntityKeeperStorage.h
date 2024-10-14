#pragma once

#include <Common/Scheduler/Workload/WorkloadEntityStorageBase.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/ZooKeeperCachingGetter.h>


namespace DB
{

/// Loads RESOURCE and WORKLOAD sql objects from Keeper.
class WorkloadEntityKeeperStorage : public WorkloadEntityStorageBase
{
public:
    WorkloadEntityKeeperStorage(const ContextPtr & global_context_, const String & zookeeper_path_);
    ~WorkloadEntityKeeperStorage() override;

    bool isReplicated() const override { return true; }
    String getReplicationID() const override { return zookeeper_path; }

    void loadEntities() override;
    void stopWatching() override;

private:
    OperationResult storeEntityImpl(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        ASTPtr create_entity_query,
        bool throw_if_exists,
        bool replace_if_exists,
        const Settings & settings) override;

    OperationResult removeEntityImpl(
        const ContextPtr & current_context,
        WorkloadEntityType entity_type,
        const String & entity_name,
        bool throw_if_not_exists) override;

    void processWatchQueue();

    zkutil::ZooKeeperPtr getZooKeeper();

    void startWatchingThread();
    void stopWatchingThread();

    void createRootNodes(const zkutil::ZooKeeperPtr & zookeeper);

    std::pair<String, Int32> getDataAndSetWatch(const zkutil::ZooKeeperPtr & zookeeper);

    void refreshAllEntities(const zkutil::ZooKeeperPtr & zookeeper); // TODO(serxa): get rid of it
    void refreshEntities(const zkutil::ZooKeeperPtr & zookeeper);

    zkutil::ZooKeeperCachingGetter zookeeper_getter;
    String zookeeper_path;
    Int32 current_version = 0;

    ThreadFromGlobalPool watching_thread;
    std::atomic<bool> entities_loaded = false;
    std::atomic<bool> watching_flag = false;

    std::shared_ptr<ConcurrentBoundedQueue<bool>> watch_queue; // TODO(serxa): rework it into something that is not a queue

    LoggerPtr log;
};

}
