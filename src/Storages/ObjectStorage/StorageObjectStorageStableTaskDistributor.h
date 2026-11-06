#pragma once

#include <Client/Connection.h>
#include <Common/Logger.h>
#include <Interpreters/Cluster.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <deque>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <chrono>
#include <optional>
#include <condition_variable>
#include <memory>

namespace DB
{

class StorageObjectStorageStableTaskDistributor
{
public:
    StorageObjectStorageStableTaskDistributor(
        std::shared_ptr<IObjectIterator> iterator_,
        std::vector<std::string> && ids_of_nodes_,
        bool send_over_whole_archive_,
        std::chrono::milliseconds wait_for_owner_ = std::chrono::milliseconds(0));

    ObjectInfos getNextTasks(size_t number_of_current_replica, size_t max_tasks);

private:
    struct QueuedTask
    {
        String identifier;
        ObjectInfoPtr object;
        size_t owner = 0;
        size_t weight = 0;
        bool taken = false;
    };
    using QueuedTaskPtr = std::shared_ptr<QueuedTask>;

    struct Placement
    {
        size_t owner = 0;
        std::optional<size_t> stealer;
    };

    Placement getPlacement(const String & identifier) const;

    void takeOwnTasks(size_t replica, size_t max_tasks, ObjectInfos & result);
    void takeTasksFromIterator(size_t replica, size_t max_tasks, ObjectInfos & result);
    void stealTasks(size_t replica, size_t max_tasks, ObjectInfos & result);

    void enqueue(ObjectInfoPtr object, String identifier, const Placement & placement);
    ObjectInfoPtr take(const QueuedTaskPtr & task);

    const std::shared_ptr<IObjectIterator> iterator;
    const bool send_over_whole_archive;
    const std::chrono::milliseconds wait_for_owner;
    const std::vector<std::string> ids_of_nodes;

    std::mutex mutex;
    std::condition_variable replica_seen_cv;
    std::condition_variable iterator_cv;
    bool iterator_in_use = false;
    std::vector<std::deque<QueuedTaskPtr>> owner_queues;
    std::vector<std::deque<QueuedTaskPtr>> stealer_queues;
    std::unordered_map<String, QueuedTaskPtr> unprocessed_files;
    std::vector<size_t> remaining_bytes;
    std::vector<size_t> remaining_count;
    std::vector<bool> replica_seen;
    std::optional<std::chrono::steady_clock::time_point> stealing_allowed_since;
    bool iterator_exhausted = false;

    LoggerPtr log = getLogger("StorageClusterTaskDistributor");
};

}
