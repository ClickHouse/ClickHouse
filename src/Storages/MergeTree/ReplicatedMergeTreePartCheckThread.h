#pragma once

#include <set>
#include <map>
#include <list>
#include <mutex>
#include <thread>
#include <atomic>
#include <boost/noncopyable.hpp>
#include <Poco/Event.h>
#include <base/types.h>
#include <Core/BackgroundSchedulePool.h>
#include <Storages/CheckResults.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

class StorageReplicatedMergeTree;

struct ReplicatedCheckResult
{
    enum Action
    {
        None,

        Cancelled,
        DoNothing,
        RecheckLater,

        DetachUnexpected,
        TryFetchMissing,
    };

    CheckResult status;
    Action action = None;

    bool exists_in_zookeeper;
    MergeTreeDataPartPtr part;
    time_t recheck_after_seconds = 0;
};

/** Checks the integrity of the parts requested for validation.
  *
  * Identifies the extra parts and removes them from the working set.
  * Find the missing parts and add them for download from replicas.
  * Checks the integrity of the data and, in the event of a violation,
  *  removes a part from the working set and adds it for download from replicas.
  */
class ReplicatedMergeTreePartCheckThread
{
public:
    explicit ReplicatedMergeTreePartCheckThread(StorageReplicatedMergeTree & storage_);
    ~ReplicatedMergeTreePartCheckThread();

    /// Processing of the queue to be checked is done in the background thread, which you must first start.
    void start();
    void stop();

    /// Add a part (for which there are suspicions that it is missing, damaged or not needed) in the queue for check.
    /// delay_to_check_seconds - check no sooner than the specified number of seconds.
    void enqueuePart(const String & name, time_t delay_to_check_seconds = 0);

    /// Get the number of parts in the queue for check.
    size_t size() const;

    /// Check part by name
    CheckResult checkPartAndFix(const String & part_name, std::optional<time_t> * recheck_after = nullptr, bool throw_on_broken_projection = true);

    ReplicatedCheckResult checkPartImpl(const String & part_name, bool throw_on_broken_projection);

    std::unique_lock<std::mutex> pausePartsCheck();

    /// Can be called only while holding a lock returned from pausePartsCheck()
    void cancelRemovedPartsCheck(const MergeTreePartInfo & drop_range_info);

private:
    void run();

    bool onPartIsLostForever(const String & part_name);

    std::pair<bool, MergeTreeDataPartPtr> findLocalPart(const String & part_name);

    /// Search for missing part on other replicas or covering part on all replicas (including our replica).
    /// Returns false if the part is lost forever.
    bool searchForMissingPartOnOtherReplicas(const String & part_name) const;

    StorageReplicatedMergeTree & storage;
    String log_name;
    LoggerPtr log;

    using StringSet = std::set<String>;
    struct PartToCheck
    {
        using TimePoint = std::chrono::steady_clock::time_point;
        String name;
        TimePoint time;
    };
    using PartsToCheckQueue = std::list<PartToCheck>;

    /** Parts for which you want to check one of two:
      *  - If we have the part, check, its data with its checksums, and them with ZooKeeper.
      *  - If we do not have a part, check to see if it (or the part covering it) exists anywhere on another replicas.
      */

    mutable std::mutex parts_mutex;
    StringSet parts_set;
    PartsToCheckQueue parts_queue;

    std::mutex start_stop_mutex;
    std::atomic<bool> need_stop { false };
    BackgroundSchedulePool::TaskHolder task;
};

}
