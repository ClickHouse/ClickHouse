#pragma once

#include <set>
#include <map>
#include <list>
#include <mutex>
#include <thread>
#include <atomic>
#include <boost/noncopyable.hpp>
#include <Poco/Event.h>
#include <Core/Types.h>
#include <common/logger_useful.h>
#include <Core/BackgroundSchedulePool.h>

namespace DB
{

class StorageReplicatedMergeTree;


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
    ReplicatedMergeTreePartCheckThread(StorageReplicatedMergeTree & storage_);
    ~ReplicatedMergeTreePartCheckThread();

    /// Processing of the queue to be checked is done in the background thread, which you must first start.
    void start();
    void stop();

    /// Don't create more than one instance of this object simultaneously.
    struct TemporarilyStop : private boost::noncopyable
    {
        ReplicatedMergeTreePartCheckThread * parent;

        TemporarilyStop(ReplicatedMergeTreePartCheckThread * parent) : parent(parent)
        {
            parent->stop();
        }

        TemporarilyStop(TemporarilyStop && old) : parent(old.parent)
        {
            old.parent = nullptr;
        }

        ~TemporarilyStop()
        {
            if (parent)
                parent->start();
        }
    };

    TemporarilyStop temporarilyStop() { return TemporarilyStop(this); }

    /// Add a part (for which there are suspicions that it is missing, damaged or not needed) in the queue for check.
    /// delay_to_check_seconds - check no sooner than the specified number of seconds.
    void enqueuePart(const String & name, time_t delay_to_check_seconds = 0);

    /// Get the number of parts in the queue for check.
    size_t size() const;



private:
    void run();

    void checkPart(const String & part_name);
    void searchForMissingPart(const String & part_name);

    StorageReplicatedMergeTree & storage;
    String log_name;
    Logger * log;

    using StringSet = std::set<String>;
    using PartToCheck = std::pair<String, time_t>;    /// The name of the part and the minimum time to check (or zero, if not important).
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
