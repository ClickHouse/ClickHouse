#pragma once

#include <chrono>
#include <Interpreters/FileCache/FileCacheMaintenanceTask.h>

namespace DB
{

class IFileCachePriority;

/// The maintenance jobs of a top-level priority queue, which run on the thread of `FileCacheMaintenanceTask`.
class FileCachePriorityMaintenanceJobs
{
public:
    using TimePoint = FileCacheMaintenanceTask::TimePoint;

    struct Settings
    {
        /// Invalidated entries are removed once the queue has this many of them, or once per interval.
        size_t invalidated_entries_threshold = 0;
        std::chrono::milliseconds invalidated_entries_removal_interval{0};
        size_t invalidated_entries_removal_batch = 0;
    };

    FileCachePriorityMaintenanceJobs(IFileCachePriority & priority_, const Settings & settings_);

    void addTo(FileCacheMaintenanceTask & task);

private:
    TimePoint removeInvalidatedEntriesIfDue(TimePoint now);

    IFileCachePriority & priority;
    const Settings settings;
    /// Set to the creation time, so that the first removal waits for a whole interval.
    TimePoint last_invalidated_entries_removal;
};

}
