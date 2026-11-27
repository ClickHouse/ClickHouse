#include <Interpreters/FileCache/FileCachePriorityMaintenanceJobs.h>

#include <Interpreters/FileCache/IFileCachePriority.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <base/scope_guard.h>

namespace ProfileEvents
{
    extern const Event FilesystemCacheInvalidatedEntriesCleanupThreadWorkMilliseconds;
}

namespace DB
{

FileCachePriorityMaintenanceJobs::FileCachePriorityMaintenanceJobs(IFileCachePriority & priority_, const Settings & settings_)
    : priority(priority_)
    , settings(settings_)
    , last_invalidated_entries_removal(FileCacheMaintenanceTask::Clock::now())
{
}

void FileCachePriorityMaintenanceJobs::addTo(FileCacheMaintenanceTask & task)
{
    task.addJob("remove invalidated entries", [this](TimePoint now) { return removeInvalidatedEntriesIfDue(now); });
}

FileCachePriorityMaintenanceJobs::TimePoint FileCachePriorityMaintenanceJobs::removeInvalidatedEntriesIfDue(TimePoint now)
{
    if (priority.getInvalidatedEntriesCountApprox() < settings.invalidated_entries_threshold
        && now - last_invalidated_entries_removal < settings.invalidated_entries_removal_interval)
        return last_invalidated_entries_removal + settings.invalidated_entries_removal_interval;

    last_invalidated_entries_removal = now;
    Stopwatch watch;
    SCOPE_EXIT({
        ProfileEvents::increment(
            ProfileEvents::FilesystemCacheInvalidatedEntriesCleanupThreadWorkMilliseconds, watch.elapsedMilliseconds());
    });
    const size_t removed_entries = priority.removeInvalidatedEntries(settings.invalidated_entries_removal_batch);

    /// A full batch likely means that there is more to remove.
    return removed_entries == settings.invalidated_entries_removal_batch ? now : now + settings.invalidated_entries_removal_interval;
}

}
