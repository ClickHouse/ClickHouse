#pragma once

#include <Coordination/Storage/SortedFile.h>
#include <Coordination/Storage/SortedRun.h>
#include <Common/ThreadPool.h>

#include <atomic>
#include <map>
#include <mutex>
#include <semaphore>
#include <set>
#include <string>
#include <vector>

namespace Coordination::Storage
{

struct StorageState;

/// Manages flushes and merges.
struct BackgroundWork
{
    StorageState * storage = nullptr;

    std::atomic<bool> shutting_down{false};

    std::vector<ThreadFromGlobalPool> flush_threads;
    std::vector<ThreadFromGlobalPool> merge_threads;

    /// Wake up threads. Can't use condition_variable because it's not compatible with
    /// DB::SharedMutex (and std::condition_variable_any seems more sketchy than counting_semaphore).
    std::counting_semaphore<> flush_requests{0};
    std::counting_semaphore<> merge_requests{0};

    /// Locked for planning and for publishing results.
    /// Can be locked before storage_mutex, not after.
    std::mutex mutex;

    /// file_seqno/min_file_seqno of busy memtables and files.
    std::set<uint32_t> flushes_in_progress;
    std::set<uint32_t> merges_in_progress;

    /// Number of merges currently running. Atomic so that fillAsynchronousMetrics can read it
    /// without locking `mutex` (which can't be locked while holding storage_mutex).
    std::atomic<size_t> merges_running{0};

    /// If concurrent memtable flushes finish out of order, we hold the results in this reorder
    /// buffer to publish to StorageState in file_seqno order.
    std::map<uint32_t, SortedRunPtr> flushed_files;

    /// Files to delete from disk. Filled by ~SortedFile, drained by flush and merge threads.
    FileDeleteQueuePtr file_delete_queue = std::make_shared<FileDeleteQueue>();

    /// Starts the threads.
    explicit BackgroundWork(StorageState * storage_);

    void shutdown();

    void maybeStartFlush();
    void maybeStartMerge();

private:
    void flushThread();
    void mergeThread();

    /// Perform pending file deletions. Called from flush and merge threads; there are no dedicated
    /// deletion threads.
    /// Returns false if a deletion failed (the file stays queued). The caller should then back
    /// off and retry instead of proceeding: if we can't delete files, we shouldn't keep
    /// producing them.
    bool deleteQueuedFiles() const;

    bool pickRunsToMerge(std::vector<SortedRunPtr> & out_inputs, SortedRunPtr & out_partial_output, std::vector<uint32_t> & out_seqnos_to_lock) const;
};

}
