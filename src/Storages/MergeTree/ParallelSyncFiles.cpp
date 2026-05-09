#include <Storages/MergeTree/ParallelSyncFiles.h>

#include <Common/threadPoolCallbackRunner.h>
#include <Common/setThreadName.h>
#include <IO/SharedThreadPools.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Storages/MergeTree/MergeTreeWriterStream.h>


namespace DB
{

namespace
{

template <typename Container, typename SyncFn>
void parallelSyncImpl(const Container & items, SyncFn && sync_fn)
{
    if (items.empty())
        return;

    if (items.size() == 1 || !getIOThreadPool().isInitialized())
    {
        for (const auto & item : items)
            sync_fn(item);
        return;
    }

    /// fsync calls on different files are independent at the kernel level
    /// (subject to underlying device serialization), so we issue them in parallel.
    /// Submitting a few tasks to IOThreadPool is cheap relative to fsync latency.
    ThreadPoolCallbackRunnerLocal<void> runner(getIOThreadPool().get(), ThreadName::DEFAULT_THREAD_POOL);
    for (const auto & item : items)
    {
        /// Capture both `item` (a pointer) and `sync_fn` by value: `ThreadPoolCallbackRunnerLocal`
        /// forbids capturing locals by reference, since an exception while enqueuing could unwind
        /// the caller's stack before the runner's destructor waits for already-running tasks.
        runner.enqueueAndKeepTrack([item, sync_fn]() { sync_fn(item); });
    }
    runner.waitForAllToFinishAndRethrowFirstError();
}

}

void parallelSyncFiles(const std::vector<WriteBufferFromFileBase *> & files)
{
    parallelSyncImpl(files, [](WriteBufferFromFileBase * file) { file->sync(); });
}

void parallelSyncFiles(const std::vector<const MergeTreeWriterStream *> & streams)
{
    parallelSyncImpl(streams, [](const MergeTreeWriterStream * stream) { stream->sync(); });
}

}
