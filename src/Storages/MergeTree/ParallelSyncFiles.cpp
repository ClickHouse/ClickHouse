#include <Storages/MergeTree/ParallelSyncFiles.h>

#include <Common/threadPoolCallbackRunner.h>
#include <Common/setThreadName.h>
#include <IO/SharedThreadPools.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Storages/MergeTree/MergeTreeWriterStream.h>
#include <Common/ErrnoException.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

#include <base/scope_guard.h>

#include <fcntl.h>
#include <unistd.h>


namespace ProfileEvents
{
    extern const Event FileSync;
    extern const Event FileSyncElapsedMicroseconds;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_FSYNC;
}

namespace
{

void syncPath(const std::string & path)
{
    int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd == -1)
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_OPEN_FILE, path, "Cannot open file {} to fsync", path);

    SCOPE_EXIT({
        /// The file is only read, and the result of the sync is checked below, so a failed `close` loses nothing.
        [[maybe_unused]] int err = ::close(fd);
    });

    ProfileEvents::increment(ProfileEvents::FileSync);
    Stopwatch watch;
#if defined(OS_DARWIN)
    int res = ::fsync(fd);
#else
    int res = ::fdatasync(fd);
#endif
    ProfileEvents::increment(ProfileEvents::FileSyncElapsedMicroseconds, watch.elapsedMicroseconds());

    if (res == -1)
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_FSYNC, path, "Cannot fsync {}", path);
}

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

void parallelSyncPaths(const std::vector<std::string> & paths, const std::function<void(size_t)> & on_failure)
{
    /// Group the files into batches, so that the number of tasks does not depend on the number of files.
    static constexpr size_t batch_size = 64;
    std::vector<std::pair<size_t, size_t>> batches;
    for (size_t i = 0; i < paths.size(); i += batch_size)
        batches.emplace_back(i, std::min(paths.size(), i + batch_size));

    /// The tasks may not capture locals by reference, see `parallelSyncImpl`.
    const auto * paths_ptr = &paths;
    const auto * on_failure_ptr = &on_failure;
    parallelSyncImpl(batches, [paths_ptr, on_failure_ptr](const std::pair<size_t, size_t> & batch)
    {
        for (size_t i = batch.first; i < batch.second; ++i)
        {
            try
            {
                syncPath((*paths_ptr)[i]);
            }
            catch (...)
            {
                (*on_failure_ptr)(i);
                throw;
            }
        }
    });
}

}
