#include <Storages/MergeTree/ParallelSyncFiles.h>

#include <Common/threadPoolCallbackRunner.h>
#include <Common/setThreadName.h>
#include <IO/SharedThreadPools.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Storages/MergeTree/MergeTreeWriterStream.h>
#include <Common/ErrnoException.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

#include <base/scope_guard.h>

#include <array>
#include <map>
#include <mutex>

#include <fcntl.h>
#include <sys/stat.h>
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
    extern const int CANNOT_FSTAT;
    extern const int CANNOT_FSYNC;
}

namespace FailPoints
{
    extern const char parallel_sync_paths_fail_fsync[];
}

namespace
{

/// The files, for which the fsync failed. After a failed fsync, the kernel may report success
/// for the fsync of the same file through a new file descriptor, although the data was not written
/// (see "Handling errors during writeback" in the kernel documentation of VFS). So every later attempt
/// to sync such a file fails as well. For example, a retry of the insert that is deduplicated against
/// the part with this file, or a part produced by a mutation that hardlinks this file.
///
/// The files are identified by their inodes, which are shared by the hardlinks. The failed files are kept open,
/// so that their inodes are not reused for other files. Such failures are rare, so it costs nothing.
///
/// The attempts to sync the same file are serialized, so that another attempt cannot start the fsync after this one
/// consumed the error, but succeed before the failure is remembered.
class FailedFiles
{
public:
    static FailedFiles & instance()
    {
        static FailedFiles failed_files;
        return failed_files;
    }

    std::mutex & getMutex(const struct stat & st)
    {
        return mutexes[(st.st_dev * 0x9E3779B97F4A7C15ULL + st.st_ino) % num_mutexes];
    }

    /// Must be called under the mutex for this file.
    bool contains(const struct stat & st)
    {
        std::lock_guard lock(files_mutex);
        return files.contains({st.st_dev, st.st_ino});
    }

    /// Must be called under the mutex for this file. Takes the ownership of the file descriptor.
    void add(const struct stat & st, int fd)
    {
        std::lock_guard lock(files_mutex);
        if (!files.emplace(std::pair{st.st_dev, st.st_ino}, fd).second)
        {
            [[maybe_unused]] int err = ::close(fd);
        }
    }

private:
    static constexpr size_t num_mutexes = 1024;
    std::array<std::mutex, num_mutexes> mutexes;

    std::mutex files_mutex;
    std::map<std::pair<dev_t, ino_t>, int> files;
};

void syncPath(const std::string & path)
{
    int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd == -1)
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_OPEN_FILE, path, "Cannot open file {} to fsync", path);

    SCOPE_EXIT({
        /// The file is only read, and the result of the sync is checked below, so a failed `close` loses nothing.
        if (fd != -1)
        {
            [[maybe_unused]] int err = ::close(fd);
        }
    });

    struct stat st{};
    if (::fstat(fd, &st) == -1)
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_FSTAT, path, "Cannot fstat {}", path);

    auto & failed_files = FailedFiles::instance();
    std::lock_guard lock(failed_files.getMutex(st));

    if (failed_files.contains(st))
        throw Exception(ErrorCodes::CANNOT_FSYNC, "The fsync of file {} failed earlier, so its data may not be durable", path);

    ProfileEvents::increment(ProfileEvents::FileSync);
    Stopwatch watch;
#if defined(OS_DARWIN)
    int res = ::fsync(fd);
#else
    int res = ::fdatasync(fd);
#endif
    ProfileEvents::increment(ProfileEvents::FileSyncElapsedMicroseconds, watch.elapsedMicroseconds());

    fiu_do_on(FailPoints::parallel_sync_paths_fail_fsync,
    {
        res = -1;
        errno = EIO;
    });

    if (res == -1)
    {
        int saved_errno = errno;
        failed_files.add(st, fd);
        fd = -1;
        errno = saved_errno;
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_FSYNC, path, "Cannot fsync {}", path);
    }
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

void parallelSyncPaths(const std::vector<std::string> & paths)
{
    /// Group the files into batches, so that the number of tasks does not depend on the number of files.
    static constexpr size_t batch_size = 64;
    std::vector<std::pair<size_t, size_t>> batches;
    for (size_t i = 0; i < paths.size(); i += batch_size)
        batches.emplace_back(i, std::min(paths.size(), i + batch_size));

    /// The tasks may not capture locals by reference, see `parallelSyncImpl`.
    const auto * paths_ptr = &paths;
    parallelSyncImpl(batches, [paths_ptr](const std::pair<size_t, size_t> & batch)
    {
        /// Sync all files of the batch even if some of them fail, so that all failed files are remembered.
        std::exception_ptr first_exception;
        for (size_t i = batch.first; i < batch.second; ++i)
        {
            try
            {
                syncPath((*paths_ptr)[i]);
            }
            catch (...)
            {
                if (!first_exception)
                    first_exception = std::current_exception();
            }
        }
        if (first_exception)
            std::rethrow_exception(first_exception);
    });
}

}
