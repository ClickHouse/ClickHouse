#include <Storages/MergeTree/ParallelSyncFiles.h>

#include <Common/threadPoolCallbackRunner.h>
#include <Common/setThreadName.h>
#include <IO/PlatformFileIO.h>
#include <IO/SharedThreadPools.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Storages/MergeTree/MergeTreeWriterStream.h>
#include <Common/ErrnoException.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <base/sanitizer_defs.h>

#include <base/scope_guard.h>

#include <array>
#include <atomic>
#include <map>
#include <mutex>

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
    extern const int CANNOT_FSTAT;
    extern const int CANNOT_FSYNC;
}

namespace FailPoints
{
    extern const char parallel_sync_paths_fail_fsync[];
    extern const char parallel_sync_paths_interrupt_fsync[];
}

namespace
{

/// The files, for which the fsync failed. After a failed fsync, the kernel may report success
/// for the fsync of the same file through a new file descriptor, although the data was not written
/// (see "Handling errors during writeback" in the kernel documentation of VFS). So every later attempt
/// to sync such a file fails as well. For example, a retry of the insert that is deduplicated against
/// the part with this file, or a part produced by a mutation that hardlinks this file.
///
/// The files are identified by their inodes, which are shared by the hardlinks: the device and the inode on POSIX,
/// the volume serial number and the file index on Windows (the `st_dev` and `st_ino` of its CRT `fstat` are
/// zero for every file, so they cannot tell files apart), see `PlatformFileVersion`. The failed files are kept open,
/// so that their inodes are not reused for other files. When the last hardlink of a failed file is removed
/// (for example, its part is merged away or its table is dropped), no new attempt to sync it is possible,
/// so its descriptor is closed, and its disk space is released, see `releaseUnlinked`.
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

    /// The hash of the file identity wraps around modulo 2^64 on purpose.
    NO_SANITIZE_UNSIGNED_OVERFLOW std::mutex & getMutex(const PlatformFileVersion & file)
    {
        return mutexes[(file.device_id * 0x9E3779B97F4A7C15ULL + file.file_id) % num_mutexes];
    }

    /// Must be called under the mutex for this file.
    bool contains(const PlatformFileVersion & file)
    {
        std::lock_guard lock(files_mutex);
        return files.contains({file.device_id, file.file_id});
    }

    /// Must be called under the mutex for this file. Takes the ownership of the file descriptor.
    void add(const PlatformFileVersion & file, int fd)
    {
        std::lock_guard lock(files_mutex);
        if (!files.emplace(std::pair{file.device_id, file.file_id}, fd).second)
        {
            [[maybe_unused]] int err = ::close(fd);
        }
        has_files.store(true, std::memory_order_relaxed);
    }

    /// Close the failed files that have no hardlinks left.
    void releaseUnlinked()
    {
        if (!has_files.load(std::memory_order_relaxed))
            return;

        std::vector<PlatformFileVersion> unlinked;
        {
            std::lock_guard lock(files_mutex);
            for (const auto & [key, fd] : files)
            {
                if (platformIsDescriptorUnlinked(fd) == 1)
                {
                    PlatformFileVersion file;
                    file.device_id = key.first;
                    file.file_id = key.second;
                    unlinked.push_back(file);
                }
            }
        }

        for (const auto & file : unlinked)
        {
            /// Serialize with the attempts to sync this file, which take the mutex for the file before `files_mutex`.
            std::lock_guard file_lock(getMutex(file));
            std::lock_guard lock(files_mutex);
            auto it = files.find({file.device_id, file.file_id});
            if (it == files.end())
                continue;

            /// The snapshot above is stale: another call could have released this file already, and its inode
            /// could be reused for a new file, whose fsync failed as well. So check again that the file is unlinked.
            if (platformIsDescriptorUnlinked(it->second) != 1)
                continue;

            [[maybe_unused]] int err = ::close(it->second);
            files.erase(it);
        }

        std::lock_guard lock(files_mutex);
        has_files.store(!files.empty(), std::memory_order_relaxed);
    }

private:
    static constexpr size_t num_mutexes = 1024;
    std::array<std::mutex, num_mutexes> mutexes;

    std::mutex files_mutex;
    std::map<std::pair<UInt64, UInt64>, int> files;
    std::atomic<bool> has_files{false};
};

void syncPath(const std::string & path)
{
#if defined(OS_WINDOWS)
    /// `platformFDataSync` is `_commit`, i.e. `FlushFileBuffers`, which fails with access denied on
    /// a handle without `GENERIC_WRITE`. POSIX lets a read-only descriptor flush the file it names.
    int fd = platformOpenFile(path, O_WRONLY | O_CLOEXEC);
#else
    int fd = platformOpenFile(path, O_RDONLY | O_CLOEXEC);
#endif
    if (fd == -1)
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_OPEN_FILE, path, "Cannot open file {} to fsync", path);

    SCOPE_EXIT({
        /// Nothing is written through this descriptor, and the result of the sync is checked below, so a failed `close` loses nothing.
        if (fd != -1)
        {
            [[maybe_unused]] int err = ::close(fd);
        }
    });

    PlatformFileVersion file;
    if (platformFileVersionOfDescriptor(fd, file) == -1)
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_FSTAT, path, "Cannot fstat {}", path);

    auto & failed_files = FailedFiles::instance();
    std::lock_guard lock(failed_files.getMutex(file));

    if (failed_files.contains(file))
        throw Exception(ErrorCodes::CANNOT_FSYNC, "The fsync of file {} failed earlier, so its data may not be durable", path);

    int res = 0;
    [[maybe_unused]] bool injected_interrupt = false;
    while (true)
    {
        ProfileEvents::increment(ProfileEvents::FileSync);
        Stopwatch watch;
        res = platformFDataSync(fd);
        ProfileEvents::increment(ProfileEvents::FileSyncElapsedMicroseconds, watch.elapsedMicroseconds());

        fiu_do_on(FailPoints::parallel_sync_paths_interrupt_fsync,
        {
            if (!injected_interrupt)
            {
                injected_interrupt = true;
                res = -1;
                errno = EINTR;
            }
        });

        fiu_do_on(FailPoints::parallel_sync_paths_fail_fsync,
        {
            res = -1;
            errno = EIO;
        });

        /// An interrupted sync says nothing about the data of the file, so it is retried instead of marking the file as failed.
        if (res == -1 && errno == EINTR)
            continue;
        break;
    }

    if (res == -1)
    {
        int saved_errno = errno;
        failed_files.add(file, fd);
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

void parallelSyncPaths(const std::vector<std::string> & paths, std::vector<UInt8> & failed)
{
    failed.assign(paths.size(), 0);

    FailedFiles::instance().releaseUnlinked();

    /// Group the files into batches, so that the number of tasks does not depend on the number of files.
    static constexpr size_t batch_size = 64;
    std::vector<std::pair<size_t, size_t>> batches;
    for (size_t i = 0; i < paths.size(); i += batch_size)
        batches.emplace_back(i, std::min(paths.size(), i + batch_size));

    /// The tasks may not capture locals by reference, see `parallelSyncImpl`.
    const auto * paths_ptr = &paths;
    auto * failed_ptr = &failed;
    parallelSyncImpl(batches, [paths_ptr, failed_ptr](const std::pair<size_t, size_t> & batch)
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
                /// Every task writes only the elements of its own batch.
                if (getCurrentExceptionCode() == ErrorCodes::CANNOT_FSYNC)
                    (*failed_ptr)[i] = 1;
                if (!first_exception)
                    first_exception = std::current_exception();
            }
        }
        if (first_exception)
            std::rethrow_exception(first_exception);
    });
}

}
