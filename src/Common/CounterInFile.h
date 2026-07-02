#pragma once

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>

#include <functional>
#include <string>
#include <mutex>
#include <filesystem>

#include <Poco/Exception.h>

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>

#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <base/defines.h>
#include <base/types.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int CANNOT_OPEN_FILE;
        extern const int CANNOT_READ_ALL_DATA;
        extern const int ATTEMPT_TO_READ_AFTER_EOF;
        extern const int CANNOT_FSTAT;
    }
}

namespace fs = std::filesystem;

/** Stores a number in the file.
 * Designed for rare calls (not designed for performance).
 */
class CounterInFile
{
private:
    static constexpr size_t SMALL_READ_WRITE_BUFFER_SIZE = 16;

public:
    /// path - the name of the file, including the path
    explicit CounterInFile(const std::string & path_) : path(path_) {}

    /** Add `delta` to the number in the file and return the new value.
     *
     * If `create_if_need` is true, a missing OR empty file is treated as if it
     * contained zero, then the new value is written. An empty file can be left
     * behind by a previous writer that was killed between `truncate(0)` and the
     * subsequent write (the lock is released on process death), or by a
     * concurrent `O_CREAT` race. Recovering in place rather than throwing
     * keeps the counter usable without manual operator intervention.
     *
     * If `create_if_need` is false, a missing OR empty file is an error and
     * the file is NOT created: a missing file surfaces the open error (ENOENT)
     * and an existing empty file is rejected, so a failed probe leaves no
     * counter state behind.
     *
     * `min_initial_value`, if greater than zero, sets a lower bound for the
     * counter value used as the starting point when the file was missing or
     * empty. It is intended for recovery: a caller that knows from external
     * context (for example, the maximum existing backup-directory number in
     * `shadow/`) what the counter must be at can pass it here, so the
     * recovered counter does not collide with already-allocated resources.
     * Values found in a non-empty file are used as-is and are NOT clamped, so
     * a healthy counter is never disturbed.
     *
     * `min_initial_value_provider`, if set, is evaluated to obtain an
     * additional lower bound, but ONLY on the recovery path (missing or empty
     * file) and ONLY while the file is locked. It exists so an expensive or
     * failure-prone computation of the bound (for example, scanning every
     * configured disk for existing backup directories) is not paid on a healthy
     * counter, where the bound is ignored anyway, and so the computation observes
     * a consistent state under the same lock that guards the recovery. A throw
     * from the provider aborts the call without writing the counter.
     *
     * Concurrency: an exclusive `flock` is taken on the file for the duration
     * of the read-modify-write sequence, so concurrent callers in the same or
     * different processes see consistent values.
     *
     * `locked_callback` is called while the file is locked. The new value is
     * passed to it. It can be used to do something atomically with the
     * increment (for example, renaming files).
     */
    template <typename Callback>
    Int64 add(Int64 delta, Callback && locked_callback,
              bool create_if_need = false, Int64 min_initial_value = 0,
              const std::function<Int64()> & min_initial_value_provider = {})
    {
        std::lock_guard lock(mutex);

        Int64 res = -1;

        /// Only create the file when the caller opted in. With create_if_need
        /// false a missing file must surface as ENOENT and must NOT be created:
        /// this is a shared helper and a failed probe must not leave persistent
        /// counter state behind. Existing zero-length files are still handled
        /// under the lock below (recovered when create_if_need, rejected when
        /// not).
        int open_flags = O_RDWR | O_CLOEXEC;
        if (create_if_need)
            open_flags |= O_CREAT;
        int fd = ::open(path.c_str(), open_flags, 0666);
        if (-1 == fd)
            DB::ErrnoException::throwFromPath(DB::ErrorCodes::CANNOT_OPEN_FILE, path, "Cannot open file {}", path);

        try
        {
            int flock_ret = flock(fd, LOCK_EX);
            if (-1 == flock_ret)
                DB::ErrnoException::throwFromPath(DB::ErrorCodes::CANNOT_OPEN_FILE, path, "Cannot lock file {}", path);

            /// Determine the file state UNDER the lock. A pre-lock check is
            /// TOCTOU-unsafe: another process can create or truncate the file
            /// between our check and our `flock`. A size-0 file is also a valid
            /// outcome of an interrupted prior writer and must be treated the
            /// same as a missing file rather than as a fatal corruption.
            struct stat st{};
            if (-1 == ::fstat(fd, &st))
                DB::ErrnoException::throwFromPath(DB::ErrorCodes::CANNOT_FSTAT, path, "Cannot fstat file {}", path);

            bool file_is_empty_or_missing = (st.st_size == 0);

            if (file_is_empty_or_missing && !create_if_need)
            {
                throw Poco::Exception("File " + path + " does not exist or is empty. "
                "You must create it manually with appropriate value or 0 for first start.");
            }

            if (!file_is_empty_or_missing)
            {
                DB::ReadBufferFromFileDescriptor rb(fd, SMALL_READ_WRITE_BUFFER_SIZE);
                DB::readIntText(res, rb);
            }
            else
            {
                /// Recovery path only: the provider can be expensive or can fail
                /// (e.g. a per-disk scan), so it is evaluated here under the lock
                /// rather than eagerly by the caller, and never on a healthy
                /// counter where its bound would be ignored.
                Int64 effective_min = min_initial_value;
                if (min_initial_value_provider)
                    effective_min = std::max(effective_min, min_initial_value_provider());
                res = std::max<Int64>(0, effective_min);
            }

            if (delta || file_is_empty_or_missing)
            {
                res += delta;

                DB::WriteBufferFromFileDescriptor wb(fd, SMALL_READ_WRITE_BUFFER_SIZE);
                wb.seek(0, SEEK_SET);
                wb.truncate(0);
                DB::writeIntText(res, wb);
                DB::writeChar('\n', wb);
                wb.finalize();
                wb.sync();
            }

            locked_callback(res);
        }
        catch (...)
        {
            [[maybe_unused]] int err = close(fd);
            chassert(!err || errno == EINTR);
            throw;
        }

        [[maybe_unused]] int err = close(fd);
        chassert(!err || errno == EINTR);
        return res;
    }

    Int64 add(Int64 delta, bool create_if_need = false, Int64 min_initial_value = 0,
              const std::function<Int64()> & min_initial_value_provider = {})
    {
        return add(delta, [](UInt64){}, create_if_need, min_initial_value, min_initial_value_provider);
    }

    const std::string & getPath() const
    {
        return path;
    }

    /// Change the path to the file.
    void setPath(std::string path_)
    {
        path = path_;
    }

    // Not thread-safe and not synchronized between processes.
    void fixIfBroken(UInt64 value)
    {
        bool file_exists = fs::exists(path);

        int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0666);
        if (-1 == fd)
            DB::ErrnoException::throwFromPath(DB::ErrorCodes::CANNOT_OPEN_FILE, path, "Cannot open file {}", path);

        try
        {
            bool broken = true;

            if (file_exists)
            {
                DB::ReadBufferFromFileDescriptor rb(fd, SMALL_READ_WRITE_BUFFER_SIZE);
                try
                {
                    UInt64 current_value = 0;
                    DB::readIntText(current_value, rb);
                    char c = 0;
                    DB::readChar(c, rb);
                    if (rb.count() > 0 && c == '\n' && rb.eof())
                        broken = false;
                }
                catch (const DB::Exception & e)
                {
                    if (e.code() != DB::ErrorCodes::CANNOT_READ_ALL_DATA && e.code() != DB::ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
                        throw;
                }
            }

            if (broken)
            {
                DB::WriteBufferFromFileDescriptor wb(fd, SMALL_READ_WRITE_BUFFER_SIZE);
                wb.seek(0, SEEK_SET);
                wb.truncate(0);
                DB::writeIntText(value, wb);
                DB::writeChar('\n', wb);
                wb.finalize();
                wb.sync();
            }
        }
        catch (...)
        {
            [[maybe_unused]] int err = close(fd);
            chassert(!err || errno == EINTR);
            throw;
        }

        [[maybe_unused]] int err = close(fd);
        chassert(!err || errno == EINTR);
    }

private:
    std::string path;
    std::mutex mutex;
};
