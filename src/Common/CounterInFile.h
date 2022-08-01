#pragma once

#include <fcntl.h>
#include <sys/file.h>

#include <string>
#include <iostream>
#include <mutex>
#include <filesystem>

#include <Poco/Exception.h>

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Common/Exception.h>
#include <base/types.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int CANNOT_OPEN_FILE;
        extern const int CANNOT_READ_ALL_DATA;
        extern const int ATTEMPT_TO_READ_AFTER_EOF;
    }
}

namespace fs = std::filesystem;

/** Stores a number in the file.
 * Designed for rare calls (not designed for performance).
 */
class CounterInFile
{
private:
    static inline constexpr size_t SMALL_READ_WRITE_BUFFER_SIZE = 16;

public:
    /// path - the name of the file, including the path
    explicit CounterInFile(const std::string & path_) : path(path_) {}

    /** Add `delta` to the number in the file and return the new value.
     * If the `create_if_need` parameter is not set to true, then
     *  the file should already have a number written (if not - create the file manually with zero).
     *
     * To protect against race conditions between different processes, file locks are used.
     * (But when the first file is created, the race condition is possible, so it's better to create the file in advance.)
     *
     * `locked_callback` is called when the counter file is locked. A new value is passed to it.
     * `locked_callback` can be used to do something atomically with incrementing the counter (for example, renaming files).
     */
    template <typename Callback>
    Int64 add(Int64 delta, Callback && locked_callback, bool create_if_need = false)
    {
        std::lock_guard lock(mutex);

        Int64 res = -1;

        bool file_doesnt_exists = !fs::exists(path);
        if (file_doesnt_exists && !create_if_need)
        {
            throw Poco::Exception("File " + path + " does not exist. "
            "You must create it manually with appropriate value or 0 for first start.");
        }

        int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0666);
        if (-1 == fd)
            DB::throwFromErrnoWithPath("Cannot open file " + path, path, DB::ErrorCodes::CANNOT_OPEN_FILE);

        try
        {
            int flock_ret = flock(fd, LOCK_EX);
            if (-1 == flock_ret)
                DB::throwFromErrnoWithPath("Cannot lock file " + path, path, DB::ErrorCodes::CANNOT_OPEN_FILE);

            if (!file_doesnt_exists)
            {
                DB::ReadBufferFromFileDescriptor rb(fd, SMALL_READ_WRITE_BUFFER_SIZE);
                try
                {
                    DB::readIntText(res, rb);
                }
                catch (const DB::Exception & e)
                {
                    /// A more understandable error message.
                    if (e.code() == DB::ErrorCodes::CANNOT_READ_ALL_DATA || e.code() == DB::ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
                        throw DB::ParsingException("File " + path + " is empty. You must fill it manually with appropriate value.", e.code());
                    else
                        throw;
                }
            }
            else
                res = 0;

            if (delta || file_doesnt_exists)
            {
                res += delta;

                DB::WriteBufferFromFileDescriptor wb(fd, SMALL_READ_WRITE_BUFFER_SIZE);
                wb.seek(0, SEEK_SET);
                wb.truncate(0);
                DB::writeIntText(res, wb);
                DB::writeChar('\n', wb);
                wb.sync();
                wb.finalize();
            }

            locked_callback(res);
        }
        catch (...)
        {
            close(fd);
            throw;
        }

        close(fd);
        return res;
    }

    Int64 add(Int64 delta, bool create_if_need = false)
    {
        return add(delta, [](UInt64){}, create_if_need);
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
            DB::throwFromErrnoWithPath("Cannot open file " + path, path, DB::ErrorCodes::CANNOT_OPEN_FILE);

        try
        {
            bool broken = true;

            if (file_exists)
            {
                DB::ReadBufferFromFileDescriptor rb(fd, SMALL_READ_WRITE_BUFFER_SIZE);
                try
                {
                    UInt64 current_value;
                    DB::readIntText(current_value, rb);
                    char c;
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
                wb.sync();
                wb.finalize();
            }
        }
        catch (...)
        {
            close(fd);
            throw;
        }

        close(fd);
    }

private:
    std::string path;
    std::mutex mutex;
};
