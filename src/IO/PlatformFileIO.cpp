#include <IO/PlatformFileIO.h>

#include <base/pathToString.h>

#include <algorithm>
#include <cerrno>
#include <limits>

#include <fcntl.h>
#include <sys/stat.h>

#if defined(OS_WINDOWS)
#include <io.h>
#include <sys/utime.h>
#include <Poco/UnWindows.h>
#else
#include <sys/file.h>
#include <unistd.h>
#include <utime.h>
#endif

namespace DB
{

#if defined(OS_WINDOWS)

namespace
{

/// Shared by all four: resolve the descriptor to a handle, clamp the count to the `DWORD` the
/// Win32 calls take, and optionally point at an explicit offset.
HANDLE toHandle(int fd)
{
    auto * handle = reinterpret_cast<HANDLE>(_get_osfhandle(fd));
    if (handle == INVALID_HANDLE_VALUE)
        errno = EBADF;
    return handle;
}

DWORD clampCount(size_t bytes)
{
    return static_cast<DWORD>(std::min<size_t>(bytes, std::numeric_limits<DWORD>::max()));
}

OVERLAPPED makeOffset(size_t offset)
{
    OVERLAPPED overlapped{};
    overlapped.Offset = static_cast<DWORD>(offset & 0xFFFFFFFFull);
    overlapped.OffsetHigh = static_cast<DWORD>(offset >> 32);
    return overlapped;
}

}

Int64 platformRead(int fd, char * to, size_t bytes)
{
    auto * handle = toHandle(fd);
    if (handle == INVALID_HANDLE_VALUE)
        return -1;

    DWORD bytes_read = 0;
    if (!ReadFile(handle, to, clampCount(bytes), &bytes_read, nullptr))
    {
        /// A pipe (or redirected stdin) whose write end has been closed reports the normal end
        /// of the stream as `ERROR_BROKEN_PIPE`, not `ERROR_HANDLE_EOF`; POSIX `read` returns 0
        /// there. `ERROR_NO_DATA` is the same condition on a nonblocking pipe.
        const DWORD error = GetLastError();
        if (error == ERROR_HANDLE_EOF || error == ERROR_BROKEN_PIPE || error == ERROR_NO_DATA)
            return 0;
        errno = EIO;
        return -1;
    }
    return bytes_read;
}

Int64 platformPRead(int fd, char * to, size_t bytes, size_t offset)
{
    auto * handle = toHandle(fd);
    if (handle == INVALID_HANDLE_VALUE)
        return -1;

    OVERLAPPED overlapped = makeOffset(offset);
    DWORD bytes_read = 0;
    if (!ReadFile(handle, to, clampCount(bytes), &bytes_read, &overlapped))
    {
        /// Reading at or past the end with an explicit offset is reported as an error rather than
        /// as a short read; it is the end of the file, not a failure.
        if (GetLastError() == ERROR_HANDLE_EOF)
            return 0;
        errno = EIO;
        return -1;
    }
    return bytes_read;
}

Int64 platformWrite(int fd, const char * from, size_t bytes)
{
    auto * handle = toHandle(fd);
    if (handle == INVALID_HANDLE_VALUE)
        return -1;

    DWORD bytes_written = 0;
    if (!WriteFile(handle, from, clampCount(bytes), &bytes_written, nullptr))
    {
        errno = EIO;
        return -1;
    }
    return bytes_written;
}

int platformFDataSync(int fd)
{
    return ::_commit(fd);
}

namespace
{

int lockFile(int fd, DWORD flags)
{
    auto * handle = toHandle(fd);
    if (handle == INVALID_HANDLE_VALUE)
        return -1;

    OVERLAPPED overlapped{};
    if (!LockFileEx(handle, flags, 0, MAXDWORD, MAXDWORD, &overlapped))
    {
        errno = GetLastError() == ERROR_LOCK_VIOLATION ? EWOULDBLOCK : EACCES;
        return -1;
    }
    return 0;
}

}

int platformLockFileExclusive(int fd, bool blocking)
{
    return lockFile(fd, LOCKFILE_EXCLUSIVE_LOCK | (blocking ? 0u : LOCKFILE_FAIL_IMMEDIATELY));
}

int platformLockFileShared(int fd, bool blocking)
{
    return lockFile(fd, blocking ? 0u : LOCKFILE_FAIL_IMMEDIATELY);
}

int platformUnlockFile(int fd)
{
    auto * handle = toHandle(fd);
    if (handle == INVALID_HANDLE_VALUE)
        return -1;

    OVERLAPPED overlapped{};
    if (!UnlockFileEx(handle, 0, MAXDWORD, MAXDWORD, &overlapped))
    {
        errno = EACCES;
        return -1;
    }
    return 0;
}

int platformTruncate(const std::string & path, UInt64 size)
{
    auto * handle = CreateFileW(
        pathFromString(path).c_str(), GENERIC_WRITE, FILE_SHARE_READ, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (handle == INVALID_HANDLE_VALUE)
    {
        errno = GetLastError() == ERROR_FILE_NOT_FOUND ? ENOENT : EACCES;
        return -1;
    }

    LARGE_INTEGER position;
    position.QuadPart = static_cast<LONGLONG>(size);
    const bool ok = SetFilePointerEx(handle, position, nullptr, FILE_BEGIN) && SetEndOfFile(handle);
    CloseHandle(handle);

    if (!ok)
    {
        errno = EIO;
        return -1;
    }
    return 0;
}

int platformOpenReadWrite(const std::string & path)
{
    return ::_wopen(pathFromString(path).c_str(), _O_RDWR | _O_BINARY);
}

int platformOpenFile(const std::string & path, int flags, int mode)
{
    return ::_wopen(pathFromString(path).c_str(), flags | _O_BINARY, mode);
}

int platformStat(const std::string & path, struct stat & out)
{
    return ::wstat(pathFromString(path).c_str(), &out);
}

int platformUnlink(const std::string & path)
{
    return ::_wunlink(pathFromString(path).c_str());
}

int platformRmdir(const std::string & path)
{
    return ::_wrmdir(pathFromString(path).c_str());
}

int platformSetFileTimes(const std::string & path, time_t access_time, time_t modification_time)
{
    struct _utimbuf times{};
    times.actime = access_time;
    times.modtime = modification_time;
    return ::_wutime(pathFromString(path).c_str(), &times);
}

namespace
{

int fileVersionFromHandle(HANDLE handle, PlatformFileVersion & out)
{
    BY_HANDLE_FILE_INFORMATION info{};
    if (!GetFileInformationByHandle(handle, &info))
    {
        errno = EIO;
        return -1;
    }

    /// `FILETIME` counts 100-nanosecond ticks since 1601-01-01; shift it to the Unix epoch.
    const auto ticks = (static_cast<UInt64>(info.ftLastWriteTime.dwHighDateTime) << 32) | info.ftLastWriteTime.dwLowDateTime;
    constexpr UInt64 ticks_per_second = 10'000'000;
    constexpr Int64 seconds_between_epochs = 11'644'473'600;
    out.mtime_sec = static_cast<Int64>(ticks / ticks_per_second) - seconds_between_epochs;
    out.mtime_nsec = static_cast<Int64>(ticks % ticks_per_second) * 100;
    out.device_id = info.dwVolumeSerialNumber;
    out.file_id = (static_cast<UInt64>(info.nFileIndexHigh) << 32) | info.nFileIndexLow;
    out.size = (static_cast<UInt64>(info.nFileSizeHigh) << 32) | info.nFileSizeLow;
    return 0;
}

}

int platformFileVersion(const std::string & path, PlatformFileVersion & out)
{
    /// `FILE_FLAG_BACKUP_SEMANTICS` so that a directory opens too, as `stat` allows.
    auto * handle = CreateFileW(
        pathFromString(path).c_str(),
        FILE_READ_ATTRIBUTES,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        nullptr,
        OPEN_EXISTING,
        FILE_FLAG_BACKUP_SEMANTICS,
        nullptr);

    if (handle == INVALID_HANDLE_VALUE)
    {
        errno = GetLastError() == ERROR_FILE_NOT_FOUND || GetLastError() == ERROR_PATH_NOT_FOUND ? ENOENT : EACCES;
        return -1;
    }

    const int res = fileVersionFromHandle(handle, out);
    CloseHandle(handle);
    return res;
}

int platformFileVersionOfDescriptor(int fd, PlatformFileVersion & out)
{
    auto * handle = toHandle(fd);
    if (handle == INVALID_HANDLE_VALUE)
        return -1;
    return fileVersionFromHandle(handle, out);
}

int platformOpenDirectory(const std::string & path)
{
    /// The only consumer of this descriptor is `platformFDataSync`, i.e. `_commit`, i.e.
    /// `FlushFileBuffers` - which requires the handle to have the `GENERIC_WRITE` access
    /// right. A read-only handle would make every directory sync (`LocalDirectorySyncGuard`)
    /// fail with `EBADF` instead of flushing the directory's metadata.
    auto * handle = CreateFileW(
        pathFromString(path).c_str(),
        GENERIC_READ | GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        nullptr,
        OPEN_EXISTING,
        FILE_FLAG_BACKUP_SEMANTICS,
        nullptr);

    if (handle == INVALID_HANDLE_VALUE)
    {
        errno = GetLastError() == ERROR_FILE_NOT_FOUND || GetLastError() == ERROR_PATH_NOT_FOUND ? ENOENT : EACCES;
        return -1;
    }

    const int fd = _open_osfhandle(reinterpret_cast<intptr_t>(handle), 0);
    if (fd == -1)
    {
        CloseHandle(handle);
        errno = EMFILE;
    }
    return fd;
}

#else

Int64 platformRead(int fd, char * to, size_t bytes)
{
    return ::read(fd, to, bytes);
}

Int64 platformPRead(int fd, char * to, size_t bytes, size_t offset)
{
    return ::pread(fd, to, bytes, static_cast<off_t>(offset));
}

Int64 platformWrite(int fd, const char * from, size_t bytes)
{
    return ::write(fd, from, bytes);
}

int platformFDataSync(int fd)
{
#if defined(OS_DARWIN)
    /// macOS has `fdatasync` only as a stub; `fsync` is the honest one there.
    return ::fsync(fd);
#else
    return ::fdatasync(fd);
#endif
}

int platformLockFileExclusive(int fd, bool blocking)
{
    return ::flock(fd, LOCK_EX | (blocking ? 0 : LOCK_NB));
}

int platformLockFileShared(int fd, bool blocking)
{
    return ::flock(fd, LOCK_SH | (blocking ? 0 : LOCK_NB));
}

int platformUnlockFile(int fd)
{
    return ::flock(fd, LOCK_UN);
}

int platformTruncate(const std::string & path, UInt64 size)
{
    return ::truncate(path.c_str(), static_cast<off_t>(size));
}

int platformOpenReadWrite(const std::string & path)
{
    return ::open(path.c_str(), O_RDWR);
}

int platformOpenDirectory(const std::string & path)
{
    /// macOS has no `O_DIRECTORY`; a plain read-only open of a directory works there.
#if defined(O_DIRECTORY)
    return ::open(path.c_str(), O_DIRECTORY);
#else
    return ::open(path.c_str(), O_RDONLY);
#endif
}

int platformOpenFile(const std::string & path, int flags, int mode)
{
    return ::open(path.c_str(), flags, mode);
}

int platformStat(const std::string & path, struct stat & out)
{
    return ::stat(path.c_str(), &out);
}

int platformUnlink(const std::string & path)
{
    return ::unlink(path.c_str());
}

int platformRmdir(const std::string & path)
{
    return ::rmdir(path.c_str());
}

int platformSetFileTimes(const std::string & path, time_t access_time, time_t modification_time)
{
    struct utimbuf times{};
    times.actime = access_time;
    times.modtime = modification_time;
    return ::utime(path.c_str(), &times);
}

namespace
{

void fileVersionFromStat(const struct stat & st, PlatformFileVersion & out)
{
#if defined(OS_DARWIN)
    out.mtime_sec = st.st_mtimespec.tv_sec;
    out.mtime_nsec = st.st_mtimespec.tv_nsec;
#else
    out.mtime_sec = st.st_mtim.tv_sec;
    out.mtime_nsec = st.st_mtim.tv_nsec;
#endif
    out.device_id = st.st_dev;
    out.file_id = st.st_ino;
    out.size = st.st_size;
}

}

int platformFileVersion(const std::string & path, PlatformFileVersion & out)
{
    struct stat st{};
    if (0 != ::stat(path.c_str(), &st))
        return -1;
    fileVersionFromStat(st, out);
    return 0;
}

int platformFileVersionOfDescriptor(int fd, PlatformFileVersion & out)
{
    struct stat st{};
    if (0 != ::fstat(fd, &st))
        return -1;
    fileVersionFromStat(st, out);
    return 0;
}

#endif

}
