#include <IO/PlatformFileIO.h>

#include <base/pathToString.h>

#include <algorithm>
#include <cerrno>
#include <limits>

#include <fcntl.h>

#if defined(OS_WINDOWS)
#include <io.h>
#include <Poco/UnWindows.h>
#else
#include <sys/file.h>
#include <unistd.h>
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
        if (GetLastError() == ERROR_HANDLE_EOF)
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

int platformOpenDirectory(const std::string & path)
{
    /// macOS has no `O_DIRECTORY`; a plain read-only open of a directory works there.
#if defined(O_DIRECTORY)
    return ::open(path.c_str(), O_DIRECTORY);
#else
    return ::open(path.c_str(), O_RDONLY);
#endif
}

#endif

}
