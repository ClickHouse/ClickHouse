#include <IO/PlatformFileIO.h>

#include <algorithm>
#include <cerrno>
#include <limits>

#if defined(OS_WINDOWS)
#include <io.h>
#include <Poco/UnWindows.h>
#else
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

#endif

}
