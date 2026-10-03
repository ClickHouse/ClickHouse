#pragma once

#include <base/types.h>

#include <cstddef>
#include <ctime>
#include <string>

#include <sys/stat.h>

namespace DB
{

/// The descriptor-level file operations, spelled portably.
///
/// The Windows CRT differs from POSIX in three ways that matter here, none of which a typedef
/// fixes: `_read`/`_write` take their count as an `unsigned int` rather than a `size_t`; there is
/// no `pread`/`pwrite` at all; and `fdatasync` is spelled `_commit`. The positional forms go
/// through `ReadFile`/`WriteFile` with an `OVERLAPPED` offset, which - unlike `_lseeki64` followed
/// by `_read` - leaves the descriptor's shared file position alone, as `pread` promises and as
/// concurrent readers of one file depend on.
///
/// All of these report failure the way the POSIX calls do: `-1` with `errno` set.

/// `read(2)`.
Int64 platformRead(int fd, char * to, size_t bytes);

/// `pread(2)`: read at `offset` without moving the descriptor's position.
Int64 platformPRead(int fd, char * to, size_t bytes, size_t offset);

/// `write(2)`.
Int64 platformWrite(int fd, const char * from, size_t bytes);

/// `fdatasync(2)`: flush this descriptor's writes to the device. Windows makes no distinction
/// between flushing data and flushing metadata, so this is `fsync` there.
int platformFDataSync(int fd);

/// Takes an exclusive lock on the whole of an open file, released when the descriptor is closed.
/// With `blocking`, waits for a conflicting lock to go away; without it, fails immediately.
///
/// `flock(LOCK_EX)` on POSIX. Windows has `LockFileEx`, which differs in that it locks a byte
/// range rather than a file, so this locks the largest range there is. Reports failure as `-1`
/// with `errno` set, `EWOULDBLOCK` for a lock already held.
int platformLockFileExclusive(int fd, bool blocking);

/// The same, shared rather than exclusive: `flock(LOCK_SH)`, or `LockFileEx` without
/// `LOCKFILE_EXCLUSIVE_LOCK`.
int platformLockFileShared(int fd, bool blocking);

/// Releases a lock taken by either of the above. `flock(LOCK_UN)`, or `UnlockFileEx`.
int platformUnlockFile(int fd);

/// `truncate(2)`: set a file's length by path. Windows has no such call - the CRT can only resize
/// through a descriptor, and `off_t` there is 32 bits - so this opens the file and moves its end
/// with `SetFilePointerEx`/`SetEndOfFile`, which take a 64-bit offset. Reports failure as `-1`
/// with `errno` set.
int platformTruncate(const std::string & path, UInt64 size);

/// `open(O_RDWR)` by a UTF-8 path. The Windows CRT's `_open` interprets a narrow path through
/// the active code page, mangling any path with a character outside it, so this goes through
/// `_wopen` with the path converted from UTF-8. Reports failure as `-1` with `errno` set.
int platformOpenReadWrite(const std::string & path);

/// Opens a directory, for no purpose other than passing the descriptor to `platformFDataSync` -
/// which is how MergeTree makes a rename durable. `open(O_DIRECTORY)` on POSIX; on Windows the
/// CRT's `_open` refuses a directory outright, so this goes through `CreateFileW` with
/// `FILE_FLAG_BACKUP_SEMANTICS` - the flag that means "a directory handle is fine" - and adopts
/// the result into a descriptor. Reports failure as `-1` with `errno` set.
int platformOpenDirectory(const std::string & path);

/// The path-taking counterparts of `open`/`stat`/`unlink`/`rmdir`/`utime`, by a UTF-8 path.
///
/// The narrow Windows CRT functions interpret a `char *` path through the process's active code
/// page, not UTF-8 (see base/base/pathToString.h), so any path with a character outside the ACP
/// fails or names the wrong file. These wrappers convert the UTF-8 path and call the `_w`-prefixed
/// wide variants instead; on POSIX they are the plain calls. All report failure the POSIX way:
/// `-1` (or a non-zero result for `stat`) with `errno` set.

/// `open(2)`. On Windows also adds `_O_BINARY`: the CRT would otherwise open in text mode and
/// translate line endings on read and write.
int platformOpenFile(const std::string & path, int flags, int mode = 0);

/// `stat(2)`. mingw-w64's `wstat` shares the `struct stat` layout with the narrow `stat` - both
/// follow `_FILE_OFFSET_BITS` - so the out-parameter is the ordinary `struct stat`.
int platformStat(const std::string & path, struct stat & out);

/// `unlink(2)`.
int platformUnlink(const std::string & path);

/// `rmdir(2)`.
int platformRmdir(const std::string & path);

/// `utime(2)`: set a file's access and modification times.
int platformSetFileTimes(const std::string & path, time_t access_time, time_t modification_time);

/// `chmod(2)`. Windows has no POSIX permission bits; the CRT's `_wchmod` maps the owner write
/// bit onto the read-only file attribute and ignores the rest, which is the closest it gets.
int platformChmod(const std::string & path, mode_t mode);

/// A file's identity, size and modification time at full precision - the parts `stat` cannot
/// report on Windows, where the CRT's `struct stat` carries whole seconds and a zero `st_ino`
/// (see src/Common/createHardLink.cpp). NTFS itself keeps 100-nanosecond timestamps and a
/// per-volume file index; `GetFileInformationByHandle` is the call that reports them.
struct PlatformFileVersion
{
    Int64 mtime_sec = 0;
    Int64 mtime_nsec = 0;
    UInt64 device_id = 0; /// `st_dev`; the volume serial number on Windows
    UInt64 file_id = 0; /// `st_ino`; the NTFS file index on Windows
    UInt64 size = 0;
};

/// Fills `out` for the file at a UTF-8 `path` (following symlinks, as `stat` does). Reports
/// failure as `-1` with `errno` set.
int platformFileVersion(const std::string & path, PlatformFileVersion & out);

/// The same for an open descriptor, like `fstat`.
int platformFileVersionOfDescriptor(int fd, PlatformFileVersion & out);

}
