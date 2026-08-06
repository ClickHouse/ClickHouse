#pragma once

#include <base/types.h>

#include <cstddef>
#include <string>

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

}
