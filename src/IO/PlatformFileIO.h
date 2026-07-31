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

/// Opens a directory, for no purpose other than passing the descriptor to `platformFDataSync` -
/// which is how MergeTree makes a rename durable. `open(O_DIRECTORY)` on POSIX; on Windows the
/// CRT's `_open` refuses a directory outright, so this goes through `CreateFileW` with
/// `FILE_FLAG_BACKUP_SEMANTICS` - the flag that means "a directory handle is fine" - and adopts
/// the result into a descriptor. Reports failure as `-1` with `errno` set.
int platformOpenDirectory(const std::string & path);

}
