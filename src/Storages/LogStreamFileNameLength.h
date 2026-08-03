#pragma once

#include <Disks/IDisk.h>
#include <base/types.h>

#include <exception>

namespace DB
{

/// Longest derived stream name a fresh `Log`/`TinyLog` definition may accept, measured on `disk`:
/// every stream is stored as `<derived name>.bin` in one path component of that disk.
size_t maxLogStreamFileNameLength(const DiskPtr & disk);

/// Whether the failure is the filesystem refusing a too long name.
bool isFilenameTooLongError(std::exception_ptr e);

/// Replaces an in-flight too-long-name refusal with ARGUMENT_OUT_OF_BOUND naming the refused file and
/// the limit. Only translates when the BASENAME of the refused path is over the budget of `disk`:
/// ENAMETOOLONG also covers a PATH_MAX overflow, which no column name causes. Returns normally for
/// anything else, so the caller keeps failing with the original exception.
void rethrowIfLogFileNameTooLong(const DiskPtr & disk, const String & table_path);

}
